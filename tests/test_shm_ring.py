"""Behavioral tests for the non-authoritative mmap sequence notifier."""

from __future__ import annotations

import mmap
import multiprocessing
import os
import stat
import struct
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from modulith.adapters import _shm_ring as shm_ring_module
from modulith.adapters._shm_ring import (
    _HEADER_SIZE,
    _HEADER_STRUCT,
    _MAGIC,
    _SLOT_SIZE,
    _VERSION,
    ShmRing,
)

_PROCESS_TIMEOUT_SECONDS = 5.0
# Budget for a spawn child to boot a cold interpreter, import, and report.
# Deliberately far larger than the reaper deadline above: this one is a
# liveness bound on a slow (Windows/macOS, antivirus-scanned) CI runner,
# whereas _PROCESS_TIMEOUT_SECONDS is how long we wait for an already-finished
# child to be collected before escalating to terminate/kill.
_SPAWN_STARTUP_TIMEOUT_SECONDS = 60.0


class _SpawnProcess(Protocol):
    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


def _notify_from_spawn(path: str, capacity: int, sequence: int, result_queue) -> None:
    """Attach in a fresh interpreter, publish one hint, and report availability."""
    notifier = ShmRing(path, capacity=capacity)
    result_queue.put((notifier.available, notifier.notify(sequence)))
    notifier.close()


def _stop_process(process: _SpawnProcess) -> None:
    """Reap a child before any test assertion can leave it running."""
    process.join(_PROCESS_TIMEOUT_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(_PROCESS_TIMEOUT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join(_PROCESS_TIMEOUT_SECONDS)


@pytest.fixture()
def notifier(tmp_path: Path) -> Iterator[ShmRing]:
    instance = ShmRing(tmp_path / "hints.mmap", capacity=4, create=True)
    yield instance
    instance.close()


def test_create_and_attach_share_sequence_hints(notifier: ShmRing) -> None:
    assert notifier.available
    assert notifier.notify(5)

    attached = ShmRing(notifier.path, capacity=4)
    try:
        assert attached.available
        assert attached.read_hints(after_sequence=0) == [5]
    finally:
        attached.close()


def test_sequence_zero_is_a_valid_hint(notifier: ShmRing) -> None:
    assert notifier.notify(0)
    assert notifier.read_hints() == [0]


@pytest.mark.parametrize("sequence", [True, -1, 1 << 64])
def test_invalid_sequences_are_rejected_without_touching_the_ring(
    notifier: ShmRing, sequence: int
) -> None:
    assert not notifier.notify(sequence)
    assert notifier.read_hints() == []


def test_constructor_rejects_empty_capacity_and_reports_its_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="capacity"):
        ShmRing(tmp_path / "invalid.mmap", capacity=0, create=True)

    notifier = ShmRing(tmp_path / "properties.mmap", capacity=1, create=True)
    try:
        assert notifier.name == str(notifier.path)
        assert notifier.capacity == 1
    finally:
        notifier.close()
        notifier.unlink()


def test_read_hints_returns_only_sequences_past_the_cursor(notifier: ShmRing) -> None:
    assert notifier.notify(1)
    assert notifier.notify(3)

    assert notifier.read_hints(after_sequence=0) == [1, 3]
    assert notifier.read_hints(after_sequence=1) == [3]
    assert notifier.read_hints(after_sequence=3) == []


def test_spawned_process_can_attach_and_notify(notifier: ShmRing) -> None:
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_notify_from_spawn,
        args=(str(notifier.path), notifier.capacity, 7, result_queue),
    )

    process.start()
    try:
        # Wait on the child's own result, not on the reaper: _stop_process is a
        # force-kill guard, so using it as the startup budget makes a merely
        # slow runner SIGTERM the child and surface as `assert -15 == 0` —
        # a message that accuses ShmRing of a regression it did not commit.
        assert result_queue.get(timeout=_SPAWN_STARTUP_TIMEOUT_SECONDS) == (True, True)
        _stop_process(process)
        assert not process.is_alive(), "spawned notifier process survived terminate/kill"
        assert process.exitcode == 0
        assert notifier.read_hints(after_sequence=0) == [7]
    finally:
        _stop_process(process)
        result_queue.close()
        result_queue.join_thread()


def test_wrapped_slots_only_expose_current_sequences(tmp_path: Path) -> None:
    notifier = ShmRing(tmp_path / "wrap.mmap", capacity=2, create=True)
    try:
        assert all(notifier.notify(sequence) for sequence in (1, 2, 3))
        assert notifier.read_hints(after_sequence=0) == [2, 3]
        assert notifier.read_hints(after_sequence=2) == [3]
    finally:
        notifier.close()


def test_torn_and_misplaced_slots_are_ignored(notifier: ShmRing) -> None:
    assert notifier.notify(5)
    offset = _HEADER_SIZE + (5 % notifier.capacity) * _SLOT_SIZE
    with notifier.path.open("r+b") as file_handle:
        with mmap.mmap(file_handle.fileno(), 0) as mapping:
            struct.pack_into("<Q", mapping, offset + 8, 123)
    assert notifier.read_hints(after_sequence=0) == []

    with notifier.path.open("r+b") as file_handle:
        with mmap.mmap(file_handle.fileno(), 0) as mapping:
            struct.pack_into("<QQ", mapping, offset, 6, 6 ^ ((1 << 64) - 1))
    assert notifier.read_hints(after_sequence=0) == []


def test_incompatible_header_degrades_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "incompatible.mmap"
    creator = ShmRing(path, capacity=4, create=True)
    creator.close()
    with path.open("r+b") as file_handle:
        file_handle.seek(8)
        file_handle.write(struct.pack("<I", _VERSION + 1))

    notifier = ShmRing(path, capacity=4)
    assert not notifier.available
    assert not notifier.notify(1)
    assert notifier.read_hints(after_sequence=0) == []
    notifier.close()


def test_mapping_io_errors_degrade_reads_writes_and_close_to_safe_noops(
    notifier: ShmRing,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_write(*_args: object) -> None:
        raise struct.error("mapping write failed")

    class FailingSlotStruct:
        @staticmethod
        def unpack(_data: object) -> tuple[int, int]:
            raise struct.error("mapping read failed")

    with monkeypatch.context() as context:
        context.setattr(struct, "pack_into", fail_write)
        assert not notifier.notify(1)
    with monkeypatch.context() as context:
        context.setattr(shm_ring_module, "_SLOT_STRUCT", FailingSlotStruct())
        assert notifier.read_hints() == []

    class FailingClose:
        def close(self) -> None:
            raise OSError("mapping close failed")

    # Release the real handles before replacing them with close failures.
    assert notifier._mapping is not None
    assert notifier._file is not None
    notifier._mapping.close()
    notifier._file.close()
    notifier._mapping = cast(Any, FailingClose())
    notifier._file = cast(Any, FailingClose())
    notifier.close()
    assert not notifier.available


def test_directory_and_missing_parent_paths_remain_unavailable(tmp_path: Path) -> None:
    directory_path = tmp_path / "directory"
    directory_path.mkdir()
    directory = ShmRing(directory_path, capacity=4)
    missing_parent = ShmRing(tmp_path / "missing" / "hints.mmap", capacity=4, create=True)

    assert not directory.available
    assert not missing_parent.available
    assert not missing_parent.path.exists()
    directory.close()
    missing_parent.close()


def test_missing_and_disabled_notifiers_are_safe_noops(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.mmap"
    missing = ShmRing(missing_path, capacity=4)
    disabled = ShmRing(tmp_path / "disabled.mmap", capacity=4, create=True, enabled=False)

    assert not missing.available
    assert not missing_path.exists()
    assert not missing.notify(1)
    assert missing.read_hints() == []
    assert not disabled.available
    assert not disabled.path.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits are not portable")
def test_created_file_is_private(tmp_path: Path) -> None:
    path = tmp_path / "private.mmap"
    notifier = ShmRing(path, capacity=4, create=True)
    try:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        notifier.close()


def test_close_is_idempotent_and_disables_operations(notifier: ShmRing) -> None:
    notifier.close()
    notifier.close()

    assert not notifier.available
    assert not notifier.notify(1)
    assert notifier.read_hints() == []


def test_create_never_replaces_an_existing_notifier(tmp_path: Path) -> None:
    path = tmp_path / "active.mmap"
    active = ShmRing(path, capacity=4, create=True)
    assert active.notify(3)
    original = path.read_bytes()

    mismatch = ShmRing(path, capacity=8, create=True)
    try:
        assert not mismatch.available
        assert path.read_bytes() == original
        assert active.read_hints(after_sequence=0) == [3]
    finally:
        mismatch.close()
        active.close()


def test_attach_retries_while_existing_file_is_sized(tmp_path: Path) -> None:
    path = tmp_path / "sizing.mmap"
    path.touch(mode=0o600)
    capacity = 4

    def finish_creation() -> None:
        time.sleep(0.02)
        total_size = _HEADER_SIZE + capacity * _SLOT_SIZE
        with path.open("r+b") as file_handle:
            file_handle.truncate(total_size)
            with mmap.mmap(file_handle.fileno(), total_size) as mapping:
                mapping[:_HEADER_SIZE] = _HEADER_STRUCT.pack(
                    _MAGIC, _VERSION, _HEADER_SIZE, capacity, _SLOT_SIZE
                )

    creator = threading.Thread(target=finish_creation)
    creator.start()
    notifier = ShmRing(path, capacity=capacity)
    creator.join(timeout=1)
    try:
        assert notifier.available
    finally:
        notifier.close()


def test_create_leaves_no_extra_hard_link_visible_to_a_racing_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A peer that stats the hint file mid-create must never see st_nlink > 1.

    The file is installed with a hard link from a private scratch name, and a
    state file carrying more than one link is rejected outright as a possible
    hijack (_state_path._validate_regular_file). Closing the creator's
    descriptor is the last thing that happens while the scratch name could
    still exist, so it is the widest point of the window: sample the link
    count there.
    """
    path = tmp_path / "racy.mmap"
    real_close = os.close
    link_counts: list[int] = []

    def sampling_close(fd: int) -> None:
        if path.exists():
            link_counts.append(path.stat().st_nlink)
        real_close(fd)

    monkeypatch.setattr(os, "close", sampling_close)
    notifier = ShmRing(path, capacity=4, create=True)
    monkeypatch.undo()
    try:
        assert notifier.available
        assert link_counts, "the creator never closed its descriptor"
        assert link_counts == [1] * len(link_counts)
        assert path.stat().st_nlink == 1
    finally:
        notifier.close()
