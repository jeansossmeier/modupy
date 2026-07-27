"""Best-effort cross-process sequence hints backed by a file mapping.

The durable store remains authoritative. This notifier contains no payload,
claim, cursor, consumer-group, or completion state. Readers therefore ignore
anything that is missing, stale, torn, corrupt, wrapped, or incompatible.
"""

from __future__ import annotations

import mmap
import os
import struct
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import BinaryIO

_MAGIC = b"MLTHHINT"
_VERSION = 1
_HEADER_SIZE = 64
_SLOT_SIZE = 16
_HEADER_STRUCT = struct.Struct("<8sIIQQ32x")
_SLOT_STRUCT = struct.Struct("<QQ")
_SEQUENCE_MASK = (1 << 64) - 1
_ATTACH_ATTEMPTS = 10
_ATTACH_DELAY_SECONDS = 0.01
_NATIVE_LITTLE_ENDIAN = sys.byteorder == "little"


class ShmRing:
    """A non-authoritative ring of durable-sequence hints.

    Hints only: a slot holds a sequence number and its complement, never a
    payload, claim, cursor or consumer-group. Publishers call ``notify`` after
    the durable store has committed; consumers call ``read_hints`` to skip
    ahead of their safety poll. Losing every hint costs latency, never a
    message.
    """

    def __init__(
        self,
        name: str | os.PathLike[str],
        capacity: int,
        *,
        create: bool = False,
        enabled: bool = True,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")

        self._path = Path(name).expanduser().resolve()
        self._capacity = capacity
        self._file: BinaryIO | None = None
        self._mapping: mmap.mmap | None = None
        self._closed = False
        self._created = False
        if not enabled:
            return

        if create and not self._path.exists():
            self._created = self._create_atomically()
        self._attach_boundedly()

    @property
    def name(self) -> str:
        return str(self._path)

    @property
    def path(self) -> Path:
        return self._path

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def available(self) -> bool:
        return not self._closed and self._mapping is not None

    def notify(self, durable_sequence: int) -> bool:
        """Publish one sequence hint, returning False on any notifier failure."""
        mapping = self._mapping
        if (
            not self.available
            or isinstance(durable_sequence, bool)
            or not 0 <= durable_sequence <= _SEQUENCE_MASK
            or mapping is None
        ):
            return False

        offset = _HEADER_SIZE + (durable_sequence % self._capacity) * _SLOT_SIZE
        try:
            # Write the complement first. A reader racing either write sees the
            # previous valid pair or a mismatch that it safely discards.
            struct.pack_into("<Q", mapping, offset + 8, durable_sequence ^ _SEQUENCE_MASK)
            struct.pack_into("<Q", mapping, offset, durable_sequence)
            return True
        except (BufferError, OSError, ValueError, struct.error):
            return False

    def read_hints(self, after_sequence: int = -1) -> list[int]:
        """Return every intact hint newer than the caller's cursor, ascending."""
        mapping = self._mapping
        if not self.available or mapping is None:
            return []

        hints: list[int] = []
        try:
            if _NATIVE_LITTLE_ENDIAN and self._nothing_newer(mapping, after_sequence):
                return []
            for index in range(self._capacity):
                offset = _HEADER_SIZE + index * _SLOT_SIZE
                sequence, complement = _SLOT_STRUCT.unpack(mapping[offset : offset + _SLOT_SIZE])
                if complement != sequence ^ _SEQUENCE_MASK:
                    continue
                if sequence % self._capacity != index or sequence <= after_sequence:
                    continue
                hints.append(sequence)
        except (BufferError, OSError, ValueError, struct.error):
            return []
        return sorted(hints)

    def _nothing_newer(self, mapping: mmap.mmap, after_sequence: int) -> bool:
        """Rule out the whole ring in one strided pass over the sequence words.

        An idle consumer re-reads the ring every few milliseconds for the whole
        length of its safety poll, and the per-slot ``unpack`` below costs
        O(capacity) each time — enough to keep a worker process busy doing
        nothing at the default capacity, and enough to stall its event loop at
        the largest accepted one. No slot can yield a hint above
        ``after_sequence`` if no sequence word exceeds it, so this pass answers
        the idle case without touching the slower path.

        Only valid on a little-endian host: the slot layout is explicitly
        little-endian while ``memoryview.cast`` reads native words.
        """
        end = _HEADER_SIZE + self._capacity * _SLOT_SIZE
        words = memoryview(mapping)[_HEADER_SIZE:end].cast("Q")
        peak = max(words[0::2])
        if peak > after_sequence:
            # A never-notified ring is all zeros, which a cursor of -1 cannot
            # rule out on sequence words alone; its complements settle it.
            return peak == 0 and max(words[1::2]) == 0
        return True

    def close(self) -> None:
        """Close this process's handles; repeated calls are safe."""
        if self._closed:
            return
        self._closed = True
        mapping, self._mapping = self._mapping, None
        file_handle, self._file = self._file, None
        if mapping is not None:
            with suppress(BufferError, OSError, ValueError):
                mapping.close()
        if file_handle is not None:
            with suppress(OSError):
                file_handle.close()

    def unlink(self) -> None:
        """Compatibility cleanup for a file this instance created and closed."""
        if self._closed and self._created:
            with suppress(OSError):
                self._path.unlink(missing_ok=True)

    def _create_atomically(self) -> bool:
        """Install a fully initialized private file without replacing a peer."""
        total_size = _HEADER_SIZE + self._capacity * _SLOT_SIZE
        file_descriptor = -1
        temporary_name = ""
        try:
            file_descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                dir=self._path.parent,
            )
            if hasattr(os, "fchmod"):
                os.fchmod(file_descriptor, 0o600)
            os.ftruncate(file_descriptor, total_size)
            with mmap.mmap(file_descriptor, total_size) as mapping:
                mapping[:_HEADER_SIZE] = _HEADER_STRUCT.pack(
                    _MAGIC, _VERSION, _HEADER_SIZE, self._capacity, _SLOT_SIZE
                )
                mapping.flush()
            os.fsync(file_descriptor)
            os.link(temporary_name, self._path)
            # Drop the temporary name here rather than leaving it to the
            # ``finally`` clause. Between the link and the unlink the inode
            # carries two names, and a peer worker starting at the same moment
            # rejects a hint file with st_nlink != 1 outright (see
            # _state_path._validate_regular_file, which treats extra links as a
            # possible hijack) — a hard startup failure caused entirely by this
            # process's own scratch name. Unlinking on the success path leaves
            # only the two adjacent syscalls exposed instead of also spanning
            # the descriptor close below; link-then-unlink is the tightest a
            # create that must not clobber a peer can get.
            with suppress(OSError):
                os.unlink(temporary_name)
                temporary_name = ""
            return True
        except (FileExistsError, OSError):
            return False
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            if temporary_name:
                with suppress(OSError):
                    Path(temporary_name).unlink(missing_ok=True)

    def _attach_boundedly(self) -> None:
        """Retry only the short create/size window, then remain unavailable."""
        expected_size = _HEADER_SIZE + self._capacity * _SLOT_SIZE
        for attempt in range(_ATTACH_ATTEMPTS):
            try:
                file_handle = self._path.open("r+b", buffering=0)
            except FileNotFoundError:
                file_handle = None
            except OSError:
                return

            if file_handle is not None:
                mapping: mmap.mmap | None = None
                try:
                    if os.fstat(file_handle.fileno()).st_size == expected_size:
                        mapping = mmap.mmap(file_handle.fileno(), expected_size)
                        header = _HEADER_STRUCT.unpack(mapping[:_HEADER_SIZE])
                        if header == (
                            _MAGIC,
                            _VERSION,
                            _HEADER_SIZE,
                            self._capacity,
                            _SLOT_SIZE,
                        ):
                            self._file = file_handle
                            self._mapping = mapping
                            return
                except (BufferError, OSError, ValueError, struct.error):
                    pass
                if mapping is not None:
                    mapping.close()
                file_handle.close()
            if attempt + 1 < _ATTACH_ATTEMPTS:
                time.sleep(_ATTACH_DELAY_SECONDS)
