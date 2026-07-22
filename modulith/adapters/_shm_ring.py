"""Best-effort cross-process sequence hints backed by a file mapping.

The durable store remains authoritative. This notifier contains no payload,
claim, cursor, consumer-group, or completion state. Readers therefore ignore
anything that is missing, stale, torn, corrupt, wrapped, or incompatible.
"""

from __future__ import annotations

import mmap
import os
import struct
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

# Kept temporarily for callers that used this constant to size payload slots.
_SLOT_META_SIZE = _SLOT_SIZE


class ShmRing:
    """A non-authoritative ring of durable-sequence hints.

    ``slot_size`` and ``write_lock`` remain accepted only so existing
    constructors can move to this notifier before their broker wiring changes.
    """

    def __init__(
        self,
        name: str | os.PathLike[str],
        capacity: int,
        slot_size: int | None = None,
        *,
        create: bool = False,
        write_lock: object | None = None,
        enabled: bool = True,
    ) -> None:
        del slot_size, write_lock
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

    @property
    def max_payload_size(self) -> int:
        """Compatibility value: payloads never belong in the hint file."""
        return 0

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

    def read_hints(
        self,
        after_sequence: int = -1,
        through_sequence: int | None = None,
    ) -> list[int]:
        """Return intact hints in the caller's authoritative sequence window."""
        mapping = self._mapping
        if not self.available or mapping is None:
            return []
        if through_sequence is not None and through_sequence <= after_sequence:
            return []

        hints: list[int] = []
        try:
            for index in range(self._capacity):
                offset = _HEADER_SIZE + index * _SLOT_SIZE
                sequence, complement = _SLOT_STRUCT.unpack(mapping[offset : offset + _SLOT_SIZE])
                if complement != sequence ^ _SEQUENCE_MASK:
                    continue
                if sequence % self._capacity != index or sequence <= after_sequence:
                    continue
                if through_sequence is None or sequence <= through_sequence:
                    hints.append(sequence)
        except (BufferError, OSError, ValueError, struct.error):
            return []
        return sorted(hints)

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

    # The old broker must spill to its durable store until it adopts notify().
    def write(self, *_: object, **__: object) -> bool:
        return False

    def claim(self, *_: object, **__: object) -> list[tuple[int, bytes, bytes, int]]:
        return []

    def ack(self, *_: object, **__: object) -> None:
        pass

    def renew(self, *_: object, **__: object) -> bool:
        return False

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
