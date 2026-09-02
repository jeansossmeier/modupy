"""Dedicated single-thread execution for one SQLite queue connection."""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any

from ._shm_store import SqliteQueueStore


class SerialStoreExecutor:
    """Serialize async calls onto one lazily-created synchronous store."""

    def __init__(self, factory: Callable[[], SqliteQueueStore]) -> None:
        self._factory = factory
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="modulith-shm-store",
        )
        # Keyed per running loop: an asyncio.Lock binds to the first loop that
        # contends it and raises (or wedges the other side) for every other
        # loop thereafter, but this object is reached from more than one loop
        # by design (the async worker's loop and sync.publish_sync's
        # daemon-thread loop). Each loop only needs to serialize against
        # itself here — the single-worker executor already serializes the
        # actual SQLite calls across loops. A weak key lets a closed loop's
        # entry disappear once nothing else holds it — sync.py's
        # _run_nested_dispatch creates and closes a fresh loop per call, and a
        # plain dict would grow one dead Lock per call for the life of the
        # process.
        self._locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._store: SqliteQueueStore | None = None
        self._worker_thread_id: int | None = None
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _lock_for_running_loop(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[loop] = lock
        return lock

    async def _call(self, method: str, *args: Any) -> Any:
        if self._closed:
            raise RuntimeError("SHM cold store is closed")
        async with self._lock_for_running_loop():
            if self._closed:
                raise RuntimeError("SHM cold store is closed")
            loop = asyncio.get_running_loop()
            operation = partial(self._invoke, method, args)
            future = loop.run_in_executor(self._executor, operation)
            return await _await_settled(future)

    def _invoke(self, method: str, args: tuple[Any, ...]) -> Any:
        self._worker_thread_id = threading.get_ident()
        if self._store is None:
            self._store = self._factory()
        return getattr(self._store, method)(*args)

    async def close(self) -> None:
        """Shield one close path so a cancelled caller can safely retry it."""
        task = self._close_task
        if task is None:
            task = asyncio.create_task(self._close_once())
            self._close_task = task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except BaseException:
            if self._close_task is task:
                self._close_task = None
            raise

    async def _close_once(self) -> None:
        async with self._lock_for_running_loop():
            if self._closed:
                return
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._close_worker)
            self._executor.shutdown(wait=True)
            self._closed = True

    def _close_worker(self) -> None:
        self._worker_thread_id = threading.get_ident()
        if self._store is not None:
            self._store.close()
            self._store = None


async def _await_settled(future: asyncio.Future[Any]) -> Any:
    """Let queued SQLite work settle before exposing caller cancellation."""
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if not future.cancelled():
            try:
                future.result()
            except BaseException:
                pass
        raise
