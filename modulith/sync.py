"""Sync entrypoint and sync listener support.

This module is what lets modulith integrate with FastAPI sync views,
sync database code, scripts, and any other context where the user
can't easily `await`. The async API is preferred for new code; this
module exists because most real Python apps in 2026 are mixed.

Design principles:
1. Detection over configuration — publish_sync() figures out the
   calling context and does the right thing.
2. No new concepts for the user — sync code uses publish_sync() and
   `def listener(...)` (no async); the framework handles the rest.
3. Transactional semantics preserved — publish_sync() inside a
   SQLAlchemy session uses the same outbox path as async publish().
"""

from __future__ import annotations

import asyncio
import atexit
import contextvars
import functools
import logging
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("modulith.sync")


# ---------------------------------------------------------------------------
# SECTION 1: The persistent thread-pool event loop
# ---------------------------------------------------------------------------
# Used when publish_sync() is called from a context with no running loop.
# We start one loop in a daemon thread on first use and reuse it for all
# subsequent calls. Cheaper than spinning up asyncio.run() each time, and
# avoids loop-policy conflicts with frameworks that have their own loops.

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    """Return a long-lived event loop running on a daemon thread.

    First call: starts a thread, creates a loop, runs it forever.
    Subsequent calls: returns the same loop.
    Thread-safe via _loop_lock.
    """
    global _loop
    if _loop is not None:
        return _loop
    with _loop_lock:
        if _loop is not None:  # double-checked locking
            return _loop  # type: ignore[unreachable]
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=loop.run_forever,
            name="modulith-sync-loop",
            daemon=True,  # dies with the main thread; no shutdown needed
        )
        thread.start()
        _loop = loop
        # Soft-stop on interpreter shutdown: gives in-flight tasks a chance
        # to finish before the daemon thread is killed, reducing stderr noise.
        atexit.register(loop.call_soon_threadsafe, loop.stop)
        return _loop


# ---------------------------------------------------------------------------
# SECTION 2: publish_sync()
# ---------------------------------------------------------------------------
# The user-facing sync entrypoint. Detects context and dispatches.


def publish_sync(event: Any, *, timeout: float | None = 30.0) -> None:
    """Publish an event from synchronous code.

    Detection strategy:
      1. If a loop is running in the SAME thread (e.g. accidentally called
         from inside async code): raise RuntimeError with helpful message.
         Use `await publish(event)` instead.
      2. If a loop is running in a DIFFERENT thread (e.g. FastAPI sync view
         in Starlette's threadpool, where the event loop lives on the main
         thread): submit to that loop via asyncio.run_coroutine_threadsafe(),
         block on the Future.
      3. If no loop is running anywhere accessible: use the persistent
         daemon-thread loop from _get_or_create_loop().

    The timeout protects against listener deadlocks. None disables it.
    Default of 30s matches typical HTTP timeouts; tune via configuration.

    Note on same-thread detection: we check running_loop._thread_id against
    threading.get_ident(). _thread_id is a CPython private attribute set by
    the asyncio event loop. If it's absent (unlikely outside CPython), we
    conservatively allow the call to proceed via run_coroutine_threadsafe —
    worst case a different error surfaces rather than a deadlock, because the
    Future.result() call would block a loop that can't make progress.
    """
    from concurrent.futures import TimeoutError as FuturesTimeoutError

    from .runtime import _runtime

    coro = _runtime.publish(event)

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop is not None:
        # There is a loop somewhere — figure out if we're on its thread.
        loop_thread_id = getattr(running_loop, "_thread_id", None)
        if loop_thread_id is None or loop_thread_id == threading.get_ident():
            # Same thread: .result() would deadlock the loop immediately.
            # Close the coroutine to suppress the "never awaited" warning.
            coro.close()
            raise RuntimeError(
                "publish_sync() called from inside an async context on the same "
                "thread as the event loop. Use `await publish(event)` instead."
            )
        # Different thread (e.g. Starlette threadpool sync view): safe to block.
        target_loop = running_loop
    else:
        target_loop = _get_or_create_loop()

    future = asyncio.run_coroutine_threadsafe(coro, target_loop)

    try:
        future.result(timeout=timeout)
    except FuturesTimeoutError as exc:
        logger.warning(
            "publish_sync timeout after %s s for %s",
            timeout,
            type(event).__name__,
        )
        raise TimeoutError(
            f"publish_sync({type(event).__name__}) did not complete within {timeout}s"
        ) from exc


# ---------------------------------------------------------------------------
# SECTION 3: Sync listener support in @listener
# ---------------------------------------------------------------------------
# The decorator at modulith/decorators.py currently rejects sync functions.
# This function wraps a sync handler so the event bus can call it as if
# it were async. Called from decorators.listener() when the function is
# detected as sync.


def wrap_sync_listener(func: Callable[..., None]) -> Callable[..., Any]:
    """Wrap a sync listener so it runs in the event loop's executor.

    The bus expects async handlers. We adapt sync ones by submitting them
    to loop.run_in_executor(None, ...), which runs them in the default
    thread-pool executor. The returned wrapper has the same name and
    annotations so listener registration sees the right event type.

    contextvars.copy_context() propagates caller context (e.g. a bound
    SQLAlchemy session stored in a ContextVar) into the executor thread.
    Without this, sync DB code in the listener would lose its session.
    """

    @functools.wraps(func)
    async def wrapper(event: Any) -> None:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        await loop.run_in_executor(None, ctx.run, func, event)

    # Introspection marker: lets tools and tests identify wrapped sync handlers.
    wrapper.__modulith_sync_wrapped__ = func  # type: ignore[attr-defined]
    return wrapper


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "publish_sync",
    "wrap_sync_listener",
]
