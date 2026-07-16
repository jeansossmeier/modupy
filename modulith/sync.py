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
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger("modulith.sync")


class PublishSyncTimeout(TimeoutError):
    """publish_sync's OWN budget timeout: the dispatch did not complete
    within ``timeout`` seconds and was cancelled.

    Distinct by type from a TimeoutError raised BY application code (a
    listener), which propagates out of publish_sync unchanged — on Python
    3.11+ ``concurrent.futures.TimeoutError`` IS ``TimeoutError``, so
    without the dedicated type the two were indistinguishable and the
    testing plugin's scenario runner swallowed real application failures
    as budget overruns (W3 R4-W3-01). Subclasses TimeoutError, so existing
    ``except TimeoutError`` handlers keep working.
    """


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


# Set (in the copied context) for the duration of a sync listener's body by
# wrap_sync_listener below. Lets a nested publish_sync() — a sync listener
# publishing a follow-up event — detect that it is running inside the shared
# executor and pick the fresh-thread dispatch path instead of competing for
# the same bounded pool that is running its caller.
_in_sync_listener: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_modulith_in_sync_listener", default=False
)


def publish_sync(event: Any, *, timeout: float | None = 30.0) -> None:
    """Publish an event from synchronous code.

    Detection strategy:
      1. A loop is running in the current thread (accidentally called from
         inside async code): raise RuntimeError — use ``await publish(event)``
         instead. ``asyncio.get_running_loop()`` is thread-local, so a running
         loop it returns is by construction *this* thread's; a loop running on
         a different thread (e.g. FastAPI's main-thread loop while a sync view
         runs in Starlette's threadpool) is invisible here and falls through
         to case 2 — exactly what those callers need.
      2. Called from inside a *sync listener* (itself running in the daemon
         loop's executor): dispatch on a fresh short-lived thread + loop.
         Nested dispatches must not compete for the same bounded executor
         that is running their caller — with a shared pool, N concurrent
         outer listeners each blocking on a nested publish_sync() exhaust the
         pool (spurious TimeoutErrors under load; a permanent deadlock with
         ``timeout=None``).
      3. Otherwise (no loop in this thread): dispatch on the persistent
         daemon-thread loop from _get_or_create_loop() and block on the
         result. Covers plain scripts AND threadpool sync views.

    The timeout protects against listener deadlocks. None disables it.
    Default of 30s matches typical HTTP timeouts; tune via configuration.
    On timeout, the submitted dispatch is cancelled (best-effort: the
    cancellation lands at the coroutine's next await point, so a sync
    listener already blocking inside an executor thread still runs to
    completion there, but the abandoned dispatch no longer accumulates on
    the shared persistent loop).

    Calling publish_sync() while modulith is bootstrapping on this same
    thread (module code imported by discovery) raises RuntimeError
    immediately instead of deadlocking against the bootstrap lock until the
    timeout expires.
    """
    from concurrent.futures import TimeoutError as FuturesTimeoutError

    from .runtime import _runtime

    if _runtime._bootstrapping_thread == threading.get_ident():
        raise RuntimeError(
            "publish_sync() called while modulith is bootstrapping on this "
            "thread (typically from module code imported during discovery). "
            "Import time is for registering listeners; publish after startup."
        )

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop is not None:
        # Same thread as a running loop: .result() would deadlock it.
        raise RuntimeError(
            "publish_sync() called from inside an async context on the same "
            "thread as the event loop. Use `await publish(event)` instead."
        )

    coro = _runtime.publish(event)

    if _in_sync_listener.get():
        _run_nested_dispatch(coro, event, timeout)
        return

    future = asyncio.run_coroutine_threadsafe(coro, _get_or_create_loop())

    try:
        future.result(timeout=timeout)
    except FuturesTimeoutError as exc:
        if future.done() and not future.cancelled() and future.exception() is exc:
            # The dispatch COMPLETED by raising this very exception — on 3.11+
            # FuturesTimeoutError is TimeoutError, so a listener's own
            # TimeoutError lands in this handler too. That is an application
            # failure, not a budget overrun: surface it unchanged
            # (W3 R4-W3-01). Identity — not ``done()`` alone — is the
            # discriminator: an expired wait raises a FRESH bare TimeoutError
            # that is never the future's stored exception, so even when the
            # dispatch completes inside the race window between budget expiry
            # and this check, a genuine overrun still converts to
            # PublishSyncTimeout below instead of leaking the bare error.
            raise
        # Budget overrun. Cancel the dispatch: without this, a hung listener
        # kept running (or hanging) invisibly on the process-lifetime daemon
        # loop after the caller was already told it failed — one abandoned
        # task per timed-out call, forever. (A no-op when the dispatch
        # completed inside the race window above.)
        future.cancel()
        logger.warning(
            "publish_sync timeout after %s s for %s",
            timeout,
            type(event).__name__,
        )
        raise PublishSyncTimeout(
            f"publish_sync({type(event).__name__}) did not complete within {timeout}s"
        ) from exc


def _run_nested_dispatch(
    coro: Coroutine[Any, Any, None], event: Any, timeout: float | None
) -> None:
    """Run a nested publish coroutine on a fresh thread with its own loop.

    Used when publish_sync() is called from inside a sync listener (see
    detection case 2). The fresh loop gives the nested dispatch its own
    event loop AND its own default executor, so nested sync listeners never
    queue behind — or deadlock against — the bounded pool slot occupied by
    their caller.

    Two properties a bare ``asyncio.run(coro)`` on a fresh thread doesn't
    give us, both needed here:

      * ContextVar propagation. A brand-new OS thread starts with an empty
        top-level ``contextvars.Context`` — any ContextVar the calling
        (outer-listener executor) thread had bound was silently invisible to
        the nested dispatch. Captured via ``contextvars.copy_context()``
        before the thread starts and run inside that copy.
      * Cross-thread cancellation on timeout. ``asyncio.run`` hands back no
        handle once it's running, so a timed-out nested call used to abandon
        the coroutine to run (or hang) unobserved for as long as it liked —
        forever, for a permanently-hung listener. Creating the task
        ourselves keeps a reference this function can cancel via
        ``call_soon_threadsafe`` from the calling thread, bounding the
        nested thread's lifetime to "until cancellation lands" instead of
        "until the hung coroutine finishes."
    """
    from concurrent.futures import Future
    from concurrent.futures import TimeoutError as FuturesTimeoutError

    done: Future[None] = Future()
    loop_ready = threading.Event()
    state: dict[str, Any] = {}
    ctx = contextvars.copy_context()

    def _run_on_fresh_loop() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        state["loop"] = loop
        try:
            task = loop.create_task(coro)
            state["task"] = task
            loop_ready.set()
            try:
                loop.run_until_complete(task)
            except BaseException as exc:
                done.set_exception(exc)
            else:
                done.set_result(None)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    thread = threading.Thread(
        target=ctx.run, args=(_run_on_fresh_loop,), name="modulith-sync-nested", daemon=True
    )
    thread.start()
    loop_ready.wait()
    try:
        done.result(timeout=timeout)
    except FuturesTimeoutError as exc:
        if done.done() and not done.cancelled() and done.exception() is exc:
            # The nested dispatch completed by raising this very TimeoutError
            # — an application failure, not the budget (W3 R4-W3-01). Same
            # identity discrimination as publish_sync's own wait: the bare
            # TimeoutError an expired wait raises is never the future's stored
            # exception, so a genuine overrun converts to PublishSyncTimeout
            # even when the dispatch completes inside the race window between
            # wait expiry and this check.
            raise
        loop = state["loop"]
        task = state["task"]
        loop.call_soon_threadsafe(task.cancel)
        logger.warning(
            "publish_sync timeout after %s s for %s (nested dispatch)",
            timeout,
            type(event).__name__,
        )
        raise PublishSyncTimeout(
            f"publish_sync({type(event).__name__}) did not complete within {timeout}s"
        ) from exc


# ---------------------------------------------------------------------------
# SECTION 3: Sync listener support in @listener
# ---------------------------------------------------------------------------
# This function wraps a sync handler so the event bus can call it as if
# it were async. Called from decorators.listener() when the function is
# detected as sync.


def _run_in_listener_context(func: Callable[..., None], event: Any) -> None:
    """Executor-thread shim: run the sync handler with the nesting flag set.

    Runs inside the copied context (see wrap_sync_listener), so the flag is
    visible only to code the handler itself calls — a nested publish_sync()
    uses it to pick the fresh-thread dispatch path.
    """
    token = _in_sync_listener.set(True)
    try:
        func(event)
    finally:
        _in_sync_listener.reset(token)


def wrap_sync_listener(func: Callable[..., None]) -> Callable[..., Any]:
    """Wrap a sync listener so it runs in the event loop's executor.

    The bus expects async handlers. We adapt sync ones by submitting them
    to loop.run_in_executor(None, ...), which runs them in the default
    thread-pool executor. The returned wrapper has the same name and
    annotations so listener registration sees the right event type.

    contextvars.copy_context() propagates caller context (values bound in
    ContextVars) into the executor thread. Two sharp edges around the
    popular "bound DB session in a ContextVar" pattern:

      * Only *synchronous* SQLAlchemy sessions/engines work here. An
        ``AsyncSession`` does NOT: sync-style calls on its underlying
        ``sync_session`` must run inside SQLAlchemy's greenlet bridge
        (``AsyncSession.run_sync``), not an arbitrary executor thread, and
        raise ``MissingGreenlet`` if tried. Use an async listener for
        AsyncSession work.
      * Multiple sync listeners for the same event run CONCURRENTLY on
        separate executor threads, each with a *copy* of the same context —
        so a shared bound resource (one session object, say) is touched from
        several OS threads at once with no synchronization. Share only
        thread-safe resources across sync listeners, or give each listener
        its own session/lock.
    """

    @functools.wraps(func)
    async def wrapper(event: Any) -> None:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        await loop.run_in_executor(None, ctx.run, _run_in_listener_context, func, event)

    # Introspection marker: lets tools and tests identify wrapped sync handlers.
    wrapper.__modulith_sync_wrapped__ = func  # type: ignore[attr-defined]
    return wrapper


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "PublishSyncTimeout",
    "publish_sync",
    "wrap_sync_listener",
]
