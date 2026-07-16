"""Behavioral regression tests for modulith/sync.py.

Each test cites the audit finding id it reproduces. Written failing-first
against the pre-fix code (strict TDD; verified red on the base revision).

NOTE: no `from __future__ import annotations` — these tests use @listener with
locally-defined event classes, whose annotations must stay real objects (same
constraint as tests/test_sync.py).
"""

import asyncio
import os
import threading
import time
from dataclasses import dataclass

import pytest

from modulith import configure, event, listener, publish_sync
from modulith.runtime import _runtime


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry."""
    from modulith import manifest as manifest_module

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# A3-r3-131 / S1-r2-104 — timeout must cancel the underlying dispatch
# ---------------------------------------------------------------------------


def test_publish_sync_timeout_cancels_underlying_dispatch() -> None:
    """A3-r3-131 / S1-r2-104: after publish_sync() raises TimeoutError, the
    submitted dispatch must be cancelled — not left running (or hanging)
    forever on the shared persistent daemon-thread loop."""
    configure(package="synccancel", auto_discover=False)

    cancelled = threading.Event()

    @event
    @dataclass(frozen=True)
    class HangEvent:
        pass

    @listener
    async def hang_forever(evt: HangEvent) -> None:
        try:
            await asyncio.Event().wait()  # never set — hangs until cancelled
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(TimeoutError):
        publish_sync(HangEvent(), timeout=0.2)

    assert cancelled.wait(5), "dispatch kept running after the caller timed out"


# ---------------------------------------------------------------------------
# A3-r2-76 — nested publish_sync must not exhaust the shared executor
# ---------------------------------------------------------------------------


def test_nested_publish_sync_from_sync_listeners_does_not_exhaust_pool() -> None:
    """A3-r2-76: sync listeners that call publish_sync() for a follow-up event
    (saga-style) used to share the daemon loop's single bounded executor with
    the nested dispatch; with as many concurrent outer dispatches as the pool
    has workers, every nested publish deadlocked until its timeout. Nested
    dispatches must complete."""
    configure(package="syncnested", auto_discover=False)

    # The default executor created by loop.run_in_executor(None, ...).
    pool_size = min(32, (os.cpu_count() or 1) + 4)
    barrier = threading.Barrier(pool_size, timeout=15)
    inner_done: list[int] = []
    inner_lock = threading.Lock()
    outer_errors: list[Exception] = []

    @event
    @dataclass(frozen=True)
    class OuterEvent:
        pass

    @event
    @dataclass(frozen=True)
    class InnerEvent:
        pass

    @listener
    def on_inner(evt: InnerEvent) -> None:
        with inner_lock:
            inner_done.append(1)

    @listener
    def on_outer(evt: OuterEvent) -> None:
        # Hold every executor worker at once, then publish the follow-up:
        # with the shared-pool bug there is no free worker left for ANY
        # nested InnerEvent listener, so every nested publish times out.
        barrier.wait()
        publish_sync(InnerEvent(), timeout=5.0)

    def call_outer() -> None:
        try:
            publish_sync(OuterEvent(), timeout=30.0)
        except Exception as exc:
            outer_errors.append(exc)

    threads = [threading.Thread(target=call_outer, daemon=True) for _ in range(pool_size)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not any(t.is_alive() for t in threads)
    assert outer_errors == []
    assert len(inner_done) == pool_size


# ---------------------------------------------------------------------------
# S1-r4-194 — publish_sync during bootstrap must fail fast, not deadlock
# ---------------------------------------------------------------------------


_IMPORT_TIME_PUBLISH_MODULE = """
    from dataclasses import dataclass
    from modulith import event
    from modulith.sync import publish_sync

    @event
    @dataclass(frozen=True)
    class BootPing:
        pass

    captured = []
    try:
        publish_sync(BootPing(), timeout=5.0)
    except Exception as exc:
        captured.append((type(exc).__name__, str(exc)))
"""


# ---------------------------------------------------------------------------
# Task 6 (runtime-sync) — nested dispatch: ContextVar propagation, cancellation,
# timeout typing, and bounded thread lifetime
# ---------------------------------------------------------------------------


def test_nested_dispatch_propagates_callers_contextvars() -> None:
    """ContextVar: _run_nested_dispatch spawns a bare native thread for the
    fresh loop it gives the nested coroutine. Threads start with an empty
    top-level contextvars.Context — without explicitly copying the calling
    (outer-listener executor) thread's context into it, every ContextVar the
    caller had bound (a request-scoped value, an app-level session) was
    silently invisible to the nested publish's dispatch."""
    import contextvars

    from modulith.sync import _run_nested_dispatch

    cv: contextvars.ContextVar[str] = contextvars.ContextVar("cv", default="unset")
    cv.set("bound-by-outer-listener")

    seen: dict[str, str] = {}

    async def coro() -> None:
        seen["value"] = cv.get()

    _run_nested_dispatch(coro(), object(), timeout=5.0)

    assert seen["value"] == "bound-by-outer-listener"


def test_nested_dispatch_timeout_cancels_underlying_task() -> None:
    """cancellation: a nested dispatch that overruns its budget must request
    cancellation of the coroutine on its own fresh loop — not abandon it to
    run (or hang) unobserved forever."""
    from modulith.sync import PublishSyncTimeout, _run_nested_dispatch

    cancelled = threading.Event()

    async def hang_forever() -> None:
        try:
            await asyncio.Event().wait()  # never set — hangs until cancelled
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(PublishSyncTimeout):
        _run_nested_dispatch(hang_forever(), object(), timeout=0.2)

    assert cancelled.wait(5), "nested dispatch kept running after the caller timed out"


def test_nested_dispatch_timeout_raises_publish_sync_timeout_not_bare_timeout() -> None:
    """timeout: a genuine nested-dispatch budget overrun must surface as
    PublishSyncTimeout (a distinct type from an application TimeoutError),
    exactly like the top-level publish_sync() budget contract."""
    from modulith.sync import PublishSyncTimeout, _run_nested_dispatch

    async def hang_forever() -> None:
        await asyncio.Event().wait()

    with pytest.raises(PublishSyncTimeout, match="did not complete within"):
        _run_nested_dispatch(hang_forever(), object(), timeout=0.05)


def test_nested_dispatch_threads_do_not_linger_after_repeated_timeouts() -> None:
    """bounded-thread: repeated timed-out nested dispatches must not
    accumulate live 'modulith-sync-nested' threads. Cancelling the
    underlying task (see the cancellation regression above) bounds each
    thread's lifetime instead of leaving it running unobserved for as long
    as its hung coroutine happens to take — which, for a permanently-hung
    listener, was forever."""
    from modulith.sync import PublishSyncTimeout, _run_nested_dispatch

    async def hang_forever() -> None:
        await asyncio.Event().wait()

    for _ in range(5):
        with pytest.raises(PublishSyncTimeout):
            _run_nested_dispatch(hang_forever(), object(), timeout=0.05)

    def _live_nested_threads() -> list[threading.Thread]:
        return [
            t for t in threading.enumerate() if t.name == "modulith-sync-nested" and t.is_alive()
        ]

    deadline = time.monotonic() + 5.0
    while _live_nested_threads() and time.monotonic() < deadline:
        time.sleep(0.05)

    assert _live_nested_threads() == [], "timed-out nested dispatch threads never terminated"


def test_publish_sync_at_import_time_during_bootstrap_fails_fast(make_fake_app) -> None:
    """S1-r4-194: publish_sync() called from module code imported by discovery
    (the bootstrap thread holds the runtime lock) used to block the whole
    bootstrap until the timeout expired, then surface as a misleading
    TimeoutError. It must fail immediately with a clear RuntimeError."""
    make_fake_app({"boot": _IMPORT_TIME_PUBLISH_MODULE})
    configure(package="fakeapp")
    _runtime.ensure_bootstrapped()

    import fakeapp.boot as boot

    assert boot.captured, "publish_sync unexpectedly succeeded during bootstrap"
    exc_type, message = boot.captured[0]
    assert exc_type == "RuntimeError"
    assert "bootstrapping" in message
