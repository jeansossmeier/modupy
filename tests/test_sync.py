"""Behavioral tests for the sync entrypoint and sync-listener support.

Covers modulith/sync.py — publish_sync() context detection/dispatch (including
in a forked child) and wrap_sync_listener() executor dispatch + contextvars
propagation — plus the end-to-end sync @listener path. No mocks: real runtime,
real event loop(s), real threads, and a real forked process (same style as the
rest of the suite).

The runtime needs configuration before bootstrap; these tests use
auto_discover=False so bootstrap builds the bus and flushes the listeners they
register directly, without importing a real package from disk.

Importing publish_sync from the top-level package (not modulith.sync) also
guards the public-API export — sync-only users have no other entrypoint.

NOTE: this module intentionally does NOT use `from __future__ import
annotations`. @listener reads the event type from the listener's parameter
annotation at runtime; PEP 563 would stringize it. The event classes here are
defined in local scope, so the realistic resolution path (typing.get_type_hints
over a module's globals) cannot see them — real annotations keep these tests
focused on sync behavior rather than annotation resolution.
"""

import contextvars
import json
import os
import signal
import threading
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from modulith import bootstrap, configure, event, listener, publish, publish_sync
from modulith import sync as sync_module
from modulith.sync import wrap_sync_listener


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Each test gets a clean runtime + manifest registry."""
    from modulith import manifest as manifest_module
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# publish_sync() — context detection & dispatch
# ---------------------------------------------------------------------------


def test_publish_sync_no_running_loop_dispatches() -> None:
    """Pure sync context (no running loop) → dispatches via the daemon loop."""
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class Pinged:
        value: int

    received: list[int] = []

    @listener
    async def on_ping(evt: Pinged) -> None:
        received.append(evt.value)

    publish_sync(Pinged(value=7))

    assert received == [7]


def test_publish_sync_from_worker_thread_dispatches() -> None:
    """Starlette-style sync view: a worker thread with no running loop.

    Starlette runs sync views in a threadpool, so the calling thread has no
    running event loop and publish_sync() falls back to the persistent daemon
    loop. (asyncio.get_running_loop() only ever returns the *current* thread's
    loop, so the "submit to a loop on another thread" branch is not reached
    this way — the threadpool-view path is exactly this daemon-loop fallback.)
    """
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class Tick:
        n: int

    received: list[int] = []

    @listener
    async def on_tick(evt: Tick) -> None:
        received.append(evt.n)

    errors: list[Exception] = []

    def worker() -> None:
        try:
            publish_sync(Tick(n=9))
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=10)

    assert not errors
    assert received == [9]


def test_publish_sync_timeout_raises() -> None:
    """A listener that does not finish in time → TimeoutError."""
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class Slow:
        pass

    gate = threading.Event()

    @listener
    def slow_listener(evt: Slow) -> None:
        # Sync listener runs in the loop's executor; block it so the dispatch
        # cannot complete within the timeout window.
        gate.wait(timeout=5)

    # publish() bootstraps on the daemon loop's thread. Left to it, a loaded
    # machine can spend the whole budget there, and that bootstrap finishes
    # after the reset fixture, leaving the next test's runtime bootstrapped.
    bootstrap()
    try:
        with pytest.raises(TimeoutError):
            publish_sync(Slow(), timeout=0.05)
    finally:
        # Release the executor thread so no work lingers past the test.
        gate.set()


async def test_publish_sync_on_loop_thread_raises() -> None:
    """Called from inside a running loop on the same thread → RuntimeError.

    This test runs inside the event loop (asyncio_mode=auto), so publish_sync()
    must detect the same-thread loop and refuse, pointing the caller at
    `await publish(...)` instead.
    """
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class E:
        pass

    with pytest.raises(RuntimeError, match="async context"):
        publish_sync(E())


# ---------------------------------------------------------------------------
# publish_sync() after os.fork()
# ---------------------------------------------------------------------------


def _in_forked_child(work: Callable[[], Any], *, budget: float) -> dict[str, Any] | None:
    """Run ``work()`` in a forked child and return the JSON report it sent back.

    The report is ``{"result": ...}`` or ``{"error": repr(exc)}``. A child still
    running after ``budget`` seconds is killed and reported as ``None``.
    """
    read_fd, write_fd = os.pipe()
    with warnings.catch_warnings():
        # Python 3.12+ warns that fork() in a multi-threaded process may deadlock.
        # The parent's loop thread makes it multi-threaded on purpose: that is the
        # scenario under test.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        status = 1
        try:
            os.close(read_fd)
            try:
                report: dict[str, Any] = {"result": work()}
            except BaseException as exc:
                report = {"error": repr(exc)}
            os.write(write_fd, json.dumps(report).encode())
            status = 0
        finally:
            os._exit(status)
    os.close(write_fd)
    try:
        deadline = time.monotonic() + budget
        while True:
            done, wait_status = os.waitpid(pid, os.WNOHANG)
            if done:
                break
            if time.monotonic() > deadline:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                return None
            time.sleep(0.01)
        data = os.read(read_fd, 65536)
        if not data:
            return {"error": f"no report; exit code {os.waitstatus_to_exitcode(wait_status)}"}
        parsed: dict[str, Any] = json.loads(data)
        return parsed
    finally:
        os.close(read_fd)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is unavailable on this platform")
@pytest.mark.parametrize("loop_lock_held", [False, True], ids=["lock_free", "lock_held"])
def test_publish_sync_works_in_a_forked_child(loop_lock_held: bool) -> None:
    """A child forked after the parent used publish_sync() gets its own loop thread.

    fork() copies the parent's cached loop but not the daemon thread that ran it,
    so a child that reused the loop queued its dispatch where nothing ran it and
    timed out (hung forever under timeout=None). A lock that another parent
    thread held at fork time is copied locked, and nothing in the child would
    ever release it.
    """
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class Forked:
        who: str

    received: list[str] = []

    @listener
    async def on_forked(evt: Forked) -> None:
        received.append(evt.who)

    publish_sync(Forked(who="parent"))
    assert received == ["parent"]

    def child_publishes() -> list[str]:
        publish_sync(Forked(who="child"), timeout=5)
        return received

    loop_lock = sync_module._loop_lock
    if loop_lock_held:
        loop_lock.acquire()
    try:
        report = _in_forked_child(child_publishes, budget=20)
    finally:
        if loop_lock_held:
            loop_lock.release()

    assert report is not None, "the forked child did not finish within 20s and was killed"
    assert report == {"result": ["parent", "child"]}


# ---------------------------------------------------------------------------
# wrap_sync_listener() — executor dispatch + contextvars propagation
# ---------------------------------------------------------------------------


async def test_wrap_sync_listener_runs_in_executor_with_contextvars() -> None:
    """The wrapper runs the sync handler off the loop thread and carries the
    caller's ContextVar values into the executor thread."""
    cv: contextvars.ContextVar[str] = contextvars.ContextVar("cv", default="unset")
    cv.set("propagated")

    @dataclass(frozen=True)
    class E:
        pass

    seen: dict[str, object] = {}

    def handler(evt: E) -> None:
        seen["thread"] = threading.get_ident()
        seen["cv"] = cv.get()

    wrapper = wrap_sync_listener(handler)

    # Marker that verify_manifest (D1) and tooling use to recognize wrappers.
    assert wrapper.__modulith_sync_wrapped__ is handler

    await wrapper(E())

    assert seen["cv"] == "propagated"
    assert seen["thread"] != threading.get_ident()


# ---------------------------------------------------------------------------
# End-to-end: a sync @listener dispatched via async publish()
# ---------------------------------------------------------------------------


async def test_sync_listener_runs_on_publish() -> None:
    """A sync @listener actually receives events dispatched via publish()."""
    configure(package="synctest", auto_discover=False)

    @event
    @dataclass(frozen=True)
    class E:
        n: int

    calls: list[int] = []

    @listener
    def handle(evt: E) -> None:  # sync def — wrapped for executor dispatch
        calls.append(evt.n)

    await publish(E(n=3))

    assert calls == [3]
