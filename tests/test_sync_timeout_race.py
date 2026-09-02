"""Deterministic regression tests for publish_sync's budget-overrun exception
type under the budget-expiry/dispatch-completion race.

``Future.result(timeout=...)`` raises a BARE ``concurrent.futures.TimeoutError``
— a fresh object created inside ``result()`` — when the wait expires while the
future is not yet done. The old handlers in ``publish_sync`` /
``_run_nested_dispatch`` distinguished "the application raised TimeoutError"
from "budget overrun" with ``future.done()`` alone, but the future can complete
in the race window between the wait expiring and the ``done()`` check — in
which case a GENUINE budget overrun re-raised the bare TimeoutError instead of
``PublishSyncTimeout``, so callers catching ``PublishSyncTimeout`` (and the
testing plugin's budget handling) saw the wrong type.

The race is nearly impossible to hit naturally, so these tests fake the wait
primitive: ``result()`` raises the bare TimeoutError an expired wait produces,
while ``done()`` already reports True (the dispatch completed inside the
window). Every budget-overrun exit must surface ``PublishSyncTimeout``; an
exception raised BY the dispatch itself — identified by identity, i.e. the
caught error IS ``future.exception()`` — must keep propagating unchanged
(application TimeoutError must propagate; end-to-end pin in
tests/test_scenario_timeout_error.py).

NOTE: the faked dispatch never runs, so no runtime configuration or listener
registration is needed here — these tests target the wait-site semantics only.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from typing import Any

import pytest

from modulith.sync import PublishSyncTimeout, _run_nested_dispatch, publish_sync


class _Ping:
    """Plain event stand-in; the faked dispatch never actually publishes it."""


class _RaceFuture:
    """Wait-primitive fake reproducing the race window deterministically.

    ``result()`` raises the bare TimeoutError an expired wait produces, while
    ``done()`` already reports completion — exactly the interleaving where the
    dispatch finishes between budget expiry and the handler's inspection.
    ``dispatch_outcome`` is what ``exception()`` reports the completed dispatch
    ended with (None: completed successfully inside the window).
    """

    def __init__(self, dispatch_outcome: BaseException | None = None) -> None:
        self.dispatch_outcome = dispatch_outcome
        self.cancel_calls = 0

    def result(self, timeout: float | None = None) -> None:
        raise concurrent.futures.TimeoutError()

    def done(self) -> bool:
        return True

    def cancelled(self) -> bool:
        return False

    def exception(self, timeout: float | None = None) -> BaseException | None:
        return self.dispatch_outcome

    def cancel(self) -> bool:
        self.cancel_calls += 1
        return False  # already done — cancelling is a no-op, like the real Future

    # _run_nested_dispatch's worker thread completes the future itself; the
    # fake is already "done", so completion attempts are absorbed.
    def set_result(self, result: None) -> None:
        pass

    def set_exception(self, exc: BaseException) -> None:
        pass


def _install_fake_dispatch_future(monkeypatch: pytest.MonkeyPatch, fake: _RaceFuture) -> None:
    """Point publish_sync's wait at ``fake`` instead of a real dispatch."""

    def fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _RaceFuture:
        coro.close()  # never dispatched; silence "coroutine never awaited"
        return fake

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)


@pytest.mark.parametrize(
    "dispatch_outcome",
    [None, TimeoutError("listener's own failure, landed inside the window")],
    ids=["completed-ok-in-window", "completed-raising-in-window"],
)
def test_budget_overrun_race_raises_publish_sync_timeout(
    monkeypatch: pytest.MonkeyPatch, dispatch_outcome: BaseException | None
) -> None:
    """Budget expired, dispatch completed inside the race window: the caller
    must still get PublishSyncTimeout — never the bare TimeoutError the
    expired wait raised."""
    fake = _RaceFuture(dispatch_outcome)
    _install_fake_dispatch_future(monkeypatch, fake)

    with pytest.raises(PublishSyncTimeout):
        publish_sync(_Ping(), timeout=0.01)


def test_budget_overrun_race_nested_dispatch_raises_publish_sync_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same race on the nested (fresh-thread) dispatch path: the Future that
    _run_nested_dispatch waits on completes between the wait expiring and the
    done() check; the caller must get PublishSyncTimeout."""
    monkeypatch.setattr(concurrent.futures, "Future", _RaceFuture)

    async def noop() -> None:
        pass

    with pytest.raises(PublishSyncTimeout):
        _run_nested_dispatch(noop(), _Ping(), 0.01)


def test_budget_overrun_after_the_nested_loop_closed_raises_publish_sync_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worst case of the same race: the nested dispatch not only completed, its
    fresh loop is already CLOSED by the time the budget handler cancels the
    task. ``call_soon_threadsafe`` raises ``RuntimeError: Event loop is closed``
    on a closed loop, so an unguarded cancel hands the caller that instead of
    PublishSyncTimeout — the exception type the whole path exists to guarantee.
    """
    loops: list[asyncio.AbstractEventLoop] = []
    real_new_event_loop = asyncio.new_event_loop

    def recording_new_event_loop() -> asyncio.AbstractEventLoop:
        loop = real_new_event_loop()
        loops.append(loop)
        return loop

    class _ClosedLoopFuture(_RaceFuture):
        """Raises the expired-wait TimeoutError only once the worker thread has
        run the dispatch to completion AND torn its loop down, which pins the
        interleaving instead of racing for it."""

        def result(self, timeout: float | None = None) -> None:
            deadline = time.monotonic() + 5.0
            while not (loops and loops[0].is_closed()):
                if time.monotonic() > deadline:
                    raise AssertionError("nested dispatch loop never closed")
                time.sleep(0.001)
            raise concurrent.futures.TimeoutError()

    monkeypatch.setattr(asyncio, "new_event_loop", recording_new_event_loop)
    monkeypatch.setattr(concurrent.futures, "Future", _ClosedLoopFuture)

    async def noop() -> None:
        pass

    with pytest.raises(PublishSyncTimeout):
        _run_nested_dispatch(noop(), _Ping(), 0.01)


def test_dispatch_raised_timeouterror_still_propagates_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard against over-correction: when the caught
    TimeoutError IS the dispatch's own stored exception — a listener raised it
    and ``result()`` re-raised the exact object, no budget expiry involved —
    publish_sync must surface it unchanged, never as PublishSyncTimeout."""

    class _CompletedRaising(_RaceFuture):
        """The dispatch completed by raising; result() re-raises the stored
        exception object, exactly like the real Future.__get_result()."""

        def result(self, timeout: float | None = None) -> None:
            assert self.dispatch_outcome is not None
            raise self.dispatch_outcome

    app_error = TimeoutError("listener bug surfacing through publish_sync")
    fake = _CompletedRaising(app_error)
    _install_fake_dispatch_future(monkeypatch, fake)

    with pytest.raises(TimeoutError, match="listener bug") as excinfo:
        publish_sync(_Ping(), timeout=0.01)

    assert excinfo.value is app_error
    assert not isinstance(excinfo.value, PublishSyncTimeout)


def test_new_event_loop_startup_failure_does_not_hang_loop_ready_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """asyncio.new_event_loop() failing (e.g. fd exhaustion under load) must
    not leave the caller blocked on the unbounded loop_ready.wait(): the
    worker's startup exception must reach the caller promptly, well inside
    the requested budget, instead of the caller hanging forever because
    loop_ready.set() was never reached."""

    def failing_new_event_loop() -> asyncio.AbstractEventLoop:
        raise OSError("fd exhaustion")

    monkeypatch.setattr(asyncio, "new_event_loop", failing_new_event_loop)

    async def noop() -> None:
        pass

    outcome: dict[str, BaseException] = {}

    def call() -> None:
        try:
            _run_nested_dispatch(noop(), _Ping(), 5.0)
        except BaseException as exc:  # relaying across the thread boundary
            outcome["exc"] = exc

    caller = threading.Thread(target=call, daemon=True)
    caller.start()
    caller.join(timeout=1.0)

    assert not caller.is_alive(), "caller blocked past its budget instead of failing promptly"
    assert isinstance(outcome.get("exc"), OSError)
