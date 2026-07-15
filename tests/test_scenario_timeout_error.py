"""Application-raised TimeoutError must fail scenario tests.

``Scenario._fire_trigger`` may swallow ONLY its own budget timeout (the
``within(seconds=...)`` window). A TimeoutError raised BY the application —
the trigger coroutine itself, or a listener — is a real failure and must
propagate; swallowing it produced false-green tests.

Note: like tests/test_testing_plugin.py, this file deliberately does *not*
use ``from __future__ import annotations`` so that ``@listener`` sees real
annotation objects (module-level event classes).
"""

from dataclasses import dataclass

import pytest

from modulith import event, listener, publish, publish_sync
from modulith.decorators import configure


@event
@dataclass(frozen=True)
class Placed:
    order_id: str


@event
@dataclass(frozen=True)
class Confirmed:
    order_id: str


def _isolate() -> None:
    """Configure a clean, discovery-free runtime for scenario tests."""
    configure(package="modulith_w3r4_metatest", auto_discover=False)


def test_call_trigger_app_timeouterror_propagates(scenario):
    """W3 R4-W3-01: an async trigger that publishes the expected event and
    THEN raises TimeoutError is an application bug — the scenario must fail
    loudly, not return the event and go false-green."""
    _isolate()

    async def buggy_trigger() -> None:
        await publish(Confirmed(order_id="z"))
        raise TimeoutError("app-level timeout bug that should fail this test")

    with pytest.raises(TimeoutError, match="app-level timeout bug"):
        scenario.call(buggy_trigger).expect_event(Confirmed).within(seconds=5)


def test_publish_trigger_listener_timeouterror_propagates(scenario):
    """W3 R4-W3-01: a listener raising TimeoutError under a publish trigger
    must fail the test even though another listener produced the expected
    event — the app error must not be misread as the scenario's budget."""
    _isolate()

    @listener
    async def confirm(evt: Placed) -> None:
        await publish(Confirmed(order_id=evt.order_id))

    @listener
    async def broken(evt: Placed) -> None:
        raise TimeoutError("listener bug that should fail this test")

    with pytest.raises(TimeoutError, match="listener bug"):
        scenario.publish(Placed(order_id="1")).expect_event(Confirmed).within(seconds=5)


def test_publish_sync_propagates_listener_timeouterror():
    """W3 R4-W3-01 (origin): publish_sync must re-raise a listener's own
    TimeoutError unchanged instead of misclassifying it as its budget
    timeout (concurrent.futures.TimeoutError IS TimeoutError on 3.11+)."""
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()
    _isolate()
    _runtime.ensure_bootstrapped()

    @listener
    async def broken(evt: Placed) -> None:
        raise TimeoutError("listener bug surfacing through publish_sync")

    try:
        with pytest.raises(TimeoutError, match="listener bug"):
            publish_sync(Placed(order_id="x"), timeout=5)
    finally:
        _runtime._reset_for_testing()
