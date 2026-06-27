"""Meta-tests for the modulith pytest plugin (``modulith.testing``).

The plugin ships with modulith and is auto-loaded via the ``pytest11`` entry
point, so its fixtures (``modulith_app``, ``modulith_module``, ``scenario``)
and markers (``modulith_isolated``) are available here without imports. These
tests exercise the plugin's own behavior: event capture, module isolation, the
fluent Scenario API, and subprocess-per-test isolation.

Note: this file deliberately does *not* use ``from __future__ import
annotations`` so that ``@listener`` sees real annotation objects (module-level
event classes), matching how application code is typically written.
"""

import os
from dataclasses import dataclass

import pytest

from modulith import event, listener, publish, publish_sync
from modulith.decorators import configure


# Module-level event types so @listener annotations resolve to real classes.
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


@event
@dataclass(frozen=True)
class OrderConfirmed:
    order_id: str


def _isolate() -> None:
    """Configure a clean, discovery-free runtime for capture/scenario tests."""
    configure(package="modulith_metatest", auto_discover=False)


# ---------------------------------------------------------------------------
# modulith_app — event capture
# ---------------------------------------------------------------------------


def test_modulith_app_starts_empty(modulith_app) -> None:
    assert modulith_app.published_events == []
    assert modulith_app.listener_calls == []


def test_modulith_app_captures_published_events(modulith_app) -> None:
    _isolate()
    publish_sync(OrderPlaced(order_id="a1"))

    captured = modulith_app.published_events_of_type(OrderPlaced)
    assert captured == [OrderPlaced(order_id="a1")]


def test_modulith_app_captures_listener_dispatch(modulith_app) -> None:
    _isolate()

    @listener
    async def on_placed(evt: OrderPlaced) -> None:
        pass

    publish_sync(OrderPlaced(order_id="b2"))

    assert any(name.endswith("on_placed") for name, _ in modulith_app.listener_calls)
    assert OrderPlaced(order_id="b2") in [evt for _, evt in modulith_app.listener_calls]


# ---------------------------------------------------------------------------
# scenario — fluent event-flow assertions
# ---------------------------------------------------------------------------


def test_scenario_within_returns_matching_event(scenario) -> None:
    _isolate()

    @listener
    async def confirm(evt: OrderPlaced) -> None:
        await publish(OrderConfirmed(order_id=evt.order_id))

    result = (
        scenario.publish(OrderPlaced(order_id="123"))
        .expect_event(OrderConfirmed)
        .matching(lambda e: e.order_id == "123")
        .within(seconds=2)
    )

    assert isinstance(result, OrderConfirmed)
    assert result.order_id == "123"


def test_scenario_within_times_out_when_event_absent(scenario) -> None:
    _isolate()
    # No listener produces OrderConfirmed → the expectation can never be met.
    with pytest.raises(AssertionError):
        (
            scenario.publish(OrderPlaced(order_id="x"))
            .expect_event(OrderConfirmed)
            .within(seconds=0.2)
        )


def test_scenario_call_preserves_args_and_kwargs(scenario) -> None:
    _isolate()
    captured: dict[str, str] = {}

    def trigger(a: str, *, b: str) -> None:
        captured["a"] = a
        captured["b"] = b
        publish_sync(OrderConfirmed(order_id=f"{a}-{b}"))

    result = scenario.call(trigger, "x", b="y").expect_event(OrderConfirmed).within(seconds=2)

    assert captured == {"a": "x", "b": "y"}
    assert result.order_id == "x-y"


# ---------------------------------------------------------------------------
# modulith_module — sibling isolation + mocking
# ---------------------------------------------------------------------------


def test_modulith_module_mocks_named_siblings(make_fake_app, modulith_module) -> None:
    import importlib
    import sys
    from unittest.mock import MagicMock

    make_fake_app({"orders": "VALUE = 'real-orders'", "inventory": "VALUE = 'real-inv'"})
    importlib.import_module("fakeapp.inventory")  # load the real modules first
    importlib.import_module("fakeapp.orders")

    assert sys.modules["fakeapp.inventory"].VALUE == "real-inv"

    with modulith_module("fakeapp.orders", mock_modules=["fakeapp.inventory"]):
        assert isinstance(sys.modules["fakeapp.inventory"], MagicMock)
        assert "fakeapp.orders" in sys.modules  # target module preserved

    # sys.modules restored to the real module after exit.
    assert not isinstance(sys.modules["fakeapp.inventory"], MagicMock)
    assert sys.modules["fakeapp.inventory"].VALUE == "real-inv"


# ---------------------------------------------------------------------------
# @pytest.mark.modulith_isolated — subprocess-per-test isolation
# ---------------------------------------------------------------------------


@pytest.mark.modulith_isolated
def test_isolated_marker_runs_in_subprocess() -> None:
    # If the marker worked, this test was re-run in a dedicated subprocess
    # with the guard env var set. If it ran inline (no isolation), the guard
    # is absent and this assertion fails — so a pass *proves* isolation.
    assert os.environ.get("MODULITH_ISOLATED_SUBPROCESS") == "1"


@pytest.mark.modulith_no_outbox
def test_no_outbox_marker_disables_outbox_configuration() -> None:
    from modulith.builtin import outbox

    outbox.configure(store=object(), serializer=object())

    assert outbox._store is None
