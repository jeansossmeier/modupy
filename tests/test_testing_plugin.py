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


def test_modulith_module_reimports_target_after_mocking_siblings(
    make_fake_app, modulith_module
) -> None:
    """A target already imported BEFORE entering isolation keeps bindings
    resolved against its REAL siblings (``from fakeapp.inventory import
    VALUE`` snapshots the name at import time). Leaving the target cached
    while its sibling is swapped for a MagicMock silently defeats the mock —
    ``modulith_module`` must re-import the target itself after installing the
    mocks, so it is already correctly bound by the time the ``with`` body
    runs (no manual reimport needed by the caller)."""
    import importlib
    import sys

    make_fake_app(
        {
            "orders": "from fakeapp.inventory import VALUE\n",
            "inventory": "VALUE = 'real-inv'",
        }
    )
    importlib.import_module("fakeapp.inventory")
    real_orders = importlib.import_module("fakeapp.orders")
    assert real_orders.VALUE == "real-inv"

    with modulith_module("fakeapp.orders", mock_modules=["fakeapp.inventory"]):
        assert not isinstance(sys.modules["fakeapp.orders"].VALUE, str)  # bound to the mock now

    # Restored to the original module object untouched after exit.
    assert sys.modules["fakeapp.orders"] is real_orders
    assert sys.modules["fakeapp.orders"].VALUE == "real-inv"


# ---------------------------------------------------------------------------
# @pytest.mark.modulith_isolated — subprocess-per-test isolation
# ---------------------------------------------------------------------------


@pytest.mark.modulith_isolated
def test_isolated_marker_runs_in_subprocess() -> None:
    # If the marker worked, this test was re-run in a dedicated subprocess
    # with the guard env var set. If it ran inline (no isolation), the guard
    # is absent and this assertion fails — so a pass *proves* isolation.
    assert os.environ.get("MODULITH_ISOLATED_SUBPROCESS") == "1"


def test_isolated_marker_survives_a_parent_run_under_coverage(tmp_path) -> None:
    """The isolated child re-runs a single nodeid. Forwarding the parent's
    ``--cov*`` options unchanged made pytest-cov re-apply the parent's
    ``--cov-fail-under`` to that one test's coverage, so the child exited 1
    and the marker failed for any downstream project whose CI runs
    ``pytest --cov``. Modelled as a downstream project: a normal test covers
    the package fully (the parent's own gate passes), and an isolated test
    alone would never reach the threshold."""
    import subprocess
    import sys

    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "__init__.py").write_text(
        "def used():\n    return 1\n\n\ndef also_used():\n    return 2\n", encoding="utf-8"
    )
    (tmp_path / "test_downstream.py").write_text(
        "import os\n"
        "import pytest\n"
        "import pkg\n"
        "\n"
        "def test_covers_everything():\n"
        "    assert pkg.used() == 1\n"
        "    assert pkg.also_used() == 2\n"
        "\n"
        "@pytest.mark.modulith_isolated\n"
        "def test_isolated():\n"
        "    assert os.environ.get('MODULITH_ISOLATED_SUBPROCESS') == '1'\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--cov=pkg", "--cov-fail-under=100"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "2 passed" in completed.stdout


@pytest.mark.modulith_no_outbox
def test_no_outbox_marker_disables_outbox_configuration() -> None:
    from modulith.builtin import outbox

    outbox.configure(store=object(), serializer=object())

    assert outbox._store is None


# ---------------------------------------------------------------------------
# ModulithTestApp.reset() — public API
# ---------------------------------------------------------------------------


def test_modulith_test_app_reset_clears_captured_state(modulith_app) -> None:
    """reset() is public API — it must clear both captured lists
    (published events and listener dispatches) so a test can reuse one handle
    across phases."""
    _isolate()

    @listener
    async def on_placed(evt: OrderPlaced) -> None:
        pass

    publish_sync(OrderPlaced(order_id="r1"))
    assert modulith_app.published_events
    assert modulith_app.listener_calls

    modulith_app.reset()

    assert modulith_app.published_events == []
    assert modulith_app.listener_calls == []
