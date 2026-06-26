"""pytest plugin for modulith.

Provides fixtures for testing applications built with modulith without
the global-state nightmares that come with Python's import system and
asyncio loops.

Implementation status: SKELETON. ~150 lines when complete.

Distributed in two ways:
  1. As `modulith[test]` extra in v1 — included with main package.
  2. Eventually as `pytest-modulith` standalone in v2 — separate release
     cadence, smaller install for users who only test.

Registered as a pytest plugin via the `pytest11` entry point in
pyproject.toml. Once `pip install modulith[test]` runs, fixtures are
available in any test without imports.

Three primary fixtures:

  modulith_app    — fresh runtime per test, in-memory bus, no I/O
  modulith_module — module-isolated tests with mocked dependencies
  scenario        — fluent API for event-driven flow tests

Plus markers:

  @pytest.mark.modulith_isolated — run in a subprocess for true isolation
  @pytest.mark.modulith_no_outbox — disable outbox for this test
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Fixture: modulith_app — fresh runtime per test
# ---------------------------------------------------------------------------


@dataclass
class ModulithTestApp:
    """Test handle exposing the captured runtime state.

    Tests use this to assert on what was published, who listened,
    what the configuration was. Reset between tests automatically.
    """

    published_events: list[Any] = field(default_factory=list)
    listener_calls: list[tuple[str, Any]] = field(default_factory=list)

    def published_events_of_type(self, event_type: type) -> list[Any]:
        """Return only events of the given type — useful for assertions."""
        return [e for e in self.published_events if isinstance(e, event_type)]

    def reset(self) -> None:
        """Clear all captured state."""
        self.published_events.clear()
        self.listener_calls.clear()


@pytest.fixture
def modulith_app() -> ModulithTestApp:
    """Provide a fresh modulith runtime for each test.

    IMPLEMENTATION TODO:
    1. Reset the global runtime singleton:
         from modulith.runtime import _runtime
         _runtime._reset()  # add this method to Runtime class
    2. Build a ModulithTestApp instance.
    3. Register a "spy" plugin via the runtime's plugin manager that
       hooks modulith_after_event_published and modulith_on_listener_dispatch
       to populate the test app's lists.
    4. Yield the test app to the test.
    5. After test: reset runtime, clear listeners, restore sys.modules
       to the snapshot taken before the test.

    The sys.modules snapshot is essential — modules imported during one
    test must not leak into the next. Snapshot before test, restore after.
    """
    raise NotImplementedError("Phase 2 — see TODO above")


# ---------------------------------------------------------------------------
# Fixture: modulith_module — module-isolated tests
# ---------------------------------------------------------------------------


@contextmanager
def _module_isolation(
    target_module: str,
    *,
    mock_modules: list[str] | None = None,
):
    """Context manager: only target_module is loaded; others are mocked.

    IMPLEMENTATION TODO:
    1. Snapshot sys.modules.
    2. Remove every module starting with the application package, except
       target_module and its submodules.
    3. For each module in mock_modules: install a MagicMock at that path
       in sys.modules so importers get a mock instead of the real module.
    4. yield
    5. Restore sys.modules to the snapshot.
    """
    raise NotImplementedError("Phase 2")


@pytest.fixture
def modulith_module():
    """Test a single module in isolation from siblings.

    Usage:
        def test_orders_in_isolation(modulith_module):
            with modulith_module("orders", mock_modules=["inventory"]):
                from myapp.orders import create_order
                create_order(...)

    Returns the _module_isolation context manager. Tests call it with
    the target module name and any mocks they need.
    """
    return _module_isolation


# ---------------------------------------------------------------------------
# Fixture: scenario — fluent event-driven test API
# ---------------------------------------------------------------------------


class Scenario:
    """Fluent builder for event-driven flow tests.

    Spring Modulith has Scenario; we mirror the API. Pattern:

        scenario.publish(OrderPlaced(...)) \\
                .expect_event(OrderConfirmed) \\
                .matching(lambda e: e.order_id == "123") \\
                .within(seconds=2)

    Each method returns self for chaining. .within() is the terminal
    operation — it polls the test app's published_events list with
    timeout, raises on miss.
    """

    def __init__(self, app: ModulithTestApp) -> None:
        self._app = app
        self._initial_event: Any = None
        self._initial_call: Callable | None = None
        self._initial_call_args: tuple = ()
        self._expected_type: type | None = None
        self._predicate: Callable[[Any], bool] | None = None

    def publish(self, event: Any) -> Scenario:
        """Publish an event as the trigger.

        IMPLEMENTATION TODO: store the event, return self.
        Actual publishing happens in within() to allow setup to complete first.
        """
        self._initial_event = event
        return self

    def call(self, fn: Callable, *args: Any, **kwargs: Any) -> Scenario:
        """Call a function as the trigger (alternative to publish)."""
        self._initial_call = fn
        self._initial_call_args = args
        # IMPLEMENTATION: also store kwargs
        return self

    def expect_event(self, event_type: type) -> Scenario:
        """Set the expected event type to wait for."""
        self._expected_type = event_type
        return self

    def matching(self, predicate: Callable[[Any], bool]) -> Scenario:
        """Add a predicate that the expected event must satisfy."""
        self._predicate = predicate
        return self

    def within(self, seconds: float) -> Any:
        """Terminal: trigger and poll for the expected event.

        IMPLEMENTATION TODO:
        1. Mark current state of self._app.published_events (length).
        2. Trigger: if self._initial_event, call publish_sync;
                    if self._initial_call, call it.
        3. Poll loop with timeout:
             deadline = time.monotonic() + seconds
             while time.monotonic() < deadline:
                 new_events = self._app.published_events[mark:]
                 for event in new_events:
                     if isinstance(event, self._expected_type):
                         if self._predicate is None or self._predicate(event):
                             return event
                 await asyncio.sleep(0.01)
             raise AssertionError(f"Expected event {self._expected_type.__name__} "
                                  f"not seen within {seconds}s")
        4. Use anyio for cross-loop compatibility if pytest-asyncio config differs.
        """
        raise NotImplementedError("Phase 2 — see TODO above")


@pytest.fixture
def scenario(modulith_app: ModulithTestApp) -> Scenario:
    """Provide a fresh Scenario builder bound to the test app."""
    return Scenario(modulith_app)


# ---------------------------------------------------------------------------
# Markers — declarative behavior toggles for individual tests
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    """Register modulith markers so pytest doesn't warn about them."""
    config.addinivalue_line(
        "markers",
        "modulith_isolated: run this test in a subprocess for true isolation",
    )
    config.addinivalue_line(
        "markers",
        "modulith_no_outbox: disable the transactional outbox for this test",
    )


# ---------------------------------------------------------------------------
# Hook: subprocess-per-test isolation for marked tests
# ---------------------------------------------------------------------------

# IMPLEMENTATION TODO:
# Use pytest-xdist's worker_id mechanism, OR a custom pytest_runtest_protocol
# hook that detects @pytest.mark.modulith_isolated and reruns the test in
# a subprocess via subprocess.run([sys.executable, "-m", "pytest", "::test_id"]).
#
# The subprocess approach is heavier (~100ms fork) but gives true isolation —
# import-time side effects can't leak between tests. Use sparingly; mark
# tests that genuinely need it.


__all__ = [
    "ModulithTestApp",
    "Scenario",
    "modulith_app",
    "modulith_module",
    "scenario",
]
