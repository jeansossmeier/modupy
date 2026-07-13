"""pytest plugin for modulith.

Provides fixtures for testing applications built with modulith without
the global-state nightmares that come with Python's import system and
asyncio loops.

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

import asyncio
import inspect
import os
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from .markers import hookimpl

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


class _SpyPlugin:
    """A plugin that records hook activity into a ModulithTestApp.

    Registered with the runtime's plugin manager (via ``_extra_plugins``)
    so it observes every publish and every listener dispatch without the
    application code knowing it exists. pluggy matches hook arguments by
    name, so each method only needs to accept the kwargs it uses.
    """

    def __init__(self, app: ModulithTestApp) -> None:
        self._app = app

    @hookimpl
    def modulith_after_event_published(self, event: Any) -> None:
        self._app.published_events.append(event)

    @hookimpl
    def modulith_on_listener_dispatch(self, listener_name: str, event: Any) -> None:
        self._app.listener_calls.append((listener_name, event))


@pytest.fixture
def modulith_app() -> Iterator[ModulithTestApp]:
    """Provide a fresh modulith runtime for each test.

    Resets the global runtime singleton, registers an event-capturing spy
    plugin, and yields a handle exposing what was published and dispatched.
    On teardown the runtime is reset again and ANY module first imported
    during the test — application, third-party, or stdlib — is dropped from
    ``sys.modules`` so import-time state can't leak into the next test.
    Only ``modulith``'s own modules are preserved: their identity backs the
    runtime singleton and other global state.

    Two consequences of the delete-and-reimport strategy for modules kept
    alive across the test boundary: a re-import yields *new* class objects
    (``isinstance`` checks against instances created in an earlier test
    fail), and a module whose import-time side effects register against a
    persistent external registry (e.g. a prometheus_client-style collector)
    can crash on re-registration in a later test. Import such modules at
    collection time (module scope / conftest), before this fixture's
    snapshot, so they are never deleted.
    """
    from .runtime import _runtime

    snapshot = set(sys.modules)
    _runtime._reset_for_testing()

    test_app = ModulithTestApp()
    _runtime._extra_plugins.append(_SpyPlugin(test_app))

    try:
        yield test_app
    finally:
        _runtime._reset_for_testing()
        for name in set(sys.modules) - snapshot:
            if name.split(".")[0] == "modulith":
                continue
            del sys.modules[name]


# ---------------------------------------------------------------------------
# Fixture: modulith_module — module-isolated tests
# ---------------------------------------------------------------------------


@contextmanager
def _module_isolation(
    target_module: str,
    *,
    mock_modules: list[str] | None = None,
) -> Iterator[None]:
    """Context manager: only ``target_module`` is loaded; others are mocked.

    Within the block, every sibling under the application package (the top
    segment of ``target_module``) is removed from ``sys.modules`` except the
    target, its submodules, and its ancestors; each name in ``mock_modules``
    is replaced with a ``MagicMock`` so importers get a stand-in. On exit the
    original ``sys.modules`` is restored exactly.
    """
    mocks = mock_modules or []
    app_package = target_module.split(".")[0]
    ancestors = {
        ".".join(target_module.split(".")[:i]) for i in range(1, target_module.count(".") + 1)
    }
    snapshot = dict(sys.modules)

    for name in list(sys.modules):
        if not (name == app_package or name.startswith(app_package + ".")):
            continue
        if name == target_module or name.startswith(target_module + "."):
            continue
        if name in ancestors:
            continue
        del sys.modules[name]

    for name in mocks:
        sys.modules[name] = MagicMock(name=name)

    try:
        yield
    finally:
        for name in set(sys.modules) - set(snapshot):
            del sys.modules[name]
        for name, module in snapshot.items():
            sys.modules[name] = module


@pytest.fixture
def modulith_module() -> Callable[..., Any]:
    """Test a single module in isolation from siblings.

    Usage::

        def test_orders_in_isolation(modulith_module):
            with modulith_module("myapp.orders", mock_modules=["myapp.inventory"]):
                from myapp.orders import create_order
                create_order(...)

    Returns the ``_module_isolation`` context manager. Tests call it with
    the target module name and any mocks they need.
    """
    return _module_isolation


# ---------------------------------------------------------------------------
# Fixture: scenario — fluent event-driven test API
# ---------------------------------------------------------------------------


class Scenario:
    """Fluent builder for event-driven flow tests.

    Spring Modulith has Scenario; we mirror the API. Pattern::

        scenario.publish(OrderPlaced(...)) \\
                .expect_event(OrderConfirmed) \\
                .matching(lambda e: e.order_id == "123") \\
                .within(seconds=2)

    Each method returns self for chaining. ``.within()`` is the terminal
    operation — it triggers the publish/call, then polls the test app's
    captured ``published_events`` for the expected event, raising on miss.
    """

    def __init__(self, app: ModulithTestApp) -> None:
        self._app = app
        self._initial_event: Any = None
        self._initial_call: Callable[..., Any] | None = None
        self._initial_call_args: tuple[Any, ...] = ()
        self._initial_call_kwargs: dict[str, Any] = {}
        self._expected_type: type | None = None
        self._predicate: Callable[[Any], bool] | None = None

    def publish(self, event: Any) -> Scenario:
        """Publish an event as the trigger.

        The event is stored and actually published in ``within()`` so the
        rest of the chain (and any test setup) completes first.
        """
        self._initial_event = event
        return self

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Scenario:
        """Call a function as the trigger (alternative to publish)."""
        self._initial_call = fn
        self._initial_call_args = args
        self._initial_call_kwargs = kwargs
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

        The trigger phase and the poll phase share ONE ``seconds`` budget:
        the deadline is computed before the trigger fires, a ``publish``
        trigger is bounded via ``publish_sync(..., timeout=seconds)``, and a
        coroutine ``call`` trigger gets the remaining budget. A trigger that
        overruns the budget is cancelled (best-effort — the cancellation
        lands at the coroutine's next await point) so it cannot keep running
        on the shared daemon loop and dispatch into a later test's runtime;
        events captured before the overrun are still checked. A miss always
        raises the documented ``AssertionError``, never a bare TimeoutError.

        ``publish_sync`` (and any synchronous trigger function) blocks until
        all listeners — and the events they publish in turn — have been
        dispatched and captured, so the poll loop usually finds the event on
        its first pass. The timeout is a safety net for genuinely async
        fan-out and a clean failure mode when the event never arrives.
        """
        if self._expected_type is None:
            raise ValueError("call expect_event(...) before within(...)")
        if self._initial_event is None and self._initial_call is None:
            raise ValueError("call publish(...) or call(...) before within(...)")

        mark = len(self._app.published_events)
        deadline = time.monotonic() + seconds
        self._fire_trigger(seconds, deadline)

        while True:
            for event in self._app.published_events[mark:]:
                if isinstance(event, self._expected_type) and (
                    self._predicate is None or self._predicate(event)
                ):
                    return event
            if time.monotonic() >= deadline:
                break
            time.sleep(0.01)

        raise AssertionError(
            f"expected event {self._expected_type.__name__} not seen within {seconds}s"
        )

    def _fire_trigger(self, seconds: float, deadline: float) -> None:
        """Fire the publish/call trigger, bounded by the shared budget.

        ONLY the scenario's own budget overrun is swallowed (W3 R4-W3-01):
        the trigger is cancelled and ``within()`` falls through to its poll
        loop, which checks whatever was captured before the overrun and
        raises the documented AssertionError on a miss. A TimeoutError
        raised BY the application — the trigger coroutine itself, or a
        listener — is a real failure and propagates; swallowing it produced
        false-green tests.
        """
        from .sync import PublishSyncTimeout, publish_sync

        if self._initial_event is not None:
            try:
                publish_sync(self._initial_event, timeout=seconds)
            except PublishSyncTimeout:
                # The trigger overran the shared budget; publish_sync already
                # cancelled the abandoned dispatch. An application-raised
                # TimeoutError is NOT this type and propagates.
                pass
            return

        assert self._initial_call is not None
        result = self._initial_call(*self._initial_call_args, **self._initial_call_kwargs)
        if inspect.iscoroutine(result):
            self._await_call_trigger(result, deadline)

    @staticmethod
    def _await_call_trigger(coro: Any, deadline: float) -> None:
        """Block on a coroutine trigger, swallowing ONLY the budget overrun.

        A budget overrun cancels the trigger so it cannot outlive this test
        on the shared daemon loop and dispatch into a later test's runtime.
        A TimeoutError raised by the coroutine itself is an application
        failure and propagates (W3 R4-W3-01).
        """
        from .sync import _get_or_create_loop

        future: Future[Any] = asyncio.run_coroutine_threadsafe(coro, _get_or_create_loop())
        try:
            future.result(timeout=max(0.0, deadline - time.monotonic()))
        except TimeoutError:
            if future.done() and future.exception() is not None:
                # future.result() re-raised a TimeoutError from the app
                # coroutine itself, not the budget mechanism
                # (concurrent.futures.TimeoutError is an alias of
                # TimeoutError on Python >= 3.11) — a real application
                # failure; surface it.
                raise
            future.cancel()


@pytest.fixture
def scenario(modulith_app: ModulithTestApp) -> Scenario:
    """Provide a fresh Scenario builder bound to the test app."""
    return Scenario(modulith_app)


# ---------------------------------------------------------------------------
# Markers — declarative behavior toggles for individual tests
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the plugin's ini options."""
    parser.addini(
        "modulith_isolated_timeout",
        "Seconds an @pytest.mark.modulith_isolated subprocess may run before "
        "it is killed and reported as a failed test (default: 300).",
        default="300",
    )


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


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Apply per-test marker behavior before the test body runs."""
    if item.get_closest_marker("modulith_no_outbox") is None:
        return

    from .builtin import outbox

    item._modulith_original_outbox_configure = outbox.configure  # type: ignore[attr-defined]
    outbox._reset_for_testing()

    def _disabled_configure(*_args: Any, **_kwargs: Any) -> None:
        outbox._reset_for_testing()

    outbox.configure = _disabled_configure


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    """Restore any monkeypatched modulith test marker state."""
    original = getattr(item, "_modulith_original_outbox_configure", None)
    if original is None:
        return

    from .builtin import outbox

    outbox.configure = original
    outbox._reset_for_testing()


# ---------------------------------------------------------------------------
# Hook: subprocess-per-test isolation for marked tests
# ---------------------------------------------------------------------------

# Set in the child process so the re-run there executes the test inline
# instead of recursing into another subprocess.
_ISOLATION_GUARD = "MODULITH_ISOLATED_SUBPROCESS"

# Options never forwarded to the isolated child. The child runs with the
# cacheprovider plugin disabled (``-p no:cacheprovider``), so cache-backed
# options would be rejected there as unrecognized, and stepwise depends on
# the cache. ``--basetemp`` is per-run private: pytest WIPES that directory
# at startup, so sharing the parent's would destroy its live tmp artifacts.
_CHILD_UNSAFE_FLAGS = {
    "--cache-clear",
    "--cache-show",
    "--failed-first",
    "--ff",
    "--lf",
    "--last-failed",
    "--new-first",
    "--nf",
    "--stepwise",
    "--stepwise-reset",
    "--stepwise-skip",
    "--sw",
    "--sw-reset",
    "--sw-skip",
}
# Unsafe options that take a value (possibly as a separate argv token).
_CHILD_UNSAFE_VALUE_OPTS = {"--basetemp", "--lfnf", "--last-failed-no-failures"}


def _forwarded_parent_args(config: pytest.Config) -> list[str]:
    """The parent invocation's CLI args, minus positional test targets and
    options that are meaningless or destructive in the isolated child.

    The child re-runs a single nodeid, so everything else about the parent
    invocation — custom ``pytest_addoption`` flags, ``-m``/``-k`` filters,
    verbosity, coverage options — must carry over; dropping them silently
    reverted isolated tests to option defaults. Positional targets are
    identified by membership in ``config.option.file_or_dir`` (an option
    *value* that string-equals a positional target would be dropped too —
    a heuristic, but pytest itself offers no cleaner split).
    """
    positionals = set(config.option.file_or_dir or [])
    forwarded: list[str] = []
    skip_next = False
    for arg in config.invocation_params.args:
        if skip_next:
            skip_next = False
            continue
        base = arg.split("=", 1)[0]
        if base in _CHILD_UNSAFE_VALUE_OPTS:
            skip_next = "=" not in arg
            continue
        if base in _CHILD_UNSAFE_FLAGS:
            continue
        if arg in positionals:
            continue
        forwarded.append(arg)
    return forwarded


def _stream_text(stream: str | bytes | None) -> str:
    """Best-effort text for a captured child stream.

    ``subprocess.TimeoutExpired`` may carry ``None`` (POSIX) or bytes for a
    stream even when the run was started with ``text=True``.
    """
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode(errors="replace")
    return stream


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> bool | None:
    """Run ``@pytest.mark.modulith_isolated`` tests in a fresh subprocess.

    True isolation: import-time side effects and global state from other
    tests can't leak in. We re-invoke pytest on this single test in a child
    process (guarded by an env var to prevent infinite recursion), then
    synthesize a report from the child's exit code. Returning ``None`` for
    every other case hands control straight back to pytest's default
    protocol, so unmarked tests are completely unaffected.

    The child inherits the parent invocation's CLI arguments (see
    ``_forwarded_parent_args``), runs from pytest's rootdir so the nodeid
    resolves regardless of the parent's cwd, and is killed after
    ``modulith_isolated_timeout`` seconds (ini option, default 300) so one
    hung test can't block the suite forever. The launch itself happens
    inside the reported call, so any failure — fork/exec error, timeout,
    nonzero exit — fails only this test item, never the whole session.
    """
    if item.get_closest_marker("modulith_isolated") is None:
        return None
    if os.environ.get(_ISOLATION_GUARD) == "1":
        return None  # already inside the child — run normally

    from _pytest.runner import CallInfo

    ihook = item.ihook
    ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)

    env = dict(os.environ)
    env[_ISOLATION_GUARD] = "1"
    argv = [
        sys.executable,
        "-m",
        "pytest",
        *_forwarded_parent_args(item.config),
        item.nodeid,
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        "-q",
    ]

    def _outcome() -> None:
        raw_timeout = item.config.getini("modulith_isolated_timeout")
        try:
            timeout = float(raw_timeout)
        except (TypeError, ValueError):
            raise AssertionError(
                f"invalid modulith_isolated_timeout ini value {raw_timeout!r}: "
                "expected a number of seconds"
            ) from None
        try:
            completed = subprocess.run(
                argv,
                env=env,
                # Nodeids are rootdir-relative; the parent's incidental cwd
                # need not be (and often isn't) the rootdir.
                cwd=str(item.config.rootpath),
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                f"isolated subprocess for {item.nodeid} timed out after "
                f"{timeout}s (tune via the modulith_isolated_timeout ini "
                f"option)\n"
                f"--- stdout ---\n{_stream_text(exc.stdout)}\n"
                f"--- stderr ---\n{_stream_text(exc.stderr)}"
            ) from exc
        if completed.returncode != 0:
            raise AssertionError(
                f"isolated subprocess for {item.nodeid} exited "
                f"{completed.returncode}\n"
                f"--- stdout ---\n{completed.stdout}\n"
                f"--- stderr ---\n{completed.stderr}"
            )

    call = CallInfo.from_call(_outcome, when="call")
    report = ihook.pytest_runtest_makereport(item=item, call=call)
    ihook.pytest_runtest_logreport(report=report)
    # The test body ran entirely in the child, so this item's per-test
    # plugin hooks (setup/teardown) never ran here. We must still reconcile
    # the fixture stack to nextitem, or the next item's setup trips
    # "previous item was not torn down properly". Drive SetupState directly
    # rather than the pytest_runtest_teardown hook: the latter also invokes
    # other plugins' teardown (e.g. logging's caplog stash cleanup) whose
    # matching setup we skipped. teardown_exact only finalizes the collector
    # stack and does not require this item to have been set up.
    item.session._setupstate.teardown_exact(nextitem)
    ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)
    return True


__all__ = [
    "ModulithTestApp",
    "Scenario",
    "modulith_app",
    "modulith_module",
    "scenario",
]
