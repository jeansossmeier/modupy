"""pytest plugin for modulith.

Provides fixtures for testing applications built with modulith without
the global-state nightmares that come with Python's import system and
asyncio loops.

Registered as a pytest plugin via the `pytest11` entry point in
pyproject.toml, under the entry-point name `modulith`. That entry point is
unconditional — fixtures and markers load in any pytest run where modupy
is installed, regardless of which extras were requested at install time.
The `modupy[test]` extra only adds the libraries the fixtures need (e.g.
`httpx` for `modulith_app`'s test client); it does not gate registration.
Eventually the plugin may move into a standalone `pytest-modulith`
distribution (v2) — separate release cadence, smaller install for users
who only test — but the fixture/marker names will stay identical.

Do not add `pytest_plugins = ["modulith.testing"]` to a conftest.py: pytest
would try to register this already-registered module a second time, under
a different name, and abort the whole session. To disable the plugin, use
`-p no:modulith` (the entry-point name), not the module path.

Three primary fixtures:

  modulith_app    — fresh runtime per test, in-memory bus, no I/O
  modulith_module — module-isolated tests with mocked dependencies
  scenario        — fluent API for event-driven flow tests

Plus markers:

  @pytest.mark.modulith_isolated — run in a subprocess for true isolation
  @pytest.mark.modulith_no_outbox — disable outbox for this test
"""

from __future__ import annotations as _annotations

import asyncio
import contextlib
import dataclasses
import importlib
import inspect
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
import typing
import unittest.mock

import pytest

from . import manifest, markers

if typing.TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from concurrent.futures import Future
    from typing import Any

# ---------------------------------------------------------------------------
# Fixture: modulith_app — fresh runtime per test
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ModulithTestApp:
    """Test handle exposing the captured runtime state.

    Tests use this to assert on what was published, who listened,
    what the configuration was. Reset between tests automatically.
    """

    published_events: list[Any] = dataclasses.field(default_factory=list)
    listener_calls: list[tuple[str, Any]] = dataclasses.field(default_factory=list)

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

    @markers.hookimpl
    def modulith_after_event_published(self, event: Any) -> None:
        self._app.published_events.append(event)

    @markers.hookimpl
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


@contextlib.contextmanager
def _module_isolation(
    target_module: str,
    *,
    mock_modules: list[str] | None = None,
) -> Iterator[None]:
    """Context manager: only ``target_module`` is loaded; others are mocked.

    Within the block, every module under the application package (the top
    segment of ``target_module``) except its ancestors is removed from
    ``sys.modules`` — including ``target_module`` and its submodules. Each
    name in ``mock_modules`` is then replaced with a ``MagicMock`` so
    importers get a stand-in. On exit the original ``sys.modules`` is
    restored exactly.

    The target must be dropped and re-imported too, not just its siblings:
    if it was already imported before entering this block, the cached module
    would otherwise sit there untouched, its names resolved via ``from
    <sibling> import x`` snapshotted against the REAL sibling at that
    earlier import time — silently defeating the mock. Re-importing it here
    (after the mocks are installed) re-resolves those names fresh, so it is
    already correctly bound by the time the caller's test body runs.

    Manifests declared by the removed modules live in a separate registry
    (``modulith.manifest``), not ``sys.modules`` — deleting a module doesn't
    clear its manifest entry. Without also resetting that registry, the
    re-import re-runs ``declare_module()`` for an already-declared package
    and hits its "already declared" guard. So manifest entries under the
    application package are snapshotted and cleared here too, and restored
    alongside ``sys.modules`` on exit.
    """
    mocks = mock_modules or []
    app_package = target_module.split(".")[0]
    ancestors = {
        ".".join(target_module.split(".")[:i]) for i in range(1, target_module.count(".") + 1)
    }
    snapshot = dict(sys.modules)
    manifest_snapshot = dict(manifest._manifests)

    for name in list(sys.modules):
        if not (name == app_package or name.startswith(app_package + ".")):
            continue
        if name in ancestors:
            continue
        del sys.modules[name]

    for package in list(manifest._manifests):
        if package == app_package or package.startswith(app_package + "."):
            del manifest._manifests[package]

    for name in mocks:
        sys.modules[name] = unittest.mock.MagicMock(name=name)

    importlib.import_module(target_module)

    try:
        yield
    finally:
        for name in set(sys.modules) - set(snapshot):
            del sys.modules[name]
        for name, module in snapshot.items():
            sys.modules[name] = module
        manifest._manifests.clear()
        manifest._manifests.update(manifest_snapshot)


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
        if not math.isfinite(seconds) or seconds < 0:
            # A NaN budget makes every `>=` comparison against the deadline
            # False, so the poll loop's break condition never fires — the
            # test hangs forever instead of failing. +/-inf and negative
            # budgets are equally nonsensical. Reject before firing anything.
            raise ValueError(f"seconds must be a finite, non-negative number, got {seconds!r}")

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

        ONLY the scenario's own budget overrun is swallowed:
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
        result_box: dict[str, Any] = {}

        def _invoke() -> None:
            try:
                result_box["result"] = self._initial_call(  # type: ignore[misc]
                    *self._initial_call_args, **self._initial_call_kwargs
                )
            except BaseException as exc:  # re-raised on the calling thread below
                result_box["error"] = exc

        # A plain synchronous trigger runs on a background thread so it is
        # bounded by the shared budget like every other trigger kind — called
        # directly on this thread, a stalled trigger hung within() forever.
        # Python threads can't be force-killed, so an overrun is swallowed
        # (best-effort, matching the coroutine trigger's cancellation) and the
        # thread is left to finish in the background as a daemon.
        thread = threading.Thread(target=_invoke, daemon=True)
        thread.start()
        thread.join(timeout=max(0.0, deadline - time.monotonic()))

        if thread.is_alive():
            return
        if "error" in result_box:
            raise result_box["error"]
        result = result_box.get("result")
        if inspect.iscoroutine(result):
            self._await_call_trigger(result, deadline)

    @staticmethod
    def _await_call_trigger(coro: Any, deadline: float) -> None:
        """Block on a coroutine trigger, swallowing ONLY the budget overrun.

        A budget overrun cancels the trigger so it cannot outlive this test
        on the shared daemon loop and dispatch into a later test's runtime.
        A TimeoutError raised by the coroutine itself is an application
        failure and propagates.
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
    # Popped so a pytest the isolated test itself launches cannot append to
    # the parent's result file.
    result_path = os.environ.pop(_RESULT_FILE_ENV, None)
    if result_path and os.environ.get(_ISOLATION_GUARD) == "1":
        config.pluginmanager.register(_IsolatedResultWriter(result_path))
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
# Path of a private JSON-lines file the child appends its test reports to. A
# file of its own, because a user's --junitxml is forwarded to the child.
_RESULT_FILE_ENV = "MODULITH_ISOLATED_RESULT_FILE"


class _IsolatedResultWriter:
    """Child-side plugin: record each report's outcome for the parent."""

    def __init__(self, path: str) -> None:
        self._path = path

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        longrepr = report.longrepr
        record = {
            "when": report.when,
            "outcome": report.outcome,
            # A skip's longrepr is a (path, lineno, reason) tuple that the
            # terminal and junitxml reporters unpack; anything else is text.
            "longrepr": list(longrepr)
            if isinstance(longrepr, tuple)
            else (None if longrepr is None else str(longrepr)),
            "wasxfail": getattr(report, "wasxfail", None),
        }
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")


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
    verbosity — must carry over; dropping them silently reverted isolated
    tests to option defaults. ``--cov*`` options are forwarded verbatim too
    but neutralized by the trailing ``--no-cov`` the caller appends: they
    cannot be filtered out here because ``--cov`` takes an optional value
    (``--cov myapp`` is two tokens), so dropping the flag alone would leave a
    stray positional in the child. Positional targets are
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
    synthesize a report from the child's exit code and the outcome it
    records to a private result file, so a skip or xfail stays one and a
    child that ran no test fails. Returning ``None`` for
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
    # The child measures one nodeid, so an inherited --cov-fail-under would
    # always trip and fail the isolated test for reasons unrelated to it.
    # Only pass --no-cov when pytest-cov actually armed itself for this run:
    # the option does not exist otherwise and pytest would exit the child 4.
    if item.config.pluginmanager.hasplugin("_cov"):
        argv.append("--no-cov")

    def _outcome() -> dict[str, Any] | None:
        """Run the child; return its skip/xfail/xpass record, or None for a pass."""
        raw_timeout = item.config.getini("modulith_isolated_timeout")
        try:
            timeout = float(raw_timeout)
        except (TypeError, ValueError):
            raise AssertionError(
                f"invalid modulith_isolated_timeout ini value {raw_timeout!r}: "
                "expected a number of seconds"
            ) from None
        fd, result_path = tempfile.mkstemp(prefix="modulith-isolated-", suffix=".jsonl")
        os.close(fd)
        env[_RESULT_FILE_ENV] = result_path
        try:
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
            with open(result_path, encoding="utf-8") as fh:
                records = [json.loads(line) for line in fh if line.strip()]
        finally:
            os.unlink(result_path)
        if not records:
            raise AssertionError(
                f"isolated subprocess for {item.nodeid} exited 0 without running "
                "the test (a child-side plugin or option deselected or "
                "suppressed it)\n"
                f"--- stdout ---\n{completed.stdout}\n"
                f"--- stderr ---\n{completed.stderr}"
            )
        return next(
            (r for r in records if r["outcome"] != "passed" or r["wasxfail"] is not None),
            None,
        )

    call = CallInfo.from_call(_outcome, when="call")
    report = ihook.pytest_runtest_makereport(item=item, call=call)
    # skip/xfail marks are evaluated in the child only, so its record decides
    # the outcome; the parent-side report alone would read every one as passed.
    child = call.result if call.excinfo is None else None
    if child is not None:
        report.outcome = child["outcome"]
        longrepr = child["longrepr"]
        report.longrepr = tuple(longrepr) if isinstance(longrepr, list) else longrepr
        if child["wasxfail"] is not None:
            report.wasxfail = child["wasxfail"]
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
