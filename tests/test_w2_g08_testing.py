"""W2 G08 regression tests for the modulith pytest plugin (``modulith.testing``).

Covers audit findings on ``Scenario.within()``'s timeout contract and on the
``@pytest.mark.modulith_isolated`` subprocess re-invocation.

Note: like tests/test_testing_plugin.py, this file deliberately does *not*
use ``from __future__ import annotations`` so that ``@listener`` sees real
annotation objects (module-level event classes).
"""

import asyncio
import time
from dataclasses import dataclass

import pytest

from modulith import event, listener
from modulith.decorators import configure
from modulith.testing import ModulithTestApp, Scenario

pytest_plugins = "pytester"


# Module-level event types so @listener annotations resolve to real classes.
@event
@dataclass(frozen=True)
class SlowTrigger:
    n: int


@event
@dataclass(frozen=True)
class NeverSeen:
    n: int


def _isolate() -> None:
    """Configure a clean, discovery-free runtime for scenario tests."""
    configure(package="modulith_w2g08_metatest", auto_discover=False)


def _roundtrip(loop: asyncio.AbstractEventLoop) -> None:
    """Deterministic barrier: one full iteration of the shared sync loop.

    Blocks until the loop has processed everything scheduled before this
    call (call_soon_threadsafe callbacks run FIFO), so tests can sequence
    'the loop saw X' without wall-clock sleeps.
    """
    asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(timeout=5)


# ---------------------------------------------------------------------------
# Scenario.within() timeout contract
# ---------------------------------------------------------------------------


def test_within_call_coroutine_timeout_raises_assertion_error():
    """A11-r2-98: a ``.call()`` coroutine trigger that overruns the budget
    must surface as the documented AssertionError, not as a bare
    concurrent.futures.TimeoutError escaping ``within()``."""
    from modulith.sync import _get_or_create_loop

    loop = _get_or_create_loop()
    blocker = asyncio.Event()

    async def stalls_forever() -> None:
        await blocker.wait()

    scenario = Scenario(ModulithTestApp())
    try:
        with pytest.raises(AssertionError, match="not seen within"):
            scenario.call(stalls_forever).expect_event(NeverSeen).within(seconds=0.1)
    finally:
        loop.call_soon_threadsafe(blocker.set)


def test_within_timeout_cancels_call_coroutine_trigger():
    """A11-r5-221: a ``.call()`` coroutine trigger that times out must be
    cancelled — not left running on the shared daemon-loop, where it can
    resume later and dispatch into a subsequent test's runtime."""
    from modulith.sync import _get_or_create_loop

    loop = _get_or_create_loop()
    blocker = asyncio.Event()
    leaked: list[str] = []

    async def stalls_then_records() -> None:
        await blocker.wait()
        leaked.append("orphan ran to completion")

    scenario = Scenario(ModulithTestApp())
    with pytest.raises(AssertionError):
        scenario.call(stalls_then_records).expect_event(NeverSeen).within(seconds=0.1)

    # Give the loop two full iterations to deliver the cancellation that
    # within() requested, THEN release the blocker: a cancelled task can
    # never reach the append, an orphaned one records the leak.
    _roundtrip(loop)
    _roundtrip(loop)
    loop.call_soon_threadsafe(blocker.set)
    _roundtrip(loop)
    _roundtrip(loop)
    assert leaked == []


def test_within_call_coroutine_shares_single_time_budget():
    """A11-r2-98 (budget doubling): the trigger phase and the poll phase
    share ONE ``seconds`` budget; previously each got its own full window,
    silently waiting ~2x the requested time."""

    async def slow_trigger() -> None:
        await asyncio.sleep(0.8)

    scenario = Scenario(ModulithTestApp())
    start = time.monotonic()
    with pytest.raises(AssertionError):
        scenario.call(slow_trigger).expect_event(NeverSeen).within(seconds=1.0)
    elapsed = time.monotonic() - start
    assert elapsed < 1.5, (
        f"within(seconds=1.0) took {elapsed:.2f}s — trigger and poll phases "
        "each consumed their own full budget window"
    )


def test_within_publish_trigger_respects_seconds_budget(scenario):
    """A11-r3-150: a ``.publish()`` trigger must be bounded by the caller's
    ``within(seconds=...)`` budget, not by publish_sync's own hardcoded 30s
    default — and the AssertionError must not overstate how long was waited."""
    _isolate()

    @listener
    async def stall(evt: SlowTrigger) -> None:
        await asyncio.sleep(5)

    start = time.monotonic()
    with pytest.raises(AssertionError, match="not seen within"):
        scenario.publish(SlowTrigger(n=1)).expect_event(NeverSeen).within(seconds=0.3)
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, (
        f"within(seconds=0.3) blocked for {elapsed:.2f}s — the trigger's "
        "publish_sync call was not bounded by the within() budget"
    )


# ---------------------------------------------------------------------------
# @pytest.mark.modulith_isolated — subprocess re-invocation
# ---------------------------------------------------------------------------


def test_isolated_subprocess_timeout_fails_test_not_suite(pytester):
    """A11-r1-37: the isolated-test subprocess has a bounded runtime.

    A hung isolated test must be killed after ``modulith_isolated_timeout``
    seconds and reported as a single failed test — not block the entire
    pytest run forever."""
    pytester.makeini(
        """
        [pytest]
        modulith_isolated_timeout = 1
        """
    )
    pytester.makepyfile(
        test_hang="""
        import time
        import pytest

        @pytest.mark.modulith_isolated
        def test_hangs_forever():
            time.sleep(20)
        """
    )
    start = time.monotonic()
    result = pytester.runpytest()
    elapsed = time.monotonic() - start
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*timed out after*"])
    assert elapsed < 15, f"outer run blocked {elapsed:.1f}s on a hung isolated test"


def test_isolated_subprocess_launch_failure_fails_only_that_test(pytester):
    """A11-r1-38: an exception from the subprocess launch itself (fork/exec
    failure) must fail just the marked test, not crash the whole session
    with an INTERNALERROR that prevents every other test from running."""
    pytester.makeconftest(
        """
        import subprocess

        def _boom(*args, **kwargs):
            raise OSError("simulated: fork/exec failed")

        subprocess.run = _boom
        """
    )
    pytester.makepyfile(
        test_launch_failure="""
        import pytest

        @pytest.mark.modulith_isolated
        def test_isolated_launch_fails():
            pass

        def test_after_should_still_run():
            pass
        """
    )
    # Subprocess-based inner run so the conftest's global subprocess.run
    # patch is confined to a throwaway interpreter.
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1, passed=1)
    result.stdout.no_fnmatch_line("*INTERNALERROR*")


def test_isolated_subprocess_forwards_cli_options(pytester):
    """A11-r3-151: the child re-invocation must forward the parent's CLI
    arguments (custom pytest_addoption flags, -m/-k filters, ...) instead of
    silently reverting isolated tests to option defaults."""
    pytester.makeconftest(
        """
        import pytest

        def pytest_addoption(parser):
            parser.addoption("--target-env", action="store", default="local")

        @pytest.fixture
        def target_env(request):
            return request.config.getoption("--target-env")
        """
    )
    pytester.makepyfile(
        test_cli_opt="""
        import pytest

        @pytest.mark.modulith_isolated
        def test_isolated_sees_cli_option(target_env):
            assert target_env == "staging"
        """
    )
    result = pytester.runpytest("--target-env=staging")
    result.assert_outcomes(passed=1)


def test_isolated_subprocess_runs_from_rootdir_not_parent_cwd(pytester):
    """A11-r1-39: the child must resolve ``item.nodeid`` against pytest's
    rootdir, not the parent process's incidental cwd — otherwise any run
    where cwd != rootdir fails with a misleading 'file not found'."""
    proj = pytester.mkdir("proj")
    proj.joinpath("pytest.ini").write_text("[pytest]\n")
    proj.joinpath("test_cwd_iso.py").write_text(
        "import os\n"
        "import pytest\n"
        "\n"
        "@pytest.mark.modulith_isolated\n"
        "def test_runs_isolated():\n"
        "    assert os.environ.get('MODULITH_ISOLATED_SUBPROCESS') == '1'\n"
    )
    # cwd stays at pytester.path; rootdir resolves to proj/ via its pytest.ini.
    result = pytester.runpytest("proj/test_cwd_iso.py")
    result.assert_outcomes(passed=1)
