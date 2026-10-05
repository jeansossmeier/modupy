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

pytest_plugins = "pytester"


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


_APP_PURGE_TESTS = """
import importlib
import sys

from modulith.decorators import configure
from modulith.runtime import _runtime

BOOTSTRAP = {bootstrap}


def test_first_imports_a_library_and_an_app_module(modulith_app):
    configure(package="purgeapp", auto_discover=False)
    if BOOTSTRAP:
        _runtime.ensure_bootstrapped()
    importlib.import_module("purgelib")
    importlib.import_module("purgeapp.orders")


def test_second_sees_only_the_app_module_purged():
    assert "purgelib" in sys.modules
    assert "purgeapp.orders" not in sys.modules
"""


@pytest.mark.parametrize("bootstrap", [False, True], ids=["configured", "bootstrapped"])
def test_modulith_app_purges_only_the_applications_modules(pytester, bootstrap: bool) -> None:
    """Dropping third-party modules between tests breaks libraries that cannot
    be re-imported (SQLAlchemy's compiled extensions) and strands hooks bound
    to the discarded copy, so teardown removes only the application package's
    modules — whether the package came from ``configure`` or from bootstrap."""
    pytester.makepyfile(purgelib="VALUE = 1\n")
    pytester.mkpydir("purgeapp")
    pytester.mkpydir("purgeapp/orders")
    pytester.makepyfile(test_purge=_APP_PURGE_TESTS.format(bootstrap=bootstrap))

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")

    result.assert_outcomes(passed=2)


_UNBOOTSTRAPPED_PURGE_TESTS = """
import importlib
import sys


def test_first_imports_an_app_module_without_bootstrapping(modulith_app):
    importlib.import_module("purgeapp.orders")


def test_second_sees_the_app_module_purged():
    assert "purgeapp.orders" not in sys.modules
"""


@pytest.mark.parametrize(
    "pyproject",
    ['[tool.modulith]\npackage = "purgeapp"\n', '[project]\nname = "purgeapp"\nversion = "0"\n'],
    ids=["tool-modulith-package", "project-name"],
)
def test_modulith_app_purges_the_project_package_when_the_test_never_bootstrapped(
    pytester, pyproject: str
) -> None:
    """Application modules a test imported without bootstrapping would otherwise
    enter every later test's snapshot and survive its purge, while the modules
    importing them are dropped and re-imported: tables defined again on a
    surviving SQLAlchemy ``MetaData`` then fail with "already defined"."""
    pytester.makepyprojecttoml(pyproject)
    pytester.mkpydir("purgeapp")
    pytester.mkpydir("purgeapp/orders")
    pytester.makepyfile(test_purge=_UNBOOTSTRAPPED_PURGE_TESTS)

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")

    result.assert_outcomes(passed=2)


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


def test_modulith_module_mock_survives_auto_discovering_bootstrap(
    make_fake_app, modulith_module
) -> None:
    """Bootstrap's auto-discovery walks the app package on disk and imports
    every sibling it finds. A sibling named in ``mock_modules`` must stay the
    mock for the whole block — whether it is reached through ``sys.modules``,
    ``from fakeapp import inventory`` or the target's own import."""
    import importlib
    import sys
    from unittest.mock import MagicMock

    from modulith.decorators import configure
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "orders": "from fakeapp.inventory import VALUE\nfrom fakeapp import inventory\n",
            "inventory": "VALUE = 'real-inv'",
        }
    )
    importlib.import_module("fakeapp.orders")  # real inventory is imported and attached to fakeapp
    real_inventory = sys.modules["fakeapp.inventory"]

    with modulith_module("fakeapp.orders", mock_modules=["fakeapp.inventory"]):
        configure(package="fakeapp")
        _runtime.ensure_bootstrapped()

        assert isinstance(sys.modules["fakeapp.inventory"], MagicMock)
        orders = sys.modules["fakeapp.orders"]
        assert isinstance(orders.VALUE, MagicMock)
        assert isinstance(orders.inventory, MagicMock)
        assert isinstance(importlib.import_module("fakeapp").inventory, MagicMock)

    assert importlib.import_module("fakeapp").inventory is real_inventory


def test_modulith_module_reenters_when_target_already_declared_a_manifest(
    make_fake_app, modulith_module
) -> None:
    """CHANGELOG.md advertises modulith_module as clearing manifests between
    uses. A target module whose manifest was already declared before entering
    isolation — an earlier import in the same process, or a second use of the
    fixture for the same target across two tests — must reimport cleanly
    instead of hitting declare_module's 'already declared' guard."""
    import importlib

    from modulith.manifest import get_manifest

    make_fake_app(
        {"orders": "from . import _manifest\n"},
        extra_files={
            "orders/_manifest.py": (
                "from modulith.manifest import declare_module\ndeclare_module()\n"
            ),
        },
    )
    importlib.import_module("fakeapp.orders")
    original_manifest = get_manifest("fakeapp.orders")
    assert original_manifest is not None

    with modulith_module("fakeapp.orders"):
        pass

    with modulith_module("fakeapp.orders"):
        pass

    # Restored exactly, like sys.modules — the pre-block manifest survives.
    assert get_manifest("fakeapp.orders") is original_manifest


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


_ISOLATED_OUTCOMES = """
import pytest

@pytest.mark.modulith_isolated
def test_passes():
    pass

@pytest.mark.modulith_isolated
@pytest.mark.skip(reason="declared skip")
def test_marked_skip():
    raise AssertionError("body must not run")

@pytest.mark.modulith_isolated
@pytest.mark.skipif(True, reason="platform gate")
def test_skipif():
    raise AssertionError("body must not run")

@pytest.mark.modulith_isolated
def test_imperative_skip():
    pytest.skip("optional dependency missing")

@pytest.mark.modulith_isolated
@pytest.mark.xfail(reason="known bug")
def test_xfail():
    raise AssertionError("known failure")

@pytest.mark.modulith_isolated
@pytest.mark.xfail(reason="fixed upstream")
def test_xpass():
    pass

@pytest.mark.modulith_isolated
@pytest.mark.xfail(reason="must stay broken", strict=True)
def test_strict_xpass():
    pass
"""


def test_isolated_test_reports_the_childs_skip_and_xfail_outcomes(pytester) -> None:
    """The isolated body runs in the child, so the parent never evaluates
    skip/xfail marks. A child that skipped or xfailed exits 0; reading only
    the exit code reported those tests as PASSED although their bodies never
    ran (or failed as expected)."""
    pytester.makepyfile(test_outcomes=_ISOLATED_OUTCOMES)

    result = pytester.runpytest("-rsxX")

    result.assert_outcomes(passed=1, skipped=3, xfailed=1, xpassed=1, failed=1)
    result.stdout.fnmatch_lines_random(
        [
            "SKIPPED*test_outcomes.py:*: declared skip",
            "SKIPPED*test_outcomes.py:*: platform gate",
            "SKIPPED*test_outcomes.py:*: optional dependency missing",
            "XFAIL*test_xfail*known bug",
            "XPASS*test_xpass*fixed upstream",
            "*XPASS(strict)*must stay broken*",
        ]
    )


def test_isolated_child_that_runs_no_test_fails_the_parent(pytester) -> None:
    """A plugin can end the child with exit 0 without running the test (for
    example one that suppresses pytest's no-tests-ran exit code). The parent
    must fail the test rather than read the clean exit as a pass."""
    pytester.makeconftest(
        """
        import os

        def pytest_collection_modifyitems(config, items):
            if os.environ.get("MODULITH_ISOLATED_SUBPROCESS") == "1":
                config.hook.pytest_deselected(items=list(items))
                items[:] = []

        def pytest_sessionfinish(session):
            if os.environ.get("MODULITH_ISOLATED_SUBPROCESS") == "1":
                session.exitstatus = 0
        """
    )
    pytester.makepyfile(
        test_no_run="""
        import pytest

        @pytest.mark.modulith_isolated
        def test_isolated():
            pass
        """
    )

    result = pytester.runpytest()

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        ["*isolated subprocess for test_no_run.py::test_isolated exited 0 without running*"]
    )


def test_isolated_child_that_exits_0_before_finishing_the_test_fails_the_parent(
    pytester,
) -> None:
    """A body that ends the child with exit 0 (``os._exit(0)`` in a worker
    entrypoint, ``pytest.exit(returncode=0)``) leaves only the setup record
    behind. The body never finished, so the parent must not report a pass;
    a setup-phase skip or xfail still leaves no call record and keeps its
    outcome."""
    pytester.makepyfile(
        test_early_exit="""
        import os
        import pytest

        @pytest.mark.modulith_isolated
        def test_os_exit():
            os._exit(0)

        @pytest.mark.modulith_isolated
        def test_pytest_exit():
            pytest.exit("stop here", returncode=0)

        @pytest.mark.modulith_isolated
        @pytest.mark.skip(reason="declared skip")
        def test_setup_skip():
            raise AssertionError("body must not run")

        @pytest.mark.modulith_isolated
        @pytest.mark.xfail(run=False, reason="never run")
        def test_setup_xfail():
            raise AssertionError("body must not run")
        """
    )

    result = pytester.runpytest("-rsx")

    result.assert_outcomes(failed=2, skipped=1, xfailed=1)
    result.stdout.fnmatch_lines_random(
        [
            "*isolated subprocess for test_early_exit.py::test_os_exit exited 0 "
            "before the test finished*",
            "*isolated subprocess for test_early_exit.py::test_pytest_exit exited 0 "
            "before the test finished*",
            "SKIPPED*test_early_exit.py:*: declared skip",
            "XFAIL*test_setup_xfail*never run",
        ]
    )


@pytest.mark.parametrize(
    ("ini_addopts", "cli_args", "autoload_env"),
    [
        pytest.param("-p modulith.testing", [], True, id="ini-module-path"),
        pytest.param("-p modulith", [], True, id="ini-entry-point-name"),
        pytest.param("", ["-p", "modulith.testing"], True, id="cli-module-path"),
        pytest.param(
            "--disable-plugin-autoload -p modulith.testing", [], False, id="ini-autoload-flag"
        ),
    ],
)
def test_isolated_tests_run_when_the_plugin_is_loaded_explicitly(
    pytester, monkeypatch, ini_addopts: str, cli_args: list[str], autoload_env: bool
) -> None:
    """A project that disables plugin autoload loads this plugin with ``-p``.
    The child clears ini ``addopts``, so it must still receive the plugin (and
    not a second copy under another name); otherwise it never records an
    outcome and every isolated test fails."""
    from pathlib import Path

    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]))
    if autoload_env:
        monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    else:
        monkeypatch.delenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", raising=False)
    pytester.makeini(f"[pytest]\naddopts = {ini_addopts}\n")
    pytester.makepyfile(
        test_explicit="""
        import os
        import pytest

        @pytest.mark.modulith_isolated
        def test_runs_isolated():
            assert os.environ.get("MODULITH_ISOLATED_SUBPROCESS") == "1"

        @pytest.mark.modulith_isolated
        def test_skips_isolated():
            pytest.skip("child outcome")
        """
    )

    result = pytester.runpytest(*cli_args, "-rs")

    result.assert_outcomes(passed=1, skipped=1)
    result.stdout.fnmatch_lines(["SKIPPED*test_explicit.py:*: child outcome"])


def test_isolated_outcomes_survive_a_user_junitxml(pytester) -> None:
    """The child's outcome travels on a channel of its own, not on a junit
    report; the user's report must still record the isolated skip and xfail."""
    import xml.etree.ElementTree as ET

    pytester.makepyfile(
        test_junit="""
        import pytest

        @pytest.mark.modulith_isolated
        def test_passes():
            pass

        @pytest.mark.modulith_isolated
        def test_imperative_skip():
            pytest.skip("optional dependency missing")

        @pytest.mark.modulith_isolated
        @pytest.mark.xfail(reason="known bug")
        def test_xfail():
            raise AssertionError("known failure")
        """
    )
    xml_path = pytester.path / "report.xml"

    result = pytester.runpytest(f"--junitxml={xml_path}")

    result.assert_outcomes(passed=1, skipped=1, xfailed=1)
    cases = {case.get("name"): case for case in ET.parse(xml_path).getroot().iter("testcase")}
    skips = {
        name: skipped.get("message")
        for name, case in cases.items()
        if (skipped := case.find("skipped")) is not None
    }
    assert skips == {
        "test_imperative_skip": "optional dependency missing",
        "test_xfail": "known bug",
    }
    assert set(cases) == {"test_passes", "test_imperative_skip", "test_xfail"}


@pytest.mark.parametrize(
    "junit_args",
    [
        pytest.param(["--junitxml=report.xml"], id="junitxml-equals"),
        pytest.param(["--junit-xml=report.xml"], id="junit-xml-equals"),
        pytest.param(["--junitxml", "report.xml"], id="junitxml-separate"),
    ],
)
def test_isolated_child_writes_no_junit_report_under_the_rootdir(
    pytester, monkeypatch, junit_args: list[str]
) -> None:
    """The child runs from the rootdir, so a relative --junitxml forwarded to it
    would leave a stray report there while the parent writes its own relative to
    the directory pytest was started in."""
    import xml.etree.ElementTree as ET

    pytester.makeini("[pytest]\n")
    pytester.makepyfile(
        test_junit_dir="""
        import pytest

        @pytest.mark.modulith_isolated
        def test_isolated():
            pass
        """
    )
    sub = pytester.mkdir("sub")
    monkeypatch.chdir(sub)

    result = pytester.runpytest("../test_junit_dir.py", *junit_args)

    result.assert_outcomes(passed=1)
    assert not (pytester.path / "report.xml").exists()
    cases = ET.parse(sub / "report.xml").getroot().iter("testcase")
    assert [case.get("name") for case in cases] == ["test_isolated"]


def test_isolated_result_survives_a_test_that_patches_builtins_open(pytester) -> None:
    """The child writes its result while the test's own patches are still
    active, so a test replacing ``builtins.open`` must not break that write."""
    pytester.makepyfile(
        test_patched_open="""
        import builtins
        import pytest

        @pytest.mark.modulith_isolated
        def test_patches_open(monkeypatch):
            def boom(*args, **kwargs):
                raise OSError("open is patched")

            monkeypatch.setattr(builtins, "open", boom)
        """
    )

    result = pytester.runpytest()

    result.assert_outcomes(passed=1)


_FLAKY_ISOLATED = """
import pytest

attempts = []

@pytest.mark.modulith_isolated
def test_passes_on_second_attempt():
    attempts.append(1)
    assert len(attempts) == 2

@pytest.mark.modulith_isolated
def test_passes_first_time():
    pass
"""


@pytest.mark.parametrize(
    ("cli_args", "expected"),
    [
        pytest.param([], {"passed": 1, "failed": 1}, id="no-reruns"),
        pytest.param(["--reruns", "2"], {"passed": 2}, id="reruns"),
    ],
)
def test_isolated_tests_report_their_final_outcome_under_pytest_rerunfailures(
    pytester, cli_args: list[str], expected: dict[str, int]
) -> None:
    """pytest-rerunfailures 14+ builds its per-attempt state on the item from
    the setup report, so a call report arriving first crashed the whole session
    for any isolated test, with or without ``--reruns``. The child retries on
    its own (the parent's ``--reruns`` is forwarded), and the intermediate
    ``rerun`` record it writes must not become the parent's outcome."""
    pytest.importorskip("pytest_rerunfailures")
    pytester.makepyfile(test_flaky=_FLAKY_ISOLATED)

    result = pytester.runpytest(*cli_args)

    assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
    assert result.parseoutcomes() == expected


def test_isolated_test_reports_a_teardown_error_of_the_scope_it_ends(pytester) -> None:
    """The isolated item is the last user of a module-scoped fixture, so the
    parent tears that scope down on its behalf. A failing finalizer is a
    teardown error of that item, as for an inline test, not a crash of the
    session."""
    pytester.makepyfile(
        test_scope_teardown="""
        import pytest

        @pytest.fixture(scope="module")
        def shared():
            yield
            raise RuntimeError("module finalizer failed")

        def test_uses_shared(shared):
            pass

        @pytest.mark.modulith_isolated
        def test_ends_the_module():
            pass
        """
    )

    result = pytester.runpytest()

    result.assert_outcomes(passed=2, errors=1)
    result.stdout.fnmatch_lines(["*ERROR at teardown of test_ends_the_module*"])


@pytest.mark.modulith_no_outbox
def test_no_outbox_marker_disables_outbox_configuration() -> None:
    from modulith.builtin import outbox

    outbox.configure(store=object(), serializer=object())

    assert outbox._store is None


# ---------------------------------------------------------------------------
# ModulithTestApp.reset() — public API
# ---------------------------------------------------------------------------


def test_no_public_attribute_escapes_testing_declared_api() -> None:
    """Mirrors modulith's own leak guard
    (test_bootstrap_public_api.py::test_no_public_attribute_escapes_the_declared_api)
    for modulith.testing: docs/STABILITY.md gives it the identical stability
    promise, so a stray unaliased import (MagicMock, dataclass, ...) must not
    show up in dir(modulith.testing)/editor completion. pytest_* names are
    exempt: pytest discovers hook implementations by that literal name, so
    they can't be aliased or added to __all__ without breaking hook
    registration, and docs/STABILITY.md documents modulith.testing's surface
    as its fixtures and classes only, not its hook-function plumbing."""
    import types as _types

    import modulith.testing

    leaked = sorted(
        name
        for name, value in vars(modulith.testing).items()
        if not name.startswith("_")
        and not name.startswith("pytest_")
        and not isinstance(value, _types.ModuleType)
        and name not in modulith.testing.__all__
    )

    assert leaked == []


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
