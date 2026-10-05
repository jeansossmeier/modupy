"""Guard tests for CI/packaging configuration drift.

Two classes of drift are locked down here:

- Integration coverage: CI must run the integration suite in a dedicated
  lane (installing the ``integration`` extra, which carries testcontainers
  and the real Postgres drivers), the default test job must deselect
  ``-m integration`` (those tests silently skip without Docker drivers,
  giving false green), and mypy in CI must type-check ``tests/`` too.
- Tool version constraints: the standalone tool installs in ci.yml (ruff,
  mypy, ...) must mirror pyproject.toml exactly, so CI can never resolve a
  tool major that local development forbids.

They parse the *actual* files in the repository so any future drift
between ``.github/workflows/ci.yml`` and ``pyproject.toml`` fails fast in
the default test lane instead of surfacing as a broken CI run months later.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from packaging.requirements import Requirement

REPO_ROOT = Path(__file__).resolve().parent.parent
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
RELEASE_YML = REPO_ROOT / ".github" / "workflows" / "release.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
ISSUE_TEMPLATE_CONFIG = REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "config.yml"


def _ci_text() -> str:
    return CI_YML.read_text(encoding="utf-8")


def _release_text() -> str:
    return RELEASE_YML.read_text(encoding="utf-8")


def _pyproject() -> dict[str, object]:
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)


def _optional_dependencies() -> dict[str, list[str]]:
    project = _pyproject()["project"]
    assert isinstance(project, dict)
    extras = project["optional-dependencies"]
    assert isinstance(extras, dict)
    return {name: list(reqs) for name, reqs in extras.items()}


def test_default_pytest_job_deselects_integration() -> None:
    """The default test job must not rely on silent auto-skip.

    Without ``-m "not integration"`` the integration tests are collected in
    the default job, where the drivers (testcontainers/asyncpg/psycopg) are
    absent, and skip silently — a permanently-green lane that never proves
    anything about the e2e suite. Deselection must be explicit.
    """
    ci = _ci_text()
    assert re.search(r'pytest\s+-m\s+"not integration"', ci), (
        "default CI pytest job must deselect the integration marker "
        'explicitly (pytest -m "not integration" ...)'
    )


def test_integration_job_runs_marked_suite() -> None:
    """CI must actually execute the integration suite.

    The lane must install the ``integration`` extra (the only extra that
    carries testcontainers + real Postgres drivers) and run
    ``pytest -m integration``.
    """
    ci = _ci_text()
    assert re.search(r"""uv pip install --system -e ["']?\.\[integration\]["']?""", ci), (
        "CI must have a job installing the `.[integration]` extra"
    )
    assert re.search(r"pytest\s+-m\s+integration\b", ci), (
        "CI must have a job running `pytest -m integration`"
    )


def test_integration_extra_provides_testcontainers_drivers() -> None:
    """The extra CI installs must carry the real e2e drivers."""
    extras = _optional_dependencies()
    assert "integration" in extras
    names = {re.split(r"[><=!~\[]", req, maxsplit=1)[0] for req in extras["integration"]}
    assert "testcontainers" in names
    assert "asyncpg" in names
    assert "psycopg" in names


def test_test_extra_is_only_the_pytest_plugin_dependencies() -> None:
    """``modupy[test]`` is what users install for the pytest plugin, and
    ``modulith/testing.py`` imports only pytest and the standard library."""
    reqs = _optional_dependencies()["test"]
    assert {Requirement(req).name for req in reqs} == {"pytest", "pytest-asyncio"}, (
        f"test extra {reqs!r} must hold only pytest and pytest-asyncio"
    )


def test_test_suite_extra_layers_on_the_test_extra() -> None:
    """``test-suite`` carries what the default suite imports, on top of ``test``."""
    reqs = _optional_dependencies()["test-suite"]
    assert "modupy[test]" in reqs
    names = {Requirement(req).name for req in reqs}
    assert {"pytest-timeout", "build", "hatchling", "sqlalchemy", "alembic", "aiosqlite"} <= names
    assert {"pyyaml", "httpx2", "modupy"} <= names


def test_integration_extra_requires_the_suite_extra() -> None:
    """The integration lane runs the default suite's imports plus real drivers."""
    reqs = _optional_dependencies()["integration"]
    assert "modupy[test-suite]" in reqs
    assert "modupy[test]" not in reqs


def test_focused_shm_job_installs_the_suite_extra() -> None:
    """The focused SHM job runs tests that import the suite's dependencies."""
    installs = [
        step["run"]
        for step in _job_steps("shm")
        if isinstance(step.get("run"), str) and "uv pip install" in step["run"]
    ]
    assert len(installs) == 1, f"shm job must have one install step, found {installs!r}"
    assert '-e ".[test-suite]"' in installs[0]


def test_mypy_job_type_checks_tests() -> None:
    """mypy in CI must cover tests/, not just modulith/ — and it
    must run under --strict. The earlier form of this regex made ``--strict``
    optional, so CI silently dropping it would still have passed the guard."""
    ci = _ci_text()
    assert re.search(r"mypy\s+--strict\s+modulith/?\s+tests/?", ci), (
        "CI mypy invocation must run `mypy --strict` over both modulith/ and tests/"
    )


def _integration_job_steps() -> list[dict[str, Any]]:
    """The integration job's steps, located structurally via yaml.safe_load."""
    workflow = yaml.safe_load(_ci_text())
    assert isinstance(workflow, dict)
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict), "ci.yml must define jobs"
    integration = jobs.get("integration")
    assert isinstance(integration, dict), "ci.yml must define an `integration` job"
    steps = integration.get("steps")
    assert isinstance(steps, list) and steps, "integration job must define steps"
    for step in steps:
        assert isinstance(step, dict)
    return steps


def _run_guard_script(
    script: str, workdir: Path, pytest_output: str, pytest_exit: int = 0
) -> subprocess.CompletedProcess[str]:
    """Execute the integration job's guard script against a synthetic run.

    A fake ``pytest`` executable (first on PATH) emits ``pytest_output`` and
    exits ``pytest_exit``, so the guard's summary logic runs against a
    controlled outcome. The script is executed with ``bash -e -c`` because
    that mirrors how GitHub Actions runs a ``run:`` block without an explicit
    ``shell:`` key on Linux (default shell ``bash -e {0}``) — the guard's
    fail-on-pytest-failure behavior depends on those exact semantics.
    """
    bin_dir = workdir / "bin"
    bin_dir.mkdir(parents=True)
    fake_pytest = bin_dir / "pytest"
    fake_pytest.write_text(
        "#!/usr/bin/env bash\n"
        "cat <<'MODULITH_FAKE_PYTEST_OUTPUT'\n"
        f"{pytest_output}\n"
        "MODULITH_FAKE_PYTEST_OUTPUT\n"
        f"exit {pytest_exit}\n",
        encoding="utf-8",
    )
    fake_pytest.chmod(0o755)
    run_dir = workdir / "cwd"
    run_dir.mkdir()
    env = dict(os.environ, PATH=f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    return subprocess.run(
        ["bash", "-e", "-c", script],
        cwd=run_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_integration_job_cannot_go_green_without_docker(tmp_path: Path) -> None:
    """The integration lane must FAIL when Docker is unreachable.

    tests/conftest.py ``pytest.skip``s the entire integration suite when the
    Docker probe fails, and pytest exits 0 on all-skipped — so a runner-image
    Docker breakage would turn the lane permanently green while proving
    nothing (the exact rubber-stamp the unit-lane comment in the same file
    warns about). Two independent guards are required:

      * a ``docker info`` preflight step ordered before the pytest step, and
      * summary guard logic in the pytest step that fails the job on skipped
        tests, on no passed tests, and on a failing pytest exit.

    This test pins the guard's SEMANTICS, not its wording: it locates the
    steps structurally in the parsed workflow and EXECUTES the pytest step's
    script against synthetic pytest outcomes (a string-presence pin survived
    refactors that kept the words but broke the behavior).
    """
    steps = _integration_job_steps()
    run_steps = [
        (index, step["run"]) for index, step in enumerate(steps) if isinstance(step.get("run"), str)
    ]

    preflight_indexes = [
        index for index, run in run_steps if re.search(r"(?m)^\s*docker info\b", run)
    ]
    assert preflight_indexes, (
        "integration job must run a `docker info` preflight step so an "
        "unreachable Docker daemon fails the job instead of skipping the suite"
    )

    pytest_steps = [
        (index, run)
        for index, run in run_steps
        if re.search(r"(?m)^\s*pytest\b[^\n]*-m integration\b", run)
    ]
    assert len(pytest_steps) == 1, (
        "integration job must have exactly one step running `pytest -m integration`"
    )
    pytest_index, guard_script = pytest_steps[0]
    assert min(preflight_indexes) < pytest_index, (
        "the `docker info` preflight must run BEFORE the pytest step"
    )

    verbose_noise = "tests/test_postgres_integration.py::test_outbox_roundtrip PASSED"

    # A clean, fully-passed run must leave the job green.
    clean = _run_guard_script(
        guard_script,
        tmp_path / "clean",
        f"{verbose_noise}\n============ 42 passed in 12.34s ============",
    )
    assert clean.returncode == 0, (
        f"guard must exit 0 on an all-passed summary; got {clean.returncode}:\n"
        f"{clean.stdout}\n{clean.stderr}"
    )

    # ANY skipped test means the Docker/testcontainers path silently degraded
    # mid-run — the guard must fail the job even though pytest exited 0.
    with_skips = _run_guard_script(
        guard_script,
        tmp_path / "with_skips",
        f"{verbose_noise}\n======= 37 passed, 5 skipped in 10.42s =======",
    )
    assert with_skips.returncode != 0, (
        "guard must exit non-zero when the pytest summary reports skipped tests"
    )

    # The all-skipped rubber-stamp: pytest exits 0 having proven nothing.
    all_skipped = _run_guard_script(
        guard_script,
        tmp_path / "all_skipped",
        "============ 42 skipped in 0.51s ============",
    )
    assert all_skipped.returncode != 0, (
        "guard must exit non-zero when the summary reports no passed tests "
        "(an all-skip exits 0 and rubber-stamps the lane)"
    )

    # Real test failures must still fail the job: the summary reports passed
    # tests and no skips, so only pytest's own exit status (propagated through
    # the tee pipeline — pipefail semantics) can fail the guard here.
    failing = _run_guard_script(
        guard_script,
        tmp_path / "failing",
        f"{verbose_noise}\n======= 1 failed, 41 passed in 12.00s =======",
        pytest_exit=1,
    )
    assert failing.returncode != 0, (
        "guard must exit non-zero when pytest itself fails — dropping pipefail "
        "would let `pytest | tee` swallow the failure exit"
    )


def test_standalone_tool_pins_mirror_pyproject() -> None:
    """No constraint drift between ci.yml and pyproject.

    Every quoted requirement that ci.yml installs standalone (e.g.
    ``uv pip install --system "ruff>=0.4,<1.0"``) must appear verbatim in a
    pyproject.toml optional-dependency list. This is exactly the failure
    mode that let CI resolve mypy 2.0 while pyproject demanded <2.0: the
    duplicated spec drifted. Verbatim mirroring makes the drift a test
    failure in the default lane.
    """
    ci = _ci_text()
    standalone_pins = re.findall(r'uv pip install --system "([^"]+)"', ci)
    pyproject_reqs = {req for reqs in _optional_dependencies().values() for req in reqs}
    for pin in standalone_pins:
        assert pin in pyproject_reqs, (
            f"ci.yml installs {pin!r} but pyproject.toml declares no identical "
            "requirement — the two constraints have drifted"
        )


def test_version_fallback_literal_mirrors_pyproject() -> None:
    """``modulith/__init__.py`` carries a hardcoded ``__version__`` fallback for
    source checkouts with no distribution metadata. Nothing gated it: release.yml
    only compares the git tag against pyproject.toml, and the one test that reads
    ``__version__`` compares the imported value to itself. Read the literal out of
    the source (importing it yields the *installed* metadata, which cannot detect
    the drift) and pin it to pyproject.
    """
    source = (REPO_ROOT / "modulith" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'^\s*__version__ = "([^"]+)"', source, re.MULTILINE)
    assert match, "modulith/__init__.py must keep a literal __version__ fallback"

    project = _pyproject()["project"]
    assert isinstance(project, dict)
    assert match.group(1) == project["version"], (
        f"modulith/__init__.py falls back to {match.group(1)!r} but pyproject.toml "
        f"declares {project['version']!r} — bump both together"
    )


# ---------------------------------------------------------------------------
# Coverage gates, integration matrix, scripts/examples lint, wheel smoke,
# API-reference drift check
# ---------------------------------------------------------------------------


def _jobs(text: str | None = None) -> dict[str, Any]:
    workflow = yaml.safe_load(text if text is not None else _ci_text())
    assert isinstance(workflow, dict)
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    return jobs


def _job_steps(job_name: str, text: str | None = None) -> list[dict[str, Any]]:
    """Return one parsed job's steps so YAML comments cannot satisfy guards."""
    job = _jobs(text).get(job_name)
    assert isinstance(job, dict), f"ci.yml must define a `{job_name}` job"
    steps = job.get("steps")
    assert isinstance(steps, list) and steps, f"`{job_name}` job must define steps"
    assert all(isinstance(step, dict) for step in steps)
    return steps


def _shell_commands(script: str) -> list[list[str]]:
    """Tokenize executable shell lines while discarding shell comments."""
    commands: list[list[str]] = []
    for raw_line in script.replace("\\\n", " ").splitlines():
        tokens = shlex.split(raw_line.strip(), comments=True, posix=True)
        if tokens:
            commands.append(tokens)
    return commands


def _has_shell_command(script: str, expected_prefix: list[str]) -> bool:
    return any(
        command[: len(expected_prefix)] == expected_prefix for command in _shell_commands(script)
    )


def test_unit_matrix_collects_coverage_once_on_311() -> None:
    """The parsed test job must run both package coverage gates on Python 3.11."""
    test_job = _jobs().get("test")
    assert isinstance(test_job, dict), "ci.yml must define a `test` job"
    strategy = test_job.get("strategy")
    assert isinstance(strategy, dict), "test job must define a matrix strategy"
    matrix = strategy.get("matrix")
    assert isinstance(matrix, dict)
    assert matrix.get("python-version") == ["3.11", "3.12", "3.13", "3.14"], (
        "test matrix must cover exactly Python 3.11, 3.12, 3.13, and 3.14"
    )

    coverage_run = [
        step
        for step in _job_steps("test")
        if isinstance(step.get("run"), str)
        and _has_shell_command(
            step["run"],
            ["coverage", "run", "--source=modulith", "--branch", "-m", "pytest"],
        )
    ]
    assert len(coverage_run) == 1, (
        "test job must have exactly one executable "
        "`coverage run --source=modulith --branch -m pytest` command"
    )
    coverage_step = coverage_run[0]
    condition = coverage_step.get("if")
    assert isinstance(condition, str)
    assert condition.strip().replace('"', "'") == "matrix.python-version == '3.11'", (
        "coverage step must run only when matrix.python-version is 3.11"
    )
    script = coverage_step["run"]
    assert _has_shell_command(script, ["coverage", "report", "--fail-under=90"]), (
        "coverage step must execute the package-wide 90% report gate"
    )
    assert _has_shell_command(
        script,
        [
            "coverage",
            "report",
            "--include=modulith/builtin/outbox.py",
            "--fail-under=100",
        ],
    ), "coverage step must execute the 100% outbox report gate"


def test_shell_command_guards_ignore_comments() -> None:
    """Commented commands are documentation, not executable CI coverage gates."""
    commented = (
        "# coverage run --source=modulith --branch -m pytest\n# coverage report --fail-under=90\n"
    )
    assert not _has_shell_command(
        commented,
        ["coverage", "run", "--source=modulith", "--branch", "-m", "pytest"],
    )


def test_focused_shm_matrix_covers_supported_operating_systems() -> None:
    """The focused SHM lane must span every supported OS and Python boundary."""
    shm = _jobs().get("shm")
    assert isinstance(shm, dict), "ci.yml must define a focused `shm` job"
    assert shm.get("runs-on") == "${{ matrix.os }}"
    strategy = shm.get("strategy")
    assert isinstance(strategy, dict)
    matrix = strategy.get("matrix")
    assert isinstance(matrix, dict)
    assert matrix.get("os") == ["ubuntu-24.04", "macos-latest", "windows-latest"]
    assert matrix.get("python-version") == ["3.11", "3.13"]


def _runner_labels(text: str) -> set[str]:
    """Every runner label a workflow's jobs can run on, ``matrix.os`` expanded."""
    labels: set[str] = set()
    for job in _jobs(text).values():
        runs_on = job.get("runs-on")
        if runs_on == "${{ matrix.os }}":
            runs_on = job["strategy"]["matrix"]["os"]
        if runs_on is not None:
            labels.update([runs_on] if isinstance(runs_on, str) else runs_on)
    return labels


@pytest.mark.parametrize("workflow", [CI_YML, RELEASE_YML], ids=lambda path: path.name)
def test_linux_runners_are_pinned_to_a_ubuntu_release(workflow: Path) -> None:
    """``ubuntu-latest`` moves to Ubuntu 26 on 2026-10-19 (actions/runner-images#14748),
    so a job on that label changes OS image without a commit; a versioned label
    changes only when we do."""
    linux = {
        label
        for label in _runner_labels(workflow.read_text(encoding="utf-8"))
        if label.startswith("ubuntu")
    }
    assert linux, f"{workflow.name} must run at least one job on Linux"
    assert all(re.fullmatch(r"ubuntu-\d{2}\.\d{2}", label) for label in linux), (
        f"{workflow.name} runs on floating Linux labels: {sorted(linux)}"
    )


# First major of each action whose action.yml runs on Node 24. Earlier majors
# target Node 20, which runners already force onto Node 24 with a deprecation
# warning.
NODE24_ACTION_FLOORS = {
    "actions/checkout": 5,
    "actions/setup-python": 6,
    "actions/upload-artifact": 6,
    "actions/download-artifact": 7,
    "astral-sh/setup-uv": 7,
}


@pytest.mark.parametrize("workflow", [CI_YML, RELEASE_YML], ids=lambda path: path.name)
def test_workflow_actions_run_on_node24_majors(workflow: Path) -> None:
    text = workflow.read_text(encoding="utf-8")
    stale = []
    for job in _jobs(text).values():
        for step in job.get("steps", []):
            action, _, ref = str(step.get("uses", "")).partition("@")
            # yaml.safe_load drops comments; a SHA pin names its release in one.
            label = re.search(rf"\buses: {re.escape(str(step.get('uses')))} # (v\d+)\.", text)
            if label and re.fullmatch(r"[0-9a-f]{40}", ref):
                ref = label[1]
            major = re.fullmatch(r"v(\d+)", ref)
            if action in NODE24_ACTION_FLOORS and not (
                major and int(major[1]) >= NODE24_ACTION_FLOORS[action]
            ):
                stale.append(step["uses"])
    assert stale == [], f"{workflow.name} uses actions below their Node 24 major: {stale}"


def test_integration_matrix_covers_311_and_313() -> None:
    """Integration lane must exercise both ends of the supported Python range."""
    jobs = _jobs()
    integration = jobs.get("integration")
    assert isinstance(integration, dict)
    strategy = integration.get("strategy")
    assert isinstance(strategy, dict), "integration job must define a matrix strategy"
    matrix = strategy.get("matrix")
    assert isinstance(matrix, dict)
    versions = matrix.get("python-version")
    assert isinstance(versions, list)
    assert "3.11" in versions and "3.13" in versions, (
        f"integration matrix must include 3.11 and 3.13, got {versions!r}"
    )


def test_lint_and_typecheck_include_scripts_and_examples() -> None:
    """Lint/format/typecheck must cover packaged tooling and examples."""
    ci = _ci_text()
    assert re.search(r"ruff check\b[^\n]*scripts", ci), "ruff check must include scripts/"
    assert re.search(r"ruff check\b[^\n]*examples", ci), "ruff check must include examples/"
    assert re.search(r"ruff format --check\b[^\n]*scripts", ci), (
        "ruff format --check must include scripts/"
    )
    assert re.search(r"ruff format --check\b[^\n]*examples", ci), (
        "ruff format --check must include examples/"
    )
    assert re.search(r"mypy\s+--strict\b[^\n]*scripts", ci), "mypy --strict must include scripts/"
    assert re.search(r"mypy\s+--strict\b[^\n]*examples", ci), "mypy --strict must include examples/"


def test_build_smoke_installs_all_extras_and_runs_migration() -> None:
    """Wheel smoke must install with all extras and exercise a SQLite migration."""
    ci = _ci_text()
    assert re.search(r"pip install[^\n]*\[all\]|pip install[^\n]*\.\[all\]", ci) or (
        "dist/*.whl" in ci and "[all]" in ci
    ), "build smoke must install the wheel with the [all] extra"
    # Assert the migration is actually driven, not merely that the word
    # "modulith" occurs somewhere in a file that names the package on almost
    # every line — that half of the original condition could never fail.
    assert "alembic" in ci, "build smoke must drive the packaged Alembic migrations"
    assert re.search(r'command\.upgrade\(\s*cfg\s*,\s*"head"\s*\)', ci), (
        "build smoke must run `alembic upgrade head`, not just import alembic"
    )
    assert "sqlite:///" in ci, "build smoke's migration must target SQLite"
    assert "gen_api_reference" in ci or "API_REFERENCE" in ci, (
        "CI must run the API-reference drift check"
    )


def test_build_job_executes_package_artifact_assertions() -> None:
    """Wheel and sdist inspection must reject missing modules and private files."""
    inspection_scripts = [
        step["run"]
        for step in _job_steps("build")
        if isinstance(step.get("run"), str) and step.get("name") == "Inspect distribution contents"
    ]
    assert len(inspection_scripts) == 1
    executable_lines = [
        line.strip()
        for line in inspection_scripts[0].splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert any(line.startswith("assert not missing") for line in executable_lines)
    assert any(line.startswith("assert not leaked") for line in executable_lines)
    assert any('("wheel", wheel_names)' in line for line in executable_lines)
    assert any('("sdist", sdist_names)' in line for line in executable_lines)

    uploads = [
        step
        for step in _job_steps("build")
        if isinstance(step.get("uses"), str) and step["uses"].startswith("actions/upload-artifact@")
    ]
    assert len(uploads) == 1
    upload_config = uploads[0].get("with")
    assert isinstance(upload_config, dict)
    assert upload_config.get("path") == "dist/"


def test_coverage_outbox_path_is_fully_covered() -> None:
    """Outbox plugin must stay at 100% coverage (fail_under via omit/paths)."""
    tool = _pyproject().get("tool", {})
    assert isinstance(tool, dict)
    coverage = tool.get("coverage", {})
    assert isinstance(coverage, dict)
    # Either a dedicated paths/fail_under for outbox, or an explicit CI step.
    ci = _ci_text()
    has_outbox_gate = (
        "builtin/outbox" in ci or "modulith/builtin/outbox" in ci or "outbox.py" in str(coverage)
    )
    assert has_outbox_gate, (
        "CI or coverage config must enforce 100% coverage on modulith/builtin/outbox.py"
    )


@pytest.mark.parametrize(
    "workflow_label,workflow_text",
    [("ci.yml", _ci_text()), ("release.yml", _release_text())],
)
def test_build_job_artifact_assertions_cover_non_py_runtime_files(
    workflow_label: str, workflow_text: str
) -> None:
    """The inspection `required` set must also guard alembic.ini/migrations/py.typed.

    Without these, an `exclude`/`only-include` narrowing of
    ``[tool.hatch.build.targets.wheel]`` could silently drop the packaged
    Alembic config or the PEP 561 marker with every CI job staying green.
    Both workflows build their own `Inspect distribution contents` step
    (ci.yml's `build` job, release.yml's own build-and-publish job), so the
    guard must hold identically in each.
    """
    inspection_scripts = [
        step["run"]
        for step in _job_steps("build", workflow_text)
        if isinstance(step.get("run"), str) and step.get("name") == "Inspect distribution contents"
    ]
    assert len(inspection_scripts) == 1, f"{workflow_label} must have one inspection step"
    script = inspection_scripts[0]
    for literal in (
        '"modulith/adapters/alembic.ini"',
        '"modulith/adapters/migrations/env.py"',
        '"modulith/adapters/migrations/script.py.mako"',
        '"modulith/py.typed"',
    ):
        assert literal in script, f"inspection `required` set must include {literal}"
    assert re.search(r'adapters\.glob\(\s*"migrations/versions/\*\.py"\s*\)', script), (
        "inspection `required` set must guard every migrations/versions/*.py file"
    )


@pytest.mark.parametrize(
    "workflow_label,workflow_text",
    [("ci.yml", _ci_text()), ("release.yml", _release_text())],
)
def test_build_smoke_loads_the_packaged_alembic_ini(
    workflow_label: str, workflow_text: str
) -> None:
    """The smoke test must drive the real alembic.ini the extract docs tell users to use.

    Hand-building `Config()` and calling `set_main_option("script_location", ...)`
    never reads `modulith/adapters/alembic.ini` from disk, so it can pass even if
    that file (or the documented `alembic -c <path>` invocation it backs) is broken.
    Both workflows run their own smoke test, so the guard must hold in each.
    """
    smoke_scripts = [
        step["run"]
        for step in _job_steps("build", workflow_text)
        if isinstance(step.get("run"), str) and step.get("name") == "Smoke-test wheel installs"
    ]
    assert len(smoke_scripts) == 1, f"{workflow_label} must have one smoke-test step"
    script = smoke_scripts[0]
    assert '"alembic.ini"' in script, "smoke test must reference the packaged alembic.ini file"
    assert re.search(r"Config\(\s*str\(\w*alembic\w*\)\s*\)", script), (
        "smoke test must construct Config(str(<variable pointing at the packaged alembic.ini>))"
    )
    assert 'set_main_option("script_location"' not in script, (
        "smoke test must rely on the packaged alembic.ini's own script_location, "
        "not override it, or it never proves the shipped file is well-formed"
    )


def test_header_comment_lists_all_optional_extras() -> None:
    """The top-of-file comment enumerating extras must not drift from reality."""
    header = "\n".join(PYPROJECT.read_text(encoding="utf-8").splitlines()[:10])
    for extra in _optional_dependencies():
        assert re.search(rf"\b{re.escape(extra)}\b", header), (
            f"pyproject.toml header comment must list the `{extra}` extra"
        )


def test_redis_extra_floor_supports_aclose() -> None:
    """redis-py 5.0.0 lacks `Redis.aclose()`; RedisStreamsBroker.close() calls it."""
    reqs = _optional_dependencies()["redis"]
    assert any(re.match(r"redis>=5\.0\.1\b", req) for req in reqs), (
        f"redis extra {reqs!r} must floor at >=5.0.1 (5.0.0 has close() but not aclose())"
    )


TEST_CLIENT_IMPORT = re.compile(
    r"^\s*(?:from|import)\s+(?:fastapi|starlette)\.testclient\b", re.MULTILINE
)


def _projects_testing_with_the_starlette_client() -> list[Path]:
    projects = [
        REPO_ROOT,
        *sorted(p.parent for p in (REPO_ROOT / "examples").glob("*/pyproject.toml")),
    ]
    return [
        project
        for project in projects
        if any(
            TEST_CLIENT_IMPORT.search(source.read_text(encoding="utf-8"))
            for source in (project / "tests").rglob("*.py")
        )
    ]


@pytest.mark.parametrize(
    "project",
    _projects_testing_with_the_starlette_client(),
    ids=lambda project: project.relative_to(REPO_ROOT).as_posix(),
)
def test_projects_using_the_starlette_test_client_declare_httpx2(project: Path) -> None:
    """Starlette's test client imports ``httpx2`` first and falls back to ``httpx`` with a
    StarletteDeprecationWarning, so ``-W error`` fails the import of any project that
    tests with it and does not install ``httpx2``."""
    with (project / "pyproject.toml").open("rb") as fh:
        extras = tomllib.load(fh)["project"]["optional-dependencies"]
    extra = "test-suite" if project == REPO_ROOT else "test"
    names = {Requirement(req).name for req in extras[extra]}
    assert "httpx2" in names, (
        f"{project.name}: the `{extra}` extra must declare httpx2, it declares {sorted(names)}"
    )


def test_test_suite_extra_floors_httpx2_at_the_release_starlette_accepts() -> None:
    """Starlette's own ``full`` extra requires ``httpx2>=2.0.0``; the test client is not
    exercised against an older release."""
    reqs = _optional_dependencies()["test-suite"]
    assert any(re.match(r"httpx2>=2\.0\b", req) for req in reqs), (
        f"test-suite extra {reqs!r} must floor httpx2 at >=2.0"
    )


def test_issue_template_contact_links_do_not_point_at_discussions() -> None:
    """Discussions is disabled on this repo; no contact_link may route there.

    A user with a general question would otherwise hit a dead end: blank
    issues are disabled, the two issue forms cover bugs/features only, and a
    Discussions link 404s on a repo with the feature turned off.
    """
    config = yaml.safe_load(ISSUE_TEMPLATE_CONFIG.read_text(encoding="utf-8"))
    assert isinstance(config, dict)
    links = config.get("contact_links")
    assert isinstance(links, list) and links
    for link in links:
        url = link.get("url", "")
        assert "/discussions" not in url, (
            f"contact_link {link.get('name')!r} points at {url!r}, but this "
            "repository has Discussions disabled"
        )


def test_disabled_rules_example_names_a_real_verifier_rule() -> None:
    """The commented-out `disabled_rules` example must name a rule that exists."""
    verifier_src = (REPO_ROOT / "modulith" / "builtin" / "verifier.py").read_text(encoding="utf-8")
    real_rules = set(re.findall(r'rule="([^"]+)"', verifier_src))
    assert real_rules, 'expected to find rule="..." literals in verifier.py'
    pyproject_text = PYPROJECT.read_text(encoding="utf-8")
    example_line = next(
        line for line in pyproject_text.splitlines() if "disabled_rules" in line and "e.g." in line
    )
    example_names = re.findall(r'"([^"]+)"', example_line.split("e.g.", 1)[1])
    assert example_names, f"expected a quoted rule name in {example_line!r}"
    for name in example_names:
        assert name in real_rules, (
            f"disabled_rules example names {name!r}, which is not a real verifier rule "
            f"(real rules: {sorted(real_rules)})"
        )


# ---------------------------------------------------------------------------
# Build backend
# ---------------------------------------------------------------------------


def test_build_backend_floor_supports_the_declared_license_form() -> None:
    """The hatchling floor must be able to emit the license metadata we declare.

    ``project.license`` is the PEP 639 SPDX expression form, which hatchling
    only understands from 1.27. An older backend still builds, but downgrades
    the wheel to Metadata-Version 2.1 with a free-text ``License:`` field, so a
    build under a constraints file that caps hatchling would ship different
    license provenance than the PyPI artifact.
    """
    build_system = _pyproject()["build-system"]
    assert isinstance(build_system, dict)
    requires = build_system["requires"]
    assert isinstance(requires, list)
    floors = [
        tuple(int(part) for part in match.group(1).split("."))
        for req in requires
        if (match := re.fullmatch(r"hatchling>=([\d.]+)", str(req).strip()))
    ]
    assert floors, f"build-system.requires {requires!r} must pin a hatchling floor"
    assert min(floors) >= (1, 27), (
        f"build-system.requires {requires!r} floors hatchling below the 1.27 "
        "that PEP 639 license expressions need"
    )


# ---------------------------------------------------------------------------
# Packaged tooling: the API-reference generator's own CLI
# ---------------------------------------------------------------------------


def test_api_reference_generator_rejects_unknown_flags() -> None:
    """An unrecognized flag must exit non-zero and write nothing.

    Argument handling used to be ``"--check" in argv[1:]``, so anything else —
    ``--checks``, ``--check --verbose``, even ``--help`` — fell through to the
    write branch and exited 0. A CI step that drifted to such an invocation
    would regenerate the reference in the workspace and pass, turning the
    staleness gate into a permanent rubber stamp.
    """
    script = REPO_ROOT / "scripts" / "gen_api_reference.py"
    reference = REPO_ROOT / "docs" / "API_REFERENCE.md"
    before = reference.read_bytes()

    typo = subprocess.run(
        [sys.executable, str(script), "--checks"], capture_output=True, text=True, timeout=120
    )
    assert typo.returncode == 2, f"unknown flag must exit 2, got {typo.returncode}"

    helped = subprocess.run(
        [sys.executable, str(script), "--help"], capture_output=True, text=True, timeout=120
    )
    assert helped.returncode == 0
    assert "--check" in helped.stdout

    assert reference.read_bytes() == before, (
        "neither an unknown flag nor --help may rewrite the committed reference"
    )


# ---------------------------------------------------------------------------
# Release workflow validation
# ---------------------------------------------------------------------------


def _release_workflow() -> dict[Any, Any]:
    """Parse the release.yml workflow."""
    assert RELEASE_YML.exists(), "release.yml must exist"
    workflow = yaml.safe_load(RELEASE_YML.read_text(encoding="utf-8"))
    assert isinstance(workflow, dict)
    return workflow


def test_release_workflow_exists() -> None:
    """release.yml must exist and be valid YAML."""
    workflow = _release_workflow()
    assert workflow is not None


def test_release_triggers_on_version_tags() -> None:
    """release.yml must trigger on v* tags."""
    workflow = _release_workflow()
    # YAML parses 'on' as the boolean True key, not the string 'on'
    on = workflow.get(True)
    assert isinstance(on, dict), "release.yml must define an 'on' trigger"
    push = on.get("push")
    assert isinstance(push, dict)
    tags = push.get("tags")
    assert isinstance(tags, list) and "v*" in tags, (
        "release.yml must trigger on push with tags: ['v*']"
    )


def test_release_has_test_build_publish_jobs() -> None:
    """release.yml must define test, build, and publish jobs."""
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    assert "test" in jobs, "release.yml must define a test job"
    assert "build" in jobs, "release.yml must define a build job"
    assert "publish" in jobs, "release.yml must define a publish job"
    assert "ci" in jobs, "release.yml must define the reusable full-CI job"


def test_ci_workflow_can_run_as_a_reusable_release_gate() -> None:
    """The full CI matrix must be callable from the release workflow."""
    workflow = yaml.safe_load(_ci_text())
    assert isinstance(workflow, dict)
    triggers = workflow.get(True)
    assert isinstance(triggers, dict)
    assert "workflow_call" in triggers

    release = _release_workflow()
    jobs = release.get("jobs")
    assert isinstance(jobs, dict)
    assert jobs["ci"] == {"name": "full CI matrix", "uses": "./.github/workflows/ci.yml"}


def test_release_publish_job_has_oidc_permission() -> None:
    """publish job must have id-token: write permission for OIDC trusted publishing."""
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    publish = jobs.get("publish")
    assert isinstance(publish, dict)
    permissions = publish.get("permissions")
    assert isinstance(permissions, dict), "publish job must define permissions"
    id_token = permissions.get("id-token")
    assert id_token == "write", (
        "publish job must have permissions.id-token: write for OIDC trusted publishing"
    )


def test_release_publish_job_uses_pypa_action() -> None:
    """publish job must pin pypa/gh-action-pypi-publish to a full commit SHA.

    The step runs with ``id-token: write``, so a movable ref such as
    ``release/v1`` would let whoever can move it publish as this project.
    """
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    publish = jobs.get("publish")
    assert isinstance(publish, dict)
    steps = publish.get("steps")
    assert isinstance(steps, list) and steps

    pypa_steps = [
        step
        for step in steps
        if isinstance(step.get("uses"), str)
        and step["uses"].startswith("pypa/gh-action-pypi-publish@")
    ]
    assert len(pypa_steps) == 1, (
        "publish job must use exactly one pypa/gh-action-pypi-publish action"
    )
    uses = pypa_steps[0]["uses"]
    assert re.fullmatch(r"pypa/gh-action-pypi-publish@[0-9a-f]{40}", uses), (
        f"publish job must pin the publish action to a full commit SHA, got {uses!r}"
    )
    # yaml.safe_load drops comments, so the version label is read from the raw text.
    assert re.search(
        rf"\buses: {re.escape(uses)} # v1\.\d+\.\d+[ \t]*$", _release_text(), re.MULTILINE
    ), "the pinned SHA must name its release in a trailing `# v1.x.y` comment"


def test_release_publish_job_pins_every_action_to_a_commit() -> None:
    """Every step of the publish job can request the PyPI token (``id-token:
    write``), so a movable tag on any action there could publish as this
    project, not only one on the publish action."""
    text = _release_text()
    unpinned = [
        step["uses"]
        for step in _jobs(text)["publish"]["steps"]
        if "uses" in step
        and not (
            re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", step["uses"])
            and re.search(
                rf"\buses: {re.escape(step['uses'])} # v\d+\.\d+\.\d+[ \t]*$", text, re.MULTILINE
            )
        )
    ]
    assert unpinned == [], f"publish job actions not pinned to a commit: {unpinned}"


def test_release_publish_job_needs_full_ci_test_and_build() -> None:
    """publish must wait for the full CI gate and release-local checks."""
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    publish = jobs.get("publish")
    assert isinstance(publish, dict)
    needs = publish.get("needs")
    assert isinstance(needs, (list, str)), "publish job must define needs"

    if isinstance(needs, str):
        needs = [needs]
    assert {"ci", "test", "build"} <= set(needs), "publish job must depend on ci, test, and build"


def test_release_artifact_is_distinct_from_reusable_ci_artifact() -> None:
    """The publish job must consume the release build, not CI's artifact."""
    release = _release_workflow()
    jobs = release.get("jobs")
    assert isinstance(jobs, dict)
    release_build = jobs.get("build")
    publish = jobs.get("publish")
    assert isinstance(release_build, dict)
    assert isinstance(publish, dict)

    uploads = [
        step
        for step in release_build.get("steps", [])
        if isinstance(step, dict)
        and isinstance(step.get("uses"), str)
        and step["uses"].startswith("actions/upload-artifact@")
    ]
    downloads = [
        step
        for step in publish.get("steps", [])
        if isinstance(step, dict)
        and isinstance(step.get("uses"), str)
        and step["uses"].startswith("actions/download-artifact@")
    ]
    assert len(uploads) == len(downloads) == 1
    upload_name = uploads[0].get("with", {}).get("name")
    download_name = downloads[0].get("with", {}).get("name")
    assert upload_name == download_name == "release-dist"
    assert upload_name != "dist"


def test_release_publish_has_no_unconditional_success_override() -> None:
    """A failed required job must prevent the publish job from running."""
    release = _release_workflow()
    jobs = release.get("jobs")
    assert isinstance(jobs, dict)
    publish = jobs.get("publish")
    assert isinstance(publish, dict)
    assert publish.get("if") != "always()"
    assert all(
        step.get("if") != "always()" for step in publish.get("steps", []) if isinstance(step, dict)
    )


def _credential_markers(publish: dict[str, Any]) -> list[str]:
    """Credential-shaped strings found anywhere in the publish job.

    Serializing the parsed job (rather than scanning it key by key) covers a
    `password` wherever it sits — an `env:` block, a non-pypa action's `with:`,
    a nested step — not just the shapes enumerated below.
    """
    text = yaml.dump(publish)
    return [marker for marker in ("PYPI_API_TOKEN", "secrets.PYPI", "password:") if marker in text]


def test_release_publish_has_no_hardcoded_token() -> None:
    """publish job must not use hardcoded PyPI tokens or secrets."""
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    publish = jobs.get("publish")
    assert isinstance(publish, dict)

    assert _credential_markers(publish) == [], (
        "publish job must publish via OIDC trusted publishing only — no "
        "credential may be spelled out in release.yml"
    )
    # OIDC trusted publishing should not use user/password
    pypa_steps = [
        step
        for step in publish.get("steps", [])
        if isinstance(step.get("uses"), str) and "pypa/gh-action-pypi-publish" in step["uses"]
    ]
    for step in pypa_steps:
        with_config = step.get("with", {})
        assert not isinstance(with_config, dict) or "password" not in with_config, (
            "publish action must not pass password via OIDC trusted publishing"
        )


def test_publish_credential_scan_detects_a_planted_password() -> None:
    """The credential guard above used to read ``"password:" not in text or
    "password:" not in str(publish)``; the right operand stringifies a dict,
    whose keys always render quoted, so it was unconditionally true and the
    whole guard could never fail. Prove the replacement actually bites."""
    leaky = {
        "runs-on": "ubuntu-latest",
        "steps": [{"uses": "some/other-action@v1", "with": {"password": "pypi-AgEIcHl"}}],
    }

    assert _credential_markers(leaky) == ["password:"]


def test_release_build_job_has_inspection_and_smoke_test() -> None:
    """build job in release.yml must include distribution inspection and smoke-test."""
    workflow = _release_workflow()
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    build = jobs.get("build")
    assert isinstance(build, dict)
    steps = build.get("steps")
    assert isinstance(steps, list) and steps

    step_names = {step.get("name", "") for step in steps if isinstance(step, dict)}
    assert "Inspect distribution contents" in step_names, (
        "build job must include 'Inspect distribution contents' step"
    )
    assert "Smoke-test wheel installs" in step_names, (
        "build job must include 'Smoke-test wheel installs' step"
    )


def test_ci_fails_when_the_lockfile_is_stale() -> None:
    """``uv.lock`` is committed, so a pyproject edit that forgets to refresh it
    must fail CI rather than leave the lock silently out of date."""
    assert any(
        isinstance(step.get("run"), str)
        and _has_shell_command(step["run"], ["uv", "lock", "--check"])
        for job in _jobs()
        for step in _job_steps(job)
    ), "ci.yml must run `uv lock --check` in some job"


def test_ci_runs_on_a_weekly_schedule() -> None:
    """Dependencies are unpinned in CI, so a scheduled run is what surfaces a
    newly released dependency that breaks the suite without any push."""
    workflow = yaml.safe_load(_ci_text())
    assert isinstance(workflow, dict)
    triggers = workflow.get("on", workflow.get(True))  # YAML 1.1 parses `on` as True
    assert isinstance(triggers, dict)
    schedule = triggers.get("schedule")
    assert isinstance(schedule, list) and schedule, "ci.yml must define a `schedule:` trigger"
    crons = [entry["cron"] for entry in schedule]
    assert all(len(cron.split()) == 5 and cron.split()[4] != "*" for cron in crons), (
        f"schedule must run weekly (a fixed day-of-week), got {crons!r}"
    )


def test_readme_names_every_python_in_the_unit_matrix() -> None:
    """The README's test-coverage sentence lists the Python versions CI runs."""
    matrix = _jobs()["test"]["strategy"]["matrix"]["python-version"]
    sentence = next(
        line
        for line in (REPO_ROOT / "README.md").read_text(encoding="utf-8").splitlines()
        if "hermetic tests on Python" in line
    )
    missing = [v for v in matrix if not re.search(rf"(?<![\d.]){re.escape(v)}(?![\d])", sentence)]
    assert not missing, f"README sentence {sentence!r} omits {missing!r}"


def test_contributing_explains_how_to_refresh_the_lockfile() -> None:
    text = (REPO_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    assert "uv lock" in text and "uv lock --check" in text


def test_sdist_excludes_the_untracked_worktree_include_file() -> None:
    """A local build from a dirty tree must not ship ``.worktreeinclude``."""
    tool = _pyproject()["tool"]
    assert isinstance(tool, dict)
    exclude = tool["hatch"]["build"]["targets"]["sdist"]["exclude"]
    assert "/.worktreeinclude" in exclude


def test_pytest_config_does_not_claim_the_suite_is_deterministic_under_w_error() -> None:
    """The suite does not pass under ``-W error`` and no lane runs it, so the
    ``filterwarnings`` comment must not promise it."""
    lines = PYPROJECT.read_text(encoding="utf-8").splitlines()
    comments = [line for line in lines if line.lstrip().startswith("#") and "-W error" in line]
    assert comments == []


def test_test_suite_extra_declares_typing_extensions() -> None:
    """tests/test_serializers.py imports ``typing_extensions`` (3.11 has no
    ``typing.TypeAliasType``); it must not rely on a transitive install."""
    names = {Requirement(req).name for req in _optional_dependencies()["test-suite"]}
    assert "typing-extensions" in names


def test_classifiers_name_every_python_in_the_unit_matrix() -> None:
    project = _pyproject()["project"]
    assert isinstance(project, dict)
    classifiers = set(project["classifiers"])
    matrix = _jobs()["test"]["strategy"]["matrix"]["python-version"]
    missing = [v for v in matrix if f"Programming Language :: Python :: {v}" not in classifiers]
    assert not missing, f"classifiers omit tested Python versions {missing!r}"
