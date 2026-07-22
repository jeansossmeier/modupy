"""Guard tests for CI/packaging configuration drift.

These reproduce and lock in the fixes for audit findings:

- S2-r1-56 / S2-r2-114: CI must run the integration suite in a dedicated
  lane (installing the ``integration`` extra, which carries testcontainers
  and the real Postgres drivers), the default test job must deselect
  ``-m integration`` (those tests silently skip without Docker drivers,
  giving false green), and mypy in CI must type-check ``tests/`` too.
- S4-r3-164 / S4-r2-125: version constraints for standalone tool installs
  in ci.yml (ruff, mypy, ...) must mirror pyproject.toml exactly, so CI
  can never resolve a tool major that local development forbids.

They parse the *actual* files in the repository so any future drift
between ``.github/workflows/ci.yml`` and ``pyproject.toml`` fails fast in
the default test lane instead of surfacing as a broken CI run months later.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _ci_text() -> str:
    return CI_YML.read_text(encoding="utf-8")


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
    """S2-r1-56: the default test job must not rely on silent auto-skip.

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
    """S2-r1-56 / S2-r2-114: CI must actually execute the integration suite.

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
    """S2-r2-114: the extra CI installs must carry the real e2e drivers."""
    extras = _optional_dependencies()
    assert "integration" in extras
    names = {re.split(r"[><=!~\[]", req, maxsplit=1)[0] for req in extras["integration"]}
    assert "testcontainers" in names
    assert "asyncpg" in names
    assert "psycopg" in names


def test_mypy_job_type_checks_tests() -> None:
    """S2-r1-56: mypy in CI must cover tests/, not just modulith/ — and it
    must run under --strict (W2 RESIDUALS item 12a: the previous regex made
    ``--strict`` optional, so CI silently dropping it would still pass this
    guard)."""
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
    """W3 R5-01: the integration lane must FAIL when Docker is unreachable.

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
    """S4-r3-164 / S4-r2-125: no constraint drift between ci.yml and pyproject.

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
            "requirement — the two constraints have drifted (S4-r3-164/S4-r2-125)"
        )


# ---------------------------------------------------------------------------
# Task 9: coverage, integration matrix, scripts/examples lint, wheel smoke,
# API-reference drift check
# ---------------------------------------------------------------------------


def _jobs() -> dict[str, Any]:
    workflow = yaml.safe_load(_ci_text())
    assert isinstance(workflow, dict)
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict)
    return jobs


def _job_steps(job_name: str) -> list[dict[str, Any]]:
    """Return one parsed job's steps so YAML comments cannot satisfy guards."""
    job = _jobs().get(job_name)
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
    assert matrix.get("python-version") == ["3.11", "3.12", "3.13"], (
        "test matrix must cover exactly Python 3.11, 3.12, and 3.13"
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
    assert matrix.get("os") == ["ubuntu-latest", "macos-latest", "windows-latest"]
    assert matrix.get("python-version") == ["3.11", "3.13"]


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
    assert "modulith" in ci and ("migrate" in ci or "alembic" in ci), (
        "build smoke must run a packaged SQLite migration"
    )
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
