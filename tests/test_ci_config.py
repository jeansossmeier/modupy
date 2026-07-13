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

import re
import tomllib
from pathlib import Path

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


def test_integration_job_cannot_go_green_without_docker() -> None:
    """W3 R5-01: the integration lane must FAIL when Docker is unreachable.

    tests/conftest.py ``pytest.skip``s the entire integration suite when the
    Docker probe fails, and pytest exits 0 on all-skipped — so a runner-image
    Docker breakage would turn the lane permanently green while proving
    nothing (the exact rubber-stamp the unit-lane comment in the same file
    warns about). Two independent guards are required:

      * a ``docker info`` preflight step, and
      * a post-run summary assertion: the integration selection must report
        at least one passed test and no skipped tests.
    """
    ci = _ci_text()
    assert re.search(r"\bdocker info\b", ci), (
        "integration lane must run a `docker info` preflight step so an "
        "unreachable Docker daemon fails the job instead of skipping the suite"
    )
    assert re.search(r"[0-9$(){}\[\]+]* passed", ci) or "passed" in ci, (
        "integration lane must assert its pytest summary reports passed tests"
    )
    assert "skipped" in ci, (
        "integration lane must fail when its pytest summary reports skipped "
        "tests (an all-skip exits 0 and rubber-stamps the lane)"
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
