"""Tests for `modulith init` and `modulith add module` — writing the starter project."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from modulith.cli import app

runner = CliRunner()

REPO = Path(__file__).resolve().parent.parent
QUICKSTART = REPO / "examples" / "quickstart"
ENV = {
    name: value
    for name, value in os.environ.items()
    if not name.startswith(("MODULITH_", "PYTEST_"))
}


def _files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in root.rglob("*")
        if path.is_file()
    }


def _run(*argv: str, cwd: Path, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", *argv],
        cwd=cwd,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _lint(project: Path, *paths: str) -> None:
    """ruff check and ruff format --check pass on ``paths``, under ``project``'s ruff config."""
    ruff = shutil.which("ruff", path=str(Path(sys.executable).parent)) or shutil.which("ruff")
    if ruff is None:
        pytest.skip("ruff is not installed")
    for check in (["check"], ["format", "--check"]):
        linted = subprocess.run([ruff, *check, *paths], cwd=project, capture_output=True, text=True)
        assert linted.returncode == 0, linted.stdout + linted.stderr


def test_init_writes_the_quickstart_example_under_the_given_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The starter is the quickstart example, not a lookalike.

    ``examples/quickstart`` is the project the README shows and CI runs, so a
    starter equal to it file for file is one that is known to work.
    """
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "--no-skill"])

    assert result.exit_code == 0, result.output
    expected = {
        path.relative_to(QUICKSTART).as_posix().replace("myapp", "bakery"): path.read_text(
            encoding="utf-8"
        ).replace("myapp", "bakery")
        for path in (QUICKSTART / "pyproject.toml", *QUICKSTART.rglob("*.py"))
    }
    written = _files(tmp_path / "bakery")
    assert ".venv/" in written.pop(".gitignore")
    assert written == expected, (
        "modulith/_starter.py no longer matches examples/quickstart; copy the changed file over"
    )
    for rel in [".gitignore", *expected]:
        assert f"  {rel}\n" in result.stdout
    assert "modulith dev bakery.main:app" in result.stdout
    assert "modulith add module NAME" in result.stdout
    assert "modulith skill install  # teaches AI coding assistants modupy" in result.stdout


def test_init_installs_the_skill_into_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery"])

    assert result.exit_code == 0, result.output
    packaged = _files(REPO / "modulith" / "skill")
    for where in (".claude/skills/modupy", ".agents/skills/modupy"):
        assert _files(tmp_path / "bakery" / where) == packaged
        assert f"installed the modupy skill in {Path('bakery', where)}\n" in result.stdout
    assert "modulith skill install" not in result.stdout


def test_init_refuses_a_modupy_skill_dir_that_is_not_the_skill_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".claude" / "skills").mkdir(parents=True)
    (tmp_path / ".claude" / "skills" / "modupy").write_text("mine", encoding="utf-8")

    result = runner.invoke(app, ["init", "bakery", "."])

    assert result.exit_code == 1
    assert "is not the modupy skill" in result.stderr
    assert _files(tmp_path) == {".claude/skills/modupy": "mine"}


def test_init_keeps_an_existing_gitignore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".gitignore").write_text("mine\n", encoding="utf-8")

    result = runner.invoke(app, ["init", "bakery", ".", "--no-skill"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / ".gitignore").read_text(encoding="utf-8") == "mine\n"
    assert "  .gitignore\n" not in result.stdout
    assert "kept the existing .gitignore" in result.stdout


def test_the_empty_starter_passes_its_test_and_grows_with_add_module(tmp_path: Path) -> None:
    created = _run("modulith", "init", "bakery", "--empty", cwd=tmp_path)
    assert created.returncode == 0, created.stderr
    project = tmp_path / "bakery"
    assert sorted(_files(project / "bakery")) == [
        "__init__.py",
        "contracts/__init__.py",
        "contracts/events.py",
        "main.py",
    ]
    assert "1 passed" in _run("pytest", "-q", cwd=project).stdout

    added = _run("modulith", "add", "module", "shipping", cwd=project)

    assert added.returncode == 0, added.stderr
    tests = _run("pytest", "-q", cwd=project)
    assert "2 passed" in tests.stdout, tests.stdout + tests.stderr
    verify = _run("modulith", "verify", cwd=project)
    assert verify.returncode == 0 and "no boundary violations" in verify.stdout, verify.stdout


def test_add_module_mounts_each_router_where_ruff_sorts_it(tmp_path: Path) -> None:
    """Into the full starter, before and after its own router imports."""
    assert _run("modulith", "init", "bakery", "--no-skill", cwd=tmp_path).returncode == 0
    project = tmp_path / "bakery"

    for module in ("billing", "shipping"):
        added = _run("modulith", "add", "module", module, cwd=project)
        assert added.returncode == 0, added.stderr
        assert f"mounted its router at /{module} in bakery/main.py" in added.stdout

    main = (project / "bakery" / "main.py").read_text(encoding="utf-8")
    imports = [line for line in main.splitlines() if line.startswith("from bakery.")]
    assert imports == [
        "from bakery.billing import router as billing_router",
        "from bakery.inventory import router as inventory_router",
        "from bakery.orders import router as orders_router",
        "from bakery.shipping import router as shipping_router",
    ]
    assert main.count("app.include_router(") == 4
    tests = _run("pytest", "-q", cwd=project)
    assert "7 passed" in tests.stdout, tests.stdout + tests.stderr
    _lint(project, "bakery", "tests")


# The project the main.py shapes below are added to; each shape imports from it.
_SHAPES_PYPROJECT = """\
[project]
name = "bakery"

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "I"]
"""
_ROUTER = "from fastapi import APIRouter\n\nrouter = APIRouter()\n"


@pytest.mark.parametrize(
    ("before", "module", "after"),
    [
        pytest.param(
            "from fastapi import FastAPI\n\nfrom bakery import settings\n\n"
            "app = FastAPI(title=settings.TITLE)\n",
            "billing",
            "from fastapi import FastAPI\n\nfrom bakery import settings\n"
            "from bakery.billing import router as billing_router\n\n"
            "app = FastAPI(title=settings.TITLE)\n"
            'app.include_router(billing_router, prefix="/billing")\n',
            id="from-package-import",
        ),
        pytest.param(
            "from fastapi import FastAPI\n\nimport bakery.settings\n\n"
            "app = FastAPI(title=bakery.settings.TITLE)\n",
            "billing",
            "from fastapi import FastAPI\n\nimport bakery.settings\n"
            "from bakery.billing import router as billing_router\n\n"
            "app = FastAPI(title=bakery.settings.TITLE)\n"
            'app.include_router(billing_router, prefix="/billing")\n',
            id="plain-import",
        ),
        pytest.param(
            "from fastapi import FastAPI\n\nfrom .orders import router\n\n"
            'app = FastAPI()\napp.include_router(router, prefix="/orders")\n',
            "billing",
            "from fastapi import FastAPI\n\n"
            "from bakery.billing import router as billing_router\n\n"
            "from .orders import router\n\n"
            'app = FastAPI()\napp.include_router(router, prefix="/orders")\n'
            'app.include_router(billing_router, prefix="/billing")\n',
            id="relative-import",
        ),
        pytest.param(
            "from fastapi import FastAPI\n\nfrom bakery.v10 import router as v10_router\n\n"
            'app = FastAPI()\napp.include_router(v10_router, prefix="/v10")\n',
            "v2",
            "from fastapi import FastAPI\n\nfrom bakery.v2 import router as v2_router\n"
            "from bakery.v10 import router as v10_router\n\n"
            'app = FastAPI()\napp.include_router(v10_router, prefix="/v10")\n'
            'app.include_router(v2_router, prefix="/v2")\n',
            id="numbers-by-value",
        ),
        pytest.param(
            "from fastapi import FastAPI\n\napp: FastAPI = FastAPI()\n",
            "billing",
            "from fastapi import FastAPI\n\n"
            "from bakery.billing import router as billing_router\n\n"
            'app: FastAPI = FastAPI()\napp.include_router(billing_router, prefix="/billing")\n',
            id="annotated-app",
        ),
        pytest.param(
            "from fastapi import FastAPI\r\n\r\napp = FastAPI()\r\n",
            "billing",
            "from fastapi import FastAPI\r\n\r\n"
            "from bakery.billing import router as billing_router\r\n\r\n"
            'app = FastAPI()\r\napp.include_router(billing_router, prefix="/billing")\r\n',
            id="crlf-line-endings",
        ),
        pytest.param(
            "# The app.\x0c(page two)\n\nfrom fastapi import FastAPI\n\napp = FastAPI()\n",
            "billing",
            "# The app.\x0c(page two)\n\nfrom fastapi import FastAPI\n\n"
            "from bakery.billing import router as billing_router\n\n"
            'app = FastAPI()\napp.include_router(billing_router, prefix="/billing")\n',
            id="form-feed",
        ),
        pytest.param(
            "from fastapi import FastAPI\n\napp = FastAPI()\n",
            "a_module_name_long_enough_to_wrap",
            "from fastapi import FastAPI\n\n"
            "from bakery.a_module_name_long_enough_to_wrap import (\n"
            "    router as a_module_name_long_enough_to_wrap_router,\n)\n\n"
            "app = FastAPI()\napp.include_router(\n"
            "    a_module_name_long_enough_to_wrap_router,\n"
            '    prefix="/a_module_name_long_enough_to_wrap",\n)\n',
            id="long-name",
        ),
    ],
)
def test_add_module_mounts_the_router_in_each_shape_of_main_py(
    before: str, module: str, after: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The import lands where ruff's isort puts it, and the app serves the module."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(_SHAPES_PYPROJECT, encoding="utf-8")
    package = tmp_path / "bakery"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "settings.py").write_text('TITLE = "Bakery"\n', encoding="utf-8")
    (package / "orders.py").write_text(_ROUTER, encoding="utf-8")
    (package / "v10.py").write_text(_ROUTER, encoding="utf-8")
    (package / "main.py").write_bytes(before.encode("utf-8"))

    result = runner.invoke(app, ["add", "module", module])

    assert result.exit_code == 0, result.output
    assert f"mounted its router at /{module} in bakery/main.py" in result.stdout
    assert (package / "main.py").read_bytes().decode("utf-8") == after
    _lint(tmp_path, "bakery/main.py")
    served = subprocess.run(
        [
            sys.executable,
            "-c",
            "from fastapi.testclient import TestClient; from bakery.main import app; "
            f"print(TestClient(app).get('/{module}').status_code)",
        ],
        cwd=tmp_path,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert served.stdout == "200\n", served.stdout + served.stderr


def test_a_long_module_name_keeps_the_project_lint_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--empty", "--no-skill"]).exit_code == 0

    result = runner.invoke(app, ["add", "module", "a_module_name_long_enough_to_wrap"])

    assert result.exit_code == 0, result.output
    tests = _run("pytest", "-q", cwd=tmp_path)
    assert "2 passed" in tests.stdout, tests.stdout + tests.stderr
    _lint(tmp_path, "bakery", "tests")


def test_add_module_leaves_an_app_from_a_factory_to_its_author(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mounting after `app = create_app()` would miss every app the factory builds."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(_SHAPES_PYPROJECT, encoding="utf-8")
    (tmp_path / "bakery").mkdir()
    (tmp_path / "bakery" / "__init__.py").write_text("", encoding="utf-8")
    main = (
        "from fastapi import FastAPI\n\n\ndef create_app() -> FastAPI:\n"
        "    return FastAPI()\n\n\napp = create_app()\n"
    )
    (tmp_path / "bakery" / "main.py").write_text(main, encoding="utf-8")

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "bakery" / "main.py").read_text(encoding="utf-8") == main
    assert '  app.include_router(billing_router, prefix="/billing")\n' in result.stderr
    tests = _run("pytest", "-q", cwd=tmp_path)
    assert "1 passed" in tests.stdout, tests.stdout + tests.stderr


@pytest.mark.skipif(
    os.name == "nt" or os.geteuid() == 0, reason="needs a file mode the user cannot write through"
)
def test_add_module_prints_the_mount_when_main_py_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--empty", "--no-skill"]).exit_code == 0
    main = tmp_path / "bakery" / "main.py"
    before = main.read_text(encoding="utf-8")
    main.chmod(0o444)

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 0, result.output
    assert main.read_text(encoding="utf-8") == before
    assert '  app.include_router(billing_router, prefix="/billing")\n' in result.stderr
    tests = _run("pytest", "-q", cwd=tmp_path)
    assert "2 passed" in tests.stdout, tests.stdout + tests.stderr


def test_add_module_without_a_main_app_writes_the_module_and_prints_the_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A src layout whose app is built elsewhere; the module's test passes all the same."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "bakery"\n\n[tool.pytest.ini_options]\npythonpath = ["src"]\n',
        encoding="utf-8",
    )
    (tmp_path / "src" / "bakery").mkdir(parents=True)
    (tmp_path / "src" / "bakery" / "__init__.py").write_text("", encoding="utf-8")

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 0, result.output
    assert sorted(_files(tmp_path)) == [
        "pyproject.toml",
        "src/bakery/__init__.py",
        "src/bakery/billing/__init__.py",
        "src/bakery/billing/api.py",
        "tests/test_billing.py",
    ]
    assert (
        "  from bakery.billing import router as billing_router\n"
        '  app.include_router(billing_router, prefix="/billing")\n'
    ) in result.stderr
    tests = _run("pytest", "-q", cwd=tmp_path)
    assert "1 passed" in tests.stdout, tests.stdout + tests.stderr


def test_add_module_points_the_next_steps_at_uv_run_in_a_uv_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--empty", "--no-skill"]).exit_code == 0
    (tmp_path / "uv.lock").write_text("", encoding="utf-8")

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 0, result.output
    assert result.stdout.endswith("next:\n  uv run pytest\n  uv run modulith verify\n")


def test_add_module_refuses_a_file_where_the_tests_folder_goes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--empty", "--no-skill"]).exit_code == 0
    shutil.rmtree(tmp_path / "tests")
    (tmp_path / "tests").write_text("mine", encoding="utf-8")
    before = _files(tmp_path)

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 1
    assert "error: tests already exists; add never overwrites it" in result.stderr
    assert _files(tmp_path) == before


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["add", "module", "Billing"], "error: 'Billing' is not a lowercase Python module name"),
        (["add", "module", "contracts"], "error: 'contracts' is the contracts package"),
        (["add", "module", "orders"], "error: bakery/orders already exists; add never"),
    ],
)
def test_add_module_refuses_and_writes_nothing(
    argv: list[str], message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--no-skill"]).exit_code == 0
    before = _files(tmp_path)

    result = runner.invoke(app, argv)

    assert result.exit_code == 1
    assert message in result.stderr
    assert _files(tmp_path) == before


def test_add_module_refuses_a_taken_test_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["init", "bakery", ".", "--no-skill"]).exit_code == 0
    (tmp_path / "tests" / "test_billing.py").write_text("mine", encoding="utf-8")
    before = _files(tmp_path)

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 1
    assert f"error: {Path('tests/test_billing.py')} already exists" in result.stderr
    assert _files(tmp_path) == before


def test_add_module_outside_a_project_says_how_to_start_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["add", "module", "billing"])

    assert result.exit_code == 1
    assert "start a project with `modulith init`" in result.stderr
    assert not list(tmp_path.iterdir())


def test_init_ci_github_writes_a_workflow_that_checks_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "--ci", "github", "--no-skill"])

    assert result.exit_code == 0, result.output
    assert "  .github/workflows/ci.yml\n" in result.stdout
    workflow: dict[Any, Any] = yaml.safe_load(
        (tmp_path / "bakery" / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    )
    runs = [step["run"] for step in workflow["jobs"]["check"]["steps"] if "run" in step]
    assert runs == [
        "uv sync --extra test",
        "uv run modulith verify",
        "uv run pytest",
        "uvx ruff check bakery tests",
        "uvx ruff format --check bakery tests",
    ]
    # `uv sync --extra test` installs what the starter's pyproject names.
    pyproject = tomllib.loads((tmp_path / "bakery" / "pyproject.toml").read_text(encoding="utf-8"))
    assert "test" in pyproject["project"]["optional-dependencies"]


def test_init_ci_refuses_an_existing_workflow_and_an_unknown_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / ".github" / "workflows" / "ci.yml").write_text("mine", encoding="utf-8")

    taken = runner.invoke(app, ["init", "bakery", ".", "--ci", "github"])
    unknown = runner.invoke(app, ["init", "bakery", "--ci", "gitlab"])

    assert taken.exit_code == 1
    assert f"error: {Path('.github/workflows/ci.yml')} already exists" in taken.stderr
    assert unknown.exit_code == 1
    assert "error: --ci 'gitlab' is not supported" in unknown.stderr
    assert _files(tmp_path) == {".github/workflows/ci.yml": "mine"}


@pytest.mark.parametrize("taken", [".github", ".github/workflows"])
def test_init_ci_refuses_a_file_where_a_workflow_folder_goes_and_writes_nothing(
    taken: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / taken).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / taken).write_text("mine", encoding="utf-8")

    result = runner.invoke(app, ["init", "bakery", ".", "--ci", "github"])

    assert result.exit_code == 1
    assert f"error: {Path(taken)} already exists; init never overwrites it" in result.stderr
    assert _files(tmp_path) == {taken: "mine"}


REQUIREMENTS = ["modupy[fastapi,cli]", "pytest", "httpx2"]


def _fake_uv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int = 0) -> Path:
    """A `uv` first on PATH that records its arguments and working directory."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    record = tmp_path / "uv-calls.txt"
    uv = bin_dir / "uv"
    uv.write_text(
        f"#!{sys.executable}\nimport os, sys\n"
        f"with open({str(record)!r}, 'a') as f:\n"
        "    f.write(os.getcwd() + ' ' + ' '.join(sys.argv[1:]) + '\\n')\n"
        f"sys.exit({exit_code})\n",
        encoding="utf-8",
    )
    uv.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return record


@pytest.mark.skipif(os.name == "nt", reason="the fake uv is a POSIX script")
def test_init_install_runs_uv_sync_and_points_the_next_steps_at_uv_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _fake_uv(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "--install", "--no-skill"])

    assert result.exit_code == 0, result.output
    assert record.read_text(encoding="utf-8") == f"{tmp_path / 'bakery'} sync --extra test\n"
    assert "next:\n  cd bakery\n  uv run pytest\n  uv run modulith dev bakery.main:app\n" in (
        result.stdout
    )
    assert "pip install" not in result.stdout


@pytest.mark.skipif(os.name == "nt", reason="the fake uv is a POSIX script")
def test_init_install_failure_exits_1_and_keeps_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_uv(tmp_path, monkeypatch, exit_code=3)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "--install", "--no-skill"])

    assert result.exit_code == 1
    assert (
        "the project is written, so install its dependencies yourself:\n"
        "  cd bakery\n  uv sync --extra test\n"
    ) in result.stderr
    assert (tmp_path / "bakery" / "bakery" / "main.py").is_file()


def test_init_install_without_uv_makes_a_venv_and_pip_installs_the_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[list[str], Path]] = []

    def run(command: list[str], *, cwd: Path, check: bool) -> None:
        calls.append((command, cwd))

    monkeypatch.setattr(shutil, "which", lambda *args, **kwargs: None)
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "--install", "--no-skill"])

    assert result.exit_code == 0, result.output
    scripts = "Scripts" if os.name == "nt" else "bin"
    root = tmp_path / "bakery"
    assert calls == [
        ([sys.executable, "-m", "venv", ".venv"], root),
        (
            [str(root / ".venv" / scripts / "python"), "-m", "pip", "install", *REQUIREMENTS],
            root,
        ),
    ]
    activate = r".venv\Scripts\activate" if os.name == "nt" else "source .venv/bin/activate"
    assert f"next:\n  cd bakery\n  {activate}\n  pytest\n" in result.stdout


def test_the_starter_passes_its_own_tests(tmp_path: Path) -> None:
    """Plus one that publishes from its own body, which finds the modules only
    because the starter names its package under `[tool.modulith]`."""
    env = ENV
    created = subprocess.run(
        [sys.executable, "-m", "modulith", "init", "bakery"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert created.returncode == 0, created.stderr
    (tmp_path / "bakery" / "tests" / "test_publish_from_a_test.py").write_text(
        "from bakery.contracts.events import OrderCreated, PaymentReceived\n\n\n"
        "def test_a_test_may_publish(scenario):\n"
        '    scenario.publish(OrderCreated(order_id="ord-9"))'
        ".expect_event(PaymentReceived).within(seconds=2)\n",
        encoding="utf-8",
    )

    tests = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=tmp_path / "bakery",
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert tests.returncode == 0, tests.stdout + tests.stderr
    assert "6 passed" in tests.stdout, tests.stdout


@pytest.mark.parametrize("cwd_importable", [False, True], ids=["console-script", "python-m"])
def test_init_never_writes_into_an_existing_path(
    cwd_importable: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under `python -m modulith` the cwd is on sys.path, so ./bakery is importable
    there; the refusal must still name the directory, not importability."""
    monkeypatch.chdir(tmp_path)
    if cwd_importable:
        monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "bakery" / ".claude" / "skills").mkdir(parents=True)
    (tmp_path / "bakery" / ".claude" / "skills" / "modupy").write_text("mine", encoding="utf-8")

    result = runner.invoke(app, ["init", "bakery"])

    assert result.exit_code == 1
    assert "error: bakery already exists; init never overwrites it" in result.stderr
    assert _files(tmp_path) == {"bakery/.claude/skills/modupy": "mine"}


def test_init_dot_starts_the_project_in_the_current_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh repository keeps what it holds and becomes the project root."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git").mkdir()
    (tmp_path / "README.md").write_text("mine", encoding="utf-8")

    result = runner.invoke(app, ["init", "bakery", "."])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "pyproject.toml").is_file()
    assert (tmp_path / "bakery" / "main.py").is_file()
    assert (tmp_path / "tests" / "test_quickstart_flow.py").is_file()
    assert (tmp_path / "README.md").read_text(encoding="utf-8") == "mine"
    assert "  cd " not in result.stdout
    assert "next:\n  pip install" in result.stdout


def test_init_into_a_new_nested_directory_says_where_to_cd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "apps/shop"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "apps" / "shop" / "bakery" / "main.py").is_file()
    assert f"  cd {Path('apps/shop')}\n" in result.stdout


def test_init_quotes_the_cd_step_for_a_directory_with_a_space(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "bakery", "my shop", "--no-skill"])

    assert result.exit_code == 0, result.output
    assert "  cd 'my shop'\n" in result.stdout


def test_init_keeps_an_earlier_install_of_the_skill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The team may have edited it or added notes; `modulith skill install` is the update."""
    monkeypatch.chdir(tmp_path)
    earlier = tmp_path / ".claude" / "skills" / "modupy"
    shutil.copytree(REPO / "modulith" / "skill", earlier)
    (earlier / "team-notes.md").write_text("ours", encoding="utf-8")
    kept = _files(earlier)

    result = runner.invoke(app, ["init", "bakery", "."])

    assert result.exit_code == 0, result.output
    assert _files(earlier) == kept
    assert f"kept the existing modupy skill in {Path('.claude/skills/modupy')}" in result.stdout
    assert _files(tmp_path / ".agents" / "skills" / "modupy") == _files(REPO / "modulith" / "skill")
    assert f"installed the modupy skill in {Path('.agents/skills/modupy')}\n" in result.stdout


@pytest.mark.parametrize("taken", ["pyproject.toml", "bakery", "tests"])
def test_init_into_a_directory_refuses_one_that_holds_the_project_paths(
    taken: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "README.md").write_text("mine", encoding="utf-8")
    if taken == "pyproject.toml":
        (tmp_path / taken).write_text("[project]\n", encoding="utf-8")
    else:
        (tmp_path / taken).mkdir()
    before = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))

    result = runner.invoke(app, ["init", "bakery", "."])

    assert result.exit_code == 1
    assert f"error: {taken} already exists; init never overwrites it" in result.stderr
    assert sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*")) == before


@pytest.mark.parametrize(
    "name", ["my-app", "MyApp", "2fast", "class", "modulith", "modupy", "tests", "../up"]
)
def test_init_refuses_a_name_that_cannot_be_the_package(
    name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", name])

    assert result.exit_code == 1
    assert f"error: {name!r}" in result.stderr
    assert not list(tmp_path.parent.glob("up")) and not list(tmp_path.iterdir())
