"""Tests for `modulith extract` — scaffolding a standalone service from a module."""

from __future__ import annotations

import importlib
import importlib.machinery
import ntpath
import os
import py_compile
import site
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from modulith import __version__, extract
from modulith.cli import app
from modulith.config import Configuration
from modulith.extract import (
    _render_env_example,
    _render_pyproject,
    _render_readme,
    write_extraction,
)
from modulith.runtime import _runtime

runner = CliRunner()

_ORDERS_MANIFEST = 'from modulith import declare_module\ndeclare_module(owns_tables=["orders"])\n'


def test_extract_happy_path_produces_service_scaffold(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={
            "__init__.py": "",
            "orders/_manifest.py": _ORDERS_MANIFEST,
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "__init__.py").exists()
    assert (out_dir / "fakeapp" / "orders" / "__init__.py").exists()
    assert (out_dir / "fakeapp" / "orders" / "_manifest.py").exists()
    assert (out_dir / "fakeapp" / "contracts").is_dir()
    assert (out_dir / "pyproject.toml").exists()
    assert (out_dir / "Dockerfile").exists()
    assert (out_dir / "README.md").exists()
    assert (out_dir / ".env.example").exists()
    assert not (out_dir / "fakeapp" / "inventory").exists()
    assert (out_dir / "fakeapp" / "__init__.py").read_text() == ""

    parsed = tomllib.loads((out_dir / "pyproject.toml").read_text())
    assert parsed["project"]["version"] == "0.1.0"
    assert parsed["build-system"]["build-backend"] == "hatchling.build"
    assert parsed["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["fakeapp"]
    assert parsed["tool"]["modulith"]["package"] == "fakeapp"
    deps = parsed["project"]["dependencies"]
    modupy_dep = next(d for d in deps if d.startswith("modupy["))
    assert modupy_dep.endswith(f"=={__version__}")

    docker = (out_dir / "Dockerfile").read_text()
    assert "modulith._worker:create_app" in docker
    assert "0.0.0.0" in docker


def test_extract_import_check_leaves_no_bytecode_in_the_output(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={"__init__.py": "", "contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert sorted(p.relative_to(out_dir) for p in out_dir.rglob("*.pyc")) == []


_IN_GATE = 'import sys\nIN_GATE = sys.argv[:1] == ["-c"]\n'


def test_import_gate_returns_when_a_descendant_holds_the_pipes(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setattr(extract, "_IMPORT_CHECK_TIMEOUT", 5)
    make_fake_app(
        {
            "orders": _IN_GATE
            + "import subprocess\n"
            + "if IN_GATE:\n"
            + "    subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(12)'])\n"
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "orders" / "__init__.py").is_file()


def test_extract_import_gate_side_effects_do_not_reach_the_output(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": _IN_GATE
            + "from pathlib import Path\n"
            + "if IN_GATE:\n"
            + "    (Path(__file__).parent / 'beside_module.txt').write_text('x')\n"
            + "    Path('in_cwd.txt').write_text('x')\n"
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out_dir.rglob("*.txt")) == []
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith(".")) == []


@pytest.mark.skipif(os.name == "nt", reason="the child interrupts its parent with SIGINT")
def test_extract_interrupt_removes_the_staging_directory(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": _IN_GATE
            + "import os, signal, time\n"
            + "if IN_GATE:\n"
            + "    os.kill(os.getppid(), signal.SIGINT)\n"
            + "    time.sleep(30)\n"
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code != 0, result.output
    assert not out_dir.exists()
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith(".")) == []


def test_extract_generated_project_builds_a_wheel(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])
    assert result.exit_code == 0, result.output

    dist_dir = tmp_path / "dist"
    build = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(dist_dir),
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PIP_NO_INDEX": "1"},
    )
    assert build.returncode == 0, build.stdout + build.stderr
    assert len(list(dist_dir.glob("*.whl"))) == 1


def test_extract_preserves_dotted_application_package(monkeypatch, request, tmp_path):
    package_dir = tmp_path / "company" / "fakeapp"
    (package_dir / "orders").mkdir(parents=True)
    (package_dir / "contracts").mkdir()
    for init in (
        tmp_path / "company" / "__init__.py",
        package_dir / "__init__.py",
        package_dir / "orders" / "__init__.py",
        package_dir / "contracts" / "__init__.py",
    ):
        init.write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("MODULITH_PACKAGE", "company.fakeapp")

    def reset_dotted_app() -> None:
        for name in list(sys.modules):
            if name == "company" or name.startswith("company."):
                del sys.modules[name]
        _runtime._reset_for_testing()

    request.addfinalizer(reset_dotted_app)
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "company" / "__init__.py").read_text() == ""
    assert (out_dir / "company" / "fakeapp" / "orders" / "__init__.py").exists()
    assert not (out_dir / "fakeapp").exists()
    assert "COPY company/ ./company/" in (out_dir / "Dockerfile").read_text()
    assert "MODULITH_APP_PACKAGE=company.fakeapp" in (out_dir / ".env.example").read_text()
    parsed = tomllib.loads((out_dir / "pyproject.toml").read_text())
    assert parsed["tool"]["modulith"]["package"] == "company.fakeapp"


def test_generated_toml_quotes_dynamic_keys_and_omits_none():
    cfg = Configuration(
        package="fakeapp",
        broker_options={"routing.key": "orders", "optional": None},
        subscriptions={"orders.v2": ["inventory"]},
        observability=None,
        explicit_keys=frozenset({"observability"}),
    )

    parsed = tomllib.loads(_render_pyproject(cfg=cfg, module="orders.v2", source_deps=[]))

    assert parsed["tool"]["modulith"]["broker_options"] == {"routing.key": "orders"}
    assert parsed["tool"]["modulith"]["subscriptions"] == {"orders.v2": ["inventory"]}
    assert "observability" not in parsed["tool"]["modulith"]


def _readme_section(readme: str, heading: str) -> str:
    _, _, tail = readme.partition(f"## {heading}\n")
    return tail.split("\n## ", 1)[0]


def _extracted_readme() -> str:
    return _render_readme(
        cfg=Configuration(package="fakeapp"),
        module="orders",
        pkg_name="fakeapp",
        helpers=[],
        notes=[],
    )


def test_extract_env_example_configures_the_runtime_outbox_and_scopes_the_db_url():
    text = _render_env_example(
        cfg=Configuration(package="fakeapp"), module="orders", package="fakeapp"
    )
    lines = text.splitlines()

    assert "MODULITH_OUTBOX=" in lines
    assert "MODULITH_OUTBOX_URL=" in lines
    assert lines[lines.index("MODULITH_DB_URL=") - 1].startswith("# Alembic migrations only")


def test_extract_readme_lists_the_outbox_variables_and_scopes_the_db_url():
    table = _readme_section(_extracted_readme(), "Environment variables")

    assert "| `MODULITH_OUTBOX` |" in table
    assert "| `MODULITH_OUTBOX_URL` |" in table
    db_row = next(line for line in table.splitlines() if line.startswith("| `MODULITH_DB_URL`"))
    assert "Alembic" in db_row
    assert "outbox" not in db_row.lower()


def test_extract_readme_says_the_worker_binds_its_store_from_the_url_and_refuses_without_one():
    outbox = _readme_section(_extracted_readme(), "Outbox")

    assert "`MODULITH_OUTBOX_URL`" in outbox
    assert "refuses to start" in outbox
    assert "outbox.configure" not in outbox
    assert "not auto-wired" not in outbox


def test_extract_readme_shows_a_local_run_through_the_supported_command():
    run_locally = _readme_section(_extracted_readme(), "Run locally")

    assert "modulith run fakeapp:app --topology processes" in run_locally


def test_database_broker_postgres_migration_has_sync_driver_and_packaged_config(monkeypatch):
    monkeypatch.setenv("MODULITH_BROKER_URL", "postgresql+asyncpg://db/orders")
    postgres_cfg = Configuration(
        package="fakeapp",
        broker="database",
    )
    postgres_project = tomllib.loads(
        _render_pyproject(cfg=postgres_cfg, module="orders", source_deps=[])
    )

    monkeypatch.delenv("MODULITH_BROKER_URL")
    sqlite_cfg = Configuration(
        package="fakeapp",
        broker="database",
        broker_options={"url": "sqlite+aiosqlite:///broker.db"},
    )
    sqlite_project = tomllib.loads(
        _render_pyproject(cfg=sqlite_cfg, module="orders", source_deps=[])
    )
    postgres_deps = postgres_project["project"]["dependencies"]
    sqlite_deps = sqlite_project["project"]["dependencies"]
    assert "psycopg[binary]>=3.1,<4.0" in postgres_deps
    assert "psycopg[binary]>=3.1,<4.0" not in sqlite_deps

    readme = _render_readme(
        cfg=postgres_cfg, module="orders", pkg_name="fakeapp", helpers=[], notes=[]
    )
    assert "modulith.adapters.__file__" in readme
    assert "-x schema=orders upgrade head" in readme
    assert "PostgreSQL requires `psycopg[binary]`" in readme


def test_render_pyproject_preserves_modupy_extras_from_source():
    """Extras from source project's modupy are merged and sorted in extracted pyproject."""
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
    )
    source_deps = ["modupy[postgres,otel]>=0.10"]

    rendered = _render_pyproject(cfg=cfg, module="orders", source_deps=source_deps)
    parsed = tomllib.loads(rendered)

    deps = parsed["project"]["dependencies"]
    modupy_dep = next(d for d in deps if d.startswith("modupy["))

    expected = f"modupy[cli,fastapi,otel,postgres,redis]=={__version__}"
    assert modupy_dep == expected


def test_render_pyproject_handles_source_without_modupy_extras():
    """Source projects without modupy extras produce unchanged behavior."""
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
    )
    source_deps = ["modupy>=0.10"]

    rendered = _render_pyproject(cfg=cfg, module="orders", source_deps=source_deps)
    parsed = tomllib.loads(rendered)

    deps = parsed["project"]["dependencies"]
    modupy_dep = next(d for d in deps if d.startswith("modupy["))

    expected = f"modupy[cli,fastapi,redis]=={__version__}"
    assert modupy_dep == expected


def test_render_pyproject_no_source_modupy_dependency():
    """When source has no modupy dependency, output is unchanged."""
    cfg = Configuration(
        package="fakeapp",
        broker="redis-streams",
    )
    source_deps = ["requests", "click"]

    rendered = _render_pyproject(cfg=cfg, module="orders", source_deps=source_deps)
    parsed = tomllib.loads(rendered)

    deps = parsed["project"]["dependencies"]
    modupy_dep = next(d for d in deps if d.startswith("modupy["))

    expected = f"modupy[cli,fastapi,redis]=={__version__}"
    assert modupy_dep == expected
    assert "requests" in deps
    assert "click" in deps


def test_render_pyproject_filters_modupy_by_requirement_name():
    source_deps = [
        "Modupy[otel]>=0.10",
        "modupy-extras>=1",
        "modupy_tools",
        "MODUPY",
        "requests",
    ]

    deps = tomllib.loads(
        _render_pyproject(
            cfg=Configuration(package="fakeapp"), module="orders", source_deps=source_deps
        )
    )["project"]["dependencies"]

    assert [dep for dep in deps if dep.startswith("modupy[")] == [
        f"modupy[cli,fastapi,otel]=={__version__}"
    ]
    assert deps[1:] == ["modupy-extras>=1", "modupy_tools", "requests"]


def test_render_pyproject_keeps_verify_disabled_rules():
    cfg = Configuration(
        package="fakeapp",
        verify_disabled_rules=("no-cyclic-dependency", "parse-error"),
        explicit_keys=frozenset({"verify_disabled_rules"}),
    )

    parsed = tomllib.loads(_render_pyproject(cfg=cfg, module="orders", source_deps=[]))

    assert parsed["tool"]["modulith"]["verify"] == {
        "disabled_rules": ["no-cyclic-dependency", "parse-error"]
    }


@pytest.mark.parametrize(
    "cfg",
    [
        Configuration(package="fakeapp"),
        Configuration(package="fakeapp", explicit_keys=frozenset({"verify_disabled_rules"})),
    ],
    ids=["unset", "explicit-empty"],
)
def test_render_pyproject_omits_verify_table_without_disabled_rules(cfg):
    parsed = tomllib.loads(_render_pyproject(cfg=cfg, module="orders", source_deps=[]))

    assert "verify" not in parsed["tool"]["modulith"]


_DEPENDENCY_SOURCE_NOTE = "`[project].dependencies` only"


@pytest.mark.parametrize(
    "pyproject",
    [
        '[project]\nname = "app"\nversion = "0"\ndynamic = ["dependencies"]\n',
        '[project]\nname = "app"\nversion = "0"\n\n[tool.poetry.dependencies]\nrequests = "^2"\n',
    ],
    ids=["dynamic-dependencies", "poetry"],
)
def test_extract_notes_when_source_dependencies_live_outside_project_dependencies(
    make_fake_app, monkeypatch, tmp_path, pyproject
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(pyproject)
    make_fake_app({"orders": ""}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    notes = _readme_section((out_dir / "README.md").read_text(), "Extraction notes")
    assert sum(_DEPENDENCY_SOURCE_NOTE in line for line in notes.splitlines()) == 1, notes


def test_extract_has_no_dependency_source_note_for_plain_project_dependencies(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0"\ndynamic = ["version"]\n'
        'dependencies = ["requests"]\n'
    )
    make_fake_app({"orders": ""}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert _DEPENDENCY_SOURCE_NOTE not in (out_dir / "README.md").read_text()


def test_extract_unknown_module_exits_one_and_lists_modules(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""})

    result = runner.invoke(app, ["extract", "nope", "--output", str(tmp_path / "out")])

    assert result.exit_code == 1
    assert "orders" in result.output
    assert "inventory" in result.output


def test_extract_boundary_violation_blocks_and_force_overrides(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    # orders reaches into inventory's private package — a hard boundary break.
    # The import is deferred so the forced extraction still imports.
    make_fake_app(
        {
            "orders": "def use():\n    from fakeapp.inventory._internal import secret\n    return secret\n",
            "inventory": "",
        },
        extra_files={
            "inventory/_internal.py": "secret = 1\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    blocked = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])
    assert blocked.exit_code == 1, blocked.output
    assert "no-internal-imports" in blocked.output

    forced = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])
    assert forced.exit_code == 0, forced.output
    readme = (out_dir / "README.md").read_text()
    assert "Extraction notes" in readme


def test_extract_force_under_strict_boundaries_extracts_with_notes(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    make_fake_app(
        {
            "orders": "def use():\n    from fakeapp.inventory._internal import secret\n    return secret\n",
            "inventory": "",
        },
        extra_files={
            "inventory/_internal.py": "secret = 1\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    forced = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])

    assert forced.exit_code == 0, forced.output
    assert "Extraction notes" in (out_dir / "README.md").read_text()


def test_extract_shared_table_blocks(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={
            "orders/models.py": 'from sqlalchemy import Table\nstock = Table("stock")\n',
            "inventory/models.py": 'from sqlalchemy import Table\nstock2 = Table("stock")\n',
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "stock" in result.output


def test_extract_blocked_when_shared_table_scan_has_parse_failures(
    make_fake_app, monkeypatch, tmp_path
):
    """A syntax error in an unrelated file must not silently shrink the
    shared-table scan to an empty result — the parse failure itself has to
    surface as a blocker, since the scan's soundness can't be established."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={
            "orders/models.py": 'from sqlalchemy import Table\nstock = Table("stock")\n',
            "inventory/models.py": 'from sqlalchemy import Table\nstock2 = Table("stock"\n',
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "could not be parsed" in result.output
    assert not out_dir.exists()


def test_extract_succeeds_for_a_module_holding_a_bom_file(make_fake_app, monkeypatch, tmp_path):
    """The interpreter reads a UTF-8 BOM file, so the shared-table scan must too:
    an unparsed file would block extraction as an incomplete scan."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""}, extra_files={"contracts/__init__.py": ""})
    legacy = b"\xef\xbb\xbf" + 'NAME = "café"\n'.encode()
    (tmp_path / "fakeapp" / "orders" / "legacy.py").write_bytes(legacy)
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "orders" / "legacy.py").read_bytes() == legacy


def test_extract_contracts_table_not_treated_as_shared(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={
            "orders/models.py": 'from sqlalchemy import Table\nt = Table("lookup_codes")\n',
            "contracts/models.py": 'from sqlalchemy import Table\nt2 = Table("lookup_codes")\n',
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output


def test_write_extraction_rejects_path_traversal_module_name(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "orders-service"

    with pytest.raises(ValueError, match="module"):
        write_extraction(
            cfg=Configuration(package="fakeapp"),
            module="../../evil",
            package_dir=tmp_path / "fakeapp",
            output=output,
            helpers=[],
            notes=[],
        )

    assert not output.exists()


def test_write_extraction_rejects_absolute_module_name(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "orders-service"
    canary = Path("/tmp/modulith-security1-canary")

    with pytest.raises(ValueError, match="module"):
        write_extraction(
            cfg=Configuration(package="fakeapp"),
            module=str(canary),
            package_dir=tmp_path / "fakeapp",
            output=output,
            helpers=[],
            notes=[],
        )

    assert not canary.exists()
    assert not output.exists()


def test_write_extraction_rejects_path_traversal_helper_name(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "orders-service"

    with pytest.raises(ValueError, match="helper"):
        write_extraction(
            cfg=Configuration(package="fakeapp"),
            module="orders",
            package_dir=tmp_path / "fakeapp",
            output=output,
            helpers=["fakeapp.../../evil"],
            notes=[],
        )

    assert not output.exists()


def test_extract_nonempty_output_dir_blocks_even_with_force(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"
    out_dir.mkdir()
    (out_dir / "stray.txt").write_text("x")

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])

    assert result.exit_code == 1, result.output


def test_extract_preserves_support_for_existing_empty_output(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "orders-service"
    output.mkdir()

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert (output / "fakeapp" / "orders" / "__init__.py").exists()


def test_extract_rejects_existing_file_without_traceback(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "existing"
    output.write_text("keep")

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert output.read_text() == "keep"


def test_extract_rejects_unwritable_output_shape_without_traceback(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    parent_file = tmp_path / "not-a-directory"
    parent_file.write_text("keep")

    result = runner.invoke(
        app, ["extract", "orders", "--output", str(parent_file / "orders-service")]
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert parent_file.read_text() == "keep"


def test_extract_rejects_output_inside_source_package(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "fakeapp" / "generated-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "inside source package" in result.output
    assert not output.exists()


def test_extract_rejects_output_symlink(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    output = tmp_path / "orders-service"
    output.symlink_to(redirected, target_is_directory=True)

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "symlink" in result.output
    assert not any(redirected.iterdir())


def test_extract_rejects_source_symlink_escaping_package(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    secret = tmp_path / "credentials.txt"
    secret.write_text("do-not-copy")
    (tmp_path / "fakeapp" / "orders" / "credentials.txt").symlink_to(secret)
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "source symlink" in result.output
    assert not output.exists()


def test_extract_rejects_source_symlink_within_package(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""}, extra_files={"shared.py": "value = 1\n"})
    (tmp_path / "fakeapp" / "orders" / "shared.py").symlink_to(tmp_path / "fakeapp" / "shared.py")
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "source symlink" in result.output
    assert not output.exists()


def test_extract_rejects_symlinked_source_package(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    package_dir = tmp_path / "fakeapp"
    real_package_dir = tmp_path / "real-fakeapp"
    package_dir.rename(real_package_dir)
    package_dir.symlink_to(real_package_dir, target_is_directory=True)
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "source package" in result.output
    assert "symlink" in result.output
    assert not output.exists()


def test_extract_rejects_nontrivial_root_package_initializer(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={"__init__.py": "from . import inventory\n"},
    )
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "__init__.py" in result.output
    assert "move" in result.output
    assert not output.exists()


def test_extract_accepts_future_import_in_root_initializer(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": ""},
        extra_files={
            "__init__.py": '"""Shop."""\nfrom __future__ import annotations\n',
            "contracts/__init__.py": "",
        },
    )
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert (output / "fakeapp" / "__init__.py").read_text() == ""


def test_extract_still_rejects_other_statements_beside_a_future_import(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": ""},
        extra_files={"__init__.py": 'from __future__ import annotations\n__version__ = "1"\n'},
    )
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "__init__.py" in result.output
    assert "move" in result.output
    assert not output.exists()


def test_extract_rejects_nontrivial_dotted_package_ancestor(monkeypatch, request, tmp_path):
    package_dir = tmp_path / "company" / "fakeapp"
    (package_dir / "orders").mkdir(parents=True)
    (tmp_path / "company" / "__init__.py").write_text("from . import fakeapp\n")
    (package_dir / "__init__.py").write_text("")
    (package_dir / "orders" / "__init__.py").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("MODULITH_PACKAGE", "company.fakeapp")

    def reset_dotted_app() -> None:
        for name in list(sys.modules):
            if name == "company" or name.startswith("company."):
                del sys.modules[name]
        _runtime._reset_for_testing()

    request.addfinalizer(reset_dotted_app)
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "company/__init__.py" in result.output
    assert "move" in result.output
    assert not output.exists()


def test_extract_rejects_nontrivial_nested_helper_package_initializer(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.shared.nested.helper import VALUE\n"},
        extra_files={
            "shared/__init__.py": "VALUE = 1\n",
            "shared/nested/__init__.py": "",
            "shared/nested/helper.py": "VALUE = 2\n",
        },
    )
    output = tmp_path / "orders-service"

    with pytest.raises(ValueError, match=r"shared/__init__\.py.*move"):
        write_extraction(
            cfg=Configuration(package="fakeapp"),
            module="orders",
            package_dir=tmp_path / "fakeapp",
            output=output,
            helpers=["fakeapp.shared.nested.helper"],
            notes=[],
        )

    assert not output.exists()


def test_extract_rejects_contracts_module_path_before_writing(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\ncontracts_module = "/modupy-contracts-escape-target"\n'
    )
    output = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(output)])

    assert result.exit_code == 1
    assert "contracts_module" in result.output
    assert not output.exists()


def test_extract_smoke_extracted_app_boots(make_fake_app, monkeypatch, tmp_path):
    """End-to-end: the extracted tree actually boots via modulith._worker.

    Re-imports the extracted copy under the same package name the fixture
    already imported once in this process, so sys.modules and the runtime
    singleton are purged first — mirroring the teardown make_fake_app itself
    performs, just run mid-test instead of at fixture teardown.
    """
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={
            "orders/_manifest.py": _ORDERS_MANIFEST,
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])
    assert result.exit_code == 0, result.output

    for name in list(sys.modules):
        if name == "fakeapp" or name.startswith("fakeapp."):
            del sys.modules[name]
    _runtime._reset_for_testing()

    monkeypatch.chdir(out_dir)
    monkeypatch.syspath_prepend(str(out_dir))
    monkeypatch.setenv("MODULITH_MODULE", "orders")
    monkeypatch.setenv("MODULITH_APP_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "test-noop-broker")

    from modulith._worker import create_app

    worker_app = create_app()

    assert worker_app.title == "modulith-orders"


def test_write_extraction_rejects_nonexistent_module(make_fake_app, monkeypatch, tmp_path):
    """Extraction must validate that the module directory exists before writing."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "nonexistent-service"

    with pytest.raises(ValueError, match="nonexistent"):
        write_extraction(
            cfg=Configuration(package="fakeapp"),
            module="nonexistent",
            package_dir=tmp_path / "fakeapp",
            output=output,
            helpers=[],
            notes=[],
        )

    assert not output.exists()


def test_extract_copies_import_closure_of_module_contracts_and_helpers(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.util import fmt\n", "inventory": ""},
        extra_files={
            "util.py": "from fakeapp.money import cents\n\ndef fmt():\n    return cents\n",
            "money.py": "cents = 100\n",
            "contracts/__init__.py": "",
            "contracts/events.py": "from fakeapp.types import Currency\n",
            "types.py": "Currency = str\n",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    for rel in ("util.py", "money.py", "types.py", "contracts/events.py"):
        assert (out_dir / "fakeapp" / rel).is_file(), rel
    assert not (out_dir / "fakeapp" / "inventory").exists()
    readme = (out_dir / "README.md").read_text()
    assert "- `fakeapp.money`" in readme
    assert "- `fakeapp.types`" in readme
    assert "- `fakeapp.util`" in readme


def test_extract_copies_helper_imported_from_bare_package(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp import telemetry, serialization\n"},
        extra_files={
            "telemetry.py": "TRACE = True\n",
            "serialization.py": "",
            "unused.py": "",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "telemetry.py").read_text() == "TRACE = True\n"
    assert (out_dir / "fakeapp" / "serialization.py").is_file()
    assert not (out_dir / "fakeapp" / "unused.py").exists()
    readme = (out_dir / "README.md").read_text()
    assert "- `fakeapp.telemetry`" in readme
    assert "- `fakeapp.serialization`" in readme
    assert "- `fakeapp`\n" not in readme


def test_extract_blocks_import_of_another_declared_module_and_force_overrides(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": "from fakeapp.util import fmt\n",
            "inventory": "def reserve():\n    return 1\n",
        },
        extra_files={
            "util.py": "def fmt():\n    from fakeapp.inventory import reserve\n    return reserve()\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    blocked = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])
    assert blocked.exit_code == 1, blocked.output
    assert "imports declared module(s): inventory" in blocked.output
    assert not out_dir.exists()

    forced = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])
    assert forced.exit_code == 0, forced.output
    assert not (out_dir / "fakeapp" / "inventory").exists()
    assert "imports declared module(s): inventory" in (out_dir / "README.md").read_text()


@pytest.mark.parametrize("guard", ["False", "0"])
def test_extract_ignores_sibling_import_under_constant_false_if(
    make_fake_app, monkeypatch, tmp_path, guard
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": f"if {guard}:\n    from fakeapp.inventory import reserve\n",
            "inventory": "def reserve():\n    return 1\n",
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert not (out_dir / "fakeapp" / "inventory").exists()
    assert "imports declared module(s)" not in (out_dir / "README.md").read_text()


def test_extract_still_blocks_sibling_import_under_version_guard_and_force_overrides(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "import sys\n"
                "if sys.version_info < (3, 0):\n"
                "    from fakeapp.inventory import reserve\n"
            ),
            "inventory": "def reserve():\n    return 1\n",
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    blocked = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])
    assert blocked.exit_code == 1, blocked.output
    assert "imports declared module(s): inventory" in blocked.output

    forced = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])
    assert forced.exit_code == 0, forced.output
    assert "imports declared module(s): inventory" in (out_dir / "README.md").read_text()


def test_extract_follows_relative_dynamic_import_with_literal_package(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "import importlib\n"
                "def load():\n"
                '    return importlib.import_module(".helper", package="fakeapp")\n'
            ),
        },
        extra_files={"helper.py": "VALUE = 1\n", "unused.py": "", "contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "helper.py").read_text() == "VALUE = 1\n"
    assert not (out_dir / "fakeapp" / "unused.py").exists()
    readme = (out_dir / "README.md").read_text()
    assert "- `fakeapp.helper`" in readme
    assert "Extraction notes" not in readme


def test_extract_blocks_relative_dynamic_import_of_another_declared_module(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "import importlib\n"
                "def load():\n"
                '    return importlib.import_module(".inventory", package="fakeapp")\n'
            ),
            "inventory": "",
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "imports declared module(s): inventory" in result.output


def test_extract_readme_lists_dynamic_imports_it_cannot_resolve(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "import importlib\n"
                "from importlib import import_module\n"
                "def load(name, pkg):\n"
                '    importlib.import_module("fakeapp." + name)\n'
                "    __import__(name)\n"
                '    import_module(".x", package=pkg)\n'
                '    importlib.import_module(".y")\n'
                '    importlib.import_module("json")\n'
                "if False:\n"
                "    importlib.import_module(name)\n"
            ),
        },
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    notes = _readme_section((out_dir / "README.md").read_text(), "Extraction notes")
    bullets = [line for line in notes.splitlines() if line.startswith("- ")]
    assert len(bullets) == 4, notes
    for bullet, line, call in zip(
        bullets,
        (4, 5, 6, 7),
        (
            "importlib.import_module('fakeapp.' + name)",
            "__import__(name)",
            "import_module('.x', package=pkg)",
            "importlib.import_module('.y')",
        ),
        strict=True,
    ):
        assert f"`fakeapp/orders/__init__.py:{line}`" in bullet, bullet
        assert call in bullet, bullet
    assert "--force" not in notes


def test_extract_force_notes_and_dynamic_import_notes_share_one_section(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "import importlib\n"
                "def use(name):\n"
                "    from fakeapp.inventory._internal import secret\n"
                "    return importlib.import_module(name), secret\n"
            ),
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n", "contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])

    assert result.exit_code == 0, result.output
    readme = (out_dir / "README.md").read_text()
    assert readme.count("## Extraction notes") == 1
    notes = _readme_section(readme, "Extraction notes")
    assert "no-internal-imports" in notes
    assert "importlib.import_module(name)" in notes


def test_extract_fails_when_extracted_module_does_not_import(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": 'import importlib\nimportlib.import_module("fakeapp." + "hidden")\n'},
        extra_files={"hidden.py": "", "contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "fakeapp.orders" in result.output
    assert "No module named 'fakeapp.hidden'" in result.output
    assert not out_dir.exists()
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".orders-service.")] == []


def test_extract_module_importing_contracts_needs_no_force(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.contracts.events import OrderPlaced\n"},
        extra_files={
            "contracts/__init__.py": "",
            "contracts/events.py": "from fakeapp.contracts.base import Base\nOrderPlaced = Base\n",
            "contracts/base.py": "Base = object\n",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "contracts" / "events.py").is_file()
    assert (out_dir / "fakeapp" / "contracts" / "base.py").is_file()
    assert "Extraction notes" not in (out_dir / "README.md").read_text()


_HIDDEN_IMPORT = 'import importlib\nimportlib.import_module("fakeapp." + "hidden")\n'


def test_extract_gate_imports_the_manifest_the_worker_imports(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": ""},
        extra_files={
            "orders/_manifest.py": _HIDDEN_IMPORT,
            "hidden.py": "",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "No module named 'fakeapp.hidden'" in result.output
    assert not out_dir.exists()


def test_extract_gate_imports_the_contracts_the_worker_imports(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": ""},
        extra_files={"contracts/__init__.py": _HIDDEN_IMPORT, "hidden.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "No module named 'fakeapp.hidden'" in result.output
    assert not out_dir.exists()


def test_extract_gate_tolerates_an_app_without_contracts_or_manifest(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert not (out_dir / "fakeapp" / "contracts").exists()
    assert not (out_dir / "fakeapp" / "orders" / "_manifest.py").exists()


def test_extract_copies_a_sourceless_helper_the_module_imports(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp import compiled\nVALUE = compiled.VALUE\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    source = tmp_path / "compiled_source.py"
    source.write_text("VALUE = 7\n")
    py_compile.compile(str(source), cfile=str(tmp_path / "fakeapp" / "compiled.pyc"), doraise=True)
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "compiled.pyc").is_file()
    assert "`fakeapp.compiled`" in (out_dir / "README.md").read_text()


def test_extract_copies_an_extension_module_helper_the_module_imports(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "try:\n    from fakeapp import fast\nexcept ImportError:\n    fast = None\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    extension = f"fast{importlib.machinery.EXTENSION_SUFFIXES[0]}"
    (tmp_path / "fakeapp" / extension).write_bytes(b"not a real extension module")
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / extension).read_bytes() == b"not a real extension module"
    assert "`fakeapp.fast`" in (out_dir / "README.md").read_text()


def test_extract_warns_about_imported_but_undeclared_distribution(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.chdir(tmp_path)
    make_fake_app({"orders": "import pytest\n"}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    notes = _readme_section((out_dir / "README.md").read_text(), "Extraction notes")
    listed = [line for line in notes.splitlines() if line.startswith("- ")]
    assert "- `pytest`" in listed, notes
    assert "- `pluggy`" not in listed, notes  # a core modupy dependency
    assert "pytest" in result.stderr
    assert "Extraction notes" in result.stderr


def test_extract_stays_quiet_about_distributions_the_dependencies_cover(
    make_fake_app, monkeypatch, tmp_path
):
    pytest.importorskip("fastapi")
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0"\ndependencies = ["pytest>=8"]\n'
    )
    make_fake_app(
        {"orders": "import pytest\nfrom fastapi import FastAPI\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert "Extraction notes" not in (out_dir / "README.md").read_text()
    assert result.stderr == ""


def test_extract_copies_helper_file_beside_same_named_non_package_dir(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.emails import send\n"},
        extra_files={
            "emails.py": "def send():\n    return 1\n",
            "emails/welcome.html": "<p>hi</p>\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "emails.py").read_text() == "def send():\n    return 1\n"
    assert not (out_dir / "fakeapp" / "emails").exists()


def test_extract_copies_helper_package_whose_initializer_does_work(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp._shared.fmt import fmt\n"},
        extra_files={
            "_shared/__init__.py": "from .fmt import fmt\n",
            "_shared/fmt.py": "def fmt():\n    return 1\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    shared = out_dir / "fakeapp" / "_shared"
    assert (shared / "__init__.py").read_text() == "from .fmt import fmt\n"
    assert (shared / "fmt.py").is_file()


def test_extract_leaves_imports_naming_no_source_file_to_the_import_gate(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": (
                "from fakeapp._ns import mod\n"
                "try:\n"
                "    import fakeapp._generated\n"
                "except ImportError:\n"
                "    pass\n"
            )
        },
        extra_files={"_ns/mod.py": "VALUE = 1\n", "contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "_ns" / "mod.py").read_text() == "VALUE = 1\n"
    assert not (out_dir / "fakeapp" / "_generated.py").exists()


def test_extract_keeps_a_namespace_helper_folder_out_of_module_discovery(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.contracts import Money\n", "inventory": ""},
        extra_files={
            "contracts/__init__.py": "from fakeapp.common.money import Money\n",
            "common/money.py": "class Money:\n    pass\n",
        },
    )
    (tmp_path / "pyproject.toml").write_text("[tool.modulith]\nstrict_boundaries = true\n")
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "common" / "money.py").is_file()
    assert not (out_dir / "fakeapp" / "common" / "__init__.py").exists()
    monolith_verify = runner.invoke(app, ["verify"])
    assert monolith_verify.exit_code == 0, monolith_verify.output
    from modulith.builtin.discovery import modulith_discover_modules

    monolith_modules = sorted(m.name for m in modulith_discover_modules("fakeapp"))
    assert monolith_modules == ["contracts", "inventory", "orders"]

    for name in list(sys.modules):
        if name == "fakeapp" or name.startswith("fakeapp."):
            del sys.modules[name]
    _runtime._reset_for_testing()
    monkeypatch.chdir(out_dir)
    monkeypatch.syspath_prepend(str(out_dir))

    extracted_verify = runner.invoke(app, ["verify"])

    assert extracted_verify.exit_code == 0, extracted_verify.output
    assert "contracts-is-sink" not in extracted_verify.output
    extracted_modules = sorted(m.name for m in modulith_discover_modules("fakeapp"))
    assert extracted_modules == ["contracts", "orders"]
    assert Path(sys.modules["fakeapp"].__file__ or "").is_relative_to(out_dir)


def test_write_extraction_mirrors_the_source_initializers_of_helper_parents(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": ""},
        extra_files={
            "shared/__init__.py": '"""Shared helpers."""\n',
            "shared/fmt.py": "VALUE = 1\n",
            "common/money.py": "VALUE = 2\n",
            "contracts/__init__.py": "",
        },
    )
    output = tmp_path / "orders-service"

    write_extraction(
        cfg=Configuration(package="fakeapp"),
        module="orders",
        package_dir=tmp_path / "fakeapp",
        output=output,
        helpers=["fakeapp.common.money", "fakeapp.shared.fmt"],
        notes=[],
    )

    assert (output / "fakeapp" / "shared" / "__init__.py").read_text() == ""
    assert (output / "fakeapp" / "shared" / "fmt.py").read_text() == "VALUE = 1\n"
    assert not (output / "fakeapp" / "common" / "__init__.py").exists()
    assert (output / "fakeapp" / "common" / "money.py").read_text() == "VALUE = 2\n"


def test_extract_copies_helpers_imported_by_a_namespace_contracts_package(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.contracts.events import OrderPlaced\n", "inventory": ""},
        extra_files={
            "contracts/events.py": (
                "from fakeapp._money import Money\n\nclass OrderPlaced:\n    amount: Money\n"
            ),
            "_money.py": "Money = int\n",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "_money.py").read_text() == "Money = int\n"
    assert (out_dir / "fakeapp" / "contracts" / "events.py").is_file()
    assert not (out_dir / "fakeapp" / "contracts" / "__init__.py").exists()
    assert "- `fakeapp._money`" in (out_dir / "README.md").read_text()


def test_extract_import_gate_refuses_first_party_code_from_the_source_tree(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from common.money import cents\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    (tmp_path / "common").mkdir()
    (tmp_path / "common" / "__init__.py").write_text("")
    (tmp_path / "common" / "money.py").write_text("cents = 100\n")
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "common.money" in result.output
    assert "outside the extracted service" in result.output
    assert "or declare it as a dependency" in result.output
    assert not out_dir.exists()


def _installed_sibling(tree: Path, package: str, distribution: str) -> None:
    """Lay out *package* beside the app with the dist-info of an installed *distribution*."""
    (tree / package).mkdir()
    (tree / package / "__init__.py").write_text("")
    dist_info = tree / f"{distribution.replace('-', '_')}-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {distribution}\nVersion: 1.0\n"
    )
    (dist_info / "top_level.txt").write_text(f"{package}\n")


@pytest.mark.parametrize(
    ("declared", "expect_refused"),
    [(["Common_Lib>=1"], False), ([], True), (["other-lib"], True)],
    ids=["declared", "not-declared", "other-distribution-declared"],
)
def test_extract_import_gate_exempts_a_distribution_declared_as_a_dependency(
    make_fake_app, monkeypatch, tmp_path, declared, expect_refused
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "import common_lib\n"}, extra_files={"contracts/__init__.py": ""})
    _installed_sibling(tmp_path, "common_lib", "common-lib")
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "app"\nversion = "0"\ndependencies = {declared!r}\n'.replace("'", '"')
    )
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    if expect_refused:
        assert result.exit_code == 1, result.output
        assert "imported common_lib from the source tree" in result.output
        assert not out_dir.exists()
    else:
        assert result.exit_code == 0, result.output
        assert (out_dir / "fakeapp" / "orders" / "__init__.py").is_file()
        assert not (out_dir / "common_lib").exists()
        assert "Common_Lib>=1" in (out_dir / "pyproject.toml").read_text()


def test_import_gate_path_check_ignores_case_on_windows_paths():
    namespace: dict[str, object] = {"os": SimpleNamespace(path=ntpath)}
    exec(extract._IMPORT_CHECK_UNDER, namespace)
    under = namespace["under"]
    assert callable(under)

    assert under(r"C:\Src\app\mod.py", r"c:\src\app") is True
    assert under(r"c:/src/app/mod.py", r"C:\SRC\App") is True
    assert under(r"C:\Src\apple\mod.py", r"c:\src\app") is False


def test_import_gate_path_check_uses_the_platform_normcase(monkeypatch):
    seen: list[str] = []

    def upper(path: str) -> str:
        seen.append(path)
        return path.upper()

    namespace: dict[str, object] = {
        "os": SimpleNamespace(path=SimpleNamespace(commonpath=os.path.commonpath, normcase=upper))
    }
    exec(extract._IMPORT_CHECK_UNDER, namespace)
    under = namespace["under"]
    assert callable(under)

    assert under("/Src/App/mod.py", "/src/app") is True
    assert sorted(seen) == ["/Src/App/mod.py", "/src/app"]


def test_import_gate_path_check_treats_another_drive_as_outside():
    namespace: dict[str, object] = {"os": SimpleNamespace(path=ntpath)}
    exec(extract._IMPORT_CHECK_UNDER, namespace)
    under = namespace["under"]
    assert callable(under)

    assert under(r"C:\Python313\Lib\os.py", r"D:\proj") is False
    assert under(r"C:\Users\me\site-packages\x.py", r"\\server\share\proj") is False
    assert under(r"D:\proj\shop\orders\__init__.py", r"D:\proj") is True
    assert under(r"D:\project\shop.py", r"D:\proj") is False


def _venv_python(venv: Path) -> Path:
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(venv)], check=True)
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _run_python(python: Path, code: str) -> str:
    return subprocess.run(
        [str(python), "-c", code], check=True, capture_output=True, text=True
    ).stdout.strip()


def _purelib(python: Path) -> Path:
    return Path(_run_python(python, "import sysconfig; print(sysconfig.get_path('purelib'))"))


def _emulate_windows_site_packages(python: Path) -> None:
    """Make the venv's ``site.getsitepackages()`` list each prefix itself, as CPython does on nt."""
    (_purelib(python) / "sitecustomize.py").write_text(
        "import os, site\n"
        "def getsitepackages(prefixes=None):\n"
        "    return [p for prefix in (prefixes or site.PREFIXES)\n"
        "            for p in (prefix, os.path.join(prefix, 'Lib', 'site-packages'))]\n"
        "site.getsitepackages = getsitepackages\n"
    )
    listed = _run_python(python, "import site, sys; print(sys.prefix in site.getsitepackages())")
    assert listed == "True"


def _leaking_service(project: Path, tmp_path: Path, python: Path) -> Path:
    """Lay out a monolith whose shop.orders imports stdlib, third-party and first-party code."""
    (_purelib(python) / "thirdparty.py").write_text("VALUE = 1\n")
    source_orders = "import json\nimport thirdparty\nfrom common.money import cents\n"
    for tree in (project, tmp_path / "service"):
        (tree / "shop").mkdir(parents=True)
        (tree / "shop" / "__init__.py").write_text("")
        (tree / "shop" / "orders.py").write_text(source_orders)
    (project / "common").mkdir()
    (project / "common" / "__init__.py").write_text("")
    (project / "common" / "money.py").write_text("cents = 100\n")
    return tmp_path / "service"


def test_import_gate_reports_a_leak_when_the_project_root_is_the_virtualenv(monkeypatch, tmp_path):
    project = tmp_path / "proj"
    python = _venv_python(project)
    service = _leaking_service(project, tmp_path, python)
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setenv("PYTHONPATH", str(project))

    with pytest.raises(ValueError) as excinfo:
        extract._check_imports(service, "shop.orders", project)

    message = str(excinfo.value)
    assert (
        f"cannot import shop.orders: ImportError: imported common, common.money from the "
        f"source tree {os.path.realpath(project)}, outside the extracted service"
    ) in message
    assert "thirdparty" not in message
    assert "json" not in message


@pytest.mark.parametrize(
    ("directory", "module"),
    [("DLLs", "_fakeext"), ("src", "vcslib")],
    ids=["extension-modules", "pip-vcs-checkouts"],
)
def test_import_gate_exempts_interpreter_library_directories_when_the_project_root_is_the_environment(
    monkeypatch, tmp_path, directory, module
):
    project = tmp_path / "proj"
    python = _venv_python(project)
    service = _leaking_service(project, tmp_path, python)
    (project / directory).mkdir()
    (project / directory / f"{module}.py").write_text("VALUE = 1\n")
    for tree in (project, service):
        (tree / "shop" / "orders.py").write_text(
            f"import {module}\nfrom common.money import cents\n"
        )
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(project), str(project / directory)]))

    with pytest.raises(ValueError) as excinfo:
        extract._check_imports(service, "shop.orders", project)

    message = str(excinfo.value)
    assert (
        f"cannot import shop.orders: ImportError: imported common, common.money from the "
        f"source tree {os.path.realpath(project)}, outside the extracted service"
    ) in message
    assert module not in message


def test_import_gate_keeps_checking_an_app_checked_out_under_the_environment_src(
    monkeypatch, tmp_path
):
    venv = tmp_path / "venv"
    python = _venv_python(venv)
    checkout = venv / "src" / "shopproject"
    service = _leaking_service(checkout, tmp_path, python)
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setenv("PYTHONPATH", str(checkout))

    with pytest.raises(ValueError) as excinfo:
        extract._check_imports(service, "shop.orders", checkout)

    assert "imported common, common.money from the source tree" in str(excinfo.value)


def test_import_gate_exempts_a_virtualenv_inside_the_project(monkeypatch, tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    python = _venv_python(project / ".venv")
    service = _leaking_service(project, tmp_path, python)
    (service / "shop" / "orders.py").write_text("import json\nimport thirdparty\n")
    monkeypatch.setattr(sys, "executable", str(python))

    extract._check_imports(service, "shop.orders", project)


@pytest.mark.parametrize("project_in_venv", ["", "proj"], ids=["venv-root", "inside-venv"])
def test_import_gate_reports_a_leak_when_site_packages_lists_a_prefix_holding_the_project(
    monkeypatch, tmp_path, project_in_venv
):
    venv = tmp_path / "venv"
    python = _venv_python(venv)
    _emulate_windows_site_packages(python)
    project = venv / project_in_venv
    project.mkdir(exist_ok=True)
    service = _leaking_service(project, tmp_path, python)
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setenv("PYTHONPATH", str(project))

    with pytest.raises(ValueError) as excinfo:
        extract._check_imports(service, "shop.orders", project)

    message = str(excinfo.value)
    assert (
        f"cannot import shop.orders: ImportError: imported common, common.money from the "
        f"source tree {os.path.realpath(project)}, outside the extracted service"
    ) in message
    assert "thirdparty" not in message
    assert "json" not in message


def test_import_gate_exempts_a_virtualenv_inside_the_project_when_site_packages_lists_it(
    monkeypatch, tmp_path
):
    project = tmp_path / "proj"
    project.mkdir()
    python = _venv_python(project / ".venv")
    _emulate_windows_site_packages(python)
    service = _leaking_service(project, tmp_path, python)
    (service / "shop" / "orders.py").write_text("import json\nimport thirdparty\n")
    monkeypatch.setattr(sys, "executable", str(python))

    extract._check_imports(service, "shop.orders", project)


def _installed_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, files: dict[str, str]) -> Path:
    """Install *files* as plain copies into a fresh virtualenv's purelib; return that purelib."""
    python = _venv_python(tmp_path / "venv")
    purelib = _purelib(python)
    for name, content in files.items():
        (purelib / name).parent.mkdir(parents=True, exist_ok=True)
        (purelib / name).write_text(content)
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.chdir(tmp_path)
    return purelib


def test_import_gate_exempts_other_distributions_beside_an_installed_copy(monkeypatch, tmp_path):
    orders = "import json\nimport thirdparty\n"
    purelib = _installed_copy(
        tmp_path,
        monkeypatch,
        {"thirdparty.py": "VALUE = 1\n", "shop/__init__.py": "", "shop/orders/__init__.py": orders},
    )
    output = tmp_path / "orders-service"

    write_extraction(
        cfg=Configuration(package="shop"),
        module="orders",
        package_dir=purelib / "shop",
        output=output,
        helpers=[],
        notes=[],
    )

    assert (output / "shop" / "orders" / "__init__.py").read_text() == orders


def test_import_gate_reports_first_party_code_from_an_installed_copy(monkeypatch, tmp_path):
    purelib = _installed_copy(
        tmp_path,
        monkeypatch,
        {
            "thirdparty.py": "VALUE = 1\n",
            "company/common.py": "cents = 100\n",
            "company/shop/__init__.py": "",
            "company/shop/orders/__init__.py": "import thirdparty\nfrom company.common import cents\n",
        },
    )
    output = tmp_path / "orders-service"

    with pytest.raises(ValueError) as excinfo:
        write_extraction(
            cfg=Configuration(package="company.shop"),
            module="orders",
            package_dir=purelib / "company" / "shop",
            output=output,
            helpers=[],
            notes=[],
        )

    assert (
        f"cannot import company.shop.orders: ImportError: imported company.common from the "
        f"source tree {os.path.realpath(purelib / 'company')}, outside the extracted service"
    ) in str(excinfo.value)
    assert not output.exists()


def test_extract_import_gate_sees_the_extracted_tree_under_pythonsafepath(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "VALUE = 1\n"}, extra_files={"contracts/__init__.py": ""})
    monkeypatch.setenv("PYTHONSAFEPATH", "1")
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "orders" / "__init__.py").read_text() == "VALUE = 1\n"


def test_extract_copies_contracts_file_beside_same_named_non_package_dir(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.contracts import OrderPlaced\n"},
        extra_files={
            "contracts.py": "OrderPlaced = object\n",
            "contracts/order_placed.avsc": "{}\n",
        },
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "fakeapp" / "contracts.py").read_text() == "OrderPlaced = object\n"
    assert not (out_dir / "fakeapp" / "contracts").exists()


def _dotted_contracts_app(make_fake_app, tmp_path, orders_source):
    make_fake_app(
        {"orders": orders_source},
        extra_files={
            "_shared/__init__.py": "from .fmt import fmt\n",
            "_shared/fmt.py": "def fmt():\n    return 1\n",
            "_shared/contracts/__init__.py": "",
            "_shared/contracts/events.py": "OrderPlaced = object\n",
        },
    )
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\ncontracts_module = "_shared.contracts"\n'
    )


def test_extract_dotted_contracts_under_helper_package_whose_initializer_does_work(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    _dotted_contracts_app(
        make_fake_app,
        tmp_path,
        "from fakeapp._shared.fmt import fmt\n"
        "from fakeapp._shared.contracts.events import OrderPlaced\n",
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    shared = out_dir / "fakeapp" / "_shared"
    assert (shared / "__init__.py").read_text() == "from .fmt import fmt\n"
    assert (shared / "contracts" / "events.py").read_text() == "OrderPlaced = object\n"


def test_extract_dotted_contracts_refuses_working_parent_initializer_not_copied_as_helper(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    _dotted_contracts_app(
        make_fake_app, tmp_path, "from fakeapp._shared.contracts.events import OrderPlaced\n"
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "_shared/__init__.py" in result.output
    assert not out_dir.exists()


def test_extract_keeps_namespace_root_without_initializer(monkeypatch, request, tmp_path):
    site = tmp_path / "site"
    (site / "company" / "common").mkdir(parents=True)
    (site / "company" / "common" / "__init__.py").write_text("x = 1\n")
    src = tmp_path / "src"
    package_dir = src / "company" / "shop"
    (package_dir / "orders").mkdir(parents=True)
    (package_dir / "contracts").mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "orders" / "__init__.py").write_text("from company.common import x\n")
    (package_dir / "contracts" / "__init__.py").write_text("")
    monkeypatch.chdir(src)
    monkeypatch.syspath_prepend(str(site))
    monkeypatch.syspath_prepend(str(src))
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(site), os.environ.get("PYTHONPATH")]))
    )
    monkeypatch.setenv("MODULITH_PACKAGE", "company.shop")

    def reset_namespace_app() -> None:
        for name in list(sys.modules):
            if name == "company" or name.startswith("company."):
                del sys.modules[name]
        _runtime._reset_for_testing()

    request.addfinalizer(reset_namespace_app)
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert not (out_dir / "company" / "__init__.py").exists()
    assert (out_dir / "company" / "shop" / "__init__.py").read_text() == ""
    assert "company/__init__.py" not in result.output
    assert "COPY company/ ./company/" in (out_dir / "Dockerfile").read_text()

    dist_dir = tmp_path / "dist"
    build = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(dist_dir),
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PIP_NO_INDEX": "1"},
    )
    assert build.returncode == 0, build.stdout + build.stderr
    (wheel,) = dist_dir.glob("*.whl")
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "company/shop/orders/__init__.py" in names
    assert "company/__init__.py" not in names


def test_extract_refuses_app_package_spread_over_portions(monkeypatch, tmp_path):
    first = tmp_path / "q1" / "nsspread" / "shop"
    second = tmp_path / "q2" / "nsspread" / "shop"
    (first / "orders").mkdir(parents=True)
    (first / "orders" / "__init__.py").write_text("")
    (second / "contracts").mkdir(parents=True)
    (second / "contracts" / "events.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path / "q2"))
    monkeypatch.syspath_prepend(str(tmp_path / "q1"))
    importlib.invalidate_caches()
    output = tmp_path / "orders-service"

    with pytest.raises(ValueError) as excinfo:
        write_extraction(
            cfg=Configuration(package="nsspread.shop"),
            module="orders",
            package_dir=first,
            output=output,
            helpers=[],
            notes=[],
        )

    assert str(first) in str(excinfo.value)
    assert str(second) in str(excinfo.value)
    assert not output.exists()


def test_extract_resolves_namespace_root_when_other_portion_precedes_project(
    monkeypatch, request, tmp_path
):
    """An editable install's .pth puts the project root after site-packages,
    so another installed portion of the PEP 420 root is first in its
    namespace path; extract must still find the project's package directory."""
    site = tmp_path / "site"
    (site / "company" / "common").mkdir(parents=True)
    (site / "company" / "common" / "__init__.py").write_text("x = 1\n")
    src = tmp_path / "src"
    package_dir = src / "company" / "shop"
    (package_dir / "orders").mkdir(parents=True)
    (package_dir / "contracts").mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "orders" / "__init__.py").write_text("from company.common import x\n")
    (package_dir / "contracts" / "__init__.py").write_text("")
    monkeypatch.chdir(src)
    monkeypatch.syspath_prepend(str(src))
    monkeypatch.syspath_prepend(str(site))
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(site), os.environ.get("PYTHONPATH")]))
    )
    monkeypatch.setenv("MODULITH_PACKAGE", "company.shop")

    def reset_namespace_app() -> None:
        for name in list(sys.modules):
            if name == "company" or name.startswith("company."):
                del sys.modules[name]
        _runtime._reset_for_testing()

    request.addfinalizer(reset_namespace_app)
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "company" / "shop" / "orders" / "__init__.py").read_text() == (
        "from company.common import x\n"
    )
    assert (out_dir / "company" / "shop" / "__init__.py").read_text() == ""
    assert not (out_dir / "company" / "__init__.py").exists()


def test_extract_names_the_source_directory_before_anything_else(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "x = 1\n", "inventory": "from fakeapp.orders import x\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert lines[0] == f"source package: {tmp_path / 'fakeapp'}"
    assert lines[1].startswith("note: also imported by: inventory")
    assert "installed copy" not in result.stderr


@pytest.mark.parametrize("lookup", ["getsitepackages", "getusersitepackages"])
def test_extract_warns_when_the_source_directory_is_an_installed_copy(
    lookup, make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""}, extra_files={"contracts/__init__.py": ""})
    monkeypatch.setattr(
        site,
        lookup,
        (lambda: [str(tmp_path)]) if lookup == "getsitepackages" else lambda: str(tmp_path),
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines()[0] == f"source package: {tmp_path / 'fakeapp'}"
    assert f"warning: {tmp_path / 'fakeapp'} lies in {tmp_path}" in result.stderr
    assert "installed copy" in result.stderr


def test_extract_gate_error_names_the_file_and_line_that_failed(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": _IN_GATE + "if IN_GATE:\n    import no_such_dependency_xyz\n"},
        extra_files={"contracts/__init__.py": ""},
    )
    out_dir = tmp_path / "orders-service"

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert result.exit_code == 1, result.output
    assert "cannot import fakeapp.orders: ModuleNotFoundError" in result.stderr
    assert "(at fakeapp/orders/__init__.py:4)" in result.stderr
    assert not out_dir.exists()


def test_extract_blocker_names_the_importing_file_and_line_of_each_sibling(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": "from fakeapp.util import fmt\nfrom fakeapp.billing import charge\n",
            "inventory": "def reserve():\n    return 1\n",
            "billing": "def charge():\n    return 1\n",
        },
        extra_files={
            "util.py": "def fmt():\n    from fakeapp.inventory import reserve\n    return reserve()\n",
            "contracts/__init__.py": "",
        },
    )
    out_dir = tmp_path / "orders-service"

    blocked = runner.invoke(app, ["extract", "orders", "--output", str(out_dir)])

    assert blocked.exit_code == 1, blocked.output
    assert (
        "imports declared module(s): billing (fakeapp/orders/__init__.py:2), "
        "inventory (fakeapp/util.py:2); the extracted service does not contain them"
    ) in blocked.stderr


def test_extract_refuses_a_package_spread_over_portions_before_listing_blockers(
    monkeypatch, request, tmp_path
):
    first = tmp_path / "q1" / "nsspread" / "shop"
    second = tmp_path / "q2" / "nsspread" / "shop"
    (first / "orders").mkdir(parents=True)
    (first / "orders" / "__init__.py").write_text("from nsspread.shop.inventory import x\n")
    (second / "inventory").mkdir(parents=True)
    (second / "inventory" / "__init__.py").write_text("x = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path / "q2"))
    monkeypatch.syspath_prepend(str(tmp_path / "q1"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "nsspread.shop")

    def reset_namespace_app() -> None:
        for name in list(sys.modules):
            if name == "nsspread" or name.startswith("nsspread."):
                del sys.modules[name]
        _runtime._reset_for_testing()

    request.addfinalizer(reset_namespace_app)
    importlib.invalidate_caches()
    out_dir = tmp_path / "orders-service"

    for flags in ([], ["--force"]):
        result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), *flags])

        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit)
        assert result.stderr.startswith("error: package 'nsspread.shop' spans 2 directories")
        assert str(first) in result.stderr
        assert str(second) in result.stderr
        assert "blocked" not in result.stderr
        assert "Traceback" not in result.output
        assert not out_dir.exists()
