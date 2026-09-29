"""Tests for `modulith extract` — scaffolding a standalone service from a module."""

from __future__ import annotations

import ntpath
import os
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
from modulith.extract import _render_pyproject, _render_readme, write_extraction
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
    assert not out_dir.exists()


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


def _leaking_service(project: Path, tmp_path: Path, python: Path) -> Path:
    """Lay out a monolith whose shop.orders imports stdlib, third-party and first-party code."""
    purelib = subprocess.run(
        [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    Path(purelib, "thirdparty.py").write_text("VALUE = 1\n")
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


def test_import_gate_exempts_a_virtualenv_inside_the_project(monkeypatch, tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    python = _venv_python(project / ".venv")
    service = _leaking_service(project, tmp_path, python)
    (service / "shop" / "orders.py").write_text("import json\nimport thirdparty\n")
    monkeypatch.setattr(sys, "executable", str(python))

    extract._check_imports(service, "shop.orders", project)


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
