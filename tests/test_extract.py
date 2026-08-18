"""Tests for `modulith extract` — scaffolding a standalone service from a module."""

from __future__ import annotations

import sys
import tomllib

from typer.testing import CliRunner

from modulith import __version__
from modulith.cli import app
from modulith.runtime import _runtime

runner = CliRunner()

_ORDERS_MANIFEST = 'from modulith import declare_module\ndeclare_module(owns_tables=["orders"])\n'


def test_extract_happy_path_produces_service_scaffold(make_fake_app, monkeypatch, tmp_path):
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
    assert (out_dir / "fakeapp" / "__init__.py").exists()
    assert (out_dir / "fakeapp" / "orders" / "__init__.py").exists()
    assert (out_dir / "fakeapp" / "orders" / "_manifest.py").exists()
    assert (out_dir / "fakeapp" / "contracts").is_dir()
    assert (out_dir / "pyproject.toml").exists()
    assert (out_dir / "Dockerfile").exists()
    assert (out_dir / "README.md").exists()
    assert (out_dir / ".env.example").exists()
    assert not (out_dir / "fakeapp" / "inventory").exists()

    parsed = tomllib.loads((out_dir / "pyproject.toml").read_text())
    assert parsed["tool"]["modulith"]["package"] == "fakeapp"
    deps = parsed["project"]["dependencies"]
    modupy_dep = next(d for d in deps if d.startswith("modupy["))
    assert modupy_dep.endswith(f"=={__version__}")

    docker = (out_dir / "Dockerfile").read_text()
    assert "modulith._worker:create_app" in docker
    assert "0.0.0.0" in docker


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
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
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


def test_extract_nonempty_output_dir_blocks_even_with_force(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""}, extra_files={"contracts/__init__.py": ""})
    out_dir = tmp_path / "orders-service"
    out_dir.mkdir()
    (out_dir / "stray.txt").write_text("x")

    result = runner.invoke(app, ["extract", "orders", "--output", str(out_dir), "--force"])

    assert result.exit_code == 1, result.output


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
