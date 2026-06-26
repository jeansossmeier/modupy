"""Tests for the `modulith audit` codebase analyzer.

The audit is pure, non-destructive static analysis over a directory tree: it
parses ``.py`` files with ``ast`` (never imports them), proposes a module
structure from the folder layout, finds cross-module imports and shared
tables, spots listener-shaped functions, and scores migration readiness. These
tests build small synthetic codebases on disk and assert on the structured
result and the rendered Markdown report.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from typer.testing import CliRunner

from modulith.audit import audit_codebase, render_report
from modulith.cli import app

runner = CliRunner()


def _write(root: Path, rel: str, source: str) -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(dedent(source), encoding="utf-8")


def _make_codebase(tmp_path: Path) -> Path:
    """A small app: orders imports inventory directly; both touch a table."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(
        root,
        "orders/service.py",
        """
        from myapp.inventory.stock import reserve
        from sqlalchemy import Table, MetaData

        orders_table = Table("orders", MetaData())
        shared = Table("customers", MetaData())

        def on_order_placed(event):
            reserve(event)
        """,
    )
    _write(root, "orders/__init__.py", "")
    _write(
        root,
        "inventory/stock.py",
        """
        from sqlalchemy import Table, MetaData

        stock_table = Table("stock", MetaData())
        also_shared = Table("customers", MetaData())

        def reserve(event):
            return event
        """,
    )
    _write(root, "inventory/__init__.py", "")
    return root


# ---------------------------------------------------------------------------
# Proposed module structure
# ---------------------------------------------------------------------------


def test_proposes_modules_from_top_level_dirs(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)

    assert set(result.proposed_modules) >= {"orders", "inventory"}
    orders_files = {p.name for p in result.proposed_modules["orders"]}
    assert "service.py" in orders_files


# ---------------------------------------------------------------------------
# Cross-module imports
# ---------------------------------------------------------------------------


def test_detects_cross_module_imports(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)

    pairs = {(src, tgt) for src, tgt, _count, _sample in result.cross_module_imports}
    assert ("orders", "inventory") in pairs


def test_relative_cross_module_import_is_detected(tmp_path: Path) -> None:
    root = tmp_path / "app"
    _write(root, "__init__.py", "")
    _write(root, "billing/__init__.py", "")
    _write(root, "billing/charge.py", "from ..accounts.ledger import debit\n")
    _write(root, "accounts/__init__.py", "")
    _write(root, "accounts/ledger.py", "def debit(): ...\n")

    result = audit_codebase(root)
    pairs = {(src, tgt) for src, tgt, _c, _s in result.cross_module_imports}
    assert ("billing", "accounts") in pairs


# ---------------------------------------------------------------------------
# Shared tables
# ---------------------------------------------------------------------------


def test_detects_tables_shared_across_modules(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)

    # "customers" is referenced from both orders and inventory.
    assert "customers" in result.shared_tables
    # "orders"/"stock" live in a single module each → not shared.
    assert "orders" not in result.shared_tables
    assert "stock" not in result.shared_tables


# ---------------------------------------------------------------------------
# Listener candidates
# ---------------------------------------------------------------------------


def test_finds_listener_shaped_functions(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)

    candidate_files = {p.name for p in result.listener_candidates}
    assert "service.py" in candidate_files  # has on_order_placed


# ---------------------------------------------------------------------------
# Readiness score
# ---------------------------------------------------------------------------


def test_clean_codebase_scores_100(tmp_path: Path) -> None:
    root = tmp_path / "tidy"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "def create(): ...\n")
    _write(root, "inventory/__init__.py", "")
    _write(root, "inventory/stock.py", "def reserve(): ...\n")

    result = audit_codebase(root)
    assert result.readiness_score == 100


def test_directly_coupled_codebase_scores_low(tmp_path: Path) -> None:
    root = tmp_path / "coupled"
    _write(root, "__init__.py", "")
    _write(root, "a/__init__.py", "")
    _write(root, "a/x.py", "from coupled.b.y import thing\n")
    _write(root, "b/__init__.py", "")
    _write(root, "b/y.py", "thing = 1\n")

    result = audit_codebase(root)
    # All cross-module interaction is direct imports, no event-shaped code.
    assert result.readiness_score < 50


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def test_render_report_contains_key_sections(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)
    report = render_report(result)

    assert "# Modulith Audit Report" in report
    assert "Readiness score" in report
    assert str(result.readiness_score) in report
    assert "orders" in report and "inventory" in report
    assert "customers" in report  # shared table surfaced
    assert "Next Steps" in report


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------


def test_audit_cli_writes_report(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    out = tmp_path / "MIGRATION.md"

    result = runner.invoke(app, ["audit", str(root), "--output", str(out)])

    assert result.exit_code == 0, result.output
    assert out.exists()
    assert "Modulith Audit Report" in out.read_text(encoding="utf-8")
    assert "readiness score" in result.output.lower()
