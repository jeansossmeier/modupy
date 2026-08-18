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


def test_test_directories_are_not_module_candidates(tmp_path: Path) -> None:
    """Test code is not a module, and its cross-module imports are not debt.

    A top-level ``tests/`` used to be proposed as a module, so every
    ``from myapp.orders import ...`` in it counted as a cross-module import
    heading for a boundary violation — punishing a codebase for the one place
    where reaching across modules is legitimate.
    """
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "def create(): ...\n")
    _write(root, "inventory/__init__.py", "")
    _write(root, "inventory/stock.py", "def reserve(): ...\n")
    _write(root, "tests/test_orders.py", "from myapp.inventory.stock import reserve\n")
    _write(root, "orders/tests/test_service.py", "from myapp.inventory.stock import reserve\n")

    result = audit_codebase(root)

    assert "tests" not in result.proposed_modules
    assert result.cross_module_imports == []
    assert result.readiness_score == 100
    # Test files are excluded from the scan entirely, not merely unattributed.
    scanned = {p.name for files in result.proposed_modules.values() for p in files}
    assert "test_orders.py" not in scanned
    assert "test_service.py" not in scanned


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


def test_shared_tables_include_foreign_key_references(tmp_path: Path) -> None:
    """A ``ForeignKey("customers.id")`` string literal in one module,
    pointing at a table ``__tablename__``-defined in another, is
    cross-module table coupling — the same signal as two modules both
    calling ``Table("x")`` directly."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(
        root,
        "customers/models.py",
        """
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Customer(Base):
            __tablename__ = "customers"
        """,
    )
    _write(root, "customers/__init__.py", "")
    _write(
        root,
        "orders/models.py",
        """
        import sqlalchemy as sa

        customer_id = sa.Column(sa.ForeignKey("customers.id"))
        """,
    )
    _write(root, "orders/__init__.py", "")

    result = audit_codebase(root)

    assert result.shared_tables == ["customers"]

    no_fk_root = tmp_path / "noref"
    _write(no_fk_root, "__init__.py", "")
    _write(
        no_fk_root,
        "customers/models.py",
        """
        from sqlalchemy.orm import DeclarativeBase

        class Base(DeclarativeBase):
            pass

        class Customer(Base):
            __tablename__ = "customers"
        """,
    )
    _write(no_fk_root, "customers/__init__.py", "")
    _write(no_fk_root, "orders/__init__.py", "")

    no_fk_result = audit_codebase(no_fk_root)

    # Readiness formula is deliberately untouched by table coupling.
    assert result.readiness_score == no_fk_result.readiness_score


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


# ---------------------------------------------------------------------------
# Parse-failure counting and disclosure
# ---------------------------------------------------------------------------


def test_files_scanned_counts_only_successful_parses(tmp_path: Path) -> None:
    """A file that fails to parse is dropped by every analysis stage — but
    ``files_scanned`` counted every discovered file regardless, silently
    implying a broken file was actually analyzed."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "def create(): ...\n")
    (root / "orders" / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    result = audit_codebase(root)

    assert result.files_scanned == 3
    assert result.parse_failures == [root / "orders" / "broken.py"]


def test_render_report_discloses_parse_failures(tmp_path: Path) -> None:
    """A broken file must be disclosed in the report, not silently excluded
    as if the codebase were fully analyzable."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    (root / "orders" / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    result = audit_codebase(root)
    report = render_report(result)

    assert "broken.py" in report
    assert "could not be parsed" in report


def test_no_parse_failures_omits_disclosure(tmp_path: Path) -> None:
    """Guard against over-fix: a fully-parseable codebase gets no failure
    disclosure noise."""
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)
    assert result.parse_failures == []
    assert "could not be parsed" not in render_report(result)


# ---------------------------------------------------------------------------
# The audit must honor a configured contracts-module name
# ---------------------------------------------------------------------------


def test_audit_codebase_accepts_contracts_module_override(tmp_path: Path) -> None:
    """``audit_codebase`` hardcoded the default ``contracts`` name — a
    codebase mid-migration with a custom ``[tool.modulith].contracts_module``
    must exempt imports of *that* module from cross-module coupling, the
    same way the verifier does."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "from myapp.shared_kernel import Money\n")
    _write(root, "shared_kernel/__init__.py", "")

    default = audit_codebase(root)
    pairs_default = {(src, tgt) for src, tgt, _c, _s in default.cross_module_imports}
    assert ("orders", "shared_kernel") in pairs_default

    configured = audit_codebase(root, contracts_module="shared_kernel")
    pairs_configured = {(src, tgt) for src, tgt, _c, _s in configured.cross_module_imports}
    assert ("orders", "shared_kernel") not in pairs_configured


def test_audit_cli_honors_configured_contracts_module(monkeypatch, tmp_path: Path) -> None:
    """The CLI must read ``[tool.modulith].contracts_module`` from the
    project the audit is run in, not the ``audit_codebase`` default."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "from myapp.shared_kernel import Money\n")
    _write(root, "shared_kernel/__init__.py", "")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\ncontracts_module = "shared_kernel"\n', encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "MIGRATION.md"

    result = runner.invoke(app, ["audit", str(root), "--output", str(out)])

    assert result.exit_code == 0, result.output
    assert "0 cross-module import pattern(s)" in result.output


def test_relative_root_scores_the_same_as_absolute_root(monkeypatch, tmp_path: Path) -> None:
    """``Path(".")`` is the CLI's default argument, so the bare
    ``modulith audit`` invocation must analyze the tree exactly as an absolute
    path does. The root package name comes from ``root.name``, which is empty
    for a relative root — every ``myapp.other`` import then looked external
    and the audit reported a fabricated 100/100."""
    root = _make_codebase(tmp_path)

    absolute = audit_codebase(root)
    monkeypatch.chdir(root)
    relative = audit_codebase(Path("."))

    assert absolute.readiness_score == relative.readiness_score
    assert {(src, tgt) for src, tgt, _c, _s in relative.cross_module_imports} == {
        (src, tgt) for src, tgt, _c, _s in absolute.cross_module_imports
    }
    assert ("orders", "inventory") in {
        (src, tgt) for src, tgt, _c, _s in relative.cross_module_imports
    }
    # A relative root also attributed root-level files to a module named "".
    assert "" not in relative.proposed_modules
