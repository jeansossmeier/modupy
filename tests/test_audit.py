"""Tests for the `modulith audit` codebase analyzer.

The audit is pure, non-destructive static analysis over a directory tree: it
parses ``.py`` files with ``ast`` (never imports them), proposes a module
structure from the folder layout, finds cross-module imports and shared
tables, spots listener-shaped functions, and scores migration readiness. These
tests build small synthetic codebases on disk and assert on the structured
result and the rendered Markdown report.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from textwrap import dedent

import pytest
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


def test_shared_tables_match_verifier_sqlalchemy_ast_forms(tmp_path: Path) -> None:
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(
        root,
        "customers/models.py",
        """
        import sqlalchemy as sa
        from sqlalchemy import Table as SATable

        metadata = sa.MetaData()
        customers = SATable(name="customers", metadata=metadata)
        accounts = SATable(name="accounts", metadata=metadata)
        regions = SATable(name="regions", metadata=metadata)

        class AuditRow:
            __tablename__: str = "audit_log"
        """,
    )
    _write(root, "customers/__init__.py", "")
    _write(
        root,
        "orders/models.py",
        """
        from sqlalchemy import ForeignKeyConstraint as FKC
        from sqlalchemy.schema import ForeignKey as FK

        customer_id = FK(column="crm.customers.id")
        audit_id = FK(column="audit_log.id")
        account_region = FKC(
            columns=("account_id", "region_id"),
            refcolumns=("crm.accounts.id", "crm.regions.id"),
        )
        """,
    )
    _write(root, "orders/__init__.py", "")

    result = audit_codebase(root)

    assert result.shared_tables == ["accounts", "audit_log", "customers", "regions"]


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

    assert "# modupy Audit Report" in report
    assert "ready for modupy adoption" in report
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
    assert "modupy Audit Report" in out.read_text(encoding="utf-8")
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


# Sources the interpreter accepts but a UTF-8 text read rejects: a UTF-8 BOM
# and a PEP 263 coding line. Each body carries a non-ASCII character so the
# latin-1 variant is not valid UTF-8.


def _utf8_bom(body: str) -> bytes:
    return b"\xef\xbb\xbf" + body.encode("utf-8")


def _latin1_coding_line(body: str) -> bytes:
    return b"# -*- coding: latin-1 -*-\n" + body.encode("latin-1")


_interpreter_sources = pytest.mark.parametrize(
    "encode", [_utf8_bom, _latin1_coding_line], ids=["utf8-bom", "latin1-coding-line"]
)


@_interpreter_sources
def test_audit_parses_a_bom_or_pep263_module(
    encode: Callable[[str], bytes], tmp_path: Path
) -> None:
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "inventory/__init__.py", "")
    (root / "orders" / "legacy.py").write_bytes(
        encode('from myapp.inventory import stock\nNAME = "café"\n')
    )

    result = audit_codebase(root)

    assert result.parse_failures == []
    assert result.files_scanned == 4
    pairs = {(src, tgt) for src, tgt, _count, _sample in result.cross_module_imports}
    assert pairs == {("orders", "inventory")}


def test_audit_still_reports_an_undecodable_file_as_a_parse_failure(tmp_path: Path) -> None:
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "def create(): ...\n")
    (root / "orders" / "broken.py").write_bytes(b"NAME = 'caf\xe9'\n")

    result = audit_codebase(root)

    assert result.files_scanned == 3
    assert result.parse_failures == [root / "orders" / "broken.py"]


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


def test_audit_codebase_shared_tables_exempt_contracts_module(tmp_path: Path) -> None:
    """A table defined in the contracts module travels with every extraction
    (extract.py's ``_populate_extraction`` unconditionally copies contracts),
    so it is never actually left behind — it must not count as a shared-table
    blocker, matching the exemption ``_find_cross_module_imports`` already
    gives contracts imports."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/models.py", 'from sqlalchemy import Table\nt = Table("lookup_codes")\n')
    _write(root, "contracts/__init__.py", "")
    _write(
        root, "contracts/models.py", 'from sqlalchemy import Table\nt2 = Table("lookup_codes")\n'
    )

    default = audit_codebase(root)
    assert "lookup_codes" not in default.shared_tables

    root2 = tmp_path / "myapp2"
    _write(root2, "__init__.py", "")
    _write(root2, "orders/__init__.py", "")
    _write(root2, "orders/models.py", 'from sqlalchemy import Table\nt = Table("lookup_codes")\n')
    _write(root2, "shared_kernel/__init__.py", "")
    _write(
        root2,
        "shared_kernel/models.py",
        'from sqlalchemy import Table\nt2 = Table("lookup_codes")\n',
    )

    configured = audit_codebase(root2, contracts_module="shared_kernel")
    assert "lookup_codes" not in configured.shared_tables
    unconfigured = audit_codebase(root2)
    assert "lookup_codes" in unconfigured.shared_tables


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


# ---------------------------------------------------------------------------
# A bare `modulith audit` at a project root audits the application package
# ---------------------------------------------------------------------------


def _write_coupled_app(package_dir: Path, import_root: str) -> None:
    """views/models/services importing each other directly: three patterns."""
    _write(package_dir, "__init__.py", "")
    _write(package_dir, "views/__init__.py", "")
    _write(
        package_dir,
        "views/orders.py",
        f"from {import_root}.models.inventory import Stock\n"
        f"from {import_root}.services.billing import charge\n",
    )
    _write(package_dir, "views/inventory.py", f"from {import_root}.models.inventory import Stock\n")
    _write(package_dir, "models/__init__.py", "")
    _write(package_dir, "models/inventory.py", "class Stock:\n    pass\n")
    _write(package_dir, "services/__init__.py", "")
    _write(
        package_dir,
        "services/billing.py",
        f"from {import_root}.views.orders import Stock\n\ndef charge():\n    pass\n",
    )


def _write_non_application_dirs(project: Path) -> None:
    """Directories that hold Python but are never the application package."""
    _write(project, "pyproject.toml", '[project]\nname = "brownfield"\nversion = "0"\n')
    _write(project, "tests/test_views.py", "from app.views.orders import charge\n")
    _write(project, "docs/conf.py", "project = 'brownfield'\n")
    _write(project, "scripts/seed.py", "from app.models.inventory import Stock\n")
    _write(project, "examples/demo.py", "from app.services.billing import charge\n")
    _write(project, "migrations/env.py", "target_metadata = None\n")
    _write(project, ".venv/lib/python3.11/site-packages/six.py", "X = 1\n")
    _write(project, "build/lib/app/__init__.py", "")
    _write(project, "manage.py", "from app.views.orders import charge\n")


def _summary_lines(output: str) -> list[str]:
    return [
        line
        for line in output.splitlines()
        if line.startswith("readiness score") or "cross-module import pattern" in line
    ]


def test_audit_from_project_root_audits_the_application_package(
    monkeypatch, tmp_path: Path
) -> None:
    """The migration guide runs a bare ``modulith audit`` at the project root.
    With the application in one top-level package, that package's
    subpackages are the module candidates — the same audit as pointing the
    command at the package — and the output names the directory it chose."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    _write_non_application_dirs(project)
    monkeypatch.chdir(project)

    from_root = runner.invoke(app, ["audit", "--output", str(tmp_path / "root.md")])
    from_package = runner.invoke(app, ["audit", "app", "--output", str(tmp_path / "pkg.md")])

    assert from_root.exit_code == 0, from_root.output
    assert from_package.exit_code == 0, from_package.output
    assert _summary_lines(from_root.stdout) == [
        "readiness score: 0/100",
        "3 cross-module import pattern(s), 0 shared table(s)",
    ]
    assert _summary_lines(from_root.stdout) == _summary_lines(from_package.stdout)
    assert f"audited {(project / 'app').resolve()}" in from_root.stdout
    report = (tmp_path / "root.md").read_text(encoding="utf-8")
    assert "- `views` (3 files)" in report
    assert "- `models` (2 files)" in report
    assert "- `services` (2 files)" in report
    assert "| `views` | `models` | 2 |" in report
    assert "| `views` | `services` | 1 |" in report
    assert "| `services` | `views` | 1 |" in report


def test_audit_from_src_layout_root_audits_the_package(monkeypatch, tmp_path: Path) -> None:
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "src" / "shopkit", "shopkit")
    _write(project, "tests/test_views.py", "from shopkit.views.orders import charge\n")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {(project / 'src' / 'shopkit').resolve()}" in result.stdout
    assert _summary_lines(result.stdout) == [
        "readiness score: 0/100",
        "3 cross-module import pattern(s), 0 shared table(s)",
    ]


def test_audit_of_flat_root_with_several_packages_stays_at_root(
    monkeypatch, tmp_path: Path
) -> None:
    """Several top-level application packages are themselves the module
    candidates; the audit must not descend into any of them."""
    project = tmp_path / "repo"
    _write(project, "inventory/__init__.py", "")
    _write(project, "inventory/stock.py", "def reserve(): ...\n")
    _write(project, "billing/__init__.py", "")
    _write(project, "billing/report.py", "from inventory.stock import reserve\n")
    _write(project, "tests/test_billing.py", "from billing.report import reserve\n")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {project.resolve()}" in result.stdout
    assert "1 cross-module import pattern(s)" in result.stdout


# ---------------------------------------------------------------------------
# One module candidate has no boundaries to score
# ---------------------------------------------------------------------------


def test_single_module_candidate_report_withholds_the_score(tmp_path: Path) -> None:
    """Every import inside a lone candidate is same-module, so the formula's
    100 measures nothing; the report must say so instead of presenting it."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")

    result = audit_codebase(project)
    report = render_report(result)

    assert set(result.proposed_modules) == {"app"}
    assert result.score_applicable is False
    assert "100/100" not in report
    assert "100% ready" not in report
    assert "only one module candidate" in report


def test_audit_cli_warns_on_single_module_candidate(tmp_path: Path) -> None:
    root = tmp_path / "solo"
    _write(root, "__init__.py", "")
    _write(root, "a.py", "from solo.b import thing\n")
    _write(root, "b.py", "thing = 1\n")

    result = runner.invoke(app, ["audit", str(root), "--output", str(tmp_path / "M.md")])

    assert result.exit_code == 0, result.output
    assert "100/100" not in result.output
    assert "readiness score: n/a" in result.stdout
    assert "only one module candidate" in result.stderr


def test_multi_module_audit_keeps_its_score(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    result = audit_codebase(root)
    assert result.score_applicable is True
    assert f"**Readiness score: {result.readiness_score}/100**" in render_report(result)
    assert "only one module candidate" not in render_report(result)


# ---------------------------------------------------------------------------
# Script directories, unreadable directories and non-directory paths
# ---------------------------------------------------------------------------


def _write_script_dirs(project: Path, import_root: str) -> None:
    """Top-level directories of loose scripts, which are never the application."""
    _write(project, "tools/gen.py", "print(1)\n")
    _write(project, "bin/run.py", f"from {import_root}.views.orders import charge\n")
    _write(project, "scripts/seed.py", "x = 1\n")


def test_audit_from_src_layout_root_with_script_directories_audits_the_package(
    monkeypatch, tmp_path: Path
) -> None:
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "src" / "shopkit", "shopkit")
    _write_script_dirs(project, "shopkit")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {(project / 'src' / 'shopkit').resolve()}" in result.stdout
    assert _summary_lines(result.stdout) == [
        "readiness score: 0/100",
        "3 cross-module import pattern(s), 0 shared table(s)",
    ]


def test_audit_from_flat_root_with_script_directories_audits_the_package(
    monkeypatch, tmp_path: Path
) -> None:
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    _write_script_dirs(project, "app")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {(project / 'app').resolve()}" in result.stdout
    assert _summary_lines(result.stdout) == [
        "readiness score: 0/100",
        "3 cross-module import pattern(s), 0 shared table(s)",
    ]


def test_audit_from_namespace_package_root_with_script_directory_audits_the_package(
    monkeypatch, tmp_path: Path
) -> None:
    """An application directory without ``__init__.py`` still outranks a
    directory of loose scripts."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    (project / "app" / "__init__.py").unlink()
    _write(project, "tools/gen.py", "print(1)\n")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {(project / 'app').resolve()}" in result.stdout
    assert "readiness score: 0/100" in result.stdout


def test_explicit_project_root_does_not_score_scripts_against_the_app(
    monkeypatch, tmp_path: Path
) -> None:
    """At an explicit project root the application package holds every
    module; a loose-script directory beside it is not a second candidate."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    _write(project, "tools/gen.py", "import app\n")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit", ".", "--output", str(tmp_path / "MIGRATION.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {project.resolve()}\n" in result.stdout
    assert "readiness score: n/a" in result.stdout
    assert "only one module candidate (`app`)" in result.stderr


def test_score_withheld_when_every_import_misses_the_module_candidates(tmp_path: Path) -> None:
    """Auditing above ``src/`` makes every ``shopkit.*`` import name a package
    that is not a candidate; a score over the remaining zero imports would be
    a fabricated 100."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "src" / "shopkit", "shopkit")
    _write(project, "lib/util/__init__.py", "")
    _write(project, "lib/util/strings.py", "def slug(): ...\n")

    result = audit_codebase(project)
    report = render_report(result)

    assert set(result.proposed_modules) == {"src", "lib"}
    assert result.score_applicable is False
    assert "100/100" not in report
    assert "`shopkit`" in report


def test_root_package_init_is_not_a_module_candidate(tmp_path: Path) -> None:
    """A package whose code all lives in one subpackage has one module
    candidate; its own ``__init__.py`` does not make a second one."""
    root = tmp_path / "app"
    _write(root, "__init__.py", "")
    _write(root, "core/__init__.py", "")
    _write(root, "core/orders.py", "from app.core.billing import charge\n")
    _write(root, "core/billing.py", "def charge(): ...\n")

    result = audit_codebase(root)
    report = render_report(result)

    assert result.score_applicable is False
    assert "100/100" not in report
    assert "only one module candidate (`core`)" in report


def test_audit_skips_an_unreadable_top_level_directory(monkeypatch, tmp_path: Path) -> None:
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    pgdata = project / "pgdata"
    _write(pgdata, "base/stray.py", "x = 1\n")
    pgdata.chmod(0)
    monkeypatch.chdir(project)
    try:
        result = runner.invoke(app, ["audit", "--output", str(tmp_path / "MIGRATION.md")])
    finally:
        pgdata.chmod(0o700)

    assert result.exit_code == 0, result.output
    assert f"audited {(project / 'app').resolve()}" in result.stdout


def test_audit_of_a_file_is_a_user_error(tmp_path: Path) -> None:
    _write(tmp_path, "app.py", "x = 1\n")

    result = runner.invoke(
        app, ["audit", str(tmp_path / "app.py"), "--output", str(tmp_path / "M.md")]
    )

    assert result.exit_code == 1, result.output
    assert "not a directory" in result.stderr
    assert not (tmp_path / "M.md").exists()


def test_explicit_path_is_audited_as_given(tmp_path: Path) -> None:
    """``modulith audit <dir>`` audits that directory; only the bare command
    looks for the application package."""
    services = tmp_path / "services"
    _write(services, "api.py", "from services.core.orders import place\n")
    _write(services, "models.py", "x = 1\n")
    _write(services, "core/orders.py", "def place(): ...\n")

    result = runner.invoke(app, ["audit", str(services), "--output", str(tmp_path / "M.md")])

    assert result.exit_code == 0, result.output
    assert f"audited {services.resolve()}\n" in result.stdout
    report = (tmp_path / "M.md").read_text(encoding="utf-8")
    assert "- `services` (2 files)" in report
    assert "- `core` (1 file)" in report


def test_report_embeds_no_absolute_paths(tmp_path: Path) -> None:
    """The report is meant to be committed and diffed between runs, so the
    machine-specific location of the checkout must not appear in it."""
    root = _make_codebase(tmp_path)
    _write(root, "orders/broken.py", "def (:\n")

    result = audit_codebase(root)
    report = render_report(result)

    assert result.cross_module_imports and result.listener_candidates and result.parse_failures
    assert str(tmp_path) not in report
    assert "`orders/broken.py`" in report
    assert "`orders/service.py`" in report


# ---------------------------------------------------------------------------
# An existing output file is replaced only with --force
# ---------------------------------------------------------------------------

HAND_WRITTEN = "MY HAND-WRITTEN MIGRATION NOTES\n"


def test_audit_refuses_to_overwrite_an_existing_output_without_force(tmp_path: Path) -> None:
    root = _make_codebase(tmp_path)
    out = tmp_path / "notes.md"
    out.write_text(HAND_WRITTEN, encoding="utf-8")

    result = runner.invoke(app, ["audit", str(root), "--output", str(out)])

    assert result.exit_code == 1, result.output
    assert str(out) in result.stderr
    assert "--force" in result.stderr
    assert "wrote audit report" not in result.stdout
    assert out.read_text(encoding="utf-8") == HAND_WRITTEN


def test_bare_audit_refuses_to_overwrite_a_hand_written_migration_md(
    monkeypatch, tmp_path: Path
) -> None:
    """``MIGRATION.md`` is the default report name, so it is the file a team
    is most likely to have written by hand before running the audit."""
    project = tmp_path / "brownfield"
    _write_coupled_app(project / "app", "app")
    notes = project / "MIGRATION.md"
    notes.write_text(HAND_WRITTEN, encoding="utf-8")
    monkeypatch.chdir(project)

    result = runner.invoke(app, ["audit"])

    assert result.exit_code == 1, result.output
    assert "MIGRATION.md" in result.stderr
    assert notes.read_text(encoding="utf-8") == HAND_WRITTEN


@pytest.mark.parametrize("existing", [True, False])
def test_audit_force_writes_the_report_to_the_output(tmp_path: Path, existing: bool) -> None:
    root = _make_codebase(tmp_path)
    out = tmp_path / "notes.md"
    if existing:
        out.write_text(HAND_WRITTEN, encoding="utf-8")

    result = runner.invoke(app, ["audit", str(root), "--output", str(out), "--force"])

    assert result.exit_code == 0, result.output
    assert out.read_text(encoding="utf-8").startswith("# modupy Audit Report")
    assert f"wrote audit report to {out}" in result.stdout


def _create_output_during_the_audit(monkeypatch, out: Path) -> None:
    """Make ``out`` appear after the existence check, while the audit runs."""
    import modulith.audit as audit_module

    real_audit = audit_module.audit_codebase

    def audit_then_create(*args, **kwargs):
        result = real_audit(*args, **kwargs)
        out.write_text(HAND_WRITTEN, encoding="utf-8")
        return result

    monkeypatch.setattr(audit_module, "audit_codebase", audit_then_create)


def test_audit_does_not_overwrite_an_output_created_after_the_check(
    monkeypatch, tmp_path: Path
) -> None:
    root = _make_codebase(tmp_path)
    out = tmp_path / "notes.md"
    _create_output_during_the_audit(monkeypatch, out)

    result = runner.invoke(app, ["audit", str(root), "--output", str(out)])

    assert result.exit_code == 1, result.output
    assert str(out) in result.stderr
    assert "--force" in result.stderr
    assert "wrote audit report" not in result.stdout
    assert out.read_text(encoding="utf-8") == HAND_WRITTEN


def test_audit_force_overwrites_an_output_created_after_the_check(
    monkeypatch, tmp_path: Path
) -> None:
    root = _make_codebase(tmp_path)
    out = tmp_path / "notes.md"
    _create_output_during_the_audit(monkeypatch, out)

    result = runner.invoke(app, ["audit", str(root), "--output", str(out), "--force"])

    assert result.exit_code == 0, result.output
    assert out.read_text(encoding="utf-8").startswith("# modupy Audit Report")


def test_audit_output_to_a_device_needs_no_force(tmp_path: Path) -> None:
    """Pins behavior that predates the refusal and must survive it:
    ``--output /dev/null`` discards the report, and a device holds nothing to
    overwrite, so only an existing regular file needs ``--force``."""
    root = _make_codebase(tmp_path)

    result = runner.invoke(app, ["audit", str(root), "--output", os.devnull])

    assert result.exit_code == 0, result.output
    assert "readiness score" in result.stdout
