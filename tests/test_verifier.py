"""Behavioral tests for the AST-based boundary verifier.

Each test builds a synthetic module tree with ``make_fake_app`` and asserts
the exact Violations (or absence) produced. Imports are collected by parsing
source — modules are never executed — so the fake modules can reference names
that don't exist on disk.
"""

from __future__ import annotations

import json
from pathlib import Path

from modulith import ModuleInfo, Violation
from modulith.builtin import verifier
from modulith.builtin.verifier import (
    BaselineEntry,
    ImportRecord,
    detect_cycles,
    filter_against_baseline,
    load_baseline,
    write_baseline,
)
from modulith.types import ViolationSeverity


def _module(name: str) -> ModuleInfo:
    return ModuleInfo(name=name, package=f"fakeapp.{name}")


# ---------------------------------------------------------------------------
# _collect_imports
# ---------------------------------------------------------------------------


def test_collect_imports(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from __future__ import annotations

                import os
                from fakeapp.payments import charge
                from .helpers import util
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.secret import Hidden
            """,
        }
    )

    records = verifier._collect_imports(_module("orders"))
    targets = {(r.target_module, tuple(r.imported_names)) for r in records}

    assert ("os", ()) in targets
    assert ("fakeapp.payments", ("charge",)) in targets
    # Relative import resolved to absolute.
    assert ("fakeapp.orders.helpers", ("util",)) in targets
    # TYPE_CHECKING-only imports are collected but tagged type_only, so the
    # boundary rules (1, 3, 4) still see them while cycle detection skips
    # them (the guard must not be an encapsulation escape hatch).
    guarded = [r for r in records if r.target_module == "fakeapp.secret"]
    assert len(guarded) == 1
    assert guarded[0].type_only
    assert all(not r.type_only for r in records if r.target_module != "fakeapp.secret")


# ---------------------------------------------------------------------------
# Rule 1: no cross-module internal imports
# ---------------------------------------------------------------------------


def test_internal_import_violation(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory._internal.store import Repo
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)

    rules = {v.rule for v in violations}
    assert "no-internal-imports" in rules
    v = next(v for v in violations if v.rule == "no-internal-imports")
    assert v.module == "orders"
    assert "inventory" in v.message


def test_importing_own_internal_is_allowed(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.orders._internal.store import Repo
            """,
        }
    )
    violations = verifier.modulith_verify_module(_module("orders"), [_module("orders")])
    assert all(v.rule != "no-internal-imports" for v in violations)


# ---------------------------------------------------------------------------
# Rule 4: cross-module type imports must come from contracts
# ---------------------------------------------------------------------------


def test_contracts_violation(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import StockItem
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


def test_importing_type_from_contracts_is_allowed(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.contracts import StockItem
            """,
            "contracts": "",
        }
    )
    mods = [_module("orders"), _module("contracts")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "use-contracts" for v in violations)


def test_lowercase_cross_module_import_is_not_a_contracts_violation(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import reserve_stock
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "use-contracts" for v in violations)


def test_contracts_module_name_is_configurable(make_fake_app) -> None:
    """The contracts module is a convention with a configurable name.

    When ``contracts_module`` is set to a custom name, type imports from that
    module are the exempt ones; the default ``contracts`` becomes ordinary.
    """
    make_fake_app(
        {
            "orders": """
                from fakeapp.shared import StockItem
            """,
            "shared": "",
        }
    )
    mods = [_module("orders"), _module("shared")]
    imports = verifier._collect_imports(_module("orders"))

    # contracts_module="shared" → importing a type from `shared` is allowed.
    allowed = verifier._check_uses_contracts_module(_module("orders"), imports, mods, "shared")
    assert all(v.rule != "use-contracts" for v in allowed)

    # Default "contracts" → `shared` is just another module, so it's flagged.
    flagged = verifier._check_uses_contracts_module(_module("orders"), imports, mods)
    assert any(v.rule == "use-contracts" for v in flagged)


# ---------------------------------------------------------------------------
# Rule 3: declared dependencies
# ---------------------------------------------------------------------------


def test_declared_dependencies(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import something
            """,
            "inventory": "",
            "payments": "",
        }
    )
    # orders declares it may depend only on payments — importing inventory is
    # a violation.
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", declared_dependencies=("payments",)
    )
    try:
        mods = [_module("orders"), _module("inventory"), _module("payments")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        assert any(v.rule == "undeclared-dependency" for v in violations)
    finally:
        manifest_module._reset_for_testing()


def test_no_manifest_means_no_declared_dependency_check(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import something
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "undeclared-dependency" for v in violations)


# ---------------------------------------------------------------------------
# Rule 2: cycle detection
# ---------------------------------------------------------------------------


def test_cycle_detection_linear_has_no_cycle(make_fake_app) -> None:
    make_fake_app(
        {
            "a": "from fakeapp.b import x",
            "b": "from fakeapp.c import y",
            "c": "",
        }
    )
    mods = [_module("a"), _module("b"), _module("c")]
    assert detect_cycles(mods) == []


def test_cycle_detection_simple_cycle(make_fake_app) -> None:
    make_fake_app(
        {
            "a": "from fakeapp.b import x",
            "b": "from fakeapp.a import y",
        }
    )
    mods = [_module("a"), _module("b")]
    cycles = detect_cycles(mods)
    assert len(cycles) == 1
    assert cycles[0].rule == "no-cyclic-dependency"
    assert "a" in cycles[0].message and "b" in cycles[0].message


def test_cycle_detection_complex(make_fake_app) -> None:
    # a -> b -> c -> a  (3-cycle) and an independent d -> e linear chain.
    make_fake_app(
        {
            "a": "from fakeapp.b import x",
            "b": "from fakeapp.c import x",
            "c": "from fakeapp.a import x",
            "d": "from fakeapp.e import x",
            "e": "",
        }
    )
    mods = [_module(n) for n in ("a", "b", "c", "d", "e")]
    cycles = detect_cycles(mods)
    assert len(cycles) == 1
    msg = cycles[0].message
    assert all(m in msg for m in ("a", "b", "c"))


# ---------------------------------------------------------------------------
# Rule 5: data ownership (best-effort, warning)
# ---------------------------------------------------------------------------


def test_data_ownership_warning(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": "",
            "reporting": """
                from sqlalchemy import Table
                t = Table("orders", metadata)
            """,
        }
    )
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", owns_tables=("orders",)
    )
    try:
        mods = [_module("orders"), _module("reporting")]
        violations = verifier.modulith_verify_module(_module("reporting"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert ownership[0].severity is ViolationSeverity.WARNING
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# Baseline (ratcheting)
# ---------------------------------------------------------------------------


def _violation(rule: str = "no-internal-imports") -> Violation:
    return Violation(
        rule=rule,
        message="orders imports inventory._internal",
        module="orders",
        location="fakeapp/orders/__init__.py:1",
    )


def test_baseline_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    write_baseline(path, [_violation()])

    loaded = load_baseline(path)
    assert len(loaded) == 1
    entry = next(iter(loaded))
    assert isinstance(entry, BaselineEntry)
    assert entry.rule == "no-internal-imports"
    assert entry.module == "orders"

    # Stable JSON (sorted keys) so git diffs are clean.
    data = json.loads(path.read_text())
    assert isinstance(data, list)


def test_load_baseline_missing_file_returns_empty(tmp_path: Path) -> None:
    # The baseline is a fingerprint -> count mapping (count-aware ratchet);
    # a missing file grandfathers nothing.
    assert load_baseline(tmp_path / "nope.json") == {}


def test_filter_against_baseline_removes_grandfathered(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    grandfathered = _violation()
    write_baseline(path, [grandfathered])
    baseline = load_baseline(path)

    new_violation = _violation(rule="use-contracts")
    remaining = filter_against_baseline([grandfathered, new_violation], baseline)

    assert grandfathered not in remaining
    assert new_violation in remaining


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_verifier_registered_as_builtin() -> None:
    from modulith import create_plugin_manager

    pm = create_plugin_manager(load_entrypoints=False)
    assert pm.has_plugin("modulith.builtin.verifier")


# ---------------------------------------------------------------------------
# regression: rule 4 runtime-vs-type discrimination, rule 5 __tablename__
# ---------------------------------------------------------------------------


def test_runtime_used_uppercase_import_is_not_a_contracts_violation(make_fake_app) -> None:
    # A runtime class/enum/exception imported from another module (raised,
    # compared, instantiated) is not a shared *type* — rule 4 must not flag it.
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments import PaymentError, PaymentStatus

                def charge(status) -> None:
                    if status == PaymentStatus.PAID:
                        return
                    raise PaymentError("unpaid")
            """,
            "payments": "",
        }
    )
    mods = [_module("orders"), _module("payments")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "use-contracts" for v in violations)


def test_annotation_only_uppercase_import_is_a_contracts_violation(make_fake_app) -> None:
    # A name used only in a type annotation IS a shared type and must route
    # through contracts — still flagged.
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import StockItem

                def handle(item: StockItem) -> None:
                    pass
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


def test_data_ownership_flags_declarative_tablename(make_fake_app) -> None:
    # Rule 5 must detect a table referenced via the declarative ORM
    # ``__tablename__`` pattern, not only SQLAlchemy Core ``Table("x")``.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": "",
            "reporting": """
                from sqlalchemy.orm import DeclarativeBase

                class Base(DeclarativeBase):
                    pass

                class OrderRow(Base):
                    __tablename__ = "orders"
            """,
        }
    )
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", owns_tables=("orders",)
    )
    try:
        mods = [_module("orders"), _module("reporting")]
        violations = verifier.modulith_verify_module(_module("reporting"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert "orders" in ownership[0].message
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# regression: F2 cross-module DB coupling via ForeignKey string literals
# ---------------------------------------------------------------------------


def test_data_ownership_flags_foreign_key_string_literal(make_fake_app) -> None:
    # Rule 5 must detect a table referenced via a SQLAlchemy ``ForeignKey``
    # string literal, not only ``Table("x")``/``__tablename__`` definitions.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "customers": "",
            "orders": """
                import sqlalchemy as sa

                customer_id = sa.Column(sa.ForeignKey("customers.id"))
            """,
        }
    )
    manifest_module._manifests["fakeapp.customers"] = manifest_module.Manifest(
        package="fakeapp.customers", owns_tables=("customers",)
    )
    try:
        mods = [_module("customers"), _module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert "customers" in ownership[0].message
        assert ownership[0].location is not None
        assert ownership[0].location.rsplit(":", 1)[-1].isdigit()
    finally:
        manifest_module._reset_for_testing()


def test_data_ownership_flags_schema_qualified_foreign_key(make_fake_app) -> None:
    # A schema-qualified FK target ("schema.table.column") must resolve to
    # the table name (second-to-last dotted segment), not the full string.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "customers": "",
            "orders": """
                import sqlalchemy as sa

                customer_id = sa.Column(sa.ForeignKey("crm.customers.id"))
            """,
        }
    )
    manifest_module._manifests["fakeapp.customers"] = manifest_module.Manifest(
        package="fakeapp.customers", owns_tables=("customers",)
    )
    try:
        mods = [_module("customers"), _module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert "customers" in ownership[0].message
    finally:
        manifest_module._reset_for_testing()


def test_data_ownership_ignores_foreign_key_within_owning_module(make_fake_app) -> None:
    # A module referencing, via ForeignKey, a table it owns itself is not a
    # violation — only cross-module references are flagged.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "customers": """
                import sqlalchemy as sa

                parent_id = sa.Column(sa.ForeignKey("customers.id"))
            """,
        }
    )
    manifest_module._manifests["fakeapp.customers"] = manifest_module.Manifest(
        package="fakeapp.customers", owns_tables=("customers",)
    )
    try:
        mods = [_module("customers")]
        violations = verifier.modulith_verify_module(_module("customers"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert ownership == []
    finally:
        manifest_module._reset_for_testing()


def test_table_ref_collection_resolves_sqlalchemy_aliases_constraints_and_annotations(
    make_fake_app,
) -> None:
    make_fake_app(
        {
            "orders": """
                import sqlalchemy as sa
                import sqlalchemy.schema as schema
                from sqlalchemy import schema as imported_schema
                from sqlalchemy import ForeignKeyConstraint as FKC
                from sqlalchemy import Table as SATable
                from sqlalchemy.schema import ForeignKey as FK

                direct = sa.Table("orders", metadata)
                module_alias = schema.Table("invoices", metadata)
                imported_module_alias = imported_schema.Table("receipts", metadata)
                symbol_alias = SATable("shipments", metadata)
                foreign_key = FK("crm.customers.id")
                constraint = FKC(["customer_id"], ["crm.customers.id"])
                keyword_table = sa.Table(name="returns", metadata=metadata)
                keyword_foreign_key = FK(column="crm.accounts.id")
                keyword_constraint = FKC(
                    columns=("customer_id", "account_id"),
                    refcolumns=("crm.customers.id", "crm.accounts.id"),
                )

                class AuditRow:
                    __tablename__: str = "audit_log"
            """
        }
    )

    refs = verifier._collect_table_refs(_module("orders"))

    assert [(table, kind) for table, _location, kind in refs] == [
        ("orders", "define"),
        ("invoices", "define"),
        ("receipts", "define"),
        ("shipments", "define"),
        ("customers", "reference"),
        ("customers", "reference"),
        ("returns", "define"),
        ("accounts", "reference"),
        ("customers", "reference"),
        ("accounts", "reference"),
        ("audit_log", "define"),
    ]
    assert all(location.startswith("fakeapp/orders/__init__.py:") for _, location, _ in refs)


def test_table_ref_collection_ignores_unrelated_same_named_callables_without_path_work(
    make_fake_app, monkeypatch
) -> None:
    make_fake_app(
        {
            "orders": """
                from helpers import ForeignKey, Table
                import factory

                local = Table("customers", metadata)
                remote = factory.ForeignKey("customers.id")
            """
        }
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("portable paths are only computed for SQLAlchemy matches")

    monkeypatch.setattr(verifier, "_portable_path", fail_if_called)

    assert verifier._collect_table_refs(_module("orders")) == []


def test_data_ownership_flags_foreign_key_constraint_alias(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "customers": "",
            "orders": """
                from sqlalchemy import ForeignKeyConstraint as FKC

                customer_fk = FKC(["customer_id"], ["crm.customers.id"])
            """,
        }
    )
    manifest_module._manifests["fakeapp.customers"] = manifest_module.Manifest(
        package="fakeapp.customers", owns_tables=("customers",)
    )
    try:
        mods = [_module("customers"), _module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [violation for violation in violations if violation.rule == "data-ownership"]
        assert len(ownership) == 1
        assert "customers" in ownership[0].message
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# Rule 5, declared-inventory half: a table a module defines but omits from owns_tables
# ---------------------------------------------------------------------------


def test_data_ownership_warns_when_defined_table_not_declared_in_owns_tables(make_fake_app) -> None:
    # When a module declares owns_tables and defines a table that is NOT in
    # that list, it should produce a WARNING.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from sqlalchemy.orm import DeclarativeBase

                class Base(DeclarativeBase):
                    pass

                class OrderLineItem(Base):
                    __tablename__ = "order_lines"
            """,
        }
    )
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", owns_tables=("orders",)
    )
    try:
        mods = [_module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert ownership[0].severity is ViolationSeverity.WARNING
        assert "order_lines" in ownership[0].message
        assert "does not declare it in owns_tables" in ownership[0].message
    finally:
        manifest_module._reset_for_testing()


def test_data_ownership_no_warning_for_empty_owns_tables(make_fake_app) -> None:
    # A module with empty/default owns_tables should not generate the "does not
    # declare" warning, even if it defines tables. The verifier should remain
    # silent for modules that haven't opted into ownership declarations.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from sqlalchemy.orm import DeclarativeBase

                class Base(DeclarativeBase):
                    pass

                class OrderLineItem(Base):
                    __tablename__ = "order_lines"
            """,
        }
    )
    # Manifest with empty owns_tables (or no manifest at all)
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", owns_tables=()
    )
    try:
        mods = [_module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        # No "does not declare" warnings for modules with empty owns_tables
        undeclared = [v for v in ownership if "does not declare it in owns_tables" in v.message]
        assert len(undeclared) == 0
    finally:
        manifest_module._reset_for_testing()


def test_data_ownership_warns_for_table_via_core_table_call(make_fake_app) -> None:
    # Also test the Core Table("name") pattern with undeclared ownership.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from sqlalchemy import Table, Column, Integer
                metadata = None
                t = Table("order_lines", metadata, Column("id", Integer))
            """,
        }
    )
    manifest_module._manifests["fakeapp.orders"] = manifest_module.Manifest(
        package="fakeapp.orders", owns_tables=("orders",)
    )
    try:
        mods = [_module("orders")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        undeclared = [v for v in ownership if "does not declare it in owns_tables" in v.message]
        assert len(undeclared) == 1
        assert "order_lines" in undeclared[0].message
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# Rule 1/3/6: dynamic imports (importlib.import_module / __import__)
# ---------------------------------------------------------------------------


def test_importlib_import_module_attribute_form_is_detected(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                import importlib

                importlib.import_module("fakeapp.inventory._internal.store")
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_importlib_import_module_named_form_is_detected(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                from importlib import import_module

                import_module("fakeapp.inventory._internal.store")
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_dunder_import_is_detected(make_fake_app) -> None:
    make_fake_app(
        {
            "orders": """
                __import__("fakeapp.inventory._internal.store")
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_importlib_import_module_non_literal_argument_is_not_detected(make_fake_app) -> None:
    # Documented residual limitation: a non-literal argument cannot be
    # resolved statically. This must not crash — it is simply invisible,
    # same as before the fix.
    make_fake_app(
        {
            "orders": """
                import importlib

                target = "fakeapp.inventory._internal.store"
                importlib.import_module(target)
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "no-internal-imports" for v in violations)


# ---------------------------------------------------------------------------
# disabled_rules: forward-compatible per-rule skip, honored once config
# starts parsing [tool.modulith.verify].disabled_rules.
# ---------------------------------------------------------------------------


def test_disabled_rules_skips_named_rule(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory._internal.store import Repo
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]

    monkeypatch.setattr(
        verifier, "_configured_disabled_rules", lambda: frozenset({"no-internal-imports"})
    )
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "no-internal-imports" for v in violations)


def test_disabled_rules_default_is_empty(make_fake_app) -> None:
    assert verifier._configured_disabled_rules() == frozenset()


# ---------------------------------------------------------------------------
# _package_dir: must never execute ancestor package code
# ---------------------------------------------------------------------------


def test_package_dir_does_not_execute_ancestor_init(make_fake_app) -> None:
    import sys

    make_fake_app(
        {"orders": ""},
        extra_files={"__init__.py": "raise RuntimeError('ancestor package executed')"},
    )
    assert "fakeapp" not in sys.modules

    directory = verifier._package_dir("fakeapp.orders")

    assert directory is not None
    assert directory.name == "orders"
    assert "fakeapp" not in sys.modules


def _reset_namespace_app() -> None:
    import sys

    from modulith.runtime import _runtime

    for name in list(sys.modules):
        if name == "company" or name.startswith("company."):
            del sys.modules[name]
    _runtime._reset_for_testing()


def _activate_namespace_portion_layout(root: Path, monkeypatch) -> tuple[Path, Path]:
    """Build a ``company.shop`` app whose ``company`` namespace root has
    another installed portion (``site``) ahead of the project's (``src``)
    on ``sys.path`` — the editable-install layout. ``orders`` imports
    ``billing._internal``, a no-internal-imports violation.

    Returns ``(src, package_dir)``.
    """
    site = root / "site"
    (site / "company" / "common").mkdir(parents=True)
    (site / "company" / "common" / "__init__.py").write_text("x = 1\n")
    src = root / "src"
    package_dir = src / "company" / "shop"
    for name in ("orders", "billing"):
        (package_dir / name).mkdir(parents=True)
    (package_dir / "__init__.py").write_text("")
    (package_dir / "orders" / "__init__.py").write_text(
        "from company.shop.billing._internal import secret\n"
    )
    (package_dir / "billing" / "__init__.py").write_text("")
    (package_dir / "billing" / "_internal.py").write_text("secret = 1\n")
    _reset_namespace_app()
    monkeypatch.chdir(src)
    monkeypatch.syspath_prepend(str(src))
    monkeypatch.syspath_prepend(str(site))
    monkeypatch.setenv("MODULITH_PACKAGE", "company.shop")
    return src, package_dir


def test_verify_reports_violation_when_other_namespace_portion_precedes_project(
    monkeypatch, request, tmp_path
) -> None:
    """An editable install's .pth puts the project root after site-packages,
    so another installed portion of a PEP 420 root comes first in the root's
    namespace path. The app's package directory must still resolve to the
    project's portion, or verify collects no imports and passes a violation."""
    from typer.testing import CliRunner

    from modulith.cli import app

    request.addfinalizer(_reset_namespace_app)
    _, package_dir = _activate_namespace_portion_layout(tmp_path, monkeypatch)

    result = CliRunner().invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "no-internal-imports" in result.output
    assert "company.shop.billing._internal" in result.output
    assert verifier._package_dir("company.shop") == package_dir


def test_baseline_location_is_relative_when_other_namespace_portion_precedes_project(
    monkeypatch, request, tmp_path
) -> None:
    from typer.testing import CliRunner

    from modulith.cli import app

    request.addfinalizer(_reset_namespace_app)
    _activate_namespace_portion_layout(tmp_path, monkeypatch)
    baseline = tmp_path / "baseline.json"

    result = CliRunner().invoke(app, ["verify", "--update-baseline", "--baseline", str(baseline)])

    assert result.exit_code == 0, result.output
    locations = [entry["location"] for entry in json.loads(baseline.read_text())]
    assert locations == ["company/shop/orders/__init__.py"]


def test_baseline_from_one_checkout_grandfathers_namespace_portion_violation_in_another(
    monkeypatch, request, tmp_path
) -> None:
    """Two copies of the same project at different absolute paths — a
    developer's checkout and a CI runner's — must fingerprint the same
    violation identically, or the ratchet reopens it on every other machine."""
    from typer.testing import CliRunner

    from modulith.cli import app

    request.addfinalizer(_reset_namespace_app)
    baseline = tmp_path / "baseline.json"

    with monkeypatch.context() as first_checkout:
        _activate_namespace_portion_layout(tmp_path / "alice", first_checkout)
        written = CliRunner().invoke(
            app, ["verify", "--update-baseline", "--baseline", str(baseline)]
        )
        assert written.exit_code == 0, written.output

    _activate_namespace_portion_layout(tmp_path / "runner", monkeypatch)
    result = CliRunner().invoke(app, ["verify", "--mode", "ratchet", "--baseline", str(baseline)])

    assert result.exit_code == 0, result.output
    assert "no-internal-imports" not in result.output


# ---------------------------------------------------------------------------
# Rule 5 message wording: "defines" vs "references"
# ---------------------------------------------------------------------------


def test_data_ownership_define_conflict_message_says_defines_not_references(
    make_fake_app,
) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "customers": "",
            "reporting": """
                from sqlalchemy import Table
                t = Table("customers", metadata)
            """,
        }
    )
    manifest_module._manifests["fakeapp.customers"] = manifest_module.Manifest(
        package="fakeapp.customers", owns_tables=("customers",)
    )
    try:
        mods = [_module("customers"), _module("reporting")]
        violations = verifier.modulith_verify_module(_module("reporting"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        assert len(ownership) == 1
        assert "defines" in ownership[0].message
        assert "references" not in ownership[0].message
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# RULE_NAMES constant: must enumerate all rule literals in the module
# ---------------------------------------------------------------------------


def test_rule_names_matches_actual_rules() -> None:
    """RULE_NAMES constant should match every rule literal in the module."""
    import inspect
    import re

    source = inspect.getsource(verifier)
    rule_literals = set(re.findall(r'rule="([^"]+)"', source))

    assert rule_literals == verifier.RULE_NAMES, (
        f"RULE_NAMES mismatch: {rule_literals} != {verifier.RULE_NAMES}"
    )


# ---------------------------------------------------------------------------
# disabled_rules validation: warn about unknown rule names
# ---------------------------------------------------------------------------


def test_disabled_rules_logs_warning_for_unknown_rule(make_fake_app, monkeypatch, caplog) -> None:
    """Unknown rule names in disabled_rules should produce a warning."""
    import logging
    from unittest.mock import MagicMock

    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory._internal.store import Repo
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]

    # Mock the runtime config to return unknown rule names
    mock_cfg = MagicMock()
    mock_cfg.verify_disabled_rules = ("use-contract", "unknown-rule")
    mock_runtime = MagicMock()
    mock_runtime.config = mock_cfg
    monkeypatch.setattr("modulith.runtime._runtime", mock_runtime, raising=False)

    with caplog.at_level(logging.WARNING):
        verifier.modulith_verify_module(_module("orders"), mods)

    # Check that a warning was logged with the unknown rule names
    assert any(
        "disabled_rules names no known rule" in record.message for record in caplog.records
    ), f"Expected warning about unknown rules, got: {[r.message for r in caplog.records]}"


def test_disabled_rules_no_warning_for_known_rule(make_fake_app, monkeypatch, caplog) -> None:
    """Known rule names should not produce warnings."""
    import logging
    from unittest.mock import MagicMock

    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory._internal.store import Repo
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]

    # Mock the runtime config to return a known rule name
    mock_cfg = MagicMock()
    mock_cfg.verify_disabled_rules = ("no-internal-imports",)
    mock_runtime = MagicMock()
    mock_runtime.config = mock_cfg
    monkeypatch.setattr("modulith.runtime._runtime", mock_runtime, raising=False)

    with caplog.at_level(logging.WARNING):
        verifier.modulith_verify_module(_module("orders"), mods)

    # Check that no warning was logged
    assert not any(
        "disabled_rules names no known rule" in record.message for record in caplog.records
    ), f"Unexpected warning for known rule, got: {[r.message for r in caplog.records]}"


# Keep ImportRecord referenced for import-time coverage of the dataclass.
assert ImportRecord is not None
