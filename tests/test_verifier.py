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


# Keep ImportRecord referenced for import-time coverage of the dataclass.
assert ImportRecord is not None
