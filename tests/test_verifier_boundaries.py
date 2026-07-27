"""Regression tests for the boundary verifier (``modulith/builtin/verifier.py``),
manifests (``modulith/manifest.py``) and the audit tool (``modulith/audit.py``).

Written test-first: every behavioral test here failed against the pre-fix code
for the reason its own docstring describes.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import types as types_module
from dataclasses import replace
from pathlib import Path
from textwrap import dedent

import pytest

from modulith import ModuleInfo, Violation
from modulith.audit import audit_codebase, render_report
from modulith.builtin import verifier
from modulith.builtin.verifier import (
    detect_cycles,
    filter_against_baseline,
    load_baseline,
    write_baseline,
)
from modulith.config import ConfigurationError
from modulith.manifest import Manifest, verify_manifest
from modulith.types import ViolationSeverity


def _module(name: str) -> ModuleInfo:
    return ModuleInfo(name=name, package=f"fakeapp.{name}")


def _write(root: Path, rel: str, source: str) -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(dedent(source), encoding="utf-8")


# ---------------------------------------------------------------------------
# Baseline fingerprint must survive unrelated line shifts
# ---------------------------------------------------------------------------


def test_baseline_survives_line_shift(tmp_path: Path) -> None:
    """A grandfathered violation must stay grandfathered when an
    unrelated edit above it shifts its line number."""
    path = tmp_path / "baseline.json"
    at_line_10 = Violation(
        rule="no-internal-imports",
        message="orders imports inventory._internal",
        module="orders",
        location="fakeapp/orders/__init__.py:10",
    )
    write_baseline(path, [at_line_10])
    baseline = load_baseline(path)

    # Same violation, one line lower (someone inserted a line above it).
    at_line_11 = replace(at_line_10, location="fakeapp/orders/__init__.py:11")
    assert filter_against_baseline([at_line_11], baseline) == []


def test_legacy_baseline_with_file_line_locations_still_matches(tmp_path: Path) -> None:
    """Baselines written by older versions carry ``file:line``
    locations; they must be normalized on load, not silently mismatched."""
    message = "orders imports inventory._internal"
    digest = hashlib.sha256(message.encode("utf-8")).hexdigest()[:8]
    path = tmp_path / "baseline.json"
    path.write_text(
        json.dumps(
            [
                {
                    "rule": "no-internal-imports",
                    "module": "orders",
                    "location": "fakeapp/orders/__init__.py:10",
                    "message_hash": digest,
                }
            ]
        ),
        encoding="utf-8",
    )
    baseline = load_baseline(path)

    shifted = Violation(
        rule="no-internal-imports",
        message=message,
        module="orders",
        location="fakeapp/orders/__init__.py:42",
    )
    assert filter_against_baseline([shifted], baseline) == []


# ---------------------------------------------------------------------------
# Count-aware ratchet: a NEW violation identical to a
# grandfathered one (same rule/module/file/message, different line) must
# not slip through the baseline
# ---------------------------------------------------------------------------


def _ratchet_violation(location: str) -> Violation:
    return Violation(
        rule="no-internal-imports",
        message="orders imports inventory._internal",
        module="orders",
        location=location,
    )


def test_new_identical_violation_fails_ratchet(tmp_path: Path) -> None:
    """With ONE grandfathered violation baselined, a SECOND
    violation carrying the identical fingerprint (new line, same file/rule/
    module/message) must be reported — the baseline grants an allowance of
    one, not a blanket pass for the fingerprint."""
    path = tmp_path / "baseline.json"
    first = _ratchet_violation("fakeapp/orders/__init__.py:10")
    write_baseline(path, [first])
    baseline = load_baseline(path)

    second = replace(first, location="fakeapp/orders/__init__.py:99")
    reported = filter_against_baseline([first, second], baseline)

    assert len(reported) == 1


def test_ratchet_allows_up_to_baselined_count(tmp_path: Path) -> None:
    """A baseline recorded with two identical violations allows
    two — and fewer than baselined stays green (the ratchet only tightens)."""
    path = tmp_path / "baseline.json"
    first = _ratchet_violation("fakeapp/orders/__init__.py:10")
    second = replace(first, location="fakeapp/orders/__init__.py:99")
    write_baseline(path, [first, second])
    baseline = load_baseline(path)

    assert filter_against_baseline([first, second], baseline) == []
    assert filter_against_baseline([first], baseline) == []  # improvement passes
    third = replace(first, location="fakeapp/orders/__init__.py:120")
    assert len(filter_against_baseline([first, second, third], baseline)) == 1


def test_write_baseline_records_fingerprint_counts(tmp_path: Path) -> None:
    """The baseline stores one entry per fingerprint with its
    count, so --update-baseline captures the multiplicity."""
    path = tmp_path / "baseline.json"
    first = _ratchet_violation("fakeapp/orders/__init__.py:10")
    second = replace(first, location="fakeapp/orders/__init__.py:99")
    write_baseline(path, [first, second])

    data = json.loads(path.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["count"] == 2


def test_old_baseline_without_count_defaults_to_one(tmp_path: Path) -> None:
    """Backward compat: entries written by older versions carry no
    ``count`` — they read as an allowance of exactly one."""
    message = "orders imports inventory._internal"
    digest = hashlib.sha256(message.encode("utf-8")).hexdigest()[:8]
    path = tmp_path / "baseline.json"
    path.write_text(
        json.dumps(
            [
                {
                    "rule": "no-internal-imports",
                    "module": "orders",
                    "location": "fakeapp/orders/__init__.py",
                    "message_hash": digest,
                }
            ]
        ),
        encoding="utf-8",
    )
    baseline = load_baseline(path)

    first = _ratchet_violation("fakeapp/orders/__init__.py:10")
    second = replace(first, location="fakeapp/orders/__init__.py:99")
    assert filter_against_baseline([first], baseline) == []
    assert len(filter_against_baseline([first, second], baseline)) == 1


def test_load_baseline_invalid_count_raises_configuration_error(tmp_path: Path) -> None:
    """A malformed ``count`` is a schema error with the same clean
    ConfigurationError treatment as the other fields."""
    path = tmp_path / "baseline.json"
    path.write_text(
        json.dumps(
            [
                {
                    "rule": "r",
                    "module": "m",
                    "location": "f.py",
                    "message_hash": "00000000",
                    "count": "two",
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="count"):
        load_baseline(path)


# ---------------------------------------------------------------------------
# TYPE_CHECKING imports visible to rules 1, 3, 4;
# still exempt from cycle detection (rule 2)
# ---------------------------------------------------------------------------


def test_type_checking_private_import_flagged_by_rule_1(make_fake_app) -> None:
    """A private-package import inside ``if TYPE_CHECKING:`` must
    still violate no-internal-imports."""
    make_fake_app(
        {
            "orders": """
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.inventory._internal.types import SecretType

                def handle(item: "SecretType") -> None:
                    pass
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_type_checking_type_import_flagged_by_rule_4(make_fake_app) -> None:
    """A cross-module type import inside ``if TYPE_CHECKING:`` is
    exactly rule 4's target case and must be flagged."""
    make_fake_app(
        {
            "orders": """
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.inventory import StockItem

                def handle(item: "StockItem") -> None:
                    pass
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


def test_type_checking_import_flagged_by_rule_3(make_fake_app) -> None:
    """A TYPE_CHECKING-only import from an undeclared module must
    violate declared_dependencies (deny-all via explicit empty tuple)."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.inventory import StockItem
            """,
            "inventory": "",
        }
    )
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", declared_dependencies=()
    )
    try:
        mods = [_module("orders"), _module("inventory")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        assert any(v.rule == "undeclared-dependency" for v in violations)
    finally:
        manifest_module._reset_for_testing()


def test_type_checking_mutual_imports_are_not_a_cycle(make_fake_app) -> None:
    """TYPE_CHECKING-guarded imports impose no runtime
    dependency — they are the sanctioned way to break a runtime cycle, so
    cycle detection (rule 2) must ignore them."""
    make_fake_app(
        {
            "a": """
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.b import B
            """,
            "b": """
                from typing import TYPE_CHECKING

                if TYPE_CHECKING:
                    from fakeapp.a import A
            """,
        }
    )
    assert detect_cycles([_module("a"), _module("b")]) == []


# ---------------------------------------------------------------------------
# TYPE_CHECKING alias must be recognized as a guard
# ---------------------------------------------------------------------------


def test_aliased_type_checking_guard_is_recognized(make_fake_app) -> None:
    """``from typing import TYPE_CHECKING as TC`` must still be
    detected as a type-only guard — mutual TC-guarded imports are not a
    runtime cycle, and the guarded records are tagged type_only."""
    make_fake_app(
        {
            "a": """
                from typing import TYPE_CHECKING as TC

                if TC:
                    from fakeapp.b import B
            """,
            "b": """
                from typing import TYPE_CHECKING as TC

                if TC:
                    from fakeapp.a import A
            """,
        }
    )
    records = verifier._collect_imports(_module("a"))
    guarded = [r for r in records if r.target_module == "fakeapp.b"]
    assert len(guarded) == 1
    assert guarded[0].type_only

    assert detect_cycles([_module("a"), _module("b")]) == []


# ---------------------------------------------------------------------------
# Rule 4 must match runtime usage by the locally-bound alias
# ---------------------------------------------------------------------------


def test_aliased_import_used_at_runtime_is_not_flagged(make_fake_app) -> None:
    """``from x import Y as Z`` with Z used at runtime is a runtime
    value, not an annotation-only type — rule 4 must not flag it."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments import PaymentStatus as PS

                def is_paid(status) -> bool:
                    return status == PS.PAID
            """,
            "payments": "",
        }
    )
    mods = [_module("orders"), _module("payments")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "use-contracts" for v in violations)


def test_aliased_annotation_only_import_is_still_flagged(make_fake_app) -> None:
    """Guard against over-fix: an aliased import used only in an
    annotation is still an annotation-only type import — flagged."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments import PaymentStatus as PS

                def handle(status: PS) -> None:
                    pass
            """,
            "payments": "",
        }
    )
    mods = [_module("orders"), _module("payments")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


# ---------------------------------------------------------------------------
# Names referenced only inside TYPE_CHECKING are not runtime uses
# ---------------------------------------------------------------------------


def test_type_checking_reference_does_not_count_as_runtime_use(make_fake_app) -> None:
    """A name referenced only inside ``if TYPE_CHECKING:`` never
    executes — it must not exempt a real annotation-only import from rule 4."""
    make_fake_app(
        {
            "orders": """
                from typing import TYPE_CHECKING
                from fakeapp.inventory import StockItem

                if TYPE_CHECKING:
                    _AliasForMypy = StockItem

                def handle(item: StockItem) -> None:
                    pass
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


# ---------------------------------------------------------------------------
# The contracts package is an import sink
# ---------------------------------------------------------------------------


def test_contracts_importing_from_module_is_flagged(make_fake_app) -> None:
    """SPEC 5.3 — contracts may not import from any application
    module; a plain runtime import must be flagged."""
    make_fake_app(
        {
            "contracts": """
                from fakeapp.orders.service import do_something

                do_something()
            """,
            "orders": "",
        }
    )
    mods = [_module("contracts"), _module("orders")]
    violations = verifier.modulith_verify_module(_module("contracts"), mods)
    sink = [v for v in violations if v.rule == "contracts-is-sink"]
    assert len(sink) == 1
    assert "orders" in sink[0].message


def test_module_importing_contracts_is_not_flagged_as_sink(make_fake_app) -> None:
    """Direction check: the sink rule only applies to the
    contracts module itself."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.contracts import OrderCreated

                event = OrderCreated
            """,
            "contracts": "",
        }
    )
    mods = [_module("orders"), _module("contracts")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "contracts-is-sink" for v in violations)


# ---------------------------------------------------------------------------
# Rule 3's message must not embed the declared_dependencies list
# ---------------------------------------------------------------------------


def test_adding_a_dependency_does_not_reopen_grandfathered_violations(
    make_fake_app, tmp_path: Path
) -> None:
    """Fixing one undeclared dependency by declaring it must not
    change the baseline fingerprint of the other grandfathered violations."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import a
                from fakeapp.shipping import b
            """,
            "inventory": "",
            "shipping": "",
            "payments": "",
        }
    )
    mods = [_module(n) for n in ("orders", "inventory", "shipping", "payments")]
    path = tmp_path / "baseline.json"
    try:
        manifest_module._manifests["fakeapp.orders"] = Manifest(
            package="fakeapp.orders", declared_dependencies=("payments",)
        )
        imports = verifier._collect_imports(_module("orders"))
        before = verifier._check_declared_dependencies(_module("orders"), imports, mods)
        assert len(before) == 2
        write_baseline(path, before)
        baseline = load_baseline(path)

        # The team legitimately declares shipping; inventory stays grandfathered.
        manifest_module._manifests["fakeapp.orders"] = Manifest(
            package="fakeapp.orders", declared_dependencies=("payments", "shipping")
        )
        after = verifier._check_declared_dependencies(_module("orders"), imports, mods)
        assert len(after) == 1
        assert filter_against_baseline(after, baseline) == []
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# declared_dependencies: None = not declared; () = deny-all
# ---------------------------------------------------------------------------


def test_manifest_without_declared_dependencies_skips_rule_3(make_fake_app) -> None:
    """Declaring a manifest for an unrelated field
    (owns_tables only) must NOT switch rule 3 into deny-all mode."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments.service import charge
            """,
            "payments": "",
        }
    )
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", owns_tables=("orders",)
    )
    try:
        mods = [_module("orders"), _module("payments")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        assert all(v.rule != "undeclared-dependency" for v in violations)
    finally:
        manifest_module._reset_for_testing()


def test_explicit_empty_declared_dependencies_means_deny_all(make_fake_app) -> None:
    """An explicit empty tuple means 'depends on
    nothing' and enforces deny-all (contracts excepted)."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments.service import charge
            """,
            "payments": "",
        }
    )
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", declared_dependencies=()
    )
    try:
        mods = [_module("orders"), _module("payments")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        assert any(v.rule == "undeclared-dependency" for v in violations)
    finally:
        manifest_module._reset_for_testing()


def test_declare_module_default_dependencies_remain_iterable_but_undeclared() -> None:
    """Omitted dependencies remain distinguishable without returning None."""
    from modulith import declare_module
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    try:
        declare_module(publishes=["SomethingHappened"])
        m = manifest_module.get_manifest(__name__)
        assert m is not None
        assert m.declared_dependencies == ()
        assert m.dependencies_declared is False
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# Rule 4 must not be blind to wildcard imports
# ---------------------------------------------------------------------------


def test_star_import_across_modules_is_flagged(make_fake_app) -> None:
    """``from other_module import *`` cannot be resolved to
    specific names — the wildcard import itself is flagged by rule 4."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import *

                thing = InventoryThing()
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    star = [v for v in violations if v.rule == "use-contracts"]
    assert len(star) == 1
    assert "*" in star[0].message


def test_star_import_from_contracts_is_allowed(make_fake_app) -> None:
    """Scope check: wildcard imports from contracts stay exempt."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.contracts import *
            """,
            "contracts": "",
        }
    )
    mods = [_module("orders"), _module("contracts")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "use-contracts" for v in violations)


# ---------------------------------------------------------------------------
# Conflicting owns_tables declarations
# ---------------------------------------------------------------------------


def test_co_owner_of_conflicted_table_is_not_falsely_flagged(make_fake_app) -> None:
    """When two manifests both declare the same table, the module
    that co-declared ownership must not be flagged as referencing 'someone
    else's' table; the conflict itself must be surfaced instead."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from sqlalchemy import Table

                t = Table("invoices", None)
            """,
            "billing": "",
        }
    )
    # orders registers FIRST (would lose a last-write-wins race to billing).
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", owns_tables=("invoices",)
    )
    manifest_module._manifests["fakeapp.billing"] = Manifest(
        package="fakeapp.billing", owns_tables=("invoices",)
    )
    try:
        mods = [_module("orders"), _module("billing")]
        violations = verifier.modulith_verify_module(_module("orders"), mods)
        ownership = [v for v in violations if v.rule == "data-ownership"]
        # No false positive against the legitimate co-owner...
        assert all("references table" not in v.message for v in ownership)
        # ...and the manifest conflict is reported, naming both claimants.
        conflicts = [v for v in ownership if "invoices" in v.message]
        assert len(conflicts) == 1
        assert "orders" in conflicts[0].message and "billing" in conflicts[0].message
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# load_baseline error handling
# ---------------------------------------------------------------------------


def test_load_baseline_invalid_json_raises_configuration_error(tmp_path: Path) -> None:
    """Malformed JSON must raise a clear ConfigurationError naming
    the path, not a raw JSONDecodeError."""
    path = tmp_path / "baseline.json"
    path.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ConfigurationError, match=re.escape(str(path))):
        load_baseline(path)


def test_load_baseline_missing_keys_raises_configuration_error(tmp_path: Path) -> None:
    """A schema-mismatched entry must raise ConfigurationError
    suggesting --update-baseline, not a raw KeyError."""
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps([{"rule": "x", "module": "y"}]), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="update-baseline"):
        load_baseline(path)


def test_load_baseline_non_list_payload_raises_configuration_error(tmp_path: Path) -> None:
    """A valid-JSON but wrong-shape payload is also rejected loudly."""
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"rule": "x"}), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="list"):
        load_baseline(path)


# ---------------------------------------------------------------------------
# Manifest verification errors carry file:line
# ---------------------------------------------------------------------------


def test_unregistered_listener_error_includes_file_and_line() -> None:
    """SPEC promises 'a clear error and file:line' when a declared
    listener wasn't registered."""

    async def orphan_listener(e: object) -> None: ...

    m = Manifest(package="fakeapp.orders", listeners=(orphan_listener,))
    errors = verify_manifest(m, registered_listeners=set())
    assert len(errors) == 1
    assert re.search(r"\.py:\d+", errors[0]), errors[0]
    assert "orphan_listener" in errors[0]


# ---------------------------------------------------------------------------
# The publishes check must flag names bound to None
# ---------------------------------------------------------------------------


def test_publishes_name_bound_to_none_is_flagged() -> None:
    """An event name explicitly bound to None (failed conditional
    import) must be flagged, per the check's own stated intent."""
    fake_pkg = types_module.ModuleType("fakeapp_none_event")
    fake_pkg.OrderCreated = None  # type: ignore[attr-defined]
    sys.modules["fakeapp_none_event"] = fake_pkg
    try:
        m = Manifest(package="fakeapp_none_event", publishes=("OrderCreated",))
        errors = verify_manifest(m, registered_listeners=set())
        assert len(errors) == 1
        assert "OrderCreated" in errors[0]
        assert "None" in errors[0]
    finally:
        del sys.modules["fakeapp_none_event"]


def test_publishes_name_present_and_not_none_is_clean() -> None:
    """Guard against over-fix: a defined event type passes."""
    fake_pkg = types_module.ModuleType("fakeapp_defined_event")
    fake_pkg.OrderCreated = object()  # type: ignore[attr-defined]
    sys.modules["fakeapp_defined_event"] = fake_pkg
    try:
        m = Manifest(package="fakeapp_defined_event", publishes=("OrderCreated",))
        assert verify_manifest(m, registered_listeners=set()) == []
    finally:
        del sys.modules["fakeapp_defined_event"]


# ---------------------------------------------------------------------------
# Audit of an empty tree must not report a confident 100/100
# ---------------------------------------------------------------------------


def test_audit_empty_directory_warns_instead_of_confident_score(tmp_path: Path) -> None:
    """Zero .py files found must be distinguishable from 'genuinely
    zero coupling' — the report warns and the module section says None."""
    root = tmp_path / "empty"
    root.mkdir()
    result = audit_codebase(root)
    assert result.files_scanned == 0

    report = render_report(result)
    assert "no Python files" in report
    assert "None — no Python modules detected." in report


def test_audit_populated_tree_reports_files_scanned(tmp_path: Path) -> None:
    """A populated tree records how many files were scanned and
    renders no empty-tree warning."""
    root = tmp_path / "app"
    _write(root, "__init__.py", "")
    _write(root, "orders/__init__.py", "")
    _write(root, "orders/service.py", "def create(): ...\n")
    result = audit_codebase(root)
    assert result.files_scanned == 3

    report = render_report(result)
    assert "no Python files" not in report


# ---------------------------------------------------------------------------
# Readiness score caveat for shared tables
# ---------------------------------------------------------------------------


def test_report_caveats_score_when_shared_tables_present(tmp_path: Path) -> None:
    """The formula stays, but the report must say
    the score excludes shared-table entanglement when tables are shared."""
    root = tmp_path / "app"
    _write(root, "__init__.py", "")
    _write(
        root,
        "orders/models.py",
        """
        from sqlalchemy import Table, MetaData

        t = Table("invoices", MetaData())
        """,
    )
    _write(root, "orders/__init__.py", "")
    _write(
        root,
        "billing/models.py",
        """
        from sqlalchemy import Table, MetaData

        t = Table("invoices", MetaData())
        """,
    )
    _write(root, "billing/__init__.py", "")

    result = audit_codebase(root)
    assert result.shared_tables == ["invoices"]
    assert result.readiness_score == 100  # shared tables are reported, not scored

    report = render_report(result)
    assert "shared-table entanglement" in report


# ---------------------------------------------------------------------------
# Stdlib/third-party name collisions in the audit import heuristic
# ---------------------------------------------------------------------------


def test_stdlib_import_colliding_with_local_dir_not_counted(tmp_path: Path) -> None:
    """``import types`` (stdlib) must not be reported as coupling
    to a local ``types/`` module directory when the root is a package."""
    root = tmp_path / "myapp"
    _write(root, "__init__.py", "")
    _write(root, "types/__init__.py", "")
    _write(root, "types/models.py", "X = 1\n")
    _write(root, "billing/__init__.py", "")
    _write(root, "billing/report.py", "import types\n\nns = types.SimpleNamespace()\n")

    result = audit_codebase(root)
    pairs = {(src, tgt) for src, tgt, _c, _s in result.cross_module_imports}
    assert ("billing", "types") not in pairs


def test_flat_layout_stdlib_collision_not_counted(tmp_path: Path) -> None:
    """In a flat (non-package) layout the stdlib name still wins
    the ambiguity — no false coupling edge."""
    root = tmp_path / "repo"
    _write(root, "types/__init__.py", "")
    _write(root, "types/models.py", "X = 1\n")
    _write(root, "billing/__init__.py", "")
    _write(root, "billing/report.py", "import types\n\nns = types.SimpleNamespace()\n")

    result = audit_codebase(root)
    pairs = {(src, tgt) for src, tgt, _c, _s in result.cross_module_imports}
    assert ("billing", "types") not in pairs


def test_flat_layout_bare_local_import_still_detected(tmp_path: Path) -> None:
    """Guard against over-fix: a genuine bare intra-tree import
    in a flat layout is still a cross-module edge."""
    root = tmp_path / "repo"
    _write(root, "inventory/__init__.py", "")
    _write(root, "inventory/stock.py", "def reserve(): ...\n")
    _write(root, "billing/__init__.py", "")
    _write(root, "billing/report.py", "from inventory.stock import reserve\n")

    result = audit_codebase(root)
    pairs = {(src, tgt) for src, tgt, _c, _s in result.cross_module_imports}
    assert ("billing", "inventory") in pairs


# ---------------------------------------------------------------------------
# Shorthand ImportFrom resolution
# ---------------------------------------------------------------------------


def test_shorthand_private_import_is_flagged(make_fake_app) -> None:
    """``from <owner> import _name`` resolves target_module to the owner
    package itself (remainder empty), so the existing first-segment check
    never sees the leading underscore. ``_name`` may be a private submodule
    rather than a public attribute — the leading-underscore convention must
    apply to the imported name too, not only to target_module's remainder."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_shorthand_public_import_is_not_flagged(make_fake_app) -> None:
    """Guard against over-fix: a shorthand import of a public (non-``_``)
    name from another module's top-level package must stay clean."""
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
    assert all(v.rule != "no-internal-imports" for v in violations)


def test_private_name_imported_from_a_submodule_is_flagged(make_fake_app) -> None:
    """The imported-name check only ran when target_module WAS the owner
    package, so the deeper — and more natural — spelling escaped: reaching
    for a neighbour's private symbol via one of its public submodules is the
    same boundary breach as reaching for it via the package root."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory.models import _Hidden
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_nested_private_subpackage_is_flagged(make_fake_app) -> None:
    """Only the FIRST remainder segment was tested for a leading underscore,
    so a private subpackage nested under a public one was invisible — the
    module docstring promises "any other ``_``-prefixed subpackage"."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory.models._priv import thing
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "no-internal-imports" for v in violations)


def test_public_name_imported_from_a_submodule_is_not_flagged(make_fake_app) -> None:
    """Guard against over-fix: a public name from a public submodule of
    another module stays clean."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory.models import StockItem
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert all(v.rule != "no-internal-imports" for v in violations)


# ---------------------------------------------------------------------------
# Parse failures must surface as violations
# ---------------------------------------------------------------------------


def test_unparseable_file_is_surfaced_as_error(make_fake_app, tmp_path: Path) -> None:
    """A file that fails to parse is currently logged and silently skipped —
    every import it would have contributed is invisible to every rule, so
    verification can pass while blind to real violations. It must surface
    as its own ERROR-severity violation instead."""
    make_fake_app({"orders": ""})
    (tmp_path / "fakeapp" / "orders" / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    violations = verifier.modulith_verify_module(_module("orders"), [_module("orders")])
    parse_errors = [v for v in violations if v.rule == "parse-error"]
    assert len(parse_errors) == 1
    assert parse_errors[0].severity is ViolationSeverity.ERROR
    assert "broken.py" in parse_errors[0].message


# ---------------------------------------------------------------------------
# Rule 4 runtime-name analysis must be per file
# ---------------------------------------------------------------------------


def test_runtime_name_in_other_file_does_not_exempt_annotation_only_import(
    make_fake_app,
) -> None:
    """``_runtime_loaded_names`` aggregated runtime names across every file
    in the module — so a runtime use of ``PaymentStatus`` in one file wrongly
    exempted an unrelated annotation-only import of a *different* name that
    merely collides on local binding in another file. Runtime usage must be
    checked against the file that actually imports the candidate."""
    make_fake_app(
        {
            "orders": "",
        },
        extra_files={
            "orders/receipts.py": """
                from fakeapp.payments import PaymentStatus

                def describe(status: PaymentStatus) -> str:
                    return str(status)
            """,
            "orders/runtime_user.py": """
                PaymentStatus = "unrelated local binding, not the import above"

                def touch() -> str:
                    return PaymentStatus
            """,
        },
    )
    mods = [_module("orders"), _module("payments")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    assert any(v.rule == "use-contracts" for v in violations)


# ---------------------------------------------------------------------------
# Ratchet baseline locations must be portable
# ---------------------------------------------------------------------------


def test_violation_location_is_package_relative_not_absolute(make_fake_app, tmp_path) -> None:
    """A baseline checked into version control must match on every
    checkout/CI runner — an absolute path like the pytest tmp_path below
    never matches on a teammate's machine. Locations must be relative to
    the application's source root."""
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        }
    )
    mods = [_module("orders"), _module("inventory")]
    violations = verifier.modulith_verify_module(_module("orders"), mods)
    located = [v for v in violations if v.location]
    assert located, "expected at least one violation with a location"
    for v in located:
        assert v.location is not None
        assert str(tmp_path) not in v.location
        assert not Path(v.location.rsplit(":", 1)[0]).is_absolute()


# ---------------------------------------------------------------------------
# strict_boundaries — enforce boundary violations fatally at bootstrap
# ---------------------------------------------------------------------------


def test_strict_boundaries_false_allows_boundary_violations_at_bootstrap(make_fake_app) -> None:
    """When strict_boundaries=False (the default), bootstrap runs no boundary
    scan at all: the whole enforcement block in ``Runtime.ensure_bootstrapped``
    is gated on the flag, so nothing is verified, nothing is logged and nothing
    warns. An app with violations starts clean — ``modulith verify`` is what
    catches them in CI."""
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        }
    )
    # strict_boundaries defaults to False — bootstrap should succeed
    _runtime.ensure_bootstrapped()
    assert _runtime._bootstrapped
    assert _runtime.config is not None
    assert _runtime.config.strict_boundaries is False


def test_strict_boundaries_true_config_is_applied_at_bootstrap(make_fake_app) -> None:
    """When strict_boundaries=True is configured, it is applied during bootstrap.

    This only checks the flag is threaded through to Configuration on a clean
    app; it does not seed a violation, so it proves nothing about enforcement.
    See test_strict_boundaries_true_raises_on_boundary_violation below (and
    the per-rule tests further down) for actual enforcement coverage."""
    from modulith.runtime import _runtime

    # Create a clean app with no boundary violations
    make_fake_app(
        {
            "orders": "",
            "inventory": "",
        }
    )
    # Configure strict_boundaries=True before bootstrap
    _runtime.configure(strict_boundaries=True)
    # Bootstrap should succeed and apply the configuration
    _runtime.ensure_bootstrapped()
    assert _runtime._bootstrapped
    assert _runtime.config is not None
    assert _runtime.config.strict_boundaries is True


def test_strict_boundaries_true_allows_clean_boundary_at_bootstrap(make_fake_app) -> None:
    """When strict_boundaries=True and there are no boundary violations,
    bootstrap succeeds normally."""
    from modulith.runtime import _runtime

    # Create a clean app with no boundary violations
    make_fake_app(
        {
            "orders": """
                # No cross-module imports
            """,
            "inventory": """
                # No cross-module imports
            """,
        }
    )
    # Configure strict_boundaries=True
    _runtime.configure(strict_boundaries=True)
    # Bootstrap should succeed because there are no violations
    _runtime.ensure_bootstrapped()
    assert _runtime._bootstrapped
    assert _runtime.config is not None
    assert _runtime.config.strict_boundaries is True


def test_strict_boundaries_true_raises_on_boundary_violation(make_fake_app) -> None:
    """When strict_boundaries=True and there are boundary violations,
    ensure_bootstrapped() raises ConfigurationError."""
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        },
        extra_files={
            "inventory/_internal/__init__.py": "# private submodule\n",
        },
    )
    # Configure package and strict_boundaries=True
    _runtime.configure(package="fakeapp", strict_boundaries=True)
    # Bootstrap should raise ConfigurationError due to boundary violation
    with pytest.raises(ConfigurationError):
        _runtime.ensure_bootstrapped()


# ---------------------------------------------------------------------------
# strict_boundaries — fatal bootstrap gate must cover every verifier rule,
# not just rule 1 (no-internal-imports). Each test below seeds a violation
# specific to one rule and asserts ensure_bootstrapped() raises with that
# rule name in the message.
# ---------------------------------------------------------------------------


def test_strict_boundaries_true_raises_on_use_contracts_violation(make_fake_app) -> None:
    """Rule 4 (use-contracts): an annotation-only cross-module type import
    not sourced from the contracts module is fatal under strict_boundaries."""
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "orders": """
                from fakeapp.payments import PaymentStatus

                def handle(status: PaymentStatus) -> None:
                    pass
            """,
            "payments": "",
        }
    )
    _runtime.configure(package="fakeapp", strict_boundaries=True)
    with pytest.raises(ConfigurationError, match="use-contracts"):
        _runtime.ensure_bootstrapped()


def test_strict_boundaries_true_raises_on_undeclared_dependency(make_fake_app) -> None:
    """Rule 3 (undeclared-dependency): an import outside a manifest's
    declared_dependencies is fatal under strict_boundaries."""
    from modulith import manifest as manifest_module
    from modulith.runtime import _runtime

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from fakeapp.payments.service import charge
            """,
            "payments": "",
        }
    )
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", declared_dependencies=()
    )
    try:
        _runtime.configure(package="fakeapp", strict_boundaries=True)
        with pytest.raises(ConfigurationError, match="undeclared-dependency"):
            _runtime.ensure_bootstrapped()
    finally:
        manifest_module._reset_for_testing()


def test_strict_boundaries_true_raises_on_data_ownership_violation(make_fake_app) -> None:
    """Rule 5 (data-ownership): fatal even though the rule's own severity is
    WARNING — strict_boundaries has no severity filter (README: 'ERROR or
    WARNING'), only a violations-present check."""
    from modulith import manifest as manifest_module
    from modulith.runtime import _runtime

    manifest_module._reset_for_testing()
    make_fake_app({"orders": "", "billing": ""})
    manifest_module._manifests["fakeapp.orders"] = Manifest(
        package="fakeapp.orders", owns_tables=("invoices",)
    )
    manifest_module._manifests["fakeapp.billing"] = Manifest(
        package="fakeapp.billing", owns_tables=("invoices",)
    )
    try:
        _runtime.configure(package="fakeapp", strict_boundaries=True)
        with pytest.raises(ConfigurationError, match="data-ownership"):
            _runtime.ensure_bootstrapped()
    finally:
        manifest_module._reset_for_testing()


def test_strict_boundaries_true_raises_on_contracts_is_sink_violation(make_fake_app) -> None:
    """Rule 6 (contracts-is-sink): the contracts module importing from an
    application module is fatal under strict_boundaries."""
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "contracts": """
                from fakeapp.orders.service import do_something

                do_something()
            """,
            "orders": "",
        }
    )
    _runtime.configure(package="fakeapp", strict_boundaries=True)
    with pytest.raises(ConfigurationError, match="contracts-is-sink"):
        _runtime.ensure_bootstrapped()


def test_strict_boundaries_true_raises_on_parse_error_violation(
    make_fake_app, tmp_path: Path
) -> None:
    """An unparseable file is an ERROR-severity violation and fatal under
    strict_boundaries — discovery only imports each module's ``__init__.py``,
    so the broken sibling file surfaces solely through the static AST scan."""
    from modulith.runtime import _runtime

    make_fake_app({"orders": ""})
    (tmp_path / "fakeapp" / "orders" / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    _runtime.configure(package="fakeapp", strict_boundaries=True)
    with pytest.raises(ConfigurationError, match="parse-error"):
        _runtime.ensure_bootstrapped()


def test_strict_boundaries_true_raises_on_cyclic_dependency(make_fake_app) -> None:
    """Rule 2 (no-cyclic-dependency, ``detect_cycles``): a cyclic module
    dependency is fatal under strict_boundaries. Cycle detection runs once
    globally at bootstrap, separately from the per-module verifier hooks
    covered by the tests above — it needs its own seeded regression.

    The mutual import is function-scoped (not top-level) so the fake app
    stays actually importable by bootstrap's real discovery step: a genuine
    top-level circular import would crash discovery with ImportError before
    ever reaching the verifier. The static AST scan that feeds detect_cycles
    still sees a function-scoped import as a real dependency edge.
    """
    from modulith.runtime import _runtime

    make_fake_app(
        {
            "a": """
                def get_b():
                    from fakeapp.b import thing
                    return thing
            """,
            "b": """
                def get_a():
                    from fakeapp.a import get_b
                    return get_b
            """,
        }
    )
    _runtime.configure(package="fakeapp", strict_boundaries=True)
    with pytest.raises(ConfigurationError, match="no-cyclic-dependency"):
        _runtime.ensure_bootstrapped()


# ---------------------------------------------------------------------------
# strict_boundaries + MODULITH_DEV_WARN_ONLY — single-process `modulith dev`'s
# warn-only downgrade (README's "CLI" section), exercised directly at the runtime
# layer rather than through the CLI (see test_cli.py for the full dev-command
# integration test).
# ---------------------------------------------------------------------------


def test_strict_boundaries_warn_only_env_var_downgrades_raise_to_warning(
    make_fake_app, monkeypatch, caplog
) -> None:
    """MODULITH_DEV_WARN_ONLY=1 downgrades the fatal raise to a logged
    warning — the mechanism single-process ``modulith dev`` relies on to
    honor its inviolable warn-only contract, including across uvicorn's
    --reload fork (an env var, unlike an in-memory flag, survives it)."""
    import logging

    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_DEV_WARN_ONLY", "1")
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        },
        extra_files={
            "inventory/_internal/__init__.py": "# private submodule\n",
        },
    )
    _runtime.configure(package="fakeapp", strict_boundaries=True)

    with caplog.at_level(logging.WARNING, logger="modulith"):
        _runtime.ensure_bootstrapped()  # must not raise

    assert _runtime._bootstrapped
    assert any(
        "boundary violations detected" in r.message and "no-internal-imports" in r.message
        for r in caplog.records
    )


def test_strict_boundaries_warn_only_env_var_does_not_affect_other_values(
    make_fake_app, monkeypatch
) -> None:
    """Any value other than the exact '1' sentinel must not downgrade the
    raise — guards against e.g. a stray MODULITH_DEV_WARN_ONLY=0 or =true
    silently widening the warn-only exception beyond single-process dev."""
    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_DEV_WARN_ONLY", "true")
    make_fake_app(
        {
            "orders": """
                from fakeapp.inventory import _internal
            """,
            "inventory": "",
        },
        extra_files={
            "inventory/_internal/__init__.py": "# private submodule\n",
        },
    )
    _runtime.configure(package="fakeapp", strict_boundaries=True)
    with pytest.raises(ConfigurationError, match="no-internal-imports"):
        _runtime.ensure_bootstrapped()
