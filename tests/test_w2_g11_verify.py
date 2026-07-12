"""W2 G11_verify: regression tests for audit findings on the boundary
verifier (``modulith/builtin/verifier.py``), manifests (``modulith/manifest.py``)
and the audit tool (``modulith/audit.py``).

Each test cites the audit finding id it reproduces. Written test-first: every
behavioral test here failed against the pre-fix code for the reason the
finding describes.
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


def _module(name: str) -> ModuleInfo:
    return ModuleInfo(name=name, package=f"fakeapp.{name}")


def _write(root: Path, rel: str, source: str) -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(dedent(source), encoding="utf-8")


# ---------------------------------------------------------------------------
# A10-r1-33 — baseline fingerprint must survive unrelated line shifts
# ---------------------------------------------------------------------------


def test_baseline_survives_line_shift(tmp_path: Path) -> None:
    """A10-r1-33: a grandfathered violation must stay grandfathered when an
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
    """A10-r1-33: baselines written by older versions carry ``file:line``
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
# A10-r1-34 / A10-r3-146 — TYPE_CHECKING imports visible to rules 1, 3, 4;
# still exempt from cycle detection (rule 2)
# ---------------------------------------------------------------------------


def test_type_checking_private_import_flagged_by_rule_1(make_fake_app) -> None:
    """A10-r1-34: a private-package import inside ``if TYPE_CHECKING:`` must
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
    """A10-r1-34: a cross-module type import inside ``if TYPE_CHECKING:`` is
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
    """A10-r3-146: a TYPE_CHECKING-only import from an undeclared module must
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
    """A10-r3-146 / A10-r1-34: TYPE_CHECKING-guarded imports impose no runtime
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
# A10-r5-217 — TYPE_CHECKING alias must be recognized as a guard
# ---------------------------------------------------------------------------


def test_aliased_type_checking_guard_is_recognized(make_fake_app) -> None:
    """A10-r5-217: ``from typing import TYPE_CHECKING as TC`` must still be
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
# A10-r2-96 — rule 4 must match runtime usage by the locally-bound alias
# ---------------------------------------------------------------------------


def test_aliased_import_used_at_runtime_is_not_flagged(make_fake_app) -> None:
    """A10-r2-96: ``from x import Y as Z`` with Z used at runtime is a runtime
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
    """A10-r2-96 (guard against over-fix): an aliased import used only in an
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
# A10-r4-185 — names referenced only inside TYPE_CHECKING are not runtime uses
# ---------------------------------------------------------------------------


def test_type_checking_reference_does_not_count_as_runtime_use(make_fake_app) -> None:
    """A10-r4-185: a name referenced only inside ``if TYPE_CHECKING:`` never
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
# A10-r1-35 — contracts is an import sink
# ---------------------------------------------------------------------------


def test_contracts_importing_from_module_is_flagged(make_fake_app) -> None:
    """A10-r1-35: SPEC 5.3 — contracts may not import from any application
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
    """A10-r1-35 (direction check): the sink rule only applies to the
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
# A10-r2-97 — rule 3 message must not embed the declared_dependencies list
# ---------------------------------------------------------------------------


def test_adding_a_dependency_does_not_reopen_grandfathered_violations(
    make_fake_app, tmp_path: Path
) -> None:
    """A10-r2-97: fixing one undeclared dependency by declaring it must not
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
# A10-r1-36 — declared_dependencies: None = not declared; () = deny-all
# ---------------------------------------------------------------------------


def test_manifest_without_declared_dependencies_skips_rule_3(make_fake_app) -> None:
    """A10-r1-36 (adjudicated): declaring a manifest for an unrelated field
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
    """A10-r1-36 (adjudicated): an explicit empty tuple means 'depends on
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


def test_declare_module_default_declared_dependencies_is_none() -> None:
    """A10-r1-36 (adjudicated): declare_module() without declared_dependencies
    stores None ('not declared'), not an empty tuple."""
    from modulith import declare_module
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    try:
        declare_module(publishes=["SomethingHappened"])
        m = manifest_module.get_manifest(__name__)
        assert m is not None
        assert m.declared_dependencies is None
    finally:
        manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# A10-r3-147 — rule 4 must not be blind to wildcard imports
# ---------------------------------------------------------------------------


def test_star_import_across_modules_is_flagged(make_fake_app) -> None:
    """A10-r3-147: ``from other_module import *`` cannot be resolved to
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
    """A10-r3-147 (scope check): wildcard imports from contracts stay exempt."""
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
# A10-r3-149 — conflicting owns_tables declarations
# ---------------------------------------------------------------------------


def test_co_owner_of_conflicted_table_is_not_falsely_flagged(make_fake_app) -> None:
    """A10-r3-149: when two manifests both declare the same table, the module
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
# A10-r5-219 — load_baseline error handling
# ---------------------------------------------------------------------------


def test_load_baseline_invalid_json_raises_configuration_error(tmp_path: Path) -> None:
    """A10-r5-219: malformed JSON must raise a clear ConfigurationError naming
    the path, not a raw JSONDecodeError."""
    path = tmp_path / "baseline.json"
    path.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ConfigurationError, match=re.escape(str(path))):
        load_baseline(path)


def test_load_baseline_missing_keys_raises_configuration_error(tmp_path: Path) -> None:
    """A10-r5-219: a schema-mismatched entry must raise ConfigurationError
    suggesting --update-baseline, not a raw KeyError."""
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps([{"rule": "x", "module": "y"}]), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="update-baseline"):
        load_baseline(path)


def test_load_baseline_non_list_payload_raises_configuration_error(tmp_path: Path) -> None:
    """A10-r5-219: a valid-JSON but wrong-shape payload is also rejected loudly."""
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"rule": "x"}), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="list"):
        load_baseline(path)


# ---------------------------------------------------------------------------
# A4-r1-11 — manifest verification errors carry file:line
# ---------------------------------------------------------------------------


def test_unregistered_listener_error_includes_file_and_line() -> None:
    """A4-r1-11: SPEC promises 'a clear error and file:line' when a declared
    listener wasn't registered."""

    async def orphan_listener(e: object) -> None: ...

    m = Manifest(package="fakeapp.orders", listeners=(orphan_listener,))
    errors = verify_manifest(m, registered_listeners=set())
    assert len(errors) == 1
    assert re.search(r"\.py:\d+", errors[0]), errors[0]
    assert "orphan_listener" in errors[0]


# ---------------------------------------------------------------------------
# A4-r2-81 — publishes check must flag names bound to None
# ---------------------------------------------------------------------------


def test_publishes_name_bound_to_none_is_flagged() -> None:
    """A4-r2-81: an event name explicitly bound to None (failed conditional
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
    """A4-r2-81 (guard against over-fix): a defined event type passes."""
    fake_pkg = types_module.ModuleType("fakeapp_defined_event")
    fake_pkg.OrderCreated = object()  # type: ignore[attr-defined]
    sys.modules["fakeapp_defined_event"] = fake_pkg
    try:
        m = Manifest(package="fakeapp_defined_event", publishes=("OrderCreated",))
        assert verify_manifest(m, registered_listeners=set()) == []
    finally:
        del sys.modules["fakeapp_defined_event"]


# ---------------------------------------------------------------------------
# A10-r4-186 — audit of an empty tree must not report a confident 100/100
# ---------------------------------------------------------------------------


def test_audit_empty_directory_warns_instead_of_confident_score(tmp_path: Path) -> None:
    """A10-r4-186: zero .py files found must be distinguishable from 'genuinely
    zero coupling' — the report warns and the module section says None."""
    root = tmp_path / "empty"
    root.mkdir()
    result = audit_codebase(root)
    assert result.files_scanned == 0

    report = render_report(result)
    assert "no Python files" in report
    assert "None — no Python modules detected." in report


def test_audit_populated_tree_reports_files_scanned(tmp_path: Path) -> None:
    """A10-r4-186: a populated tree records how many files were scanned and
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
# A10-r3-148 — readiness score caveat for shared tables
# ---------------------------------------------------------------------------


def test_report_caveats_score_when_shared_tables_present(tmp_path: Path) -> None:
    """A10-r3-148 (adjudicated): the formula stays, but the report must say
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
    assert result.readiness_score == 100  # formula unchanged (adjudicated)

    report = render_report(result)
    assert "shared-table entanglement" in report


# ---------------------------------------------------------------------------
# A10-r5-218 — stdlib/third-party name collisions in the audit import heuristic
# ---------------------------------------------------------------------------


def test_stdlib_import_colliding_with_local_dir_not_counted(tmp_path: Path) -> None:
    """A10-r5-218: ``import types`` (stdlib) must not be reported as coupling
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
    """A10-r5-218: in a flat (non-package) layout the stdlib name still wins
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
    """A10-r5-218 (guard against over-fix): a genuine bare intra-tree import
    in a flat layout is still a cross-module edge."""
    root = tmp_path / "repo"
    _write(root, "inventory/__init__.py", "")
    _write(root, "inventory/stock.py", "def reserve(): ...\n")
    _write(root, "billing/__init__.py", "")
    _write(root, "billing/report.py", "from inventory.stock import reserve\n")

    result = audit_codebase(root)
    pairs = {(src, tgt) for src, tgt, _c, _s in result.cross_module_imports}
    assert ("billing", "inventory") in pairs
