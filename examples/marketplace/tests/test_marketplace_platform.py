from importlib.metadata import entry_points

import pytest
from modulith import ModuleInfo, Violation, ViolationSeverity
from modulith import manifest as manifests
from modulith.manager import create_plugin_manager

import marketplace_platform


def verify(package: str) -> list[Violation]:
    module = ModuleInfo(name=package.rsplit(".", 1)[-1], package=package)
    plugins = create_plugin_manager(
        extra_plugins=[marketplace_platform], load_entrypoints=False, load_builtins=False
    )
    results = plugins.hook.modulith_verify_module(module=module, all_modules=[module])
    return [violation for result in results for violation in result]


def declare(monkeypatch: pytest.MonkeyPatch, package: str, *owns_tables: str) -> None:
    monkeypatch.setitem(
        manifests._manifests, package, manifests.Manifest(package, owns_tables=owns_tables)
    )


def test_an_unprefixed_table_is_reported_and_a_prefixed_one_is_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declare(monkeypatch, "marketplace.catalog", "catalog_product", "products")

    violations = verify("marketplace.catalog")

    assert [(v.rule, v.severity, v.module) for v in violations] == [
        ("table-prefix", ViolationSeverity.ERROR, "catalog")
    ]
    assert "'products'" in violations[0].message
    assert "catalog_" in violations[0].message


@pytest.mark.parametrize("table", ["catalog", "catalogue_product", "orders_order"])
def test_a_table_must_start_with_its_own_module_name_and_an_underscore(
    monkeypatch: pytest.MonkeyPatch, table: str
) -> None:
    declare(monkeypatch, "marketplace.catalog", table)

    assert [v.rule for v in verify("marketplace.catalog")] == ["table-prefix"]


def test_a_module_that_declares_no_tables_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    declare(monkeypatch, "marketplace.catalog")

    assert verify("marketplace.catalog") == []


def test_a_module_without_a_manifest_passes() -> None:
    assert verify("marketplace.catalog") == []


@pytest.mark.parametrize("package", ["billing.orders", "marketplaceish.orders"])
def test_a_module_outside_the_marketplace_is_not_checked(
    monkeypatch: pytest.MonkeyPatch, package: str
) -> None:
    declare(monkeypatch, package, "products")

    assert verify(package) == []


def test_the_plugin_is_registered_in_the_modulith_entry_point_group() -> None:
    registered = {ep.name: ep.value for ep in entry_points(group="modulith")}

    assert registered["marketplace_platform"] == "marketplace_platform"
