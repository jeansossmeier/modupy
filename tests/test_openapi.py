"""Tests for modulith.openapi: per-module OpenAPI aggregation.

``build_module_openapi`` builds one module's OpenAPI document in isolation
(the same router-mounting shape as ``modulith._worker.create_app``, minus
its lifespan/docs/``/health`` wiring). ``merge_openapi`` combines several
such documents, always prefixing each module's component schema names so
identically named models defined by two different modules never collide.
The CLI-level tests at the bottom exercise ``modulith openapi`` end to end
via ``typer.testing.CliRunner``.
"""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from typing import Any

from typer.testing import CliRunner

from modulith.cli import app as cli_app
from modulith.openapi import build_module_openapi, merge_openapi

runner = CliRunner()


def _set_worker_env(monkeypatch, module: str, package: str = "fakeapp") -> None:
    """Copied from tests/test_worker.py:53 (import-or-copy — copying wins:
    it keeps this file's fixtures independent of that one's)."""
    monkeypatch.setenv("MODULITH_MODULE", module)
    monkeypatch.setenv("MODULITH_APP_PACKAGE", package)
    monkeypatch.setenv("MODULITH_BROKER", "test-noop-broker")


# Both fake modules below define a route on the *same* Pydantic model name,
# ``Item`` — the exact scenario merge_openapi's schema prefixing exists for.
# They're real importable packages (via make_fake_app), not classes built
# inline in this file: FastAPI/pydantic resolve a route handler's forward
# refs against its *module* globals, so a model defined inside a Python
# function (rather than at module level) fails schema generation with
# "is not fully defined" — real modules sidestep that entirely.
_ORDERS_SOURCE = """
    from fastapi import APIRouter
    from pydantic import BaseModel

    class Item(BaseModel):
        name: str

    router = APIRouter()

    @router.post("/items")
    def create_item(item: Item) -> Item:
        return item
"""

_INVENTORY_SOURCE = """
    from fastapi import APIRouter
    from pydantic import BaseModel

    class Item(BaseModel):
        sku: str

    router = APIRouter()

    @router.post("/items")
    def create_item(item: Item) -> Item:
        return item
"""


# ---------------------------------------------------------------------------
# build_module_openapi
# ---------------------------------------------------------------------------


def test_build_module_openapi_returns_none_without_router() -> None:
    assert build_module_openapi("listener_only", SimpleNamespace()) is None


def test_build_module_openapi_mounts_router_under_module_prefix(make_fake_app) -> None:
    pkg = make_fake_app({"orders": _ORDERS_SOURCE})
    module = importlib.import_module(f"{pkg}.orders")

    doc = build_module_openapi("orders", module)

    assert doc is not None
    assert doc["info"]["title"] == "modulith-orders"
    assert "/orders/items" in doc["paths"]
    assert "/health" not in doc["paths"]


def test_build_module_openapi_matches_worker_paths_minus_health(make_fake_app, monkeypatch) -> None:
    """Drift guard: build_module_openapi must stay in lockstep with the
    router-mounting shape _worker.create_app() actually uses in production,
    so a change to one doesn't silently diverge from the other."""
    pkg = make_fake_app({"orders": _ORDERS_SOURCE})
    _set_worker_env(monkeypatch, "orders", package=pkg)

    from modulith._worker import create_app

    worker_paths = set(create_app().openapi()["paths"]) - {"/health"}

    module = importlib.import_module(f"{pkg}.orders")
    doc = build_module_openapi("orders", module)

    assert doc is not None
    assert set(doc["paths"]) == worker_paths


# ---------------------------------------------------------------------------
# merge_openapi
# ---------------------------------------------------------------------------


def test_merge_openapi_prefixes_schemas_and_rewrites_refs_and_info(make_fake_app) -> None:
    pkg = make_fake_app({"orders": _ORDERS_SOURCE, "inventory": _INVENTORY_SOURCE})
    orders_doc = build_module_openapi("orders", importlib.import_module(f"{pkg}.orders"))
    inventory_doc = build_module_openapi("inventory", importlib.import_module(f"{pkg}.inventory"))
    assert orders_doc is not None
    assert inventory_doc is not None

    merged, warnings = merge_openapi(
        {"orders": orders_doc, "inventory": inventory_doc},
        title="fakeapp",
        version="1.2.3",
    )

    schemas = merged["components"]["schemas"]
    assert "orders_Item" in schemas
    assert "inventory_Item" in schemas
    assert "Item" not in schemas

    ref = merged["paths"]["/orders/items"]["post"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["$ref"]
    assert ref == "#/components/schemas/orders_Item"

    assert '"#/components/schemas/Item"' not in json.dumps(merged)
    assert merged["info"] == {"title": "fakeapp", "version": "1.2.3"}
    assert not warnings


def test_merge_openapi_path_collision_keeps_first_and_warns() -> None:
    doc_a: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {"/shared": {"get": {"summary": "from a"}}},
        "components": {},
    }
    doc_b: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "b", "version": "0.0.0"},
        "paths": {"/shared": {"get": {"summary": "from b"}}},
        "components": {},
    }

    merged, warnings = merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")

    assert merged["paths"]["/shared"]["get"]["summary"] == "from a"
    collision_warnings = [w for w in warnings if "path collision" in w]
    assert len(collision_warnings) == 1
    assert "/shared" in collision_warnings[0]


def test_merge_openapi_differing_security_schemes_warns_and_keeps_first() -> None:
    doc_a: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {},
        "components": {
            "securitySchemes": {"apiKey": {"type": "apiKey", "name": "X-Key", "in": "header"}}
        },
    }
    doc_b: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "b", "version": "0.0.0"},
        "paths": {},
        "components": {"securitySchemes": {"apiKey": {"type": "http", "scheme": "bearer"}}},
    }

    merged, warnings = merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")

    assert merged["components"]["securitySchemes"]["apiKey"]["type"] == "apiKey"
    assert any("securitySchemes" in w and "apiKey" in w for w in warnings)


# ---------------------------------------------------------------------------
# CLI: modulith openapi
# ---------------------------------------------------------------------------


def test_cli_openapi_merges_two_modules(make_fake_app, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": _ORDERS_SOURCE, "inventory": _INVENTORY_SOURCE})
    output = tmp_path / "openapi.json"

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert "wrote OpenAPI for 2 module(s)" in result.output
    merged = json.loads(output.read_text(encoding="utf-8"))
    schemas = merged["components"]["schemas"]
    assert "orders_Item" in schemas
    assert "inventory_Item" in schemas
    assert '"#/components/schemas/Item"' not in json.dumps(merged)


def test_cli_openapi_reports_skipped_module_without_router(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": _ORDERS_SOURCE, "notifications": ""})
    output = tmp_path / "openapi.json"

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert "wrote OpenAPI for 1 module(s)" in result.output
    assert "skipped (no router): notifications" in result.output


def test_cli_openapi_exits_1_when_no_module_has_a_router(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"notifications": ""})
    output = tmp_path / "openapi.json"

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 1
    assert "no module exposes a router" in result.output
    assert not output.exists()


def test_cli_openapi_unwritable_output_path_is_clean_user_error(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": _ORDERS_SOURCE})
    bad_output = tmp_path / "no_such_dir" / "openapi.json"

    result = runner.invoke(cli_app, ["openapi", "--output", str(bad_output)])

    assert result.exit_code == 1
    assert "could not write" in result.output
    assert "Traceback" not in result.output
