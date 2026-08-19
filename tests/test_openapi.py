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

import builtins
import importlib
import json
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from modulith.cli import app as cli_app
from modulith.openapi import OpenAPIMergeError, build_module_openapi, merge_openapi

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

_DUPLICATE_OPERATION_ID_SOURCE = """
    from fastapi import APIRouter

    router = APIRouter()

    @router.get("/items", operation_id="sharedOperation")
    def list_items() -> list[str]:
        return []
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

    merged = merge_openapi(
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


def test_merge_openapi_rewrites_only_schema_reference_fields() -> None:
    schema_ref = "#/components/schemas/Item"
    doc: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "orders", "version": "0.0.0"},
        "paths": {
            "/items": {
                "get": {
                    "operationId": "getItem",
                    "description": schema_ref,
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "oneOf": [{"$ref": schema_ref}],
                                        "discriminator": {
                                            "propertyName": "kind",
                                            "mapping": {"item": schema_ref},
                                        },
                                    },
                                    "example": schema_ref,
                                    "examples": {"sample": {"value": {"$ref": schema_ref}}},
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {
            "schemas": {
                "Item": {
                    "type": "object",
                    "description": schema_ref,
                    "example": {"$ref": schema_ref},
                }
            }
        },
    }

    merged = merge_openapi({"orders": doc}, title="t", version="1")

    operation = merged["paths"]["/items"]["get"]
    media_type = operation["responses"]["200"]["content"]["application/json"]
    assert media_type["schema"]["oneOf"][0]["$ref"] == "#/components/schemas/orders_Item"
    assert (
        media_type["schema"]["discriminator"]["mapping"]["item"]
        == "#/components/schemas/orders_Item"
    )
    assert operation["description"] == schema_ref
    assert media_type["example"] == schema_ref
    assert media_type["examples"]["sample"]["value"]["$ref"] == schema_ref
    assert merged["components"]["schemas"]["orders_Item"]["description"] == schema_ref
    assert merged["components"]["schemas"]["orders_Item"]["example"]["$ref"] == schema_ref


def test_merge_openapi_rejects_conflicting_path_items() -> None:
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

    with pytest.raises(OpenAPIMergeError, match=r"path collision.*'/shared'"):
        merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")


def test_merge_openapi_rejects_final_schema_key_collision() -> None:
    doc_a: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {},
        "components": {"schemas": {"b_Item": {"type": "string"}}},
    }
    doc_a_b: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a_b", "version": "0.0.0"},
        "paths": {},
        "components": {"schemas": {"Item": {"type": "integer"}}},
    }

    with pytest.raises(OpenAPIMergeError, match=r"schema key collision.*a_b_Item"):
        merge_openapi({"a": doc_a, "a_b": doc_a_b}, title="t", version="1")


def test_merge_openapi_rejects_incompatible_non_schema_components() -> None:
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

    with pytest.raises(OpenAPIMergeError, match=r"components\.securitySchemes\.'apiKey'"):
        merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")


def test_merge_openapi_rejects_duplicate_operation_ids() -> None:
    def doc(path: str) -> dict[str, Any]:
        return {
            "openapi": "3.1.0",
            "info": {"title": path, "version": "0.0.0"},
            "paths": {
                path: {
                    "get": {
                        "operationId": "sharedOperation",
                        "responses": {"200": {"description": "ok"}},
                    }
                }
            },
        }

    with pytest.raises(OpenAPIMergeError, match=r"duplicate operationId.*sharedOperation"):
        merge_openapi({"a": doc("/a"), "b": doc("/b")}, title="t", version="1")


@pytest.mark.parametrize(
    ("section_name", "component"),
    [
        (
            "callbacks",
            {
                "onEvent": {
                    "{$request.body#/callbackUrl}": {
                        "post": {
                            "operationId": "sharedOperation",
                            "responses": {"200": {"description": "ok"}},
                        }
                    }
                }
            },
        ),
        (
            "pathItems",
            {
                "Reusable": {
                    "post": {
                        "operationId": "sharedOperation",
                        "responses": {"200": {"description": "ok"}},
                    }
                }
            },
        ),
    ],
)
def test_merge_openapi_checks_operation_ids_in_reusable_components(
    section_name: str, component: dict[str, Any]
) -> None:
    doc: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {
            "/items": {
                "get": {
                    "operationId": "sharedOperation",
                    "responses": {"200": {"description": "ok"}},
                }
            }
        },
        "components": {section_name: component},
    }

    with pytest.raises(OpenAPIMergeError, match=r"duplicate operationId.*sharedOperation"):
        merge_openapi({"a": doc}, title="t", version="1")


def test_merge_openapi_preserves_compatible_top_level_metadata() -> None:
    security: list[dict[str, list[str]]] = [{"apiKey": []}]
    servers = [{"url": "https://api.example.test"}]
    doc_a: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {},
        "security": security,
        "servers": servers,
        "tags": [{"name": "shared"}],
        "webhooks": {
            "orderCreated": {
                "post": {
                    "operationId": "orderCreated",
                    "responses": {"200": {"description": "ok"}},
                }
            }
        },
        "x-brand": {"name": "Example"},
    }
    doc_b: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "b", "version": "0.0.0"},
        "paths": {},
        "security": security,
        "servers": servers,
        "tags": [{"name": "shared"}, {"name": "inventory"}],
        "webhooks": {
            "stockChanged": {
                "post": {
                    "operationId": "stockChanged",
                    "responses": {"200": {"description": "ok"}},
                }
            }
        },
        "x-brand": {"name": "Example"},
    }

    merged = merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")

    assert merged["security"] == security
    assert merged["servers"] == servers
    assert merged["tags"] == [{"name": "shared"}, {"name": "inventory"}]
    assert set(merged["webhooks"]) == {"orderCreated", "stockChanged"}
    assert merged["x-brand"] == {"name": "Example"}


@pytest.mark.parametrize(
    ("field", "first", "second"),
    [
        ("openapi", "3.1.0", "3.0.3"),
        ("security", [{"apiKey": []}], [{"oauth": []}]),
        ("servers", [{"url": "https://a.test"}], [{"url": "https://b.test"}]),
        ("x-brand", {"name": "A"}, {"name": "B"}),
    ],
)
def test_merge_openapi_rejects_conflicting_top_level_metadata(
    field: str, first: Any, second: Any
) -> None:
    doc_a: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {},
        field: first,
    }
    doc_b: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "b", "version": "0.0.0"},
        "paths": {},
        field: second,
    }

    with pytest.raises(OpenAPIMergeError, match=f"top-level {field!r}"):
        merge_openapi({"a": doc_a, "b": doc_b}, title="t", version="1")


def test_merge_openapi_does_not_import_fastapi(monkeypatch) -> None:
    doc: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "a", "version": "0.0.0"},
        "paths": {},
        "security": "not-a-security-requirement-list",
    }
    real_import = builtins.__import__

    def import_without_fastapi(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "fastapi" or name.startswith("fastapi."):
            raise AssertionError("merge_openapi imported an optional dependency")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_fastapi)

    merged = merge_openapi({"a": doc}, title="t", version="1")

    assert merged["security"] == "not-a-security-requirement-list"


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


def test_cli_openapi_missing_fastapi_is_actionable(make_fake_app, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": _ORDERS_SOURCE})
    output = tmp_path / "openapi.json"
    real_import = builtins.__import__

    def import_without_fastapi(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "fastapi.openapi.models":
            raise ModuleNotFoundError("No module named 'fastapi'", name="fastapi")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_fastapi)

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 1
    assert "pip install 'modupy[fastapi]'" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()


def test_cli_openapi_validates_final_document_shape(make_fake_app, monkeypatch, tmp_path) -> None:
    from modulith import openapi as openapi_module

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    output = tmp_path / "openapi.json"
    invalid_doc: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "orders", "version": "0.0.0"},
        "paths": {},
        "security": "not-a-security-requirement-list",
    }
    monkeypatch.setattr(
        openapi_module,
        "build_module_openapi",
        lambda _module_name, _module: invalid_doc,
    )

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 1
    assert "invalid merged OpenAPI document" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()


def test_cli_openapi_merge_error_is_actionable(make_fake_app, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": _DUPLICATE_OPERATION_ID_SOURCE,
            "inventory": _DUPLICATE_OPERATION_ID_SOURCE,
        }
    )
    output = tmp_path / "openapi.json"

    result = runner.invoke(cli_app, ["openapi", "--output", str(output)])

    assert result.exit_code == 1
    assert "duplicate operationId" in result.output
    assert "Traceback" not in result.output
    assert not output.exists()
