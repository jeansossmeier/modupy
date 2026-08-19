"""Build-time OpenAPI aggregation for process-per-module topology.

Each module worker (``modulith._worker.create_app``) serves its own OpenAPI
document at ``/<module>/openapi.json``. No single running process exposes
the whole application's surface, so a full spec — for a gateway, an API
portal, or client-generation tooling — has to be assembled offline from
each module's document. This module provides the two building blocks the
``modulith openapi`` CLI command composes: ``build_module_openapi`` builds
one module's document in isolation, and ``merge_openapi`` combines several
into one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI

_SCHEMA_REF_PREFIX = "#/components/schemas/"
_HTTP_METHODS = frozenset({"get", "put", "post", "delete", "options", "head", "patch", "trace"})
_SINGLETON_FIELDS = ("openapi", "jsonSchemaDialect", "externalDocs", "security", "servers")
_EXAMPLE_FIELDS = frozenset({"example", "examples"})


class OpenAPIMergeError(ValueError):
    """Raised when module documents cannot be merged without changing semantics."""


def build_module_openapi(module_name: str, module: Any) -> dict[str, Any] | None:
    """Build one module's OpenAPI document, standalone.

    Mirrors the router-mounting shape of ``modulith._worker.create_app``
    (same title, same ``/<module>`` prefix) minus the parts that only make
    sense for a running worker process: lifespan, ``/health``, and the
    doc-serving routes. Returns None when ``module`` exposes no ``router``
    attribute — a listener-only module has nothing to document.
    """
    router = getattr(module, "router", None)
    if router is None:
        return None

    from fastapi import FastAPI

    app: FastAPI = FastAPI(title=f"modulith-{module_name}")
    app.include_router(router, prefix=f"/{module_name}")
    return app.openapi()


def _prefix_schema_ref(value: Any, module_name: str) -> Any:
    if isinstance(value, str) and value.startswith(_SCHEMA_REF_PREFIX):
        name = value[len(_SCHEMA_REF_PREFIX) :]
        return f"{_SCHEMA_REF_PREFIX}{module_name}_{name}"
    return value


def _rewrite_schema_refs(node: Any, module_name: str) -> Any:
    """Prefix ``$ref`` and discriminator mappings while preserving examples and extensions."""
    if isinstance(node, dict):
        rewritten: dict[str, Any] = {}
        for key, value in node.items():
            if key in _EXAMPLE_FIELDS or key.startswith("x-"):
                rewritten[key] = value
            elif key == "$ref":
                rewritten[key] = _prefix_schema_ref(value, module_name)
            else:
                rewritten[key] = _rewrite_schema_refs(value, module_name)

        discriminator = rewritten.get("discriminator")
        if isinstance(discriminator, dict) and isinstance(discriminator.get("mapping"), dict):
            discriminator["mapping"] = {
                key: _prefix_schema_ref(value, module_name)
                for key, value in discriminator["mapping"].items()
            }
        return rewritten
    if isinstance(node, list):
        return [_rewrite_schema_refs(item, module_name) for item in node]
    return node


def _merge_mapping(target: dict[str, Any], source: dict[str, Any], label: str) -> None:
    for key, value in source.items():
        if key in target and target[key] != value:
            raise OpenAPIMergeError(f"incompatible {label}.{key!r} collision")
        target.setdefault(key, value)


def _merge_tags(target: list[Any], source: Any) -> None:
    if not isinstance(source, list):
        target.append(source)
        return
    by_name = {
        tag.get("name"): tag
        for tag in target
        if isinstance(tag, dict) and isinstance(tag.get("name"), str)
    }
    for tag in source:
        name = tag.get("name") if isinstance(tag, dict) else None
        if isinstance(name, str) and name in by_name:
            if by_name[name] != tag:
                raise OpenAPIMergeError(f"incompatible top-level tag {name!r}")
            continue
        if tag not in target:
            target.append(tag)
            if isinstance(name, str):
                by_name[name] = tag


def _operation_ids(path_items: Any) -> list[str]:
    if not isinstance(path_items, dict):
        return []
    operation_ids: list[str] = []
    for path_item in path_items.values():
        if not isinstance(path_item, dict):
            continue
        for method in _HTTP_METHODS:
            operation = path_item.get(method)
            if not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId")
            if isinstance(operation_id, str):
                operation_ids.append(operation_id)
            callbacks = operation.get("callbacks")
            if isinstance(callbacks, dict):
                for callback in callbacks.values():
                    operation_ids.extend(_operation_ids(callback))
    return operation_ids


def _merge_top_level_metadata(
    doc: dict[str, Any],
    module_name: str,
    top_level: dict[str, Any],
    origins: dict[str, str],
    tags: list[Any],
    webhooks: dict[str, Any],
) -> tuple[bool, bool]:
    for field in _SINGLETON_FIELDS:
        if field not in doc:
            continue
        if field in top_level and top_level[field] != doc[field]:
            raise OpenAPIMergeError(
                f"conflicting top-level {field!r} values from "
                f"{origins[field]!r} and {module_name!r}"
            )
        top_level[field] = doc[field]
        origins[field] = module_name

    saw_tags = "tags" in doc
    if saw_tags:
        _merge_tags(tags, doc["tags"])

    saw_webhooks = "webhooks" in doc
    if saw_webhooks:
        module_webhooks = doc["webhooks"]
        if not isinstance(module_webhooks, dict):
            raise OpenAPIMergeError("invalid top-level 'webhooks': expected an object")
        _merge_mapping(webhooks, module_webhooks, "top-level webhooks")

    for field, value in doc.items():
        if not field.startswith("x-"):
            continue
        if field in top_level and top_level[field] != value:
            raise OpenAPIMergeError(f"conflicting top-level {field!r} values")
        top_level[field] = value
    return saw_tags, saw_webhooks


def _validate_unique_operation_ids(
    paths: dict[str, Any],
    webhooks: dict[str, Any],
    components: dict[str, dict[str, Any]],
) -> None:
    operation_ids = _operation_ids(paths) + _operation_ids(webhooks)
    operation_ids.extend(_operation_ids(components.get("pathItems")))
    callbacks = components.get("callbacks")
    if isinstance(callbacks, dict):
        for callback in callbacks.values():
            operation_ids.extend(_operation_ids(callback))

    seen: set[str] = set()
    for operation_id in operation_ids:
        if operation_id in seen:
            raise OpenAPIMergeError(f"duplicate operationId {operation_id!r}")
        seen.add(operation_id)


def merge_openapi(docs: dict[str, dict[str, Any]], *, title: str, version: str) -> dict[str, Any]:
    """Merge module OpenAPI documents without discarding incompatible definitions.

    Schema keys are module-prefixed. Exact duplicates are deduplicated,
    compatible collections are combined, and conflicting definitions fail.
    """
    merged_paths: dict[str, Any] = {}
    merged_components: dict[str, dict[str, Any]] = {}
    top_level: dict[str, Any] = {}
    top_level_origins: dict[str, str] = {}
    merged_tags: list[Any] = []
    merged_webhooks: dict[str, Any] = {}
    saw_tags = False
    saw_webhooks = False

    for module_name, doc in docs.items():
        prefixed = _rewrite_schema_refs(doc, module_name)

        module_has_tags, module_has_webhooks = _merge_top_level_metadata(
            prefixed,
            module_name,
            top_level,
            top_level_origins,
            merged_tags,
            merged_webhooks,
        )
        saw_tags = saw_tags or module_has_tags
        saw_webhooks = saw_webhooks or module_has_webhooks

        for path, item in prefixed.get("paths", {}).items():
            if path in merged_paths:
                if merged_paths[path] != item:
                    raise OpenAPIMergeError(
                        f"path collision on {path!r}: definitions are incompatible"
                    )
                continue
            merged_paths[path] = item

        for section_name, section in prefixed.get("components", {}).items():
            merged_section = merged_components.setdefault(section_name, {})
            if section_name == "schemas":
                for key, value in section.items():
                    final_key = f"{module_name}_{key}"
                    if final_key in merged_section:
                        raise OpenAPIMergeError(f"schema key collision on {final_key!r}")
                    merged_section[final_key] = value
                continue
            _merge_mapping(merged_section, section, f"components.{section_name}")

    merged = {
        "openapi": top_level.pop("openapi", "3.1.0"),
        "info": {"title": title, "version": version},
        "paths": merged_paths,
        "components": merged_components,
    }
    merged.update(top_level)
    if saw_tags:
        merged["tags"] = merged_tags
    if saw_webhooks:
        merged["webhooks"] = merged_webhooks

    _validate_unique_operation_ids(merged_paths, merged_webhooks, merged_components)
    return merged
