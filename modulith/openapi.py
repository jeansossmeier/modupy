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


def _rewrite_schema_refs(node: Any, module_name: str) -> Any:
    """Recursively prefix every ``#/components/schemas/<Name>`` string.

    Covers ``$ref`` and ``discriminator.mapping`` values alike since both
    are just strings starting with the same prefix, wherever they're
    nested in the document.

    # ponytail: string-prefix rewrite — a value that merely *starts with*
    # this prefix (never a legitimate OpenAPI field today, but
    # theoretically a user-authored description) would also get rewritten.
    # A key-aware walker that only touches actual $ref/discriminator.mapping
    # values is the fix if that ever happens for real.
    """
    if isinstance(node, str):
        if node.startswith(_SCHEMA_REF_PREFIX):
            name = node[len(_SCHEMA_REF_PREFIX) :]
            return f"{_SCHEMA_REF_PREFIX}{module_name}_{name}"
        return node
    if isinstance(node, dict):
        return {key: _rewrite_schema_refs(value, module_name) for key, value in node.items()}
    if isinstance(node, list):
        return [_rewrite_schema_refs(item, module_name) for item in node]
    return node


def merge_openapi(
    docs: dict[str, dict[str, Any]], *, title: str, version: str
) -> tuple[dict[str, Any], list[str]]:
    """Merge several modules' OpenAPI documents into one.

    Every ``components.schemas`` key is prefixed ``f"{module}_{key}"`` so
    identically named models defined by different modules never collide,
    and every ``$ref`` pointing at a rewritten schema is updated to match.
    Paths are unioned; a path present in more than one module's document
    keeps the first module's definition and appends a warning. The same
    first-wins policy applies to the other ``components.*`` sections,
    warning only when two modules disagree on a shared key's value.

    Returns the merged document and the list of warnings collected.
    """
    warnings: list[str] = []
    merged_paths: dict[str, Any] = {}
    merged_components: dict[str, dict[str, Any]] = {}

    for module_name, doc in docs.items():
        prefixed = _rewrite_schema_refs(doc, module_name)

        for path, item in prefixed.get("paths", {}).items():
            if path in merged_paths:
                warnings.append(
                    f"path collision on {path!r}: keeping the first module's "
                    f"definition, discarding {module_name!r}'s"
                )
                continue
            merged_paths[path] = item

        for section_name, section in prefixed.get("components", {}).items():
            merged_section = merged_components.setdefault(section_name, {})
            if section_name == "schemas":
                # Keys are already module-prefixed above, so they never
                # collide across modules — a plain update is safe.
                merged_section.update(
                    {f"{module_name}_{key}": value for key, value in section.items()}
                )
                continue
            for key, value in section.items():
                if key in merged_section:
                    if merged_section[key] != value:
                        warnings.append(
                            f"components.{section_name}.{key!r} differs between "
                            "modules: keeping the first module's definition"
                        )
                    continue
                merged_section[key] = value

    first_doc = next(iter(docs.values()), {})
    merged = {
        "openapi": first_doc.get("openapi", "3.1.0"),
        "info": {"title": title, "version": version},
        "paths": merged_paths,
        "components": merged_components,
    }
    return merged, warnings
