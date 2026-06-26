"""Built-in documentation generator.

Produces three artifacts from the live module model:
  1. Mermaid C4 component diagram showing modules and their dependencies
  2. Per-module Markdown "canvas" — public API, events, dependencies
  3. Mermaid sequence diagram of event flows

Why Mermaid over PlantUML:
  - Renders natively on GitHub/GitLab
  - No separate server needed
  - Live editor at mermaid.live for previewing changes
  - Markdown integration (```mermaid blocks)

Output goes to docs/modulith/ by default. Configurable via
[tool.modulith.docs].output_dir.
"""

from __future__ import annotations

import ast
import logging
from collections import defaultdict
from pathlib import Path

from modulith import ModuleInfo, hookimpl
from modulith.builtin.verifier import _collect_imports, _owning_module, _package_dir
from modulith.manifest import all_manifests, get_manifest

logger = logging.getLogger("modulith.docs")


# ---------------------------------------------------------------------------
# The hook entrypoint
# ---------------------------------------------------------------------------


@hookimpl
def modulith_render_documentation(
    modules: list[ModuleInfo],
    output_dir: str,
) -> list[str]:
    """Generate all built-in documentation artifacts.

    Returns the list of files produced (relative to output_dir).
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    produced: list[str] = []

    # Architecture overview
    arch_path = out / "architecture.mmd"
    arch_path.write_text(_render_architecture_diagram(modules))
    produced.append("architecture.mmd")

    # Per-module canvases
    canvases_dir = out / "modules"
    canvases_dir.mkdir(exist_ok=True)
    for module in modules:
        canvas_path = canvases_dir / f"{module.name}.md"
        canvas_path.write_text(_render_module_canvas(module))
        produced.append(f"modules/{module.name}.md")

    # Event flow diagram
    events_path = out / "events.mmd"
    events_path.write_text(_render_event_flow_diagram(modules))
    produced.append("events.mmd")

    return produced


# ---------------------------------------------------------------------------
# Diagram generation
# ---------------------------------------------------------------------------


def _event_edges(modules: list[ModuleInfo]) -> list[tuple[str, str, str]]:
    """Manifest-derived (publisher, consumer, event) edges across modules."""
    manifests = all_manifests()
    name_by_pkg = {m.package: m.name for m in modules}
    publishers: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, set[str]] = defaultdict(set)
    for pkg, manifest in manifests.items():
        name = name_by_pkg.get(pkg)
        if name is None:
            # A manifest registered for a package not in this render's module
            # list (stale / out-of-scope registry entry, e.g. test pollution or
            # multiple bootstraps). Skip it so we never emit an edge to a node
            # the diagram never declares — Mermaid would ghost-create it.
            continue
        for event in manifest.publishes:
            publishers[event].add(name)
        for event in manifest.consumes:
            consumers[event].add(name)

    edges: list[tuple[str, str, str]] = []
    for event in sorted(set(publishers) | set(consumers)):
        for src in sorted(publishers.get(event, set())):
            for dst in sorted(consumers.get(event, set())):
                if src != dst:
                    edges.append((src, dst, event))
    return edges


def _import_edges(modules: list[ModuleInfo]) -> list[tuple[str, str]]:
    """Fallback (importer, imported) edges when no manifests are declared."""
    seen: set[tuple[str, str]] = set()
    for module in modules:
        for record in _collect_imports(module):
            owner = _owning_module(record.target_module, modules)
            if owner is not None and owner.name != module.name:
                seen.add((module.name, owner.name))
    return sorted(seen)


def _render_architecture_diagram(modules: list[ModuleInfo]) -> str:
    """Render a Mermaid component diagram of modules and their dependencies.

    Edges come from event flow (publisher publishes → consumer consumes) when
    manifests exist; otherwise from observed cross-module imports as a weaker
    proxy.
    """
    lines = ["graph TD"]
    for module in modules:
        lines.append(f"  {module.name}[{module.name.title()} Module]")

    edges = _event_edges(modules)
    if edges:
        for src, dst, event in edges:
            lines.append(f"  {src} -->|publishes {event}| {dst}")
    else:
        for src, dst in _import_edges(modules):
            lines.append(f"  {src} --> {dst}")
    return "\n".join(lines) + "\n"


def _render_module_canvas(module: ModuleInfo) -> str:
    """Render a single module's canvas as Markdown.

    Output structure:

        # Orders Module

        **Package:** `myapp.orders`

        ## Public API
        - `create_order(customer_id: str) -> str`
        - `get_order(order_id: str) -> Order | None`

        ## Events Published
        - `OrderCreated`
        - `OrderShipped`

        ## Events Consumed
        - `PaymentReceived`

        ## Dependencies
        - `payments` (declared)

        ## Owned Tables
        - `orders`
        - `order_items`

    Sections are manifest-driven when a ``_manifest.py`` exists. When it does
    not, the events sections fall back to AST introspection (``@event`` classes
    → Published, ``@listener`` param types → Consumed) so modules using the
    ``@event``/``@listener`` API without the optional manifest still document
    their event topology. Dependencies and Owned Tables are manifest-only (they
    have no reliable introspection signal).
    """
    lines = [
        f"# {module.name.title()} Module",
        "",
        f"**Package:** `{module.package}`",
        "",
    ]

    public_api = _public_api(module)
    if public_api:
        lines.append("## Public API")
        lines.extend(f"- `{name}`" for name in public_api)
        lines.append("")

    manifest = get_manifest(module.package)
    if manifest is not None:
        published: list[str] = list(manifest.publishes)
        consumed: list[str] = list(manifest.consumes)
    else:
        published, consumed = _introspect_events(module)

    if published:
        lines.append("## Events Published")
        lines.extend(f"- `{name}`" for name in published)
        lines.append("")
    if consumed:
        lines.append("## Events Consumed")
        lines.extend(f"- `{name}`" for name in consumed)
        lines.append("")

    if manifest is not None:
        if manifest.declared_dependencies:
            lines.append("## Dependencies")
            lines.extend(f"- `{dep}` (declared)" for dep in manifest.declared_dependencies)
            lines.append("")
        if manifest.owns_tables:
            lines.append("## Owned Tables")
            lines.extend(f"- `{t}`" for t in manifest.owns_tables)
            lines.append("")
    elif not public_api and not published and not consumed:
        lines.append("_No manifest declared. Add a `_manifest.py` for richer documentation._")

    internal = _internal_files(module)
    if internal:
        lines.append("## Internal Files")
        lines.extend(f"- `{name}`" for name in internal)
        lines.append("")

    return "\n".join(lines) + "\n"


def _has_decorator(node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef, name: str) -> bool:
    """True if ``node`` carries a ``@name`` decorator (bare or attribute form)."""
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Name) and target.id == name:
            return True
        if isinstance(target, ast.Attribute) and target.attr == name:
            return True
    return False


def _annotation_name(expr: ast.expr | None) -> str | None:
    """The rightmost identifier of a type annotation (handles forward-ref strings)."""
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value.rsplit(".", 1)[-1]
    return None


def _introspect_events(module: ModuleInfo) -> tuple[list[str], list[str]]:
    """Best-effort ``(published, consumed)`` event names via AST, no manifest.

    The documented introspection fallback: a module's ``@event``-decorated
    classes are what it publishes; the event type annotated on the first
    parameter of each ``@listener`` handler is what it consumes. Names only
    (the canvas lists names), de-duplicated and sorted. Modules following the
    ``@event``/``@listener`` API but shipping no ``_manifest.py`` still get
    their event topology documented.
    """
    root = _package_dir(module.package)
    if root is None:
        return [], []
    published: set[str] = set()
    consumed: set[str] = set()
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and _has_decorator(node, "event"):
                published.add(node.name)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _has_decorator(
                node, "listener"
            ):
                params = [*node.args.posonlyargs, *node.args.args]
                if params and (evt := _annotation_name(params[0].annotation)):
                    consumed.add(evt)
    return sorted(published), sorted(consumed)


def _public_api(module: ModuleInfo) -> list[str]:
    """Public top-level functions/classes defined in the module's __init__.py."""
    root = _package_dir(module.package)
    if root is None:
        return []
    init = root / "__init__.py"
    if not init.exists():
        return []
    try:
        tree = ast.parse(init.read_text(encoding="utf-8"), filename=str(init))
    except (SyntaxError, UnicodeDecodeError):
        return []
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            if not node.name.startswith("_"):
                names.append(node.name)
    return names


def _internal_files(module: ModuleInfo) -> list[str]:
    """Private (``_``-prefixed) files/dirs under the module package."""
    root = _package_dir(module.package)
    if root is None:
        return []
    names: set[str] = set()
    for path in root.rglob("_*.py"):
        if path.name == "__init__.py":
            continue
        names.add(str(path.relative_to(root)))
    for path in root.iterdir():
        if path.is_dir() and path.name.startswith("_") and path.name != "__pycache__":
            names.add(path.name + "/")
    return sorted(names)


def _render_event_flow_diagram(modules: list[ModuleInfo]) -> str:
    """Render a Mermaid sequence diagram of event flows.

    Output structure:

        sequenceDiagram
            participant orders
            participant inventory
            participant payments
            orders->>inventory: OrderCreated
            payments->>orders: PaymentReceived

    Same data source as architecture diagram (manifests). Different shape.
    Sequence diagrams emphasize the temporal flow; component diagrams
    emphasize the topology.
    """
    lines = ["sequenceDiagram"]
    for module in modules:
        lines.append(f"  participant {module.name}")
    for src, dst, event in _event_edges(modules):
        lines.append(f"  {src}->>{dst}: {event}")
    return "\n".join(lines) + "\n"


__all__ = ["modulith_render_documentation"]
