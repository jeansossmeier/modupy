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

Output goes to docs/modulith/ by default; redirect it with
``modulith docs --output-dir=DIR``. There is no pyproject knob for it — the
generator writes exactly where the CLI points.
"""

from __future__ import annotations

import ast
import logging
from collections import defaultdict
from pathlib import Path

from modulith import ModuleInfo, hookimpl
from modulith.builtin.verifier import _collect_imports, _owning_module, _package_dir
from modulith.config import ConfigurationError
from modulith.manifest import all_manifests, get_manifest

logger = logging.getLogger("modulith.docs")

# Mermaid reserved words (flowchart + sequenceDiagram grammars) that break
# parsing when used as bare node ids or participant names — e.g. a module
# directory named ``end`` (valid Python package, reserved in Mermaid) renders
# an unparseable diagram. Module names come from the pluggable
# discovery hook, so any valid identifier is possible. Compared lowercase.
_MERMAID_RESERVED = frozenset(
    {
        # flowchart / graph
        "end",
        "graph",
        "flowchart",
        "subgraph",
        "direction",
        "style",
        "linkstyle",
        "classdef",
        "class",
        "click",
        "default",
        "interpolate",
        # sequenceDiagram
        "participant",
        "actor",
        "loop",
        "alt",
        "else",
        "opt",
        "par",
        "and",
        "rect",
        "note",
        "activate",
        "deactivate",
        "autonumber",
        "title",
        "box",
        "break",
        "critical",
        "option",
    }
)


def _mermaid_id(name: str) -> str:
    """A Mermaid-safe node/participant id for a module name.

    Names colliding with Mermaid reserved words get an ``m_`` prefix; the
    display label / participant alias still carries the real name, so the
    rendered diagram reads identically while staying parseable.
    """
    return f"m_{name}" if name.lower() in _MERMAID_RESERVED else name


def _validate_module_names(modules: list[ModuleInfo]) -> None:
    """Reject module names the docs generator cannot key files by.

    ``ModuleInfo.name`` comes from the pluggable ``modulith_discover_modules``
    hook, so it is user-controllable input, not trusted framework state.
    Validated BEFORE anything is written:

      * duplicate names — canvases are keyed by name, so
        duplicates silently overwrite each other while ``produced`` claims
        both were written; and
      * names that are not a single safe path segment — the
        canvas path ``<output_dir>/modules/<name>.md`` would escape (or nest
        outside) the output directory.
    """
    by_name: dict[str, list[str]] = defaultdict(list)
    for module in modules:
        by_name[module.name].append(module.package)
    duplicates = {name: pkgs for name, pkgs in by_name.items() if len(pkgs) > 1}
    if duplicates:
        details = "; ".join(
            f"{name!r} (packages: {', '.join(pkgs)})" for name, pkgs in sorted(duplicates.items())
        )
        raise ConfigurationError(
            f"duplicate module name(s) in documentation render: {details}. "
            "Canvas files are keyed by module name, so duplicates would "
            "silently overwrite each other — give each module a unique name."
        )

    # Exact-match duplicates are caught above, but names differing only by
    # case still collide on case-insensitive filesystems (macOS default,
    # Windows) — <output_dir>/modules/Orders.md and modules/orders.md are
    # the SAME file there, so one would silently overwrite the other.
    names_by_casefold: dict[str, set[str]] = defaultdict(set)
    for name in by_name:
        names_by_casefold[name.casefold()].add(name)
    colliding_groups = [names for names in names_by_casefold.values() if len(names) > 1]
    if colliding_groups:
        details = "; ".join(
            f"{sorted(names)!r} (packages: "
            f"{', '.join(pkg for name in sorted(names) for pkg in by_name[name])})"
            for names in colliding_groups
        )
        raise ConfigurationError(
            f"module name(s) differing only by case in documentation render: {details}. "
            "Canvas file paths collide on case-insensitive filesystems — "
            "give each module a name that is unique ignoring case."
        )
    for module in modules:
        name = module.name
        if not name or name in (".", "..") or Path(name).name != name or "\\" in name:
            raise ConfigurationError(
                f"module name {name!r} (package {module.package!r}) is not a "
                "safe file name — canvas files are written to "
                "<output_dir>/modules/<name>.md, and this name would escape "
                "or nest outside that directory."
            )


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

    Raises ConfigurationError — before anything is written — when the module
    set carries duplicate names or a name that is not a safe single path
    segment (see ``_validate_module_names``).
    """
    _validate_module_names(modules)
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
        lines.append(f"  {_mermaid_id(module.name)}[{module.name.title()} Module]")

    edges = _event_edges(modules)
    if edges:
        for src, dst, event in edges:
            lines.append(f"  {_mermaid_id(src)} -->|publishes {event}| {_mermaid_id(dst)}")
    else:
        for src, dst in _import_edges(modules):
            lines.append(f"  {_mermaid_id(src)} --> {_mermaid_id(dst)}")
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
        except (OSError, SyntaxError, UnicodeDecodeError) as exc:
            # One unreadable file (broken symlink, permission denied) or
            # unparseable file must degrade this module's introspection, not
            # abort the whole render — and never silently: name the file and
            # the reason.
            logger.warning(
                "docs: skipping %s during event introspection (%s: %s)",
                path,
                type(exc).__name__,
                exc,
            )
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
    except (OSError, SyntaxError, UnicodeDecodeError) as exc:
        # Same graceful-degrade + loud-skip contract as _introspect_events: an
        # unreadable or unparseable __init__.py yields an empty public API,
        # with the file and reason logged.
        logger.warning(
            "docs: skipping public-API scan of %s (%s: %s)",
            init,
            type(exc).__name__,
            exc,
        )
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
        pid = _mermaid_id(module.name)
        if pid != module.name:
            # Reserved word ('end' terminates blocks in sequenceDiagram too):
            # sanitized id, real name as the display alias.
            lines.append(f"  participant {pid} as {module.name}")
        else:
            lines.append(f"  participant {module.name}")
    for src, dst, event in _event_edges(modules):
        lines.append(f"  {_mermaid_id(src)}->>{_mermaid_id(dst)}: {event}")
    return "\n".join(lines) + "\n"


__all__ = ["modulith_render_documentation"]
