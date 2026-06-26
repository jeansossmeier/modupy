"""Built-in documentation generator.

Produces three artifacts from the live module model:
  1. Mermaid C4 component diagram showing modules and their dependencies
  2. Per-module Markdown "canvas" — public API, events, dependencies
  3. Mermaid sequence diagram of event flows

Implementation status: SKELETON. ~150 lines when complete.

Why Mermaid over PlantUML:
  - Renders natively on GitHub/GitLab
  - No separate server needed
  - Live editor at mermaid.live for previewing changes
  - Markdown integration (```mermaid blocks)

Output goes to docs/modulith/ by default. Configurable via
[tool.modulith.docs].output_dir.
"""

from __future__ import annotations

import logging
from pathlib import Path

from modulith import ModuleInfo, hookimpl
from modulith.manifest import get_manifest

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


def _render_architecture_diagram(modules: list[ModuleInfo]) -> str:
    """Render a Mermaid C4 component diagram.

    IMPLEMENTATION TODO:
    Output structure:

        graph TD
            orders[Orders Module]
            inventory[Inventory Module]
            payments[Payments Module]
            orders -->|publishes OrderCreated| inventory
            payments -->|publishes PaymentReceived| orders

    Build by:
    1. One node per module: f"  {module.name}[{module.name.title()} Module]"
    2. One edge per cross-module event flow: source publishes event,
       destination consumes it. Get this from manifests.
    3. If no manifests, fall back to "module A imports from module B"
       edges as a weaker proxy.
    """
    lines = ["graph TD"]
    for module in modules:
        lines.append(f"  {module.name}[{module.name.title()} Module]")
    # IMPLEMENTATION: walk manifests, add edges
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

    IMPLEMENTATION TODO:
    1. Get manifest via get_manifest(module.package).
    2. If manifest exists, drive sections from it.
    3. If no manifest, use introspection: scan the module's __init__.py
       for public function definitions, scan for @event-decorated classes.
    """
    lines = [
        f"# {module.name.title()} Module",
        "",
        f"**Package:** `{module.package}`",
        "",
    ]
    manifest = get_manifest(module.package)
    if manifest is not None:
        if manifest.publishes:
            lines.append("## Events Published")
            lines.extend(f"- `{name}`" for name in manifest.publishes)
            lines.append("")
        if manifest.consumes:
            lines.append("## Events Consumed")
            lines.extend(f"- `{name}`" for name in manifest.consumes)
            lines.append("")
        if manifest.declared_dependencies:
            lines.append("## Dependencies")
            lines.extend(f"- `{dep}` (declared)" for dep in manifest.declared_dependencies)
            lines.append("")
        if manifest.owns_tables:
            lines.append("## Owned Tables")
            lines.extend(f"- `{t}`" for t in manifest.owns_tables)
            lines.append("")
    else:
        lines.append("_No manifest declared. Add a `_manifest.py` for richer documentation._")
    return "\n".join(lines) + "\n"


def _render_event_flow_diagram(modules: list[ModuleInfo]) -> str:
    """Render a Mermaid sequence diagram of event flows.

    IMPLEMENTATION TODO:
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
    # IMPLEMENTATION: walk manifests, add arrows
    return "\n".join(lines) + "\n"


__all__ = ["modulith_render_documentation"]
