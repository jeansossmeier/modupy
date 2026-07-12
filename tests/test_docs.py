"""Tests for the built-in documentation generator.

Diagrams and canvases are driven by manifests (event publishes/consumes,
declared dependencies, owned tables) with introspection fallbacks (public API
from __init__.py, internal files from the package tree).
"""

from __future__ import annotations

from pathlib import Path

from modulith import ModuleInfo
from modulith.builtin import docs


def _module(name: str) -> ModuleInfo:
    return ModuleInfo(name=name, package=f"fakeapp.{name}")


def _declare(package: str, **kw) -> None:
    from modulith import manifest as manifest_module

    manifest_module._manifests[package] = manifest_module.Manifest(package=package, **kw)


# ---------------------------------------------------------------------------
# Architecture diagram
# ---------------------------------------------------------------------------


def test_architecture_diagram(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"orders": "", "inventory": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.inventory", consumes=("OrderCreated",))
    try:
        mmd = docs._render_architecture_diagram([_module("orders"), _module("inventory")])
    finally:
        manifest_module._reset_for_testing()

    assert mmd.startswith("graph TD")
    assert "orders[Orders Module]" in mmd
    assert "inventory[Inventory Module]" in mmd
    # Publisher -> consumer edge labelled with the event.
    assert "orders -->|publishes OrderCreated| inventory" in mmd


def test_architecture_diagram_import_fallback_without_manifests(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": "from fakeapp.payments import charge",
            "payments": "",
        }
    )
    mmd = docs._render_architecture_diagram([_module("orders"), _module("payments")])
    # No manifests → fall back to import edges.
    assert "orders --> payments" in mmd


# ---------------------------------------------------------------------------
# Module canvas
# ---------------------------------------------------------------------------


def test_module_canvas(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str

                def create_order(customer_id: str) -> str:
                    return customer_id

                def _private_helper() -> None:
                    pass
            """,
        },
        extra_files={"orders/_internal.py": "secret = 1\n"},
    )
    _declare(
        "fakeapp.orders",
        publishes=("OrderCreated",),
        consumes=("PaymentReceived",),
        declared_dependencies=("payments",),
        owns_tables=("orders", "order_items"),
    )
    try:
        canvas = docs._render_module_canvas(_module("orders"))
    finally:
        manifest_module._reset_for_testing()

    assert "# Orders Module" in canvas
    assert "`fakeapp.orders`" in canvas
    assert "## Public API" in canvas
    assert "create_order" in canvas
    assert "_private_helper" not in canvas  # private functions excluded
    assert "## Events Published" in canvas and "OrderCreated" in canvas
    assert "## Events Consumed" in canvas and "PaymentReceived" in canvas
    assert "## Dependencies" in canvas and "payments" in canvas
    assert "## Owned Tables" in canvas and "order_items" in canvas
    assert "## Internal Files" in canvas and "_internal.py" in canvas


def test_module_canvas_without_manifest(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"solo": "def go() -> None: ...\n"})
    canvas = docs._render_module_canvas(_module("solo"))
    # Still renders a header + public API from introspection.
    assert "# Solo Module" in canvas
    assert "go" in canvas


def test_module_canvas_introspects_events_without_manifest(make_fake_app) -> None:
    # No _manifest.py: events sections must come from @event/@listener
    # introspection (the documented fallback), not be silently empty.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str

                @listener
                async def on_payment(evt: PaymentReceived) -> None:
                    pass
            """,
        }
    )
    canvas = docs._render_module_canvas(_module("orders"))

    assert "## Events Published" in canvas and "OrderCreated" in canvas
    assert "## Events Consumed" in canvas and "PaymentReceived" in canvas
    # Dependencies/Owned Tables remain manifest-only (no introspection signal).
    assert "## Owned Tables" not in canvas


def test_event_edges_skip_manifests_outside_render_set(make_fake_app) -> None:
    # A manifest for a package not in the modules list (stale/out-of-scope
    # registry entry) must not produce an edge to an undeclared ghost node.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"orders": "", "inventory": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.inventory", consumes=("OrderCreated",))
    # A stray manifest for a package NOT passed to the renderer.
    _declare("fakeapp.ghost", consumes=("OrderCreated",))
    try:
        mmd = docs._render_architecture_diagram([_module("orders"), _module("inventory")])
    finally:
        manifest_module._reset_for_testing()

    assert "orders -->|publishes OrderCreated| inventory" in mmd
    assert "ghost" not in mmd  # no edge/node for the out-of-scope manifest


# ---------------------------------------------------------------------------
# Event flow diagram
# ---------------------------------------------------------------------------


def test_event_flow(make_fake_app) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"orders": "", "inventory": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.inventory", consumes=("OrderCreated",))
    try:
        mmd = docs._render_event_flow_diagram([_module("orders"), _module("inventory")])
    finally:
        manifest_module._reset_for_testing()

    assert mmd.startswith("sequenceDiagram")
    assert "participant orders" in mmd
    assert "orders->>inventory: OrderCreated" in mmd


# ---------------------------------------------------------------------------
# Full render + registration
# ---------------------------------------------------------------------------


def test_render_documentation_produces_files(make_fake_app, tmp_path: Path) -> None:
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"orders": "", "inventory": ""})
    out = tmp_path / "docs"
    try:
        produced = docs.modulith_render_documentation(
            [_module("orders"), _module("inventory")], str(out)
        )
    finally:
        manifest_module._reset_for_testing()

    assert "architecture.mmd" in produced
    assert "events.mmd" in produced
    assert "modules/orders.md" in produced
    assert (out / "architecture.mmd").exists()
    assert (out / "modules" / "orders.md").exists()


def test_docs_registered_as_builtin() -> None:
    from modulith import create_plugin_manager

    pm = create_plugin_manager(load_entrypoints=False)
    assert pm.has_plugin("modulith.builtin.docs")


# ---------------------------------------------------------------------------
# Introspection resilience to unparseable files (A11-r4-191)
# ---------------------------------------------------------------------------


def test_introspection_skips_unparseable_file_and_keeps_good_events(make_fake_app) -> None:
    """A11-r4-191: a file that fails to parse (SyntaxError) is skipped without
    aborting event introspection of the module's other, valid files."""
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str
            """,
        },
        extra_files={"orders/broken.py": "def broken(:\n    pass\n"},
    )

    published, consumed = docs._introspect_events(_module("orders"))

    assert published == ["OrderCreated"]
    assert consumed == []


def test_public_api_returns_empty_for_unparseable_init(make_fake_app) -> None:
    """A11-r4-191: an unparseable ``__init__.py`` degrades to an empty public
    API list instead of raising out of the docs generator."""
    make_fake_app({"badinit": "def broken(:\n    pass\n"})

    assert docs._public_api(_module("badinit")) == []
