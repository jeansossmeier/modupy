"""Tests for the built-in documentation generator.

Diagrams and canvases are driven by manifests (event publishes/consumes,
declared dependencies, owned tables) with introspection fallbacks (public API
from __init__.py, internal files from the package tree).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from modulith import ModuleInfo
from modulith.builtin import docs
from modulith.config import ConfigurationError


def _module(name: str) -> ModuleInfo:
    return ModuleInfo(name=name, package=f"fakeapp.{name}")


def _declare(package: str, **kw) -> None:
    from modulith import manifest as manifest_module

    manifest_module._manifests[package] = manifest_module.Manifest(package=package, **kw)


def _defines_event(name: str) -> str:
    return f"""
        from dataclasses import dataclass
        from modulith import event

        @event
        @dataclass(frozen=True)
        class {name}:
            order_id: str
    """


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


def test_contracts_canvas_without_manifest_lists_events_as_defined(make_fake_app) -> None:
    # The contracts module only defines the shared event types; the modules
    # that publish them live elsewhere, so introspection must not report every
    # event class it holds as published by it.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"contracts": _defines_event("OrderPlaced")})
    canvas = docs._render_module_canvas(_module("contracts"))

    assert "## Events Defined\n- `OrderPlaced`\n" in canvas
    assert "## Events Published" not in canvas


def test_contracts_canvas_with_manifest_keeps_the_declared_events_published(make_fake_app) -> None:
    # A manifest is the module's own declaration of what it publishes, so only
    # the introspection fallback is relabelled.
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"contracts": _defines_event("OrderPlaced")})
    _declare("fakeapp.contracts", publishes=("OrderPlaced",))
    try:
        canvas = docs._render_module_canvas(_module("contracts"))
    finally:
        manifest_module._reset_for_testing()

    assert "## Events Published\n- `OrderPlaced`\n" in canvas
    assert "## Events Defined" not in canvas


def test_docs_command_lists_the_configured_contracts_module_events_as_defined(
    make_fake_app, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The name that decides is the configured one, read through the CLI's
    bootstrap: with ``contracts_module = "shared"``, ``shared`` defines its
    events and a module that is merely called ``contracts`` publishes its own."""
    from typer.testing import CliRunner

    from modulith.cli import app

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_CONTRACTS_MODULE", "shared")
    make_fake_app(
        {
            "shared": _defines_event("OrderPlaced"),
            "contracts": _defines_event("LegacyEvent"),
            "orders": "",
        }
    )
    out = tmp_path / "generated-docs"

    result = CliRunner().invoke(app, ["docs", "--output-dir", str(out)])

    assert result.exit_code == 0, result.output
    shared = (out / "modules" / "shared.md").read_text(encoding="utf-8")
    assert "## Events Defined\n- `OrderPlaced`\n" in shared
    assert "## Events Published" not in shared
    contracts = (out / "modules" / "contracts.md").read_text(encoding="utf-8")
    assert "## Events Published\n- `LegacyEvent`\n" in contracts
    assert "## Events Defined" not in contracts


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


# ---------------------------------------------------------------------------
# Cross-platform encoding
# ---------------------------------------------------------------------------


def test_render_documentation_writes_utf8_explicitly(
    make_fake_app, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every generated artifact must be written with an explicit ``utf-8``
    encoding, not the platform-default fallback (``locale.getpreferredencoding()``
    — cp1252 on Windows), which would raise ``UnicodeEncodeError`` or mangle
    non-ASCII content."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"orders": ""})
    out = tmp_path / "docs"

    recorded_encodings: list[str | None] = []
    original_write_text = Path.write_text

    def _recording_write_text(self: Path, data: str, *args: object, **kwargs: object) -> int:
        recorded_encodings.append(kwargs.get("encoding"))  # type: ignore[arg-type]
        return original_write_text(self, data, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "write_text", _recording_write_text)
    try:
        docs.modulith_render_documentation([_module("orders")], str(out))
    finally:
        manifest_module._reset_for_testing()

    assert recorded_encodings, "expected write_text to be called"
    assert all(enc == "utf-8" for enc in recorded_encodings), recorded_encodings


def test_docs_registered_as_builtin() -> None:
    from modulith import create_plugin_manager

    pm = create_plugin_manager(load_entrypoints=False)
    assert pm.has_plugin("modulith.builtin.docs")


# ---------------------------------------------------------------------------
# Introspection resilience to unparseable files
# ---------------------------------------------------------------------------


def test_introspection_skips_unparseable_file_and_keeps_good_events(make_fake_app) -> None:
    """A file that fails to parse (SyntaxError) is skipped without
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
    """An unparseable ``__init__.py`` degrades to an empty public
    API list instead of raising out of the docs generator."""
    make_fake_app({"badinit": "def broken(:\n    pass\n"})

    assert docs._public_api(_module("badinit")) == []


# ---------------------------------------------------------------------------
# Sources the interpreter accepts but a UTF-8 text read rejects: a UTF-8 BOM
# and a PEP 263 coding line. Each body below carries a non-ASCII character so
# the latin-1 variant is not valid UTF-8.
# ---------------------------------------------------------------------------


def _utf8_bom(body: str) -> bytes:
    return b"\xef\xbb\xbf" + body.encode("utf-8")


def _latin1_coding_line(body: str) -> bytes:
    return b"# -*- coding: latin-1 -*-\n" + body.encode("latin-1")


_interpreter_sources = pytest.mark.parametrize(
    "encode", [_utf8_bom, _latin1_coding_line], ids=["utf8-bom", "latin1-coding-line"]
)


@_interpreter_sources
def test_introspection_reads_events_from_a_bom_or_pep263_module(
    encode: Callable[[str], bytes], make_fake_app, tmp_path: Path
) -> None:
    make_fake_app({"orders": ""})
    (tmp_path / "fakeapp" / "orders" / "legacy.py").write_bytes(
        encode(
            "from dataclasses import dataclass\n"
            "from modulith import event, listener\n\n"
            'LABEL = "café"\n\n\n'
            "@event\n"
            "@dataclass(frozen=True)\n"
            "class OrderCreated:\n"
            "    order_id: str\n\n\n"
            "@listener\n"
            "def on_paid(evt: PaymentReceived) -> None: ...\n"
        )
    )

    published, consumed = docs._introspect_events(_module("orders"))

    assert published == ["OrderCreated"]
    assert consumed == ["PaymentReceived"]


@_interpreter_sources
def test_public_api_is_read_from_a_bom_or_pep263_init(
    encode: Callable[[str], bytes], make_fake_app, tmp_path: Path
) -> None:
    make_fake_app({"orders": ""})
    (tmp_path / "fakeapp" / "orders" / "__init__.py").write_bytes(
        encode('LABEL = "café"\n\n\ndef place_order() -> None: ...\n\n\nclass Order: ...\n')
    )

    assert docs._public_api(_module("orders")) == ["place_order", "Order"]


def test_introspection_still_skips_an_undecodable_file_and_logs_it(
    make_fake_app, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
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
        }
    )
    (tmp_path / "fakeapp" / "orders" / "broken.py").write_bytes(b"NAME = 'caf\xe9'\n")

    with caplog.at_level(logging.WARNING, logger="modulith.docs"):
        published, consumed = docs._introspect_events(_module("orders"))

    assert published == ["OrderCreated"]
    assert consumed == []
    assert "broken.py" in caplog.text
    assert _names_byte_e9(caplog.text)


def test_public_api_still_returns_empty_for_an_undecodable_init(
    make_fake_app, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    make_fake_app({"orders": ""})
    (tmp_path / "fakeapp" / "orders" / "__init__.py").write_bytes(
        b"NAME = 'caf\xe9'\n\n\ndef place_order() -> None: ...\n"
    )

    with caplog.at_level(logging.WARNING, logger="modulith.docs"):
        assert docs._public_api(_module("orders")) == []

    assert "__init__.py" in caplog.text
    assert _names_byte_e9(caplog.text)


def _names_byte_e9(text: str) -> bool:
    # CPython before 3.14 says "can't decode byte 0xe9"; 3.14 says
    # "Non-UTF-8 code starting with '\xe9'".
    return "0xe9" in text or "\\xe9" in text


# ---------------------------------------------------------------------------
# docs.py hardening: name validation, sanitization, loud skips
# ---------------------------------------------------------------------------


def test_architecture_diagram_escapes_reserved_mermaid_node_ids(make_fake_app) -> None:
    """A module named after a Mermaid reserved word ('end') must not
    be emitted as a bare node id — that produces an unparseable diagram,
    contradicting the 'renders natively on GitHub/GitLab' claim. The sanitized
    id carries the display label with the real name; edges use the same id."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"end": "", "orders": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.end", consumes=("OrderCreated",))
    try:
        mmd = docs._render_architecture_diagram([_module("end"), _module("orders")])
    finally:
        manifest_module._reset_for_testing()

    lines = mmd.splitlines()
    assert "  end[End Module]" not in lines  # bare reserved id is a parse error
    assert "  m_end[End Module]" in lines  # sanitized id, real display label
    assert "  orders -->|publishes OrderCreated| m_end" in lines  # edges match ids
    assert "  orders[Orders Module]" in lines  # non-reserved names untouched


def test_event_flow_escapes_reserved_mermaid_participants(make_fake_app) -> None:
    """Sequence-diagram participants named after Mermaid reserved
    words ('end' terminates blocks in sequenceDiagram too) get a sanitized id
    with the real name as the display alias."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"end": "", "orders": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.end", consumes=("OrderCreated",))
    try:
        mmd = docs._render_event_flow_diagram([_module("end"), _module("orders")])
    finally:
        manifest_module._reset_for_testing()

    lines = mmd.splitlines()
    assert "  participant end" not in lines  # bare reserved participant
    assert "  participant m_end as end" in lines  # sanitized id, aliased name
    assert "  orders->>m_end: OrderCreated" in lines  # edges use the same id
    assert "  participant orders" in lines  # non-reserved names untouched


def test_mermaid_ids_of_end_and_m_end_do_not_collide(make_fake_app) -> None:
    """Sanitizing the reserved ``end`` to ``m_end`` must not merge it into a
    module that is really called ``m_end``: two modules stay two nodes."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"end": "", "m_end": "", "orders": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.end", consumes=("OrderCreated",))
    _declare("fakeapp.m_end", consumes=("OrderCreated",))
    try:
        mmd = docs._render_architecture_diagram(
            [_module("end"), _module("m_end"), _module("orders")]
        )
    finally:
        manifest_module._reset_for_testing()

    lines = mmd.splitlines()
    assert "  m_end[End Module]" in lines
    assert "  m_m_end[M_End Module]" in lines
    assert "  orders -->|publishes OrderCreated| m_end" in lines
    assert "  orders -->|publishes OrderCreated| m_m_end" in lines


def test_event_flow_participants_of_end_and_m_end_do_not_collide(make_fake_app) -> None:
    """The sequence diagram sanitizes ids with the same function, so ``end`` and
    ``m_end`` stay two participants there too."""
    from modulith import manifest as manifest_module

    manifest_module._reset_for_testing()
    make_fake_app({"end": "", "m_end": "", "orders": ""})
    _declare("fakeapp.orders", publishes=("OrderCreated",))
    _declare("fakeapp.end", consumes=("OrderCreated",))
    _declare("fakeapp.m_end", consumes=("OrderCreated",))
    try:
        mmd = docs._render_event_flow_diagram([_module("end"), _module("m_end"), _module("orders")])
    finally:
        manifest_module._reset_for_testing()

    lines = mmd.splitlines()
    assert "  participant m_end as end" in lines
    assert "  participant m_m_end as m_end" in lines
    assert "  orders->>m_end: OrderCreated" in lines
    assert "  orders->>m_m_end: OrderCreated" in lines


def test_mermaid_id_never_gives_two_module_names_one_id() -> None:
    """Names that are a reserved word wrapped in any number of ``m_`` prefixes
    (in either case), plus names that merely look similar, all keep distinct
    ids, and no id is itself a reserved word."""
    wrapped = [
        f"{'m_' * depth}{word}"
        for word in (*sorted(docs._MERMAID_RESERVED), "End", "BREAK")
        for depth in range(4)
    ]
    names = [*wrapped, "orders", "m_orders", "M_end", "m_", "m", "endpoint", "m_endpoint"]
    names_by_id: dict[str, list[str]] = {}
    for name in names:
        names_by_id.setdefault(docs._mermaid_id(name), []).append(name)

    assert {i: ns for i, ns in names_by_id.items() if len(ns) > 1} == {}
    assert not {i.lower() for i in names_by_id} & docs._MERMAID_RESERVED
    assert docs._mermaid_id("orders") == "orders"
    assert docs._mermaid_id("m_orders") == "m_orders"


def test_render_documentation_rejects_path_traversal_module_name(tmp_path: Path) -> None:
    """ModuleInfo.name comes from the pluggable discovery hook and
    is used directly as a canvas file name — a traversal name must be rejected
    loudly BEFORE anything is written, never written outside output_dir."""
    out = tmp_path / "docs"
    (tmp_path / "outside").mkdir()  # a landing zone the traversal would reach
    evil = ModuleInfo(name="../../outside/pwned", package="fakeapp.whatever")

    with pytest.raises(ConfigurationError, match="module name"):
        docs.modulith_render_documentation([evil], str(out))

    assert not (tmp_path / "outside" / "pwned.md").exists()
    assert not out.exists()  # validation precedes ALL writes


def test_render_documentation_rejects_duplicate_module_names(tmp_path: Path) -> None:
    """Two modules sharing a name silently overwrote each other's
    canvas while 'produced' claimed both were written — duplicates must raise
    a loud error naming the colliding packages instead."""
    out = tmp_path / "docs"
    v1 = ModuleInfo(name="orders", package="fakeapp.orders_v1")
    v2 = ModuleInfo(name="orders", package="fakeapp.orders_v2")

    with pytest.raises(ConfigurationError, match="orders_v1"):
        docs.modulith_render_documentation([v1, v2], str(out))

    assert not out.exists()  # nothing written for an ambiguous module set


def test_render_documentation_rejects_casefold_colliding_module_names(tmp_path: Path) -> None:
    """Module names differing only by case pass the exact-duplicate check
    but collide on case-insensitive filesystems (macOS default, Windows) —
    <output_dir>/modules/Orders.md and modules/orders.md are the SAME file
    there, so one would silently overwrite the other while `produced`
    claims both were written. Must raise like an exact duplicate."""
    out = tmp_path / "docs"
    v1 = ModuleInfo(name="Orders", package="fakeapp.orders_v1")
    v2 = ModuleInfo(name="orders", package="fakeapp.orders_v2")

    with pytest.raises(ConfigurationError, match="orders_v1"):
        docs.modulith_render_documentation([v1, v2], str(out))

    assert not out.exists()  # nothing written for an ambiguous module set


def test_introspect_events_skips_unreadable_file_with_warning(
    make_fake_app, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A broken symlink (OSError on read) must not crash the whole
    render — the file is skipped with a warning naming file and reason, and
    the module's other, valid files still contribute their events."""
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
        }
    )
    (tmp_path / "fakeapp" / "orders" / "ghost.py").symlink_to(tmp_path / "missing" / "nope.py")

    with caplog.at_level(logging.WARNING, logger="modulith.docs"):
        published, consumed = docs._introspect_events(_module("orders"))

    assert published == ["OrderCreated"]
    assert consumed == []
    assert "ghost.py" in caplog.text
    assert "FileNotFoundError" in caplog.text


def test_introspect_events_logs_warning_for_unparseable_file(
    make_fake_app, caplog: pytest.LogCaptureFixture
) -> None:
    """The SyntaxError skip at the introspection
    scan must log a warning with the file and reason, not skip silently."""
    make_fake_app(
        {"orders": ""},
        extra_files={"orders/broken.py": "def broken(:\n    pass\n"},
    )

    with caplog.at_level(logging.WARNING, logger="modulith.docs"):
        docs._introspect_events(_module("orders"))

    assert "broken.py" in caplog.text
    assert "SyntaxError" in caplog.text


def test_public_api_logs_warning_for_unparseable_init(
    make_fake_app, caplog: pytest.LogCaptureFixture
) -> None:
    """The unparseable-__init__ skip in the public
    API scan must log a warning with the file and reason, not skip silently."""
    make_fake_app({"badinit": "def broken(:\n    pass\n"})

    with caplog.at_level(logging.WARNING, logger="modulith.docs"):
        assert docs._public_api(_module("badinit")) == []

    assert "__init__.py" in caplog.text
    assert "SyntaxError" in caplog.text
