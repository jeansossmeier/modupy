"""Tests for shared plugin-contract data types.

These types appear in hookspec signatures and the public API; they need
test coverage for construction, defaults, and field semantics so plugin
authors get a stable contract.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from modulith.types import (
    EventPublication,
    ModuleInfo,
    Violation,
    ViolationSeverity,
)

# ---------------------------------------------------------------------------
# ModuleInfo
# ---------------------------------------------------------------------------


def test_module_info_minimal_construction() -> None:
    info = ModuleInfo(name="orders", package="myapp.orders")
    assert info.name == "orders"
    assert info.package == "myapp.orders"
    assert info.declared_dependencies == ()


def test_module_info_with_dependencies() -> None:
    info = ModuleInfo(
        name="orders",
        package="myapp.orders",
        declared_dependencies=("payments", "inventory"),
    )
    assert info.declared_dependencies == ("payments", "inventory")


# ---------------------------------------------------------------------------
# EventPublication
# ---------------------------------------------------------------------------


def test_event_publication_minimal_construction() -> None:
    """Regression test: only id and payload are required; the outbox plugin
    populates event_type, listener, published_at at save time."""
    pub = EventPublication(id=uuid4(), payload=b"")
    assert pub.event_type is None
    assert pub.listener is None
    assert pub.published_at is None
    assert pub.completed_at is None
    assert pub.attempt_count == 0
    assert pub.last_error is None


def test_event_publication_full_construction() -> None:
    """The outbox-plugin construction site sets every field."""
    pid = uuid4()
    now = datetime.now(tz=UTC)
    pub = EventPublication(
        id=pid,
        payload=b'{"order_id": "abc"}',
        event_type="myapp.orders.events.OrderCreated",
        listener="inventory.handlers.on_order_created",
        published_at=now,
    )
    assert pub.id == pid
    assert pub.event_type == "myapp.orders.events.OrderCreated"
    assert pub.listener == "inventory.handlers.on_order_created"
    assert pub.published_at == now
    assert pub.completed_at is None
    assert pub.attempt_count == 0


def test_event_publication_is_mutable() -> None:
    """Lifecycle updates (mark_complete, retry) mutate fields directly."""
    pub = EventPublication(id=uuid4(), payload=b"")
    pub.attempt_count = 3
    pub.last_error = "boom"
    pub.completed_at = datetime.now(tz=UTC)
    assert pub.attempt_count == 3
    assert pub.last_error == "boom"
    assert pub.completed_at is not None


# ---------------------------------------------------------------------------
# Violation / ViolationSeverity
# ---------------------------------------------------------------------------


def test_violation_default_severity_is_error() -> None:
    v = Violation(rule="no-cycles", message="A → B → A", module="myapp.orders")
    assert v.severity is ViolationSeverity.ERROR
    assert v.location is None


def test_violation_with_location() -> None:
    v = Violation(
        rule="no-internal-imports",
        message="imports from inventory._internal",
        module="myapp.orders",
        severity=ViolationSeverity.WARNING,
        location="myapp/orders/handlers.py:42",
    )
    assert v.severity is ViolationSeverity.WARNING
    assert v.location == "myapp/orders/handlers.py:42"
