"""Tests for the default JSON event serializer.

The serializer round-trips dataclass events to/from bytes. It must
preserve dataclass equality and correctly reconstruct rich field types
(datetime, date, UUID, Decimal) that JSON cannot represent natively.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from modulith.protocols import EventSerializer
from modulith.serializers import JsonEventSerializer

# ---------------------------------------------------------------------------
# Test event types — defined at module scope so they're importable by their
# fully-qualified name during deserialization (mirrors real event modules).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimpleEvent:
    order_id: str
    quantity: int


@dataclass(frozen=True)
class RichEvent:
    order_id: str
    amount: Decimal
    created_at: datetime
    due: date
    correlation_id: UUID


@dataclass(frozen=True)
class OptionalEvent:
    name: str
    note: str | None = None


@dataclass(frozen=True)
class ContainerEvent:
    stamps: list[datetime]
    amounts: dict[str, Decimal]
    ids: frozenset[UUID]
    days: tuple[date, ...]


@dataclass(frozen=True)
class LineItem:
    sku: str
    price: Decimal
    added_at: datetime


@dataclass(frozen=True)
class NestedEvent:
    order_id: str
    item: LineItem
    extra: list[LineItem]


def _fqcn(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def test_serialize_returns_bytes() -> None:
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="o1", quantity=3))
    assert isinstance(data, bytes)


def test_conforms_to_protocol() -> None:
    # runtime_checkable Protocol — duck-typed conformance check.
    assert isinstance(JsonEventSerializer(), EventSerializer)


def test_round_trip_simple_dataclass_preserves_equality() -> None:
    serializer = JsonEventSerializer()
    original = SimpleEvent(order_id="abc", quantity=7)
    data = serializer.serialize(original)
    restored = serializer.deserialize(data, _fqcn(SimpleEvent))
    assert restored == original
    assert isinstance(restored, SimpleEvent)


def test_round_trip_rich_types_preserve_equality() -> None:
    serializer = JsonEventSerializer()
    original = RichEvent(
        order_id="o-9",
        amount=Decimal("19.99"),
        created_at=datetime(2026, 6, 26, 12, 30, 45, tzinfo=UTC),
        due=date(2026, 7, 1),
        correlation_id=uuid4(),
    )
    data = serializer.serialize(original)
    restored = serializer.deserialize(data, _fqcn(RichEvent))
    assert restored == original
    assert isinstance(restored.amount, Decimal)
    assert isinstance(restored.created_at, datetime)
    assert isinstance(restored.due, date)
    assert isinstance(restored.correlation_id, UUID)


def test_decimal_precision_preserved() -> None:
    serializer = JsonEventSerializer()
    original = RichEvent(
        order_id="o",
        amount=Decimal("0.10000000000000001"),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        due=date(2026, 1, 1),
        correlation_id=uuid4(),
    )
    data = serializer.serialize(original)
    restored = serializer.deserialize(data, _fqcn(RichEvent))
    # Decimal carried as string, so precision survives (a float would not).
    assert restored.amount == Decimal("0.10000000000000001")


def test_optional_field_none_round_trips() -> None:
    serializer = JsonEventSerializer()
    original = OptionalEvent(name="x")
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(OptionalEvent))
    assert restored == original
    assert restored.note is None


def test_deserialize_resolves_class_from_fqcn() -> None:
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))
    restored = serializer.deserialize(data, _fqcn(SimpleEvent))
    assert type(restored) is SimpleEvent


def test_deserialize_unknown_module_raises() -> None:
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))
    with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
        serializer.deserialize(data, "no.such.module.Nope")


def test_allowlist_blocks_importable_but_unregistered_event_type() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[_fqcn(SimpleEvent)])
    data = serializer.serialize(
        RichEvent(
            order_id="o",
            amount=Decimal("1.00"),
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            due=date(2026, 1, 1),
            correlation_id=uuid4(),
        )
    )

    with pytest.raises(ValueError, match="not in the allowed event types"):
        serializer.deserialize(data, _fqcn(RichEvent))


def test_allowlist_allows_registered_event_type() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[SimpleEvent])
    original = SimpleEvent(order_id="allowed", quantity=2)

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(SimpleEvent))

    assert restored == original


def test_round_trip_parameterized_containers_coerce_inner_types() -> None:
    # Regression for the audit finding: list[datetime] / dict[str, Decimal] /
    # set[UUID] / tuple[date, ...] must coerce their *inner* elements, not leave
    # them as raw JSON strings.
    serializer = JsonEventSerializer()
    original = ContainerEvent(
        stamps=[datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 2, tzinfo=UTC)],
        amounts={"usd": Decimal("19.99"), "eur": Decimal("17.50")},
        ids=frozenset({uuid4(), uuid4()}),
        days=(date(2026, 1, 1), date(2026, 1, 2)),
    )
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(ContainerEvent))
    assert restored == original
    assert all(isinstance(s, datetime) for s in restored.stamps)
    assert all(isinstance(v, Decimal) for v in restored.amounts.values())
    assert all(isinstance(i, UUID) for i in restored.ids)
    assert all(isinstance(d, date) for d in restored.days)


def test_round_trip_nested_dataclass_fields() -> None:
    # Regression for the audit finding: a field typed as another @dataclass (and
    # a list of them) must serialize and reconstruct, not raise TypeError.
    serializer = JsonEventSerializer()
    original = NestedEvent(
        order_id="o-1",
        item=LineItem(sku="A", price=Decimal("3.50"), added_at=datetime(2026, 1, 1, tzinfo=UTC)),
        extra=[
            LineItem(sku="B", price=Decimal("9.00"), added_at=datetime(2026, 1, 2, tzinfo=UTC)),
        ],
    )
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(NestedEvent))
    assert restored == original
    assert isinstance(restored.item, LineItem)
    assert isinstance(restored.item.price, Decimal)
    assert isinstance(restored.extra[0], LineItem)
    assert isinstance(restored.extra[0].added_at, datetime)
