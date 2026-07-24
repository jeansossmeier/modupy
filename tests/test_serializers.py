"""Tests for the default JSON event serializer.

The serializer round-trips dataclass events to/from bytes. It must
preserve dataclass equality and correctly reconstruct rich field types
(datetime, date, UUID, Decimal) that JSON cannot represent natively.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

import pytest

from modulith.protocols import EventSerializer
from modulith.serializers import JsonEventSerializer

if TYPE_CHECKING:
    # Deliberately unimportable at runtime — mirrors an event module whose
    # annotation-only dependency isn't installed in the worker process
    # (regression scaffolding for audit A6-r5-210).
    from nonexistent_debug_module import DebugInfo

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


class PlainEvent:
    """Non-dataclass event exercising the documented vars() fallback."""

    def __init__(self, when: datetime, uid: UUID, amount: Decimal) -> None:
        self.when = when
        self.uid = uid
        self.amount = amount

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PlainEvent) and vars(self) == vars(other)


class SlottedEvent:
    """Non-dataclass event with __slots__ (no __dict__)."""

    __slots__ = ("order_id", "stamp")

    def __init__(self, order_id: str, stamp: datetime) -> None:
        self.order_id = order_id
        self.stamp = stamp


@dataclass(frozen=True)
class FlagCounts:
    counts: dict[bool, int]


@dataclass(frozen=True)
class UuidKeyed:
    scores: dict[UUID, int]


@dataclass(frozen=True)
class ForwardRefEvent:
    order_id: str
    amount: Decimal
    debug: DebugInfo | None = None


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


def test_deserialize_without_allowlist_warns_on_import_resolution() -> None:
    """Hardening: an unrestricted serializer resolves an arbitrary importable
    class from the wire ``event_type`` (see the class docstring). It must not
    do so silently — a warning gives an operator a chance to notice before a
    forged record ships unnoticed."""
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))

    with pytest.warns(RuntimeWarning, match="allowed_event_types"):
        serializer.deserialize(data, _fqcn(SimpleEvent))


def test_deserialize_with_allowlist_does_not_warn() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[SimpleEvent])
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        restored = serializer.deserialize(data, _fqcn(SimpleEvent))

    assert restored == SimpleEvent(order_id="z", quantity=1)


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


def test_non_dataclass_event_round_trip_coerces_types() -> None:
    """A6-r1-18: the non-dataclass deserialize path silently reverted rich
    fields (datetime/UUID/Decimal) to raw strings — it must coerce using the
    class/__init__ annotations, like the dataclass path does."""
    serializer = JsonEventSerializer()
    original = PlainEvent(
        when=datetime(2022, 3, 3, tzinfo=UTC), uid=uuid4(), amount=Decimal("9.99")
    )
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(PlainEvent))
    assert isinstance(restored.when, datetime)
    assert isinstance(restored.uid, UUID)
    assert isinstance(restored.amount, Decimal)
    assert restored == original


def test_dict_bool_keys_round_trip() -> None:
    """A6-r5-209: json.dumps stringifies bool dict keys to "true"/"false"
    without consulting the default hook; the round trip must restore real
    bool keys so dataclass equality survives."""
    serializer = JsonEventSerializer()
    original = FlagCounts(counts={True: 3, False: 7})
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(FlagCounts))
    assert restored == original
    assert all(isinstance(k, bool) for k in restored.counts)


def test_dict_uuid_keys_serialize_and_round_trip() -> None:
    """A6-r2-87: dict[UUID, X] fields crashed serialize() with the json
    module's own opaque TypeError (keys never hit the default hook); keys
    must be pre-encoded and coerced back on the way in."""
    serializer = JsonEventSerializer()
    original = UuidKeyed(scores={uuid4(): 1, uuid4(): 2})
    data = serializer.serialize(original)  # must not raise the raw json TypeError
    restored = serializer.deserialize(data, _fqcn(UuidKeyed))
    assert restored == original
    assert all(isinstance(k, UUID) for k in restored.scores)


def test_unsupported_dict_key_type_raises_serializer_error() -> None:
    """A6-r2-87: unsupported key types must fail loudly with the serializer's
    own message, not the json module's generic one."""

    @dataclass(frozen=True)
    class Weird:
        mapping: dict[object, int]

    serializer = JsonEventSerializer()
    with pytest.raises(TypeError, match="dict key"):
        serializer.serialize(Weird(mapping={object(): 1}))


def test_type_checking_forward_ref_does_not_block_deserialize() -> None:
    """A6-r5-210: a TYPE_CHECKING-only forward-referenced field made
    get_type_hints raise NameError, blocking reconstruction of the whole
    event even though the offending field needed no coercion."""
    serializer = JsonEventSerializer()
    original = ForwardRefEvent(order_id="o1", amount=Decimal("5.00"))
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(ForwardRefEvent))
    assert restored == original
    assert restored.debug is None
    assert isinstance(restored.amount, Decimal)  # resolvable fields still coerce


def test_slotted_event_serializes_and_round_trips() -> None:
    """A6-r5-211: the documented vars() fallback crashed with a raw TypeError
    for __slots__ classes; slots must be read as the instance attributes."""
    serializer = JsonEventSerializer()
    original = SlottedEvent(order_id="o-slot", stamp=datetime(2026, 1, 5, tzinfo=UTC))
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(SlottedEvent))
    assert restored.order_id == "o-slot"
    assert isinstance(restored.stamp, datetime)
    assert restored.stamp == original.stamp


# ---------------------------------------------------------------------------
# Enum / IntEnum round-tripping (A6-r2-88)
# ---------------------------------------------------------------------------


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


@dataclass(frozen=True)
class PriorityKeyed:
    counts: dict[Priority, int]


@dataclass(frozen=True)
class IntOrStringEvent:
    value: int | str


@dataclass(frozen=True)
class StringOrIntEvent:
    value: str | int


@dataclass(frozen=True)
class NestedUnionEvent:
    values: list[int | str]


@dataclass(frozen=True)
class EnumEvent:
    color: Color
    priority: Priority


def test_enum_field_round_trips_with_exact_json() -> None:
    """A6-r2-88: a str-valued Enum field encodes as its value (via the
    serializer's dedicated Enum branch) and decodes back to the member —
    both the wire format and the restored type identity are pinned."""
    serializer = JsonEventSerializer()
    original = EnumEvent(color=Color.BLUE, priority=Priority.HIGH)

    raw = serializer.serialize(original)
    assert raw == b'{"color":"blue","priority":2}'

    restored = serializer.deserialize(raw, _fqcn(EnumEvent))
    assert restored == original
    assert isinstance(restored.color, Color)
    assert restored.color is Color.BLUE


def test_int_enum_field_round_trips_to_member_identity() -> None:
    """A6-r2-88: IntEnum members are int subclasses, so encode bypasses the
    Enum branch entirely (json's native int encoder wins) — only decode-side
    coercion restores the member. A reordering of _coerce's checks (e.g. an
    early int fast-path) would silently break this; pin it."""
    serializer = JsonEventSerializer()
    original = EnumEvent(color=Color.RED, priority=Priority.LOW)

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(EnumEvent))

    assert restored == original
    assert isinstance(restored.priority, Priority)
    assert restored.priority is Priority.LOW


def test_numeric_enum_dict_keys_round_trip() -> None:
    """Numeric enum keys become JSON strings, then must restore their members."""
    serializer = JsonEventSerializer()
    original = PriorityKeyed(counts={Priority.LOW: 2, Priority.HIGH: 9})

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(PriorityKeyed))

    assert restored == original
    assert set(restored.counts) == {Priority.LOW, Priority.HIGH}


@pytest.mark.parametrize(
    "event",
    [
        IntOrStringEvent(value=7),
        IntOrStringEvent(value="7"),
        StringOrIntEvent(value=7),
        StringOrIntEvent(value="7"),
    ],
)
def test_non_optional_union_round_trips_independently_of_member_order(
    event: IntOrStringEvent | StringOrIntEvent,
) -> None:
    """A tagged union payload keeps the original member when coercions overlap."""
    serializer = JsonEventSerializer()

    restored = serializer.deserialize(serializer.serialize(event), _fqcn(type(event)))

    assert restored == event
    typed = cast(IntOrStringEvent | StringOrIntEvent, restored)
    assert type(typed.value) is type(event.value)


def test_nested_non_optional_union_round_trips() -> None:
    """Union tags apply recursively inside containers, not just top-level fields."""
    serializer = JsonEventSerializer()
    original = NestedUnionEvent(values=[1, "1"])

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(NestedUnionEvent))

    assert restored == original
    assert [type(value) for value in restored.values] == [int, str]


def test_legacy_untagged_union_payload_still_decodes() -> None:
    """Existing payloads predate union tags and remain readable."""
    serializer = JsonEventSerializer()

    restored = serializer.deserialize(b'{"value":7}', _fqcn(StringOrIntEvent))

    # Legacy payloads retain the historic first-member coercion behavior.
    assert restored == StringOrIntEvent(value=7)
