"""Tests for the default JSON event serializer.

The serializer round-trips dataclass events to/from bytes. It must
preserve dataclass equality and correctly reconstruct rich field types
(datetime, date, UUID, Decimal) that JSON cannot represent natively.
"""

from __future__ import annotations

import logging
import sys
import warnings
from dataclasses import InitVar, dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, NewType, Set, Tuple, cast  # noqa: UP035
from uuid import UUID, uuid4

import pytest
from typing_extensions import TypeAliasType

from modulith.config import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    MAX_PAYLOAD_BYTES,
    Configuration,
    ConfigurationError,
)
from modulith.protocols import EventSerializer
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer, _resolve_max_payload_bytes

if TYPE_CHECKING:
    # Deliberately unimportable at runtime — mirrors an event module whose
    # annotation-only dependency isn't installed in the worker process
    # (scaffolding for the TYPE_CHECKING-forward-reference regression below).
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


def test_resolve_max_payload_bytes_defaults_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    assert _resolve_max_payload_bytes(None) == DEFAULT_MAX_PAYLOAD_BYTES
    assert _resolve_max_payload_bytes({}) == DEFAULT_MAX_PAYLOAD_BYTES


def test_resolve_max_payload_bytes_reads_broker_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    assert _resolve_max_payload_bytes({"max_payload_bytes": 33554432}) == 33554432


def test_resolve_max_payload_bytes_env_overrides_broker_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", "1048576")
    assert _resolve_max_payload_bytes({"max_payload_bytes": 33554432}) == 1048576


def test_resolve_max_payload_bytes_rejects_non_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    with pytest.raises(ConfigurationError, match="max_payload_bytes"):
        _resolve_max_payload_bytes({"max_payload_bytes": "not-a-number"})


@pytest.mark.parametrize("bad_value", [0, -1, MAX_PAYLOAD_BYTES + 1])
def test_resolve_max_payload_bytes_rejects_out_of_range(
    monkeypatch: pytest.MonkeyPatch, bad_value: int
) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    with pytest.raises(ConfigurationError, match="max_payload_bytes"):
        _resolve_max_payload_bytes({"max_payload_bytes": bad_value})


@pytest.mark.parametrize("good_value", [1, MAX_PAYLOAD_BYTES])
def test_resolve_max_payload_bytes_accepts_the_range_boundaries(
    monkeypatch: pytest.MonkeyPatch, good_value: int
) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    assert _resolve_max_payload_bytes({"max_payload_bytes": good_value}) == good_value


@pytest.mark.parametrize("bad_value", [True, False, 2.9, 1.0, [5]], ids=repr)
def test_resolve_max_payload_bytes_rejects_bool_and_float(
    monkeypatch: pytest.MonkeyPatch, bad_value: object
) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    with pytest.raises(ConfigurationError, match="max_payload_bytes must be an integer"):
        _resolve_max_payload_bytes({"max_payload_bytes": bad_value})


@pytest.mark.parametrize("bad_env", ["2.9", "True", "abc", "0", str(MAX_PAYLOAD_BYTES + 1)])
def test_resolve_max_payload_bytes_rejects_a_bad_env_value_naming_the_variable(
    monkeypatch: pytest.MonkeyPatch, bad_env: str
) -> None:
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", bad_env)
    with pytest.raises(ConfigurationError, match="MODULITH_BROKER_MAX_PAYLOAD_BYTES"):
        _resolve_max_payload_bytes({"max_payload_bytes": 33554432})


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=repr)
def test_resolve_max_payload_bytes_treats_a_blank_env_value_as_unset(
    monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", blank)
    assert _resolve_max_payload_bytes(None) == DEFAULT_MAX_PAYLOAD_BYTES
    assert _resolve_max_payload_bytes({"max_payload_bytes": 33554432}) == 33554432


@pytest.mark.parametrize("blank", ["", "   "], ids=repr)
def test_resolve_max_payload_bytes_treats_a_blank_broker_option_as_unset(
    monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    assert _resolve_max_payload_bytes({"max_payload_bytes": blank}) == DEFAULT_MAX_PAYLOAD_BYTES


def test_default_serializer_resolves_cap_from_env_at_first_deserialize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", "10")
    serializer = JsonEventSerializer()
    assert serializer._max_payload_bytes is None

    data = serializer.serialize(SimpleEvent(order_id="oversized-payload", quantity=1))
    assert len(data) > 10

    with pytest.raises(ConfigurationError, match="max_payload_bytes"):
        serializer.deserialize(data, _fqcn(SimpleEvent))

    assert serializer._max_payload_bytes == 10


def test_default_serializer_resolves_cap_from_loaded_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    _runtime._config = Configuration(broker_options={"max_payload_bytes": 12})
    try:
        serializer = JsonEventSerializer()
        data = serializer.serialize(SimpleEvent(order_id="x", quantity=1))
        assert len(data) > 12

        with pytest.raises(ConfigurationError, match="max_payload_bytes"):
            serializer.deserialize(data, _fqcn(SimpleEvent))

        assert serializer._max_payload_bytes == 12
    finally:
        _runtime._reset_for_testing()


def test_explicit_max_payload_bytes_ignores_env_and_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", "999999999")
    _runtime._config = Configuration(broker_options={"max_payload_bytes": 999999999})
    try:
        serializer = JsonEventSerializer(max_payload_bytes=10)
        assert serializer._max_payload_bytes == 10

        data = serializer.serialize(SimpleEvent(order_id="oversized-payload", quantity=1))
        with pytest.raises(ConfigurationError, match="max_payload_bytes"):
            serializer.deserialize(data, _fqcn(SimpleEvent))

        assert serializer._max_payload_bytes == 10
    finally:
        _runtime._reset_for_testing()


def test_deserialize_unknown_module_raises() -> None:
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))
    with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
        serializer.deserialize(data, "no.such.module.Nope")


def test_deserialize_rejects_payload_exceeding_max_payload_bytes() -> None:
    serializer = JsonEventSerializer(max_payload_bytes=10)
    data = serializer.serialize(SimpleEvent(order_id="oversized-payload", quantity=1))
    assert len(data) > 10

    with pytest.raises(ConfigurationError, match="max_payload_bytes"):
        serializer.deserialize(data, _fqcn(SimpleEvent))


def test_deserialize_rejects_oversized_payload_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("json.loads must not run on an oversized payload")

    monkeypatch.setattr("modulith.serializers.json.loads", _boom)
    serializer = JsonEventSerializer(max_payload_bytes=5)

    with pytest.raises(ConfigurationError):
        serializer.deserialize(b"x" * 6, _fqcn(SimpleEvent))


def test_default_max_payload_bytes_rejects_payload_over_the_configured_default_cap() -> None:
    serializer = JsonEventSerializer()
    oversized = b"{" + b"1" * (DEFAULT_MAX_PAYLOAD_BYTES + 1) + b"}"

    with pytest.raises(ConfigurationError, match="max_payload_bytes"):
        serializer.deserialize(oversized, _fqcn(SimpleEvent))


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


def test_deserialize_without_allowlist_also_logs(caplog) -> None:
    """A RuntimeWarning alone is not enough: ``PYTHONWARNINGS=ignore`` and
    ``-W ignore`` silence the warnings channel wholesale, and warnings go to
    stderr rather than the app's log pipeline. The fail-open configuration
    must also reach the logs a deployment actually collects."""
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))

    with caplog.at_level(logging.WARNING, logger="modulith.serializers"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            serializer.deserialize(data, _fqcn(SimpleEvent))

    assert [r.getMessage() for r in caplog.records if "allowed_event_types" in r.getMessage()]


def test_unrestricted_deserialize_announces_once_per_instance(caplog) -> None:
    """Bounded to one announcement per serializer so a hot dispatch loop
    cannot flood the log with the same advisory."""
    serializer = JsonEventSerializer()
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))

    with caplog.at_level(logging.WARNING, logger="modulith.serializers"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            for _ in range(5):
                serializer.deserialize(data, _fqcn(SimpleEvent))

    matching = [r for r in caplog.records if "allowed_event_types" in r.getMessage()]
    assert len(matching) == 1


def test_deserialize_with_allowlist_does_not_warn(caplog) -> None:
    serializer = JsonEventSerializer(allowed_event_types=[SimpleEvent])
    data = serializer.serialize(SimpleEvent(order_id="z", quantity=1))

    with caplog.at_level(logging.WARNING, logger="modulith.serializers"):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            restored = serializer.deserialize(data, _fqcn(SimpleEvent))

    assert restored == SimpleEvent(order_id="z", quantity=1)
    assert caplog.records == []


def test_round_trip_parameterized_containers_coerce_inner_types() -> None:
    # list[datetime] / dict[str, Decimal] / set[UUID] / tuple[date, ...] must
    # coerce their *inner* elements, not leave them as raw JSON strings.
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
    # A field typed as another @dataclass (and a list of them) must serialize
    # and reconstruct, not raise TypeError.
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
    """The non-dataclass deserialize path silently reverted rich
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
    """json.dumps stringifies bool dict keys to "true"/"false"
    without consulting the default hook; the round trip must restore real
    bool keys so dataclass equality survives."""
    serializer = JsonEventSerializer()
    original = FlagCounts(counts={True: 3, False: 7})
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(FlagCounts))
    assert restored == original
    assert all(isinstance(k, bool) for k in restored.counts)


def test_dict_uuid_keys_serialize_and_round_trip() -> None:
    """dict[UUID, X] fields crashed serialize() with the json
    module's own opaque TypeError (keys never hit the default hook); keys
    must be pre-encoded and coerced back on the way in."""
    serializer = JsonEventSerializer()
    original = UuidKeyed(scores={uuid4(): 1, uuid4(): 2})
    data = serializer.serialize(original)  # must not raise the raw json TypeError
    restored = serializer.deserialize(data, _fqcn(UuidKeyed))
    assert restored == original
    assert all(isinstance(k, UUID) for k in restored.scores)


def test_unsupported_dict_key_type_raises_serializer_error() -> None:
    """Unsupported key types must fail loudly with the serializer's
    own message, not the json module's generic one."""

    @dataclass(frozen=True)
    class Weird:
        mapping: dict[object, int]

    serializer = JsonEventSerializer()
    with pytest.raises(TypeError, match="dict key"):
        serializer.serialize(Weird(mapping={object(): 1}))


def test_type_checking_forward_ref_does_not_block_deserialize() -> None:
    """A TYPE_CHECKING-only forward-referenced field made
    get_type_hints raise NameError, blocking reconstruction of the whole
    event even though the offending field needed no coercion."""
    serializer = JsonEventSerializer()
    original = ForwardRefEvent(order_id="o1", amount=Decimal("5.00"))
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(ForwardRefEvent))
    assert restored == original
    assert restored.debug is None
    assert isinstance(restored.amount, Decimal)  # resolvable fields still coerce


@pytest.mark.skipif(sys.version_info < (3, 14), reason="deferred annotations (PEP 649) need 3.14+")
def test_unresolvable_annotation_keeps_hints_of_other_fields_under_deferred_annotations(
    monkeypatch,
) -> None:
    source = (
        "from dataclasses import dataclass\n"
        "from datetime import datetime\n"
        "from decimal import Decimal\n"
        "from uuid import UUID\n"
        "@dataclass\n"
        "class DeferredEvent:\n"
        "    amount: Decimal\n"
        "    created_at: datetime\n"
        "    ref: UUID\n"
        "    debug: DebugInfo | None = None\n"
    )
    namespace: dict[str, object] = {"__name__": __name__}
    # dont_inherit: this module's postponed annotations would store strings,
    # hiding the 3.14 deferred-annotation path under test.
    exec(compile(source, "<deferred>", "exec", dont_inherit=True), namespace)
    event_type = cast(type, namespace["DeferredEvent"])
    monkeypatch.setattr(sys.modules[__name__], "DeferredEvent", event_type, raising=False)
    serializer = JsonEventSerializer()
    original = event_type(
        amount=Decimal("5.00"), created_at=datetime(2026, 1, 5, tzinfo=UTC), ref=uuid4()
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(event_type))

    assert restored == original
    assert type(restored.amount) is Decimal
    assert type(restored.created_at) is datetime
    assert type(restored.ref) is UUID
    assert restored.debug is None


def test_slotted_event_serializes_and_round_trips() -> None:
    """The documented vars() fallback crashed with a raw TypeError
    for __slots__ classes; slots must be read as the instance attributes."""
    serializer = JsonEventSerializer()
    original = SlottedEvent(order_id="o-slot", stamp=datetime(2026, 1, 5, tzinfo=UTC))
    restored = serializer.deserialize(serializer.serialize(original), _fqcn(SlottedEvent))
    assert restored.order_id == "o-slot"
    assert isinstance(restored.stamp, datetime)
    assert restored.stamp == original.stamp


# ---------------------------------------------------------------------------
# Enum / IntEnum round-tripping
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
    """A str-valued Enum field encodes as its value (via the
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
    """IntEnum members are int subclasses, so encode bypasses the
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


@dataclass(frozen=True)
class BasePayment:
    amount: int


@dataclass(frozen=True)
class CardPayment(BasePayment):
    card_last4: str


@dataclass(frozen=True)
class PremiumCardPayment(CardPayment):
    tier: str


@dataclass(frozen=True)
class OrderPaid:
    order_id: str
    payment: BasePayment
    history: list[BasePayment] = field(default_factory=list)


@dataclass(frozen=True)
class OrderPaidUnion:
    backup: BasePayment | None
    alternative: str | BasePayment


forged_instantiations: list[str] = []


@dataclass(frozen=True)
class ForgedTarget:
    """Importable dataclass outside ``BasePayment``'s tree; records instantiation."""

    amount: int

    def __post_init__(self) -> None:
        forged_instantiations.append("instantiated")


def test_base_typed_nested_field_round_trips_subclass_instances() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaid])
    original = OrderPaid(
        order_id="o-1",
        payment=CardPayment(amount=500, card_last4="4242"),
        history=[BasePayment(amount=1), PremiumCardPayment(amount=2, card_last4="1", tier="gold")],
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(OrderPaid))

    assert restored == original
    assert type(restored.payment) is CardPayment
    assert [type(p) for p in restored.history] == [BasePayment, PremiumCardPayment]


def test_base_typed_union_field_round_trips_subclass_instances() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaidUnion])
    original = OrderPaidUnion(
        backup=PremiumCardPayment(amount=3, card_last4="9", tier="gold"),
        alternative=CardPayment(amount=4, card_last4="7"),
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(OrderPaidUnion))

    assert restored == original
    assert type(restored.backup) is PremiumCardPayment
    assert type(restored.alternative) is CardPayment


def test_exact_type_nested_dataclass_keeps_untagged_wire_format() -> None:
    """A value of exactly the declared class encodes as it always did, so rows
    written before subclass envelopes existed still decode."""
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaid])
    legacy = b'{"history":[],"order_id":"o-1","payment":{"amount":5}}'

    assert serializer.serialize(OrderPaid(order_id="o-1", payment=BasePayment(amount=5))) == legacy
    assert serializer.deserialize(legacy, _fqcn(OrderPaid)) == OrderPaid(
        order_id="o-1", payment=BasePayment(amount=5)
    )


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        (
            OrderPaid,
            b'{"order_id":"o","payment":'
            b'{"__modulith_union_type__":"' + __name__.encode() + b'.ForgedTarget",'
            b'"value":{"amount":1}}}',
        ),
        (
            OrderPaidUnion,
            b'{"alternative":"x","backup":'
            b'{"__modulith_union_type__":"' + __name__.encode() + b'.ForgedTarget",'
            b'"value":{"amount":1}}}',
        ),
    ],
)
def test_subclass_tag_outside_declared_tree_is_never_instantiated(
    event_type: type, payload: bytes
) -> None:
    """The nested type tag is resolved only among the declared class's
    subclasses: a tag naming any other class decodes as the declared class
    when the fields fit, and the named class is never instantiated."""
    serializer = JsonEventSerializer(allowed_event_types=[event_type])
    forged_instantiations.clear()

    restored = serializer.deserialize(payload, _fqcn(event_type))

    decoded = getattr(restored, "payment", None) or restored.backup
    assert type(decoded) is BasePayment
    assert decoded == BasePayment(amount=1)
    assert forged_instantiations == []


def test_unimported_subclass_tag_decodes_as_declared_base_without_importing(
    tmp_path, monkeypatch, caplog
) -> None:
    """A consumer that never imported the subclass's module decodes a
    field-compatible value as the declared class, warns once per tag, and
    never imports the module the tag names."""
    module_name = f"unimported_payments_{uuid4().hex}"
    (tmp_path / f"{module_name}.py").write_text("raise RuntimeError('tag was imported')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaid])
    payload = (
        b'{"order_id":"o","payment":{"__modulith_union_type__":"'
        + module_name.encode()
        + b'.Pix","value":{"amount":5}}}'
    )

    with caplog.at_level(logging.WARNING, logger="modulith.serializers"):
        first = serializer.deserialize(payload, _fqcn(OrderPaid))
        second = serializer.deserialize(payload, _fqcn(OrderPaid))

    assert first == second == OrderPaid(order_id="o", payment=BasePayment(amount=5))
    assert type(first.payment) is BasePayment
    assert module_name not in sys.modules
    warnings_for_tag = [r for r in caplog.records if f"{module_name}.Pix" in r.getMessage()]
    assert [r.levelno for r in warnings_for_tag] == [logging.WARNING]


def test_unimported_subclass_tag_with_extra_fields_fails_to_decode() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaid])
    payload = (
        b'{"order_id":"o","payment":{"__modulith_union_type__":"elsewhere.CardOnFile",'
        b'"value":{"amount":5,"card":"x"}}}'
    )

    with pytest.raises(TypeError, match="card"):
        serializer.deserialize(payload, _fqcn(OrderPaid))


@pytest.mark.parametrize(
    "payload",
    [
        b'{"alternative":"x","backup":{"__modulith_union_type__":"'
        + __name__.encode()
        + b'.CardPayment","value":{"amount":5,"card_last4":"1","network":"visa"}}}',
        b'{"backup":null,"alternative":{"__modulith_union_type__":"'
        + __name__.encode()
        + b'.CardPayment","value":{"amount":5,"card_last4":"1","network":"visa"}}}',
        b'{"backup":null,"alternative":{"__modulith_union_type__":"elsewhere.Gone",'
        b'"value":{"amount":5,"network":"visa"}}}',
    ],
)
def test_union_field_tagged_value_that_fails_to_reconstruct_raises(payload: bytes) -> None:
    serializer = JsonEventSerializer(allowed_event_types=[OrderPaidUnion])

    with pytest.raises(TypeError, match="network"):
        serializer.deserialize(payload, _fqcn(OrderPaidUnion))


def test_union_field_tag_matching_no_member_raises() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[StringOrIntEvent])
    payload = b'{"value":{"__modulith_union_type__":"elsewhere.Gone","value":"x"}}'

    with pytest.raises(ValueError, match=r"elsewhere\.Gone"):
        serializer.deserialize(payload, _fqcn(StringOrIntEvent))


OrderId = NewType("OrderId", UUID)
CustomerId = NewType("CustomerId", str)
Money = NewType("Money", Decimal)
When = NewType("When", datetime)
Stamp = TypeAliasType("Stamp", datetime)


@dataclass(frozen=True)
class NewTypeEvent:
    order_id: OrderId
    amount: Money
    at: When
    maybe: OrderId | None = None
    ids: list[OrderId] = field(default_factory=list)


@dataclass(frozen=True)
class NewTypeUnionEvent:
    ref: OrderId | CustomerId


@dataclass(frozen=True)
class AliasEvent:
    at: Stamp


def test_newtype_fields_deserialize_as_their_supertype() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[NewTypeEvent])
    oid = OrderId(uuid4())
    original = NewTypeEvent(
        order_id=oid,
        amount=Money(Decimal("9.99")),
        at=When(datetime(2026, 9, 28, tzinfo=UTC)),
        maybe=oid,
        ids=[oid],
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(NewTypeEvent))

    assert restored == original
    assert type(restored.order_id) is UUID
    assert type(restored.amount) is Decimal
    assert type(restored.at) is datetime
    assert type(restored.maybe) is UUID
    assert [type(i) for i in restored.ids] == [UUID]


def test_stored_tagged_union_of_newtypes_still_matches_its_member() -> None:
    """Union tags name NewType members by repr; rows already stored with
    those tags must keep matching, and new rows must keep writing them."""
    serializer = JsonEventSerializer(allowed_event_types=[NewTypeUnionEvent])
    oid = UUID("12345678-1234-5678-1234-567812345678")
    stored = (
        b'{"ref":{"__modulith_union_type__":"' + __name__.encode() + b'.OrderId",'
        b'"value":"12345678-1234-5678-1234-567812345678"}}'
    )

    assert serializer.serialize(NewTypeUnionEvent(ref=OrderId(oid))) == stored
    restored = serializer.deserialize(stored, _fqcn(NewTypeUnionEvent))
    assert restored.ref == oid
    assert type(restored.ref) is UUID


def test_type_alias_field_deserializes_as_its_value() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[AliasEvent])
    original = AliasEvent(at=datetime(2026, 9, 28, 12, tzinfo=UTC))

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(AliasEvent))

    assert restored == original
    assert type(restored.at) is datetime


def _pep695_event(alias_source: str) -> type:
    namespace: dict[str, object] = {"dataclass": dataclass, "__name__": __name__}
    source = f"{alias_source}\n@dataclass\nclass PepAliasEvent:\n    sku: SkuRef\n    maybe: SkuRef | None\n"
    # dont_inherit: this module's postponed annotations would turn the hints
    # into strings that never reach the alias object.
    code = compile(source, "<pep695>", "exec", dont_inherit=True)
    exec(code, namespace)  # PEP 695 syntax does not parse on the 3.11 floor
    return cast(type, namespace["PepAliasEvent"])


@pytest.mark.skipif(sys.version_info < (3, 12), reason="PEP 695 type statements need 3.12+")
@pytest.mark.parametrize(
    "alias_source",
    [
        "type SkuRef = Sku",  # Sku importable only for the type checker
        "type SkuRef = SkuRef",
        "type SkuRef = Other\ntype Other = SkuRef",
    ],
)
@pytest.mark.timeout(10)
def test_unevaluable_or_cyclic_type_alias_field_passes_through(
    alias_source: str, monkeypatch
) -> None:
    event_type = _pep695_event(alias_source)
    monkeypatch.setattr(sys.modules[__name__], "PepAliasEvent", event_type, raising=False)
    serializer = JsonEventSerializer()

    wire = serializer.serialize(event_type(sku="ABC-1", maybe="X"))
    restored = serializer.deserialize(wire, _fqcn(event_type))

    assert wire == b'{"maybe":"X","sku":"ABC-1"}'
    assert (restored.sku, restored.maybe) == ("ABC-1", "X")


@dataclass(frozen=True)
class FixedArityTupleEvent:
    stamped: tuple[datetime, Decimal, UUID]
    keyed: tuple[int, BasePayment]
    nothing: tuple[()]


@dataclass(frozen=True)
class VariadicTupleEvent:
    payments: tuple[BasePayment, ...]
    days: tuple[date, ...]


@dataclass(frozen=True)
class BareContainerEvent:
    plain_tuple: tuple
    plain_set: set
    plain_frozenset: frozenset
    typing_tuple: Tuple  # noqa: UP006
    typing_set: Set  # noqa: UP006


@dataclass(frozen=True)
class WireStableEvent:
    pair: tuple[int, int]
    labelled: tuple[int, str]
    days: tuple[date, ...]
    ids: frozenset[UUID]
    order: list[Decimal]


def test_fixed_arity_tuple_encodes_each_element_with_its_own_hint() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[FixedArityTupleEvent])
    original = FixedArityTupleEvent(
        stamped=(datetime(2026, 1, 1, tzinfo=UTC), Decimal("1.50"), uuid4()),
        keyed=(7, CardPayment(amount=1, card_last4="4242")),
        nothing=(),
    )

    wire = serializer.serialize(original)
    restored = serializer.deserialize(wire, _fqcn(FixedArityTupleEvent))

    assert restored == original
    assert [type(e) for e in restored.stamped] == [datetime, Decimal, UUID]
    assert type(restored.keyed[1]) is CardPayment
    assert (
        b'"keyed":[7,{"__modulith_union_type__":"' + _fqcn(CardPayment).encode() + b'","value":'
    ) in wire


def test_variadic_tuple_keeps_using_its_one_element_hint() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[VariadicTupleEvent])
    original = VariadicTupleEvent(
        payments=(
            BasePayment(amount=1),
            CardPayment(amount=2, card_last4="1"),
            PremiumCardPayment(amount=3, card_last4="2", tier="gold"),
        ),
        days=(date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)),
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(VariadicTupleEvent))

    assert restored == original
    assert [type(p) for p in restored.payments] == [BasePayment, CardPayment, PremiumCardPayment]
    assert all(type(d) is date for d in restored.days)


def test_unparameterized_containers_decode_to_their_declared_type() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[BareContainerEvent])
    original = BareContainerEvent(
        plain_tuple=(1, "a"),
        plain_set={1, 2},
        plain_frozenset=frozenset({3, 4}),
        typing_tuple=(5, "b"),
        typing_set={6, 7},
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(BareContainerEvent))

    assert restored == original
    assert type(restored.plain_tuple) is tuple
    assert type(restored.plain_set) is set
    assert type(restored.plain_frozenset) is frozenset
    assert type(restored.typing_tuple) is tuple
    assert type(restored.typing_set) is set


def test_unparameterized_set_of_tuples_round_trips() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[BareContainerEvent])
    original = BareContainerEvent(
        plain_tuple=(),
        plain_set={(1, 2), (3, 4)},
        plain_frozenset=frozenset({(5, "a")}),
        typing_tuple=(),
        typing_set=set(),
    )

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(BareContainerEvent))

    assert restored == original


def test_container_wire_format_that_round_trips_today_is_unchanged() -> None:
    serializer = JsonEventSerializer(allowed_event_types=[WireStableEvent])
    original = WireStableEvent(
        pair=(1, 2),
        labelled=(3, "x"),
        days=(date(2026, 1, 1),),
        ids=frozenset({UUID(int=1)}),
        order=[Decimal("1.5")],
    )

    wire = serializer.serialize(original)

    assert wire == (
        b'{"days":["2026-01-01"],"ids":["00000000-0000-0000-0000-000000000001"],'
        b'"labelled":[3,"x"],"order":["1.5"],"pair":[1,2]}'
    )
    assert serializer.deserialize(wire, _fqcn(WireStableEvent)) == original


@dataclass
class DerivedTotalEvent:
    order_id: str
    total: Decimal = field(init=False, default=Decimal(0))


@dataclass(frozen=True)
class FrozenDerivedItem:
    label: str
    made_at: datetime = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "made_at", datetime(2000, 1, 1, tzinfo=UTC))


@dataclass(frozen=True)
class DerivedItemEvent:
    item: FrozenDerivedItem
    items: list[FrozenDerivedItem]


@dataclass
class SeededEvent:
    name: str
    seed: InitVar[int]


@dataclass
class SeedWrapperEvent:
    inner: SeededEvent


@dataclass
class DefaultedSeedEvent:
    name: str
    seed: InitVar[int] = 5


def test_init_false_field_is_restored_after_construction() -> None:
    serializer = JsonEventSerializer()
    original = DerivedTotalEvent(order_id="o-1")
    original.total = Decimal("9.50")

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(DerivedTotalEvent))

    assert restored == original
    assert restored.total == Decimal("9.50")
    assert isinstance(restored.total, Decimal)


def test_init_false_field_of_a_frozen_nested_dataclass_is_restored() -> None:
    serializer = JsonEventSerializer()
    first = FrozenDerivedItem(label="a")
    object.__setattr__(first, "made_at", datetime(2026, 3, 4, tzinfo=UTC))
    second = FrozenDerivedItem(label="b")
    object.__setattr__(second, "made_at", datetime(2026, 5, 6, tzinfo=UTC))
    original = DerivedItemEvent(item=first, items=[second])

    restored = serializer.deserialize(serializer.serialize(original), _fqcn(DerivedItemEvent))

    assert restored == original
    assert restored.item.made_at == datetime(2026, 3, 4, tzinfo=UTC)
    assert restored.items[0].made_at == datetime(2026, 5, 6, tzinfo=UTC)


def test_serialize_rejects_an_initvar_without_a_default() -> None:
    with pytest.raises(TypeError, match=r"SeededEvent.*'seed'|'seed'.*SeededEvent"):
        JsonEventSerializer().serialize(SeededEvent("n", seed=1))


def test_serialize_rejects_an_initvar_without_a_default_on_a_nested_dataclass() -> None:
    with pytest.raises(TypeError, match=r"SeededEvent.*'seed'|'seed'.*SeededEvent"):
        JsonEventSerializer().serialize(SeedWrapperEvent(SeededEvent("n", seed=1)))


def test_initvar_with_a_default_round_trips() -> None:
    serializer = JsonEventSerializer()
    original = DefaultedSeedEvent("n", seed=9)

    wire = serializer.serialize(original)

    assert wire == b'{"name":"n"}'
    assert serializer.deserialize(wire, _fqcn(DefaultedSeedEvent)) == DefaultedSeedEvent("n")
