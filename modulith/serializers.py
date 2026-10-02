"""The default JSON event serializer.

modulith's outbox stores events as bytes so any serialization strategy
works — Pydantic, MessagePack, Avro, Protobuf. The framework ships one
default: JSON over the event's dataclass fields. It's human-readable in
the database (Postgres ``JSONB``), dependency-free, and good enough for
the overwhelming majority of applications.

JSON cannot natively represent ``datetime``, ``date``, ``UUID``, or
``Decimal``. We encode those as strings on the way out and coerce them
back to their declared types on the way in, using the event class's own
type annotations as the schema. The round-trip preserves dataclass
equality — the property the outbox retry loop depends on.

Applications that need a different format implement the
:class:`modulith.protocols.EventSerializer` protocol (two methods) and
pass an instance to ``outbox.configure(serializer=...)``.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
import json
import logging
import os
import sys
import types
import typing
import warnings
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Union
from uuid import UUID

from .config import DEFAULT_MAX_PAYLOAD_BYTES, MAX_PAYLOAD_BYTES, ConfigurationError

__all__ = ["JsonEventSerializer"]

logger = logging.getLogger("modulith.serializers")

_UNION_TAG = "__modulith_union_type__"


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert an event's value graph to JSON-encodable values.

    ``datetime``/``date`` → ISO 8601 string, ``UUID`` → str, ``Decimal``
    → str (string, not float, so precision survives), ``Enum`` → its
    value, nested dataclasses → dicts of their fields, ``set``/
    ``frozenset`` → list (decode coerces back). Dict *keys* are encoded
    here too, via :func:`_encode_dict_key` — ``json.dumps`` never routes
    keys through its ``default=`` hook, so a ``dict[UUID, X]`` field used
    to crash with the json module's own opaque TypeError instead of being
    handled. Anything unsupported raises ``TypeError`` — better a loud
    failure at publish time than silent data corruption in the outbox.
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (datetime, date)):  # datetime is a subclass of date
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        return _to_jsonable(obj.value)
    # Nested dataclass: descend into its fields so a field typed as another
    # @dataclass event/value object round-trips instead of raising TypeError.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: _to_jsonable(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {_encode_dict_key(k): _to_jsonable(v) for k, v in obj.items()}
    raise TypeError(f"cannot JSON-serialize {type(obj).__name__} in event payload")


def _encode_dict_key(key: Any) -> Any:
    """Encode a dict key to something ``json.dumps`` accepts natively.

    JSON-native key types pass through (``json.dumps`` stringifies them
    itself — including ``True``/``False`` → ``"true"``/``"false"``, which
    ``_coerce`` reverses for ``dict[bool, X]`` hints). The rich key types
    ``_coerce`` knows how to decode (datetime/date/UUID/Decimal/Enum) are
    stringified the same way as values. Anything else fails loudly with a
    serializer error naming the offending key type, instead of the json
    module's generic "keys must be str, int, float, bool or None".
    """
    if key is None or isinstance(key, (bool, int, float, str)):
        return key
    if isinstance(key, (datetime, date)):
        return key.isoformat()
    if isinstance(key, UUID):
        return str(key)
    if isinstance(key, Decimal):
        return str(key)
    if isinstance(key, Enum):
        return _encode_dict_key(key.value)
    raise TypeError(f"cannot JSON-serialize dict key of type {type(key).__name__} in event payload")


def _hint_tag(hint: Any) -> str:
    """Return a stable wire tag for a union member annotation."""
    if isinstance(hint, type):
        return f"{hint.__module__}.{hint.__qualname__}"
    return repr(hint)


def _unwrap_alias(hint: Any) -> Any:
    """Resolve ``NewType`` and ``type X = ...`` aliases to the type they name.

    Callers apply this to the hint itself, never to union members before
    ``_hint_tag``: stored union tags name a ``NewType`` member by its repr.
    An alias whose lazy value cannot be evaluated (a ``TYPE_CHECKING``-only
    name) or that cycles back to itself stays opaque, so its field is not
    coerced.
    """
    seen: set[int] = set()
    while id(hint) not in seen:
        seen.add(id(hint))
        if hasattr(hint, "__supertype__"):
            hint = hint.__supertype__
        elif type(hint).__name__ == "TypeAliasType":  # typing's or typing_extensions'
            try:
                hint = hint.__value__
            except Exception:
                return hint
        else:
            return hint
    return hint


def _is_tagged(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {_UNION_TAG, "value"}
        and isinstance(value[_UNION_TAG], str)
    )


def _subclass_for_tag(base: type, tag: str) -> type | None:
    """Find the subclass of ``base`` whose ``_hint_tag`` is ``tag``.

    The tag comes from an untrusted payload, and only the top-level event
    type is checked against ``allowed_event_types``. So the tag is matched
    against ``base``'s already-imported subclass tree and never imported.
    """
    pending: list[type] = list(base.__subclasses__())
    while pending:
        candidate = pending.pop()
        if _hint_tag(candidate) == tag:
            return candidate
        pending.extend(candidate.__subclasses__())
    return None


@functools.lru_cache(maxsize=1024)
def _warn_unknown_subclass_tag(tag: str, base_tag: str) -> None:
    """Log once per tag (while cached) that a value decoded as its base class."""
    logger.warning(
        "nested type tag %r names no imported subclass of %s; decoded as %s. "
        "Import the module defining the subclass in this process to keep its type.",
        tag,
        base_tag,
        base_tag,
    )


def _to_jsonable_typed(obj: Any, hint: Any) -> Any:
    """Encode values using their annotations where JSON loses type identity.

    Union members and subclass instances in a field declared as their base
    dataclass are wrapped as ``{_UNION_TAG: <tag>, "value": ...}``.
    """
    hint = _unwrap_alias(hint)
    origin = typing.get_origin(hint)
    if origin is Union or origin is types.UnionType:
        members = [member for member in typing.get_args(hint) if member is not type(None)]
        if len(members) > 1:
            member = next(
                (
                    candidate
                    for candidate in members
                    if isinstance(candidate, type) and type(obj) is candidate
                ),
                None,
            ) or next(
                (
                    candidate
                    for candidate in members
                    if dataclasses.is_dataclass(candidate)
                    and isinstance(candidate, type)
                    and isinstance(obj, candidate)
                ),
                members[0],
            )
            return {
                _UNION_TAG: _hint_tag(member),
                "value": _to_jsonable_typed(obj, member),
            }
        if members:
            return _to_jsonable_typed(obj, members[0])
    if origin in (list, set, frozenset, tuple):
        args = typing.get_args(hint)
        item_hint = args[0] if args else None
        if isinstance(obj, (list, tuple, set, frozenset)):
            return [_to_jsonable_typed(value, item_hint) for value in obj]
    if origin is dict:
        args = typing.get_args(hint)
        if len(args) == 2 and isinstance(obj, dict):
            return {
                _encode_dict_key(key): _to_jsonable_typed(value, args[1])
                for key, value in obj.items()
            }
    if isinstance(hint, type) and isinstance(obj, dict):
        hints = _safe_type_hints(hint)
        return {key: _to_jsonable_typed(value, hints.get(key)) for key, value in obj.items()}
    if (
        dataclasses.is_dataclass(hint)
        and isinstance(hint, type)
        and type(obj) is not hint
        and isinstance(obj, hint)
    ):
        return {_UNION_TAG: _hint_tag(type(obj)), "value": _to_jsonable_typed(obj, type(obj))}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        hints = _safe_type_hints(type(obj))
        return {
            field.name: _to_jsonable_typed(getattr(obj, field.name), hints.get(field.name))
            for field in dataclasses.fields(obj)
        }
    return _to_jsonable(obj)


def _resolve_max_payload_bytes(broker_options: dict[str, Any] | None) -> int:
    """Resolve a consume-side payload cap with the same precedence every
    broker adapter uses for its own write-side cap (``MODULITH_BROKER_
    MAX_PAYLOAD_BYTES`` env var, else ``broker_options["max_payload_bytes"]``,
    else ``DEFAULT_MAX_PAYLOAD_BYTES``) — see db_broker.py's ``_broker_opt``/
    ``_opt_int``, redis_broker.py's env-or-opts chain, and shm_broker.py's own.
    A consumer resolving the cap any other way can dead-letter a payload the
    broker it reads from already accepted.

    Also enforces the same ``1..MAX_PAYLOAD_BYTES`` range every broker adapter
    validates its own cap against (db_broker.py's ``_positive_int`` plus its
    ``> MAX_PAYLOAD_BYTES`` check, redis_broker.py's range check,
    shm_broker.py's ``_bounded_positive_int``) — an unbounded resolver could
    hand a consumer a cap no broker adapter would ever accept for itself.
    """
    opts = broker_options or {}
    value = os.environ.get("MODULITH_BROKER_MAX_PAYLOAD_BYTES") or opts.get("max_payload_bytes")
    if value is None:
        return DEFAULT_MAX_PAYLOAD_BYTES
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"max_payload_bytes must be an integer, got {value!r}") from exc
    if not 1 <= resolved <= MAX_PAYLOAD_BYTES:
        raise ConfigurationError(
            f"max_payload_bytes must be an integer from 1 to {MAX_PAYLOAD_BYTES}, got {value!r}"
        )
    return resolved


def _resolve_class(fqcn: str) -> type:
    """Resolve a fully-qualified class name to the class object.

    ``fqcn`` is ``f"{cls.__module__}.{cls.__qualname__}"`` — the exact
    string the outbox stores as ``event_type``. For a top-level class the
    qualname is just the class name; for a nested class it contains dots
    (``Outer.Inner``). We can't assume the last dot separates module from
    class, so we walk decreasing module prefixes until one imports, then
    ``getattr`` down the remaining attribute path.
    """
    parts = fqcn.split(".")
    for split in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:split])
        attr_path = parts[split:]
        try:
            obj: Any = importlib.import_module(module_name)
        except ImportError:
            continue
        try:
            for attr in attr_path:
                obj = getattr(obj, attr)
        except AttributeError:
            continue
        return obj  # type: ignore[no-any-return]
    raise ImportError(f"could not resolve event type {fqcn!r}")


def _event_type_name(event_type: str | type) -> str:
    """Normalize a class or fully-qualified name to the stored event_type."""
    if isinstance(event_type, str):
        return event_type
    return f"{event_type.__module__}.{event_type.__qualname__}"


def _safe_type_hints(obj: Any) -> dict[str, Any]:
    """Resolve type hints without one bad annotation blocking all the rest.

    ``typing.get_type_hints`` resolves EVERY annotation eagerly, so a single
    ``TYPE_CHECKING``-only forward reference (a name importable only for the
    type checker) raised ``NameError`` and blocked reconstruction of the whole
    event — even when the offending field's value needed no coercion. When
    the eager pass fails, fall back to resolving each annotation on its own
    (the same eval-against-module-globals mechanism ``get_type_hints`` uses);
    names that don't resolve simply yield no coercion for that field.
    """
    try:
        resolved = typing.get_type_hints(obj)
    except Exception:
        resolved = None
    if resolved is not None:
        return {name: hint for name, hint in resolved.items() if name != "return"}

    if isinstance(obj, type):
        sources = [
            (
                dict(vars(klass).get("__annotations__", {}) or {}),
                getattr(sys.modules.get(klass.__module__), "__dict__", {}),
            )
            for klass in reversed(obj.__mro__)
        ]
    else:  # a function, e.g. cls.__init__
        sources = [
            (
                dict(getattr(obj, "__annotations__", {}) or {}),
                getattr(obj, "__globals__", {}),
            )
        ]

    hints: dict[str, Any] = {}
    for raw_annotations, globalns in sources:
        for name, annotation in raw_annotations.items():
            if name == "return":
                continue
            if not isinstance(annotation, str):
                hints[name] = annotation
                continue
            try:
                hints[name] = eval(annotation, dict(globalns))
            except Exception:
                hints[name] = None  # unresolvable → leave this field uncoerced
    return hints


def _coerce(value: Any, hint: Any) -> Any:
    """Coerce a JSON-decoded value back to its declared field type.

    Driven by the event class's annotations: a field typed ``Decimal``
    parses the stored string back into a ``Decimal``, a ``datetime`` field
    parses the ISO string, etc. ``Optional``/union types unwrap to their
    first non-``None`` member. Unknown or plain types pass through.
    """
    if value is None or hint is None:
        return value

    hint = _unwrap_alias(hint)
    origin = typing.get_origin(hint)
    if origin is Union or origin is types.UnionType:
        members = [a for a in typing.get_args(hint) if a is not type(None)]
        if _is_tagged(value):
            tag = value[_UNION_TAG]
            tagged_member = next((member for member in members if _hint_tag(member) == tag), None)
            if tagged_member is not None:
                return _coerce(value["value"], tagged_member)
            # A subclass of a dataclass member: that member's branch reconstructs
            # it and any failure propagates, never a raw tagged dict.
            bases = [m for m in members if dataclasses.is_dataclass(m) and isinstance(m, type)]
            owner = next((b for b in bases if _subclass_for_tag(b, tag) is not None), None)
            if owner is None and len(bases) == 1:
                owner = bases[0]
            if owner is None:
                raise ValueError(f"union type tag {tag!r} names no member of {hint!r}")
            return _coerce(value, owner)
        for member in members:
            try:
                return _coerce(value, member)
            except (ValueError, TypeError):
                continue
        return value

    # Parameterized containers: recurse into element/value types so rich inner
    # types survive the round-trip (list[datetime], dict[str, Decimal],
    # set[UUID], tuple[date, ...]). Without this, decode left inner elements as
    # the raw JSON strings, silently breaking equality.
    if origin is list:
        args = typing.get_args(hint)
        return [_coerce(v, args[0]) for v in value] if args and isinstance(value, list) else value
    if origin in (set, frozenset):
        args = typing.get_args(hint)
        if args and isinstance(value, list):
            return origin(_coerce(v, args[0]) for v in value)
        return value
    if origin is tuple:
        args = typing.get_args(hint)
        if args and isinstance(value, list):
            if len(args) == 2 and args[1] is Ellipsis:
                return tuple(_coerce(v, args[0]) for v in value)
            return tuple(_coerce(v, a) for v, a in zip(value, args, strict=False))
        return value
    if origin is dict:
        args = typing.get_args(hint)
        if len(args) == 2 and isinstance(value, dict):
            return {_coerce(k, args[0]): _coerce(v, args[1]) for k, v in value.items()}
        return value

    if hint is datetime:
        return datetime.fromisoformat(value)
    if hint is date:
        return date.fromisoformat(value)
    if hint is UUID:
        return UUID(value)
    if hint is Decimal:
        return Decimal(value)
    if hint is bool:
        # bool VALUES round-trip natively, but json.dumps stringifies bool
        # dict KEYS to "true"/"false" without consulting the default hook —
        # so dict[bool, ...] keys need decoding back, like int keys below.
        if isinstance(value, bool):
            return value
        if value == "true":
            return True
        if value == "false":
            return False
        return value
    if hint is int and not isinstance(value, bool):
        # JSON round-trips int values natively, but dict keys decode as strings
        # (json.dumps stringifies non-string keys), so dict[int, ...] needs this.
        return int(value)
    if hint is float:
        return float(value)
    if isinstance(hint, type) and issubclass(hint, Enum):
        if isinstance(value, str):
            values = [member.value for member in hint]
            if values and type(values[0]) is int:
                value = int(value)
        return hint(value)
    # Nested dataclass field: reconstruct recursively from the decoded dict.
    if dataclasses.is_dataclass(hint) and isinstance(hint, type) and isinstance(value, dict):
        if _is_tagged(value):
            tag = value[_UNION_TAG]
            subclass = _subclass_for_tag(hint, tag)
            if subclass is not None:
                return _coerce(value["value"], subclass)
            decoded = _coerce(value["value"], hint)
            _warn_unknown_subclass_tag(tag, _hint_tag(hint))
            return decoded
        sub_hints = _safe_type_hints(hint)
        return hint(**{k: _coerce(v, sub_hints.get(k)) for k, v in value.items()})
    return value


def _instance_attrs(event: Any) -> dict[str, Any]:
    """Read a non-dataclass event's instance attributes.

    Prefers ``__dict__`` (the documented ``vars(event)`` fallback), but also
    supports ``__slots__``-based classes — a bare ``vars()`` call crashed on
    those with the raw "vars() argument must have __dict__ attribute"
    TypeError even though slotted events serialize perfectly well.
    """
    attrs = getattr(event, "__dict__", None)
    if attrs is not None:
        return dict(attrs)
    names: list[str] = []
    for klass in type(event).__mro__:
        slots = vars(klass).get("__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)
        for name in slots:
            if name not in ("__dict__", "__weakref__") and name not in names:
                names.append(name)
    if not names:
        raise TypeError(
            f"cannot serialize {type(event).__name__}: it is not a dataclass and "
            "exposes no __dict__ or __slots__ instance attributes"
        )
    return {name: getattr(event, name) for name in names if hasattr(event, name)}


class JsonEventSerializer:
    """JSON serializer over an event's dataclass fields.

    Conforms structurally to :class:`modulith.protocols.EventSerializer`
    (duck-typed; no inheritance required).

    ``allowed_event_types`` is a deserialization allowlist: when given, only
    the listed event types (classes or fully-qualified names) may be
    reconstructed — any other ``event_type`` raises ``ValueError`` before the
    class is resolved. Set it in production whenever payloads can originate
    outside the trusted process boundary (a shared outbox table, a broker):
    ``deserialize`` imports the module named in ``event_type``, so without an
    allowlist a forged record can trigger arbitrary-module import and
    instantiation.

    Omitting it stays legal because pure *encoding* has no attack surface —
    ``serialize`` never resolves a class, and the framework's own encode-only
    instances (the outbox wire serializer, the direct-publish path) would
    otherwise have to invent an allowlist they never consult. The cost of that
    is that the first ``deserialize`` on an unrestricted instance is announced
    twice: a ``RuntimeWarning`` and a ``modulith.serializers`` log record, so
    the fail-open configuration surfaces both to a developer running with
    default warning filters and to a deployment that captures logs but not
    warnings.

    ``max_payload_bytes`` re-checks the same cap every broker adapter's
    ``publish()`` already enforces (default 16 MiB — ``config.py``'s
    ``DEFAULT_MAX_PAYLOAD_BYTES``). That cap is a write-side guard only:
    ``deserialize`` is the sole place bytes from a broker/outbox row become a
    Python object, and it is the last chokepoint every consume path shares —
    without a check here, a row larger than the configured cap (written by
    another process, another host, or a legitimately large configuration) is
    parsed in full, however large it is.

    Passing an explicit ``max_payload_bytes`` fixes the cap for this instance.
    Leaving it ``None`` (the default) defers resolution to the first
    ``deserialize`` call, via the same ``_resolve_max_payload_bytes`` env/
    ``broker_options``/default precedence the broker adapters use, read from
    whatever ``Configuration`` is loaded at that moment — never at
    construction time, since module-level instances (the outbox wire
    serializer, an application's own default-constructed serializer) are
    built before any configuration is loaded. The resolved value is cached on
    the instance after the first call.
    """

    def __init__(
        self,
        *,
        allowed_event_types: Iterable[str | type] | None = None,
        max_payload_bytes: int | None = None,
    ) -> None:
        self._allowed_event_types = (
            None
            if allowed_event_types is None
            else {_event_type_name(event_type) for event_type in allowed_event_types}
        )
        self._unrestricted_use_announced = False
        self._max_payload_bytes: int | None = max_payload_bytes

    def serialize(self, event: Any) -> bytes:
        """Encode an event instance to JSON bytes.

        Reads dataclass fields when the event is a dataclass (the
        documented ``@event @dataclass`` pattern), falling back to the
        instance attributes (``__dict__`` or ``__slots__``) otherwise.
        Keys are sorted for stable, diffable output in the outbox table.
        """
        if dataclasses.is_dataclass(event) and not isinstance(event, type):
            raw = {f.name: getattr(event, f.name) for f in dataclasses.fields(event)}
        else:
            raw = _instance_attrs(event)
        return json.dumps(
            _to_jsonable_typed(raw, type(event)),
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def deserialize(self, data: bytes, event_type: str) -> Any:
        """Decode JSON bytes back into an event instance.

        Resolves the class from ``event_type`` (the fully-qualified name),
        then reconstructs it, coercing each field back to its annotated
        type so a round-trip is equality-preserving. Non-dataclass events
        are coerced too, using the class-level annotations merged with
        ``__init__``'s parameter annotations — previously this path
        silently left datetime/UUID/Decimal fields as raw strings.

        ``data`` is untrusted at this boundary (see the class docstring's
        ``max_payload_bytes``): its size is checked before ``decode``/
        ``json.loads`` ever run, so an oversized broker/outbox row is
        rejected instead of allocated and parsed in full.
        """
        if self._max_payload_bytes is None:
            from .runtime import _runtime

            cfg = _runtime.config
            self._max_payload_bytes = _resolve_max_payload_bytes(
                cfg.broker_options if cfg is not None else None
            )
        if len(data) > self._max_payload_bytes:
            raise ConfigurationError(
                f"deserialize payload is {len(data)} bytes, exceeding "
                f"max_payload_bytes={self._max_payload_bytes}. Refusing to "
                "decode a payload larger than the configured cap."
            )
        if self._allowed_event_types is not None and event_type not in self._allowed_event_types:
            raise ValueError(f"event type {event_type!r} is not in the allowed event types")
        if self._allowed_event_types is None and not self._unrestricted_use_announced:
            # No allowlist: about to import-resolve an arbitrary class named
            # by the wire event_type (see the class docstring). Announced on
            # both channels because neither alone reaches everyone: a
            # RuntimeWarning is what a developer sees under default filters
            # but is silenced wholesale by PYTHONWARNINGS/-W ignore, while a
            # log record is what a deployment's log pipeline actually
            # captures. The flag bounds each to once per instance, so a hot
            # dispatch loop can't turn either into a flood.
            self._unrestricted_use_announced = True
            message = (
                "JsonEventSerializer with no allowed_event_types resolves an "
                "arbitrary importable class from the wire event_type. Pass "
                "allowed_event_types=[...] whenever payloads can originate "
                "outside this process (a shared outbox table, a broker)."
            )
            warnings.warn(message, RuntimeWarning, stacklevel=2)
            logger.warning("%s", message)
        cls = _resolve_class(event_type)
        raw = json.loads(data.decode("utf-8"))
        hints = _safe_type_hints(cls)
        if not dataclasses.is_dataclass(cls):
            # cls is a plain `type` here, so __init__ access is sound; mypy's
            # instance-__init__ caveat doesn't apply to resolving annotations.
            init = cls.__init__  # type: ignore[misc]
            for key, hint in _safe_type_hints(init).items():
                if hint is not None:
                    hints[key] = hint
        kwargs = {key: _coerce(val, hints.get(key)) for key, val in raw.items()}
        return cls(**kwargs)
