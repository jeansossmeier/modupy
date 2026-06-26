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
import importlib
import json
import types
import typing
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Union
from uuid import UUID

__all__ = ["JsonEventSerializer"]


def _json_default(obj: Any) -> Any:
    """Encode types JSON doesn't handle natively.

    ``datetime``/``date`` → ISO 8601 string, ``UUID`` → str, ``Decimal``
    → str (string, not float, so precision survives), ``Enum`` → its
    value. Anything else raises ``TypeError`` — better a loud failure at
    publish time than silent data corruption in the outbox.
    """
    if isinstance(obj, (datetime, date)):  # datetime is a subclass of date
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    # Nested dataclass: descend into its fields so a field typed as another
    # @dataclass event/value object round-trips instead of raising TypeError.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    # set/frozenset aren't JSON-native; encode as a list (decode coerces back).
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    raise TypeError(f"cannot JSON-serialize {type(obj).__name__} in event payload")


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


def _coerce(value: Any, hint: Any) -> Any:
    """Coerce a JSON-decoded value back to its declared field type.

    Driven by the event class's annotations: a field typed ``Decimal``
    parses the stored string back into a ``Decimal``, a ``datetime`` field
    parses the ISO string, etc. ``Optional``/union types unwrap to their
    first non-``None`` member. Unknown or plain types pass through.
    """
    if value is None or hint is None:
        return value

    origin = typing.get_origin(hint)
    if origin is Union or origin is types.UnionType:
        members = [a for a in typing.get_args(hint) if a is not type(None)]
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
    if hint is int and not isinstance(value, bool):
        # JSON round-trips int values natively, but dict keys decode as strings
        # (json.dumps stringifies non-string keys), so dict[int, ...] needs this.
        return int(value)
    if hint is float:
        return float(value)
    if isinstance(hint, type) and issubclass(hint, Enum):
        return hint(value)
    # Nested dataclass field: reconstruct recursively from the decoded dict.
    if dataclasses.is_dataclass(hint) and isinstance(hint, type) and isinstance(value, dict):
        sub_hints = typing.get_type_hints(hint)
        return hint(**{k: _coerce(v, sub_hints.get(k)) for k, v in value.items()})
    return value


class JsonEventSerializer:
    """JSON serializer over an event's dataclass fields.

    Conforms structurally to :class:`modulith.protocols.EventSerializer`
    (duck-typed; no inheritance required).
    """

    def serialize(self, event: Any) -> bytes:
        """Encode an event instance to JSON bytes.

        Reads dataclass fields when the event is a dataclass (the
        documented ``@event @dataclass`` pattern), falling back to
        ``vars(event)`` otherwise. Keys are sorted for stable, diffable
        output in the outbox table.
        """
        if dataclasses.is_dataclass(event) and not isinstance(event, type):
            raw = {f.name: getattr(event, f.name) for f in dataclasses.fields(event)}
        else:
            raw = dict(vars(event))
        return json.dumps(
            raw,
            default=_json_default,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def deserialize(self, data: bytes, event_type: str) -> Any:
        """Decode JSON bytes back into an event instance.

        Resolves the class from ``event_type`` (the fully-qualified name),
        then reconstructs it, coercing each field back to its annotated
        type so a round-trip is equality-preserving.
        """
        cls = _resolve_class(event_type)
        raw = json.loads(data.decode("utf-8"))
        if dataclasses.is_dataclass(cls):
            hints = typing.get_type_hints(cls)
            kwargs = {key: _coerce(val, hints.get(key)) for key, val in raw.items()}
            return cls(**kwargs)
        return cls(**raw)
