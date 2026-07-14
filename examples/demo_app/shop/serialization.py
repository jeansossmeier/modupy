"""A custom outbox-storage serializer, wired in when ``MODULITH_DEMO_SERIALIZER=custom``.

Demonstrates the pluggable outbox STORAGE serializer (``outbox.configure(store,
serializer)``). This affects outbox storage only — the broker wire format is
fixed JSON in v1 (``modulith.serializers.JsonEventSerializer``, used
internally by the runtime for cross-process transport regardless of which
serializer is configured here).

Duck-typed to :class:`modulith.protocols.EventSerializer`: two methods,
``serialize(event) -> bytes`` and ``deserialize(data: bytes, event_type: str)
-> Any``. No inheritance required.
"""

from __future__ import annotations

import json
from typing import Any

from modulith.serializers import JsonEventSerializer

_ENVELOPE_VERSION = 1


class VersionedJsonSerializer:
    """Wraps :class:`JsonEventSerializer` in a ``{"v": 1, "body": ...}`` envelope.

    Purely illustrative: a real custom serializer might swap in Avro,
    Protobuf, or MessagePack. This one keeps JSON but shows how a version tag
    and envelope shape can travel alongside the event body in outbox storage,
    independent of the broker's fixed wire format.
    """

    def __init__(self) -> None:
        self._inner = JsonEventSerializer()

    def serialize(self, event: Any) -> bytes:
        """Encode an event to a versioned JSON envelope."""
        body = json.loads(self._inner.serialize(event))
        envelope = {"v": _ENVELOPE_VERSION, "body": body}
        return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def deserialize(self, data: bytes, event_type: str) -> Any:
        """Decode a versioned JSON envelope back to an event instance.

        Raises ``ValueError`` with an actionable message for every malformed
        input — undecodable bytes, a non-dict envelope, an unsupported
        version, or a missing ``"body"`` key — chaining the original error
        where there is one. Without this, a corrupted row would surface as a
        raw ``JSONDecodeError``/``AttributeError``/``KeyError``: the outbox's
        dispatch path blanket-catches exceptions for retry/dead-lettering
        regardless, but the traceback should still say what's actually wrong.
        """
        try:
            envelope = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"corrupt outbox payload: not valid JSON ({exc})") from exc
        if not isinstance(envelope, dict):
            raise ValueError(
                "corrupt outbox payload: expected a JSON object envelope, got "
                f"{type(envelope).__name__}"
            )
        if envelope.get("v") != _ENVELOPE_VERSION:
            raise ValueError(f"unsupported envelope version: {envelope.get('v')!r}")
        if "body" not in envelope:
            raise ValueError('corrupt outbox payload: envelope is missing the "body" key')
        body = json.dumps(envelope["body"], separators=(",", ":"), sort_keys=True).encode("utf-8")
        return self._inner.deserialize(body, event_type)


__all__ = ["VersionedJsonSerializer"]
