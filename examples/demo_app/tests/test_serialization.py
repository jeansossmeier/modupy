"""Unit tests for ``shop.serialization.VersionedJsonSerializer``.

Covers the round trip plus every malformed-input case normalized to a clean
``ValueError`` (undecodable bytes, a non-dict envelope, an unsupported
version, and a missing ``"body"`` key) — the demo test-suite half of the
error-normalization fix in ``shop/serialization.py``.
"""

from __future__ import annotations

import json

import pytest


def test_round_trips_an_event() -> None:
    from shop.contracts.events import OrderPlaced
    from shop.serialization import VersionedJsonSerializer

    serializer = VersionedJsonSerializer()
    evt = OrderPlaced(order_id="o-1", customer_id="c-1", total=9.99)

    data = serializer.serialize(evt)
    envelope = json.loads(data)
    assert envelope == {"v": 1, "body": {"customer_id": "c-1", "order_id": "o-1", "total": 9.99}}

    restored = serializer.deserialize(data, "shop.contracts.events.OrderPlaced")
    assert restored == evt


def test_deserialize_rejects_undecodable_bytes() -> None:
    from shop.serialization import VersionedJsonSerializer

    serializer = VersionedJsonSerializer()

    with pytest.raises(ValueError, match="not valid JSON") as exc_info:
        serializer.deserialize(b"{not json", "shop.contracts.events.OrderPlaced")

    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


def test_deserialize_rejects_a_non_dict_envelope() -> None:
    from shop.serialization import VersionedJsonSerializer

    serializer = VersionedJsonSerializer()

    with pytest.raises(ValueError, match="expected a JSON object envelope"):
        serializer.deserialize(b'"hello"', "shop.contracts.events.OrderPlaced")


def test_deserialize_rejects_an_unsupported_version() -> None:
    from shop.serialization import VersionedJsonSerializer

    serializer = VersionedJsonSerializer()
    data = json.dumps({"v": 99, "body": {}}).encode("utf-8")

    with pytest.raises(ValueError, match="unsupported envelope version"):
        serializer.deserialize(data, "shop.contracts.events.OrderPlaced")


def test_deserialize_rejects_a_missing_body_key() -> None:
    from shop.serialization import VersionedJsonSerializer

    serializer = VersionedJsonSerializer()
    data = json.dumps({"v": 1}).encode("utf-8")

    with pytest.raises(ValueError, match='missing the "body" key'):
        serializer.deserialize(data, "shop.contracts.events.OrderPlaced")
