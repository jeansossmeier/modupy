"""Regression tests for the versioned JSON serializer example.

Covers the example serializer in examples/versioned_json_serializer.py:
serialization round trip, malformed-input error handling, and explicit outbox
configuration with the custom serializer.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from modulith import event, publish
from modulith.adapters.postgres_outbox import (
    Base,
    EventPublicationRow,
    PostgresPublicationStore,
)
from modulith.builtin import outbox
from modulith.builtin.outbox import bind_session, unbind_session
from modulith.runtime import _runtime


def _load_example_serializer():
    """Import examples/versioned_json_serializer.py from disk (not on sys.path)."""
    spec = importlib.util.spec_from_file_location(
        "example_versioned_json_serializer",
        Path(__file__).resolve().parent.parent / "examples" / "versioned_json_serializer.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@event
@dataclass(frozen=True)
class OrderPlacedEvent:
    """Example event type for serializer validation."""

    order_id: str
    customer_id: str
    total: float


def test_round_trips_an_event() -> None:
    """The serializer preserves events through serialize/deserialize."""
    example = _load_example_serializer()

    serializer = example.VersionedJsonSerializer(allowed_event_types=[OrderPlacedEvent])
    evt = OrderPlacedEvent(order_id="o-1", customer_id="c-1", total=9.99)

    data = serializer.serialize(evt)
    envelope = json.loads(data)
    assert envelope == {"v": 1, "body": {"customer_id": "c-1", "order_id": "o-1", "total": 9.99}}

    event_type = f"{OrderPlacedEvent.__module__}.{OrderPlacedEvent.__qualname__}"
    restored = serializer.deserialize(data, event_type)
    assert restored == evt


def test_deserialize_rejects_undecodable_bytes() -> None:
    """The serializer normalizes JSON decode errors to ValueError."""
    example = _load_example_serializer()

    serializer = example.VersionedJsonSerializer()

    with pytest.raises(ValueError, match="not valid JSON") as exc_info:
        serializer.deserialize(b"{not json", "shop.contracts.events.OrderPlaced")

    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


def test_deserialize_rejects_a_non_dict_envelope() -> None:
    """The serializer rejects envelopes that are not JSON objects."""
    example = _load_example_serializer()

    serializer = example.VersionedJsonSerializer()

    with pytest.raises(ValueError, match="expected a JSON object envelope"):
        serializer.deserialize(b'"hello"', "shop.contracts.events.OrderPlaced")


def test_deserialize_rejects_an_unsupported_version() -> None:
    """The serializer rejects envelopes with unsupported version tags."""
    example = _load_example_serializer()

    serializer = example.VersionedJsonSerializer()
    data = json.dumps({"v": 99, "body": {}}).encode("utf-8")

    with pytest.raises(ValueError, match="unsupported envelope version"):
        serializer.deserialize(data, "shop.contracts.events.OrderPlaced")


def test_deserialize_rejects_a_missing_body_key() -> None:
    """The serializer rejects envelopes missing the body."""
    example = _load_example_serializer()

    serializer = example.VersionedJsonSerializer()
    data = json.dumps({"v": 1}).encode("utf-8")

    with pytest.raises(ValueError, match='missing the "body" key'):
        serializer.deserialize(data, "shop.contracts.events.OrderPlaced")


@pytest.mark.asyncio
async def test_outbox_configure_stores_versioned_envelope(tmp_path: Path) -> None:
    """Explicit outbox.configure() with VersionedJsonSerializer stores the envelope.

    Demonstrates the documented pattern for using a custom outbox storage
    serializer. The stored envelope wraps the event body with a version tag.
    """
    _runtime._reset_for_testing()
    outbox._reset_for_testing()

    example = _load_example_serializer()

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}",
        poolclass=NullPool,
    )
    store = PostgresPublicationStore(engine=engine)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        serializer = example.VersionedJsonSerializer(allowed_event_types=[OrderPlacedEvent])
        outbox.configure(store, serializer, start_loop=False)
        _runtime.configure(package="test_example", auto_discover=False)
        _runtime.ensure_bootstrapped()

        async def _listener(evt: OrderPlacedEvent) -> None:
            """A listener makes the publish enlist an outbox row."""

        assert _runtime.event_bus is not None
        _runtime.event_bus.register(OrderPlacedEvent, _listener)

        sessionmaker = async_sessionmaker(engine)
        async with sessionmaker() as session:
            token = bind_session(session)
            try:
                await publish(OrderPlacedEvent(order_id="o-env", customer_id="c-env", total=2.5))
                await session.commit()
            finally:
                unbind_session(token)

        async with sessionmaker() as session:
            rows = (await session.execute(select(EventPublicationRow))).scalars().all()

        assert len(rows) == 1
        assert json.loads(rows[0].payload) == {
            "v": 1,
            "body": {"customer_id": "c-env", "order_id": "o-env", "total": 2.5},
        }
    finally:
        await store.dispose()
        await engine.dispose()
        _runtime._reset_for_testing()
        outbox._reset_for_testing()
