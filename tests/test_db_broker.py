"""Tests for the database-backed broker adapter (Increment 2).

Runs against a real async SQLAlchemy engine on aiosqlite (tmp-file DB +
NullPool — one connection PER session; deliberately NOT StaticPool +
``:memory:``, which shares a single connection across sessions and lets one
session's close() clobber another's in-flight work, see
tests/test_postgres_adapter.py's ``engine`` fixture docstring for the
deterministic repro this avoids).

Covered:
  * publish() fans out one row per SUBSCRIBED consumer group only; zero
    subscribers -> zero rows.
  * subscription upsert is idempotent (re-start doesn't duplicate rows).
  * claim -> dispatch -> ack removes/marks the row; the event reaches the bus.
  * a claimed row is not re-delivered to a second claim in the same group.
  * poison messages (bad payload / missing event_type) -> dead, not retried.
  * dispatch failure -> attempts increment, stays pending, then dead after cap.
  * stop() before start() and double stop() don't raise.
  * DatabaseBroker/DatabaseConsumer satisfy the Broker/Consumer protocols.
  * the consumer factory wires the broker registered for the scheme (handoff).
  * the SKIP LOCKED dialect gate selects Postgres/MySQL only.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from modulith import Broker, BrokerRegistry, Consumer, ConsumerRegistry, ConsumerSpec, event
from modulith.adapters.db_broker import (
    DatabaseBroker,
    DatabaseConsumer,
    _make_db_consumer,
    _supports_skip_locked,
    broker_schema,
)
from modulith.event_bus import InMemoryEventBus
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class WidgetCreated:
    name: str


@pytest.fixture(autouse=True)
def _reset() -> Any:
    """Isolate the runtime singleton around every test (bootstrap tests use it)."""
    _runtime._reset_for_testing()
    yield
    _runtime._reset_for_testing()


@pytest.fixture
async def engine(tmp_path: Path) -> Any:
    """A file-backed aiosqlite engine with one connection PER session
    (NullPool) — see module docstring for why not StaticPool + ``:memory:``."""
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'broker.db'}",
        poolclass=NullPool,
    )
    yield eng
    await eng.dispose()


async def _row_count(engine: Any, *, table: Any = None) -> int:
    from sqlalchemy import func, select

    _, _, message = broker_schema()
    table = table if table is not None else message
    async with engine.connect() as conn:
        result = await conn.execute(select(func.count()).select_from(table))
        return int(result.scalar_one())


async def _fetch_statuses(engine: Any) -> list[str]:
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(select(message.c.status))
        return [row[0] for row in result]


async def _until_async(predicate: Any, *, timeout: float = 5.0, interval: float = 0.02) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await predicate():
            return
        await asyncio.sleep(interval)
    if not await predicate():
        raise AssertionError("condition not met within timeout")


# ---------------------------------------------------------------------------
# Fan-out on write
# ---------------------------------------------------------------------------


async def test_publish_fans_out_one_row_per_subscribed_group_only(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-inventory")
    await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-billing")

    serializer = JsonEventSerializer()
    payload = serializer.serialize(WidgetCreated(name="w1"))
    event_type = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    await broker.publish("fakeapp.orders.WidgetCreated", payload, {"event_type": event_type})

    assert await _row_count(engine) == 2  # one per subscribed group

    rows_a = await broker.claim_batch("modulith-inventory", batch_size=10, consumer_name="c1")
    rows_b = await broker.claim_batch("modulith-billing", batch_size=10, consumer_name="c2")
    assert len(rows_a) == 1
    assert len(rows_b) == 1
    assert rows_a[0]["consumer_group"] == "modulith-inventory"
    assert rows_b[0]["consumer_group"] == "modulith-billing"


async def test_publish_zero_subscribers_writes_zero_rows(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    serializer = JsonEventSerializer()
    payload = serializer.serialize(WidgetCreated(name="w1"))
    await broker.publish(
        "fakeapp.orders.WidgetCreated",
        payload,
        {"event_type": f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"},
    )
    assert await _row_count(engine) == 0


# ---------------------------------------------------------------------------
# Subscription upsert idempotency
# ---------------------------------------------------------------------------


async def test_subscription_upsert_is_idempotent(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    _, subscription, _ = broker_schema()

    await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-inventory")
    await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-inventory")  # restart
    await broker.subscribe(["fakeapp.orders.WidgetCreated"], "modulith-inventory")  # restart again

    assert await _row_count(engine, table=subscription) == 1  # never duplicated


# ---------------------------------------------------------------------------
# Claim -> dispatch -> ack, end to end through the real consumer loop
# ---------------------------------------------------------------------------


async def test_claim_dispatch_ack_delivers_event_and_deletes_row(engine: Any) -> None:
    delivered: list[str] = []

    async def handler(evt: WidgetCreated) -> None:
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])

    broker = DatabaseBroker(engine=engine)  # default completion_mode="delete"
    target = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        await broker.publish(target, payload, {"event_type": target})

        await _until_async(lambda: _delivered(delivered))
        assert delivered == ["w1"]
        await _until_async(lambda: _zero_rows(engine))  # deleted on completion
    finally:
        await consumer.stop()


async def _delivered(delivered: list[str]) -> bool:
    return delivered == ["w1"]


async def _zero_rows(engine: Any) -> bool:
    return await _row_count(engine) == 0


async def test_completion_mode_mark_keeps_row_marked_done(engine: Any) -> None:
    delivered: list[str] = []

    async def handler(evt: WidgetCreated) -> None:
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])

    broker = DatabaseBroker(engine=engine, completion_mode="mark")
    target = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w2"))
        await broker.publish(target, payload, {"event_type": target})

        async def _done() -> bool:
            statuses = await _fetch_statuses(engine)
            return statuses == ["done"]

        await _until_async(_done)
        assert delivered == ["w2"]
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Claim-once semantics
# ---------------------------------------------------------------------------


async def test_claimed_row_not_redelivered_to_second_claim_same_group(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    await broker.subscribe([target], "modulith-inventory")
    serializer = JsonEventSerializer()
    payload = serializer.serialize(WidgetCreated(name="w1"))
    await broker.publish(target, payload, {"event_type": target})

    first = await broker.claim_batch("modulith-inventory", batch_size=10, consumer_name="c1")
    second = await broker.claim_batch("modulith-inventory", batch_size=10, consumer_name="c2")

    assert len(first) == 1
    assert second == []  # already claimed — not pending anymore


async def test_orphaned_claim_is_reclaimed_after_visibility_timeout(engine: Any) -> None:
    """A row claimed but never ack'd (consumer crashed mid-dispatch) must be
    reclaimed once its claim goes stale — otherwise it is lost forever, breaking
    the at-least-once guarantee. Within the window it stays claimed; past it, the
    next claim picks the SAME row up again."""
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    await broker.subscribe([target], "modulith-inventory")
    serializer = JsonEventSerializer()
    payload = serializer.serialize(WidgetCreated(name="w1"))
    await broker.publish(target, payload, {"event_type": target})

    # Consumer c1 claims but "crashes" before ack — the row is now 'claimed'.
    first = await broker.claim_batch(
        "modulith-inventory", batch_size=10, consumer_name="c1", reclaim_stale_seconds=100.0
    )
    assert len(first) == 1

    # Still within the reclaim window: not handed out again.
    within = await broker.claim_batch(
        "modulith-inventory", batch_size=10, consumer_name="c2", reclaim_stale_seconds=100.0
    )
    assert within == []

    # Past the window: the orphaned row is reclaimed (same id) by another consumer.
    await asyncio.sleep(0.1)
    reclaimed = await broker.claim_batch(
        "modulith-inventory", batch_size=10, consumer_name="c2", reclaim_stale_seconds=0.02
    )
    assert len(reclaimed) == 1
    assert reclaimed[0]["id"] == first[0]["id"]


# ---------------------------------------------------------------------------
# Poison messages
# ---------------------------------------------------------------------------


async def test_poison_bad_payload_dead_letters_immediately(engine: Any) -> None:
    bus = InMemoryEventBus()
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    event_type = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        await broker.publish(target, b"not-valid-json{{{", {"event_type": event_type})

        async def _dead() -> bool:
            statuses = await _fetch_statuses(engine)
            return statuses == ["dead"]

        await _until_async(_dead)
    finally:
        await consumer.stop()


async def test_poison_missing_event_type_dead_letters_immediately(engine: Any) -> None:
    bus = InMemoryEventBus()
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        # No "event_type" header at all -> stored event_type is "" (falsy) -> poison.
        await broker.publish(target, payload, {})

        async def _dead() -> bool:
            statuses = await _fetch_statuses(engine)
            return statuses == ["dead"]

        await _until_async(_dead)
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Dispatch failure -> attempts++ -> dead after cap
# ---------------------------------------------------------------------------


async def test_dispatch_failure_increments_attempts_then_dead_after_cap(engine: Any) -> None:
    calls = {"n": 0}

    async def flaky_handler(evt: WidgetCreated) -> None:
        calls["n"] += 1
        raise RuntimeError("listener broken")

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, flaky_handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    broker = DatabaseBroker(engine=engine)
    target = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
        max_attempts=2,
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        await broker.publish(target, payload, {"event_type": target})

        async def _dead() -> bool:
            statuses = await _fetch_statuses(engine)
            return statuses == ["dead"]

        await _until_async(_dead, timeout=8.0)
        assert calls["n"] >= 2  # failed at least twice before the cap dead-lettered it

        _, _, message = broker_schema()
        from sqlalchemy import select

        async with engine.connect() as conn:
            result = await conn.execute(select(message.c.attempts, message.c.last_error))
            row = result.one()
            assert row.attempts >= 2
            assert row.last_error is not None
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# stop() safety
# ---------------------------------------------------------------------------


async def test_stop_before_start_does_not_raise(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
    )
    await consumer.stop()  # never started — must not raise


async def test_double_stop_does_not_raise(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
        poll_interval_s=0.01,
    )
    await consumer.start()
    await consumer.stop()
    await consumer.stop()  # second stop — must not raise


async def test_no_targets_start_is_noop(engine: Any) -> None:
    """A leaf module with no @listener has no targets — start() must not
    launch a background task, and stop() must still be safe."""
    broker = DatabaseBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[],
    )
    await consumer.start()
    assert consumer._task is None
    await consumer.stop()


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_database_broker_satisfies_broker_protocol() -> None:
    broker = DatabaseBroker(engine=object())
    assert isinstance(broker, Broker)


def test_database_consumer_satisfies_consumer_protocol() -> None:
    consumer = DatabaseConsumer(
        broker=DatabaseBroker(engine=object()),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
    )
    assert isinstance(consumer, Consumer)


def test_missing_url_and_engine_raises_configuration_error() -> None:
    from modulith import ConfigurationError

    with pytest.raises(ConfigurationError, match="requires a SQLAlchemy URL"):
        DatabaseBroker()


# ---------------------------------------------------------------------------
# Consumer factory / registry wiring (handoff: broker registry -> consumer)
# ---------------------------------------------------------------------------


def test_consumer_factory_reuses_registered_broker_object() -> None:
    """The consumer factory pulls its backend from the broker registry rather
    than opening a second engine — one backend serves both halves."""
    broker = DatabaseBroker(engine=object())
    broker_registry = BrokerRegistry()
    broker_registry.register("database", broker)

    spec = ConsumerSpec(
        scheme="database",
        module_name="inventory",
        group="modulith-inventory",
        consumer_name="inventory:123",
        targets=("fakeapp.orders.WidgetCreated",),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        broker_registry=broker_registry,
    )
    consumer = _make_db_consumer(spec)

    assert isinstance(consumer, DatabaseConsumer)
    assert consumer._broker is broker  # same object — no new engine


def test_consumer_registry_build_wires_the_db_consumer() -> None:
    """Full plumbing through ConsumerRegistry.build, mirroring the worker's
    call path (modulith._worker._build_consumer -> consumer_registry.build)."""
    broker = DatabaseBroker(engine=object())
    broker_registry = BrokerRegistry()
    broker_registry.register("database", broker)
    registry = ConsumerRegistry()
    registry.register("database", _make_db_consumer)

    spec = ConsumerSpec(
        scheme="database",
        module_name="inventory",
        group="modulith-inventory",
        consumer_name="inventory:123",
        targets=("fakeapp.orders.WidgetCreated",),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        broker_registry=broker_registry,
    )
    consumer = registry.build("database", spec)

    assert isinstance(consumer, DatabaseConsumer)
    assert consumer._broker is broker


# ---------------------------------------------------------------------------
# Bootstrap wiring (builtin plugin, gated on cfg.broker == "database")
# ---------------------------------------------------------------------------


def test_register_brokers_noop_when_broker_not_database(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.db_broker import modulith_register_brokers

    configure(package="fakeapp", broker="memory")
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    assert "database" not in registry.schemes()


def test_register_brokers_registers_when_configured(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.db_broker import modulith_register_brokers

    configure(
        package="fakeapp",
        broker="database",
        broker_options={"url": "sqlite+aiosqlite:///:memory:"},
    )
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    assert "database" in registry.schemes()


def test_runtime_bootstrap_registers_db_consumer_factory(make_fake_app: Any) -> None:
    """Normal bootstrap wires the DB consumer factory through the built-in
    plugin path, not just a direct hook call."""
    make_fake_app({"orders": ""})
    from modulith import configure

    configure(
        package="fakeapp",
        broker="database",
        broker_options={"url": "sqlite+aiosqlite:///:memory:"},
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.consumer_registry is not None
    assert "database" in _runtime.consumer_registry.schemes()
    assert _runtime.broker_registry is not None
    assert "database" in _runtime.broker_registry.schemes()


# ---------------------------------------------------------------------------
# SKIP LOCKED dialect gate
# ---------------------------------------------------------------------------


class _FakeDialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEngine:
    def __init__(self, name: str) -> None:
        self.dialect = _FakeDialect(name)


def test_skip_locked_gate_selects_postgres_and_mysql_only() -> None:
    assert _supports_skip_locked(_FakeEngine("postgresql")) is True
    assert _supports_skip_locked(_FakeEngine("mysql")) is True
    assert _supports_skip_locked(_FakeEngine("sqlite")) is False
