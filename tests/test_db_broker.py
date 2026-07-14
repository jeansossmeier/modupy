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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from modulith import Broker, BrokerRegistry, Consumer, ConsumerRegistry, ConsumerSpec, event
from modulith.adapters.db_broker import (
    _SQLITE_BUSY_MAX_RETRIES,
    DatabaseBroker,
    DatabaseConsumer,
    _broker_opt,
    _create_engine,
    _is_already_exists,
    _is_sqlite_locked,
    _is_sqlite_url,
    _make_db_consumer,
    _opt_float,
    _opt_int,
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


async def _insert(
    engine: Any,
    *,
    id: str,
    target: str,
    group: str,
    status: str,
    age_seconds: float,
) -> str:
    """Insert one ``broker_message`` row directly with a chosen status and a
    ``created_at`` ``age_seconds`` in the past — the fixture the prune tests
    need (fine-grained control over status/age that publish() can't give)."""
    from sqlalchemy import insert

    _, _, message = broker_schema()
    created = datetime.now(UTC) - timedelta(seconds=age_seconds)
    async with engine.begin() as conn:
        await conn.execute(
            insert(message).values(
                id=id,
                target=target,
                consumer_group=group,
                event_type="fakeapp.orders.WidgetCreated",
                payload=b"{}",
                headers=None,
                status=status,
                attempts=0,
                available_at=created,
                claimed_at=created if status == "claimed" else None,
                claimed_by="c0" if status == "claimed" else None,
                created_at=created,
                last_error=None,
            )
        )
    return id


async def _all_ids(engine: Any) -> set[str]:
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(select(message.c.id))
        return {row[0] for row in result}


async def _insert_ex(
    engine: Any,
    *,
    id: str,
    status: str,
    attempts: int = 0,
    claimed_by: str | None = None,
    target: str = "A",
    group: str = "g",
) -> str:
    """Insert one row with full control over status / attempts / claimed_by —
    the fixture the owner-guard regression tests need (they assert late writes
    from a non-owning consumer are no-ops)."""
    from sqlalchemy import insert

    _, _, message = broker_schema()
    now = datetime.now(UTC)
    async with engine.begin() as conn:
        await conn.execute(
            insert(message).values(
                id=id,
                target=target,
                consumer_group=group,
                event_type="fakeapp.orders.WidgetCreated",
                payload=b"{}",
                headers=None,
                status=status,
                attempts=attempts,
                available_at=now,
                claimed_at=now if claimed_by is not None else None,
                claimed_by=claimed_by,
                created_at=now,
                last_error=None,
            )
        )
    return id


async def _fetch_row(engine: Any, row_id: str) -> Any:
    """Return ``(status, attempts, claimed_by)`` for one row, or None if gone."""
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(
            select(message.c.status, message.c.attempts, message.c.claimed_by).where(
                message.c.id == row_id
            )
        )
        return result.first()


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


def test_skip_locked_gate_selects_lockable_dialects_only() -> None:
    assert _supports_skip_locked(_FakeEngine("postgresql")) is True
    assert _supports_skip_locked(_FakeEngine("mysql")) is True
    assert _supports_skip_locked(_FakeEngine("mariadb")) is True  # 10.6+ supports it
    assert _supports_skip_locked(_FakeEngine("sqlite")) is False


# ---------------------------------------------------------------------------
# Prune: by age, by count, and the never-touch-undelivered-rows guarantee
# ---------------------------------------------------------------------------


async def test_prune_no_retention_is_noop(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert(engine, id="d", target="A", group="g", status="dead", age_seconds=99999)

    assert await broker.prune() == 0  # neither knob set -> deletes nothing
    assert await _all_ids(engine) == {"d"}


async def test_prune_by_age_deletes_old_terminal_rows_only(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert(engine, id="old_done", target="A", group="g", status="done", age_seconds=100)
    await _insert(engine, id="old_dead", target="A", group="g", status="dead", age_seconds=100)
    # Undelivered rows must survive regardless of age — pruning them is loss.
    await _insert(
        engine, id="old_pending", target="A", group="g", status="pending", age_seconds=100
    )
    await _insert(
        engine, id="old_claimed", target="A", group="g", status="claimed", age_seconds=100
    )
    # A recent terminal row is younger than the cutoff -> kept.
    await _insert(engine, id="new_done", target="A", group="g", status="done", age_seconds=1)

    deleted = await broker.prune(retention_age_seconds=50)

    assert deleted == 2
    assert await _all_ids(engine) == {"old_pending", "old_claimed", "new_done"}


async def test_prune_by_count_keeps_newest_per_partition(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    # Partition (A, g): 4 terminal rows, oldest -> newest.
    for i, age in enumerate((40, 30, 20, 10)):
        await _insert(engine, id=f"a{i}", target="A", group="g", status="done", age_seconds=age)
    # A different partition (B, g): 3 terminal rows — counted independently.
    for i, age in enumerate((25, 15, 5)):
        await _insert(engine, id=f"b{i}", target="B", group="g", status="dead", age_seconds=age)
    # A pending row in partition (A, g): not terminal -> never counted or pruned.
    await _insert(engine, id="pending", target="A", group="g", status="pending", age_seconds=99)

    deleted = await broker.prune(retention_count=2)

    assert deleted == 3  # A: 4 -> 2 (drop a0,a1); B: 3 -> 2 (drop b0)
    assert await _all_ids(engine) == {"a2", "a3", "b1", "b2", "pending"}


async def test_prune_age_then_count_compose(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    # Three rows older than the age cutoff (age-pruned), three younger (survive
    # age; then count keeps the newest 2 of them).
    for i, age in enumerate((300, 290, 280)):
        await _insert(engine, id=f"old{i}", target="A", group="g", status="done", age_seconds=age)
    for i, age in enumerate((30, 20, 10)):
        await _insert(engine, id=f"new{i}", target="A", group="g", status="done", age_seconds=age)

    deleted = await broker.prune(retention_age_seconds=100, retention_count=2)

    # age drops old0/old1/old2 (3); count then drops new0 (oldest of the 3 left).
    assert deleted == 4
    assert await _all_ids(engine) == {"new1", "new2"}


async def test_prune_count_zero_deletes_all_terminal_but_keeps_undelivered(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert(engine, id="done", target="A", group="g", status="done", age_seconds=5)
    await _insert(engine, id="dead", target="A", group="g", status="dead", age_seconds=5)
    await _insert(engine, id="pending", target="A", group="g", status="pending", age_seconds=5)
    await _insert(engine, id="claimed", target="A", group="g", status="claimed", age_seconds=5)

    deleted = await broker.prune(retention_count=0)  # keep zero terminal rows

    assert deleted == 2
    assert await _all_ids(engine) == {"pending", "claimed"}


# ---------------------------------------------------------------------------
# Consumer background prune loop
# ---------------------------------------------------------------------------


async def test_consumer_prune_loop_deletes_old_terminal_rows(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert(
        engine, id="stale", target="A", group="modulith-inventory", status="dead", age_seconds=100
    )

    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["A"],
        poll_interval_s=0.01,
        prune_interval_s=0.01,
        retention_age_seconds=1.0,
    )
    await consumer.start()
    try:
        assert consumer._prune_task is not None  # retention set -> loop running

        async def _pruned() -> bool:
            return "stale" not in await _all_ids(engine)

        await _until_async(_pruned)
    finally:
        await consumer.stop()
    assert consumer._prune_task is None  # cancelled and cleared by stop()


async def test_consumer_has_no_prune_task_without_retention(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["A"],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        assert consumer._prune_task is None  # no retention -> no prune loop
    finally:
        await consumer.stop()


def test_prune_interval_zero_disables_even_with_retention() -> None:
    consumer = DatabaseConsumer(
        broker=DatabaseBroker(engine=object()),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="c",
        group="g",
        targets=["A"],
        prune_interval_s=0.0,
        retention_age_seconds=60.0,
    )
    assert consumer._prune_enabled() is False


def test_prune_enabled_when_a_retention_knob_is_set() -> None:
    consumer = DatabaseConsumer(
        broker=DatabaseBroker(engine=object()),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="c",
        group="g",
        targets=["A"],
        retention_count=100,
    )
    assert consumer._prune_enabled() is True


def test_make_db_consumer_reads_prune_config_from_broker_options(make_fake_app: Any) -> None:
    """I4 config wiring: the factory lifts retention settings out of
    ``[tool.modulith.broker_options]`` onto the consumer."""
    make_fake_app({"orders": ""})
    from modulith import configure

    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            "poll_interval_ms": 250,
            "batch_size": 42,
            "retention_age_seconds": 3600,
            "retention_count": 100,
            "prune_interval_seconds": 30,
        },
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    spec = ConsumerSpec(
        scheme="database",
        module_name="inventory",
        group="modulith-inventory",
        consumer_name="inventory:1",
        targets=("fakeapp.orders.WidgetCreated",),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        broker_registry=_runtime.broker_registry,
    )
    consumer = _make_db_consumer(spec)

    assert isinstance(consumer, DatabaseConsumer)
    assert consumer._poll_interval_s == 0.25  # 250 ms -> seconds
    assert consumer._batch_size == 42
    assert consumer._retention_age_seconds == 3600.0
    assert consumer._retention_count == 100
    assert consumer._prune_interval_s == 30.0
    assert consumer._prune_enabled() is True


def test_make_db_consumer_uses_defaults_when_unconfigured(make_fake_app: Any) -> None:
    """No cadence/retention keys -> library defaults, prune off."""
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.db_broker import (
        _DEFAULT_BATCH_SIZE,
        _DEFAULT_POLL_INTERVAL_S,
        _DEFAULT_RECLAIM_STALE_S,
        _MAX_DELIVERY_ATTEMPTS,
    )

    configure(
        package="fakeapp",
        broker="database",
        broker_options={"url": "sqlite+aiosqlite:///:memory:"},
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    spec = ConsumerSpec(
        scheme="database",
        module_name="inventory",
        group="modulith-inventory",
        consumer_name="inventory:1",
        targets=("fakeapp.orders.WidgetCreated",),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        broker_registry=_runtime.broker_registry,
    )
    consumer = _make_db_consumer(spec)

    assert isinstance(consumer, DatabaseConsumer)
    assert consumer._poll_interval_s == _DEFAULT_POLL_INTERVAL_S
    assert consumer._batch_size == _DEFAULT_BATCH_SIZE
    assert consumer._reclaim_stale_seconds == _DEFAULT_RECLAIM_STALE_S
    assert consumer._max_attempts == _MAX_DELIVERY_ATTEMPTS
    assert consumer._prune_enabled() is False


# ---------------------------------------------------------------------------
# I5a: engine factory — pooling (pg/mysql) + SQLite WAL/busy_timeout
# ---------------------------------------------------------------------------


def test_is_sqlite_url_classifies_backends() -> None:
    assert _is_sqlite_url("sqlite+aiosqlite:///x.db") is True
    assert _is_sqlite_url("sqlite:///:memory:") is True
    assert _is_sqlite_url("postgresql+asyncpg://u:p@h/db") is False
    assert _is_sqlite_url("mysql+aiomysql://u:p@h/db") is False


async def test_sqlite_engine_enables_wal_and_busy_timeout(tmp_path: Path) -> None:
    from sqlalchemy import text

    url = f"sqlite+aiosqlite:///{tmp_path / 'wal.db'}"
    engine = _create_engine(url, {"busy_timeout_ms": 1234})
    try:
        async with engine.connect() as conn:
            journal = (await conn.execute(text("PRAGMA journal_mode"))).scalar_one()
            busy = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
        assert str(journal).lower() == "wal"
        assert int(busy) == 1234
    finally:
        await engine.dispose()


async def test_sqlite_busy_timeout_defaults_when_unset(tmp_path: Path) -> None:
    from sqlalchemy import text

    from modulith.adapters.db_broker import _DEFAULT_SQLITE_BUSY_TIMEOUT_MS

    url = f"sqlite+aiosqlite:///{tmp_path / 'wal2.db'}"
    engine = _create_engine(url, {})
    try:
        async with engine.connect() as conn:
            busy = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
        assert int(busy) == _DEFAULT_SQLITE_BUSY_TIMEOUT_MS
    finally:
        await engine.dispose()


async def test_pool_options_applied_for_non_sqlite() -> None:
    # No connection is opened (create_async_engine is lazy) — inspect the pool.
    engine = _create_engine(
        "postgresql+asyncpg://user:pass@localhost/db", {"pool_size": 7, "max_overflow": 3}
    )
    try:
        assert engine.sync_engine.pool.size() == 7
    finally:
        await engine.dispose()


async def test_pool_options_ignored_for_sqlite(tmp_path: Path) -> None:
    """pool_size given but SQLite's pool rejects it — _create_engine must drop
    it rather than raise, and still produce a working engine."""
    from sqlalchemy import text

    url = f"sqlite+aiosqlite:///{tmp_path / 'nopool.db'}"
    engine = _create_engine(url, {"pool_size": 5, "max_overflow": 2})
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# I5b: env-override resolution
# ---------------------------------------------------------------------------


def test_broker_opt_prefers_env_over_options(monkeypatch: Any) -> None:
    monkeypatch.setenv("MODULITH_BROKER_URL", "sqlite+aiosqlite:///env.db")
    assert (
        _broker_opt({"url": "sqlite+aiosqlite:///file.db"}, "url", "URL")
        == "sqlite+aiosqlite:///env.db"
    )


def test_broker_opt_blank_env_is_treated_as_unset(monkeypatch: Any) -> None:
    monkeypatch.setenv("MODULITH_BROKER_URL", "")
    assert _broker_opt({"url": "from-options"}, "url", "URL") == "from-options"


def test_broker_opt_falls_back_to_none(monkeypatch: Any) -> None:
    monkeypatch.delenv("MODULITH_BROKER_POOL_SIZE", raising=False)
    assert _broker_opt({}, "pool_size", "POOL_SIZE") is None


async def test_pool_size_env_override_reaches_engine(monkeypatch: Any) -> None:
    """``MODULITH_BROKER_POOL_SIZE`` sizes the pool with no subtable value — the
    engine factory must resolve pool options env>subtable like every other
    broker option (the module docstring documents these as env-resolvable, and
    pool sizing is a per-deployment value operators tune without editing
    pyproject)."""
    monkeypatch.setenv("MODULITH_BROKER_POOL_SIZE", "9")
    engine = _create_engine("postgresql+asyncpg://user:pass@localhost/db", {})
    try:
        assert engine.sync_engine.pool.size() == 9
    finally:
        await engine.dispose()


async def test_max_overflow_env_override_reaches_engine(monkeypatch: Any) -> None:
    """``MODULITH_BROKER_MAX_OVERFLOW`` overrides overflow with no subtable value."""
    monkeypatch.setenv("MODULITH_BROKER_MAX_OVERFLOW", "4")
    engine = _create_engine("postgresql+asyncpg://user:pass@localhost/db", {})
    try:
        assert engine.sync_engine.pool._max_overflow == 4
    finally:
        await engine.dispose()


async def test_busy_timeout_env_override_reaches_sqlite(tmp_path: Path, monkeypatch: Any) -> None:
    """``MODULITH_BROKER_BUSY_TIMEOUT_MS`` overrides the SQLite busy_timeout with
    no subtable value."""
    from sqlalchemy import text

    monkeypatch.setenv("MODULITH_BROKER_BUSY_TIMEOUT_MS", "2222")
    url = f"sqlite+aiosqlite:///{tmp_path / 'envbusy.db'}"
    engine = _create_engine(url, {})
    try:
        async with engine.connect() as conn:
            busy = (await conn.execute(text("PRAGMA busy_timeout"))).scalar_one()
        assert int(busy) == 2222
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# I5c: subscribe upsert refreshes updated_at (proves ON CONFLICT DO UPDATE ran)
# ---------------------------------------------------------------------------


async def test_subscribe_upsert_refreshes_updated_at(engine: Any) -> None:
    from sqlalchemy import select

    broker = DatabaseBroker(engine=engine)
    _, subscription, _ = broker_schema()

    async def _updated_at() -> Any:
        async with engine.connect() as conn:
            result = await conn.execute(select(subscription.c.updated_at))
            return result.scalar_one()

    await broker.subscribe(["A"], "g")
    first = await _updated_at()
    await asyncio.sleep(0.01)
    await broker.subscribe(["A"], "g")  # conflict -> DO UPDATE, not a crash
    second = await _updated_at()

    assert await _row_count(engine, table=subscription) == 1  # still one row
    assert second > first  # updated_at advanced -> the update branch ran


# ---------------------------------------------------------------------------
# I5d: SQLITE_BUSY retry wrapper
# ---------------------------------------------------------------------------


def test_is_sqlite_locked_detects_locked_operational_error() -> None:
    from sqlalchemy.exc import IntegrityError, OperationalError

    locked = OperationalError("stmt", {}, Exception("database is locked"))
    other = OperationalError("stmt", {}, Exception("no such table: x"))
    integrity = IntegrityError("stmt", {}, Exception("database is locked"))

    assert _is_sqlite_locked(locked) is True
    assert _is_sqlite_locked(other) is False
    # Only OperationalError is retryable, even if the text mentions a lock.
    assert _is_sqlite_locked(integrity) is False
    assert _is_sqlite_locked(RuntimeError("database is locked")) is False


class _FlakyBegin:
    def __init__(self, engine: _FlakyEngine) -> None:
        self._engine = engine

    async def __aenter__(self) -> object:
        self._engine.begins += 1
        if self._engine.begins <= self._engine.fail_times:
            from sqlalchemy.exc import OperationalError

            raise OperationalError("stmt", {}, Exception(self._engine.error_text))
        return object()  # a stand-in "connection" the op never touches

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FlakyEngine:
    """Minimal async-engine stand-in whose begin() raises a chosen error the
    first ``fail_times`` calls, then succeeds — for deterministic retry tests."""

    def __init__(self, *, fail_times: int, error_text: str = "database is locked") -> None:
        self.fail_times = fail_times
        self.error_text = error_text
        self.begins = 0

    def begin(self) -> _FlakyBegin:
        return _FlakyBegin(self)


async def test_write_retries_transient_sqlite_lock_then_succeeds() -> None:
    broker = DatabaseBroker(engine=_FlakyEngine(fail_times=2))
    ran = {"n": 0}

    async def op(_conn: Any) -> str:
        ran["n"] += 1
        return "ok"

    result = await broker._write(op)

    assert result == "ok"
    assert ran["n"] == 1  # op ran exactly once, after two failed begins
    assert broker._engine.begins == 3  # two lock failures + one success


async def test_write_gives_up_after_max_retries() -> None:
    from sqlalchemy.exc import OperationalError

    broker = DatabaseBroker(engine=_FlakyEngine(fail_times=99))

    async def op(_conn: Any) -> None:
        pass

    with pytest.raises(OperationalError):
        await broker._write(op)
    assert broker._engine.begins == _SQLITE_BUSY_MAX_RETRIES  # bounded, not infinite


async def test_write_does_not_retry_non_lock_errors() -> None:
    from sqlalchemy.exc import OperationalError

    broker = DatabaseBroker(engine=_FlakyEngine(fail_times=99, error_text="no such table: x"))

    async def op(_conn: Any) -> None:
        pass

    with pytest.raises(OperationalError):
        await broker._write(op)
    assert broker._engine.begins == 1  # non-lock error propagates on first try


# ---------------------------------------------------------------------------
# HIGH-1: owner/status guard on ack / fail / dead_letter
#
# Without the guard, a late write from a healthy-but-slow consumer whose row
# was already reclaimed (and possibly dead-lettered) by a peer can resurrect a
# terminal row or steal another owner's row. Every completion path must be a
# compare-and-swap on ``status='claimed' AND claimed_by=:consumer_name``.
# ---------------------------------------------------------------------------


async def test_fail_on_terminal_dead_row_is_noop(engine: Any) -> None:
    """A late fail() on an already-dead row (a peer reclaimed it, hit the cap,
    and dead-lettered it) must NOT resurrect it to 'pending'."""
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="dead", attempts=5, claimed_by=None)

    await broker.fail("r1", "late failure", consumer_name="c1", max_attempts=5)

    assert await _fetch_row(engine, "r1") == ("dead", 5, None)  # unchanged


async def test_fail_on_row_owned_by_peer_is_noop(engine: Any) -> None:
    """A late fail() from c1 on a row currently claimed by c2 must not touch
    c2's in-flight row."""
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="claimed", attempts=0, claimed_by="c2")

    await broker.fail("r1", "late failure", consumer_name="c1", max_attempts=5)

    assert await _fetch_row(engine, "r1") == ("claimed", 0, "c2")  # still c2's


async def test_fail_derives_attempts_from_db_not_caller(engine: Any) -> None:
    """The new attempt count is read from the row inside the txn, not from a
    caller snapshot — so it is correct even after a reclaim changed it."""
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="below", status="claimed", attempts=3, claimed_by="c1")
    await _insert_ex(engine, id="atcap", status="claimed", attempts=4, claimed_by="c1")

    await broker.fail("below", "boom", consumer_name="c1", max_attempts=5)
    await broker.fail("atcap", "boom", consumer_name="c1", max_attempts=5)

    below = await _fetch_row(engine, "below")
    atcap = await _fetch_row(engine, "atcap")
    assert below[0] == "pending" and below[1] == 4 and below[2] is None
    assert atcap[0] == "dead" and atcap[1] == 5 and atcap[2] is None  # 4+1 hits cap


async def test_ack_on_terminal_dead_row_is_noop_delete_mode(engine: Any) -> None:
    """A late ack() (delete mode) on an already-dead row must NOT delete it —
    the peer that dead-lettered it owns its terminal state."""
    broker = DatabaseBroker(engine=engine)  # completion_mode="delete"
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="dead", attempts=5, claimed_by=None)

    await broker.ack("r1", consumer_name="c1")

    assert await _fetch_row(engine, "r1") == ("dead", 5, None)  # not deleted


async def test_ack_on_row_owned_by_peer_is_noop(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="claimed", attempts=0, claimed_by="c2")

    await broker.ack("r1", consumer_name="c1")

    assert await _fetch_row(engine, "r1") == ("claimed", 0, "c2")  # untouched


async def test_ack_completes_owned_row_delete_and_mark(engine: Any) -> None:
    """The owner's ack still works in both completion modes."""
    delete_broker = DatabaseBroker(engine=engine)  # delete
    await delete_broker._ensure_schema()
    await _insert_ex(engine, id="d1", status="claimed", claimed_by="c1")
    await delete_broker.ack("d1", consumer_name="c1")
    assert await _fetch_row(engine, "d1") is None  # deleted

    mark_broker = DatabaseBroker(engine=engine, completion_mode="mark")
    await _insert_ex(engine, id="m1", status="claimed", claimed_by="c1")
    await mark_broker.ack("m1", consumer_name="c1")
    assert (await _fetch_row(engine, "m1"))[0] == "done"


async def test_dead_letter_on_row_owned_by_peer_is_noop(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="claimed", attempts=0, claimed_by="c2")

    await broker.dead_letter("r1", "poison", consumer_name="c1")

    assert await _fetch_row(engine, "r1") == ("claimed", 0, "c2")  # not dead-lettered


async def test_dead_letter_marks_owned_row_dead(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    await broker._ensure_schema()
    await _insert_ex(engine, id="r1", status="claimed", claimed_by="c1")

    await broker.dead_letter("r1", "poison", consumer_name="c1")

    assert (await _fetch_row(engine, "r1"))[0] == "dead"


# ---------------------------------------------------------------------------
# MEDIUM-4: completion_mode is validated at construction
# ---------------------------------------------------------------------------


def test_invalid_completion_mode_raises() -> None:
    from modulith import ConfigurationError

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=NullPool)
    with pytest.raises(ConfigurationError, match="completion_mode"):
        DatabaseBroker(engine=engine, completion_mode="bogus")


# ---------------------------------------------------------------------------
# HIGH-2: _make_db_consumer wires reclaim_stale_seconds + max_delivery_attempts
# ---------------------------------------------------------------------------


def test_make_db_consumer_reads_reclaim_and_max_attempts(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure

    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            "reclaim_stale_seconds": 12.5,
            "max_delivery_attempts": 9,
        },
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    spec = ConsumerSpec(
        scheme="database",
        module_name="inventory",
        group="modulith-inventory",
        consumer_name="inventory:1",
        targets=("fakeapp.orders.WidgetCreated",),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        broker_registry=_runtime.broker_registry,
    )
    consumer = _make_db_consumer(spec)

    assert isinstance(consumer, DatabaseConsumer)
    assert consumer._reclaim_stale_seconds == 12.5
    assert consumer._max_attempts == 9


# ---------------------------------------------------------------------------
# MEDIUM-3: _ensure_schema tolerates a cross-process CREATE TABLE race
# ---------------------------------------------------------------------------


class _RaceConn:
    def __init__(self, error_text: str | None) -> None:
        self._error_text = error_text

    async def run_sync(self, _fn: Any) -> None:
        if self._error_text is not None:
            from sqlalchemy.exc import OperationalError

            raise OperationalError("CREATE TABLE ...", {}, Exception(self._error_text))


class _RaceBegin:
    def __init__(self, conn: _RaceConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RaceConn:
        return self._conn

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _SchemaRaceEngine:
    """Minimal async-engine stand-in whose create_all (run_sync) raises a
    chosen DDL error — for deterministically exercising the schema-race path."""

    def __init__(self, *, error_text: str | None) -> None:
        self._conn = _RaceConn(error_text)

    def begin(self) -> _RaceBegin:
        return _RaceBegin(self._conn)


def test_is_already_exists_matches_concurrent_create_errors() -> None:
    from sqlalchemy.exc import OperationalError, ProgrammingError

    assert _is_already_exists(
        OperationalError("s", {}, Exception("table broker_message already exists"))
    )
    assert _is_already_exists(
        ProgrammingError("s", {}, Exception('relation "broker_message" already exists'))
    )
    assert not _is_already_exists(OperationalError("s", {}, Exception("no such table: x")))
    assert not _is_already_exists(ValueError("unrelated"))


async def test_ensure_schema_tolerates_concurrent_create() -> None:
    """A peer that wins the CREATE race leaves us an 'already exists' error;
    the schema IS present, so _ensure_schema swallows it and marks ready."""
    broker = DatabaseBroker(
        engine=_SchemaRaceEngine(error_text="table broker_message already exists")
    )
    await broker._ensure_schema()  # must not raise
    assert broker._schema_ready is True


async def test_ensure_schema_propagates_other_ddl_errors() -> None:
    from sqlalchemy.exc import OperationalError

    broker = DatabaseBroker(engine=_SchemaRaceEngine(error_text="disk I/O error"))
    with pytest.raises(OperationalError):
        await broker._ensure_schema()
    assert broker._schema_ready is False  # genuine failure is not masked


# ---------------------------------------------------------------------------
# LOW: _opt_float / _opt_int raise ConfigurationError on a non-numeric value
# ---------------------------------------------------------------------------


def test_opt_float_rejects_non_numeric() -> None:
    from modulith import ConfigurationError

    assert _opt_float(None) is None
    assert _opt_float("2.5") == 2.5
    with pytest.raises(ConfigurationError):
        _opt_float("not-a-number")


def test_opt_int_rejects_non_integer() -> None:
    from modulith import ConfigurationError

    assert _opt_int(None) is None
    assert _opt_int("7") == 7
    with pytest.raises(ConfigurationError):
        _opt_int("10.0")  # a float string is not a valid int — loud, not silent
    with pytest.raises(ConfigurationError):
        _opt_int("abc")


# ---------------------------------------------------------------------------
# LOW: DatabaseConsumer.start() is idempotent (no orphaned second poll loop)
# ---------------------------------------------------------------------------


async def test_start_is_idempotent(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="c1",
        group="g",
        targets=["t"],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:
        first_task = consumer._task
        assert first_task is not None
        await consumer.start()  # second start must be a no-op
        assert consumer._task is first_task  # same task — the first was not orphaned
    finally:
        await consumer.stop()
