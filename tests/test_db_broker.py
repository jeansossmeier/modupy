"""Tests for the database-backed broker adapter (Increment 2).

Runs against a real async SQLAlchemy engine on aiosqlite (tmp-file DB +
NullPool — one connection PER session; deliberately NOT StaticPool +
``:memory:``, which shares a single connection across sessions and lets one
session's close() clobber another's in-flight work, see
tests/test_postgres_adapter.py's ``engine`` fixture docstring for the
deterministic repro this avoids).

Covered:
  * publish() fans out one row per SUBSCRIBED consumer group only; the default
    policy rejects zero subscribers without writing a row.
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
import json
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
    _delivery_message_id,
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
from modulith.protocols import ConsumerHealth
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


async def test_publish_zero_subscribers_raises_without_writing_rows(engine: Any) -> None:
    from modulith.adapters.db_broker import NoSubscribersError

    broker = DatabaseBroker(engine=engine)
    serializer = JsonEventSerializer()
    payload = serializer.serialize(WidgetCreated(name="w1"))
    target = "fakeapp.orders.WidgetCreated"

    with pytest.raises(NoSubscribersError, match=target):
        await broker.publish(
            target,
            payload,
            {"event_type": f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"},
        )

    assert await _row_count(engine) == 0


async def test_publish_waits_until_a_subscriber_exists(engine: Any) -> None:
    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="wait",
        no_subscriber_wait_timeout_seconds=1.0,
        no_subscriber_wait_poll_interval_ms=10.0,
    )
    target = "fakeapp.orders.WidgetCreated"
    payload = JsonEventSerializer().serialize(WidgetCreated(name="w1"))

    publish_task = asyncio.create_task(broker.publish(target, payload, {"event_type": target}))
    await asyncio.sleep(0.03)
    assert publish_task.done() is False

    await broker.subscribe([target], "modulith-inventory")
    await publish_task

    assert await _row_count(engine) == 1


async def test_publish_wait_timeout_raises_without_writing_rows(engine: Any) -> None:
    from modulith.adapters.db_broker import NoSubscribersError

    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="wait",
        no_subscriber_wait_timeout_seconds=0.03,
        no_subscriber_wait_poll_interval_ms=5.0,
    )
    target = "fakeapp.orders.WidgetCreated"

    with pytest.raises(NoSubscribersError, match="timed out"):
        await broker.publish(target, b"{}", {"event_type": target})

    assert await _row_count(engine) == 0


async def test_publish_wait_does_not_hold_transaction_while_sleeping(engine: Any) -> None:
    from sqlalchemy import event as sqlalchemy_event

    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="wait",
        no_subscriber_wait_timeout_seconds=1.0,
        no_subscriber_wait_poll_interval_ms=100.0,
    )
    await broker._ensure_schema()

    active_transactions = 0
    first_attempt_finished = asyncio.Event()

    def transaction_started(*_: Any) -> None:
        nonlocal active_transactions
        active_transactions += 1

    def transaction_finished(*_: Any) -> None:
        nonlocal active_transactions
        active_transactions -= 1
        first_attempt_finished.set()

    sqlalchemy_event.listen(engine.sync_engine, "begin", transaction_started)
    sqlalchemy_event.listen(engine.sync_engine, "commit", transaction_finished)
    sqlalchemy_event.listen(engine.sync_engine, "rollback", transaction_finished)

    target = "fakeapp.orders.WidgetCreated"
    publish_task = asyncio.create_task(broker.publish(target, b"{}", {"event_type": target}))
    try:
        await asyncio.wait_for(first_attempt_finished.wait(), timeout=1.0)
        assert publish_task.done() is False
        assert active_transactions == 0

        await broker.subscribe([target], "modulith-inventory")
        await publish_task
    finally:
        sqlalchemy_event.remove(engine.sync_engine, "begin", transaction_started)
        sqlalchemy_event.remove(engine.sync_engine, "commit", transaction_finished)
        sqlalchemy_event.remove(engine.sync_engine, "rollback", transaction_finished)


async def test_store_ttl_all_groups_delivers_current_and_late_groups_once(engine: Any) -> None:
    broker = DatabaseBroker(engine=engine, no_subscriber_policy="store")
    target = "fakeapp.orders.WidgetCreated"
    headers = {"event_type": target, "trace_id": "trace-1"}
    payload = b'{"name":"w1"}'
    metadata, _, message = broker_schema()
    retained = metadata.tables["broker_retained_message"]
    delivery = metadata.tables["broker_retained_delivery"]

    await broker.subscribe([target], "current")
    await broker.publish(target, payload, headers)

    from sqlalchemy import select

    async with engine.connect() as conn:
        ledger_row = (
            await conn.execute(
                select(
                    delivery.c.retained_message_id,
                    delivery.c.consumer_group,
                    delivery.c.broker_message_id,
                )
            )
        ).one()
    assert ledger_row.broker_message_id == _delivery_message_id(
        ledger_row.retained_message_id,
        ledger_row.consumer_group,
    )

    current = await broker.claim_batch("current", batch_size=10, consumer_name="c1")
    assert len(current) == 1
    assert current[0]["payload"] == payload
    assert json.loads(current[0]["headers"]) == headers
    assert await _row_count(engine, table=retained) == 1
    assert await _row_count(engine, table=delivery) == 1

    await broker.subscribe([target], "late")
    late = await broker.claim_batch("late", batch_size=10, consumer_name="c2")
    assert len(late) == 1
    assert late[0]["payload"] == payload
    assert json.loads(late[0]["headers"]) == headers
    assert current[0]["id"] != late[0]["id"]
    assert await _row_count(engine, table=message) == 2
    assert await _row_count(engine, table=delivery) == 2

    await broker.subscribe([target], "late")
    assert await broker.claim_batch("late", batch_size=10, consumer_name="c3") == []
    assert await _row_count(engine, table=message) == 2


async def test_store_first_groups_with_current_groups_removes_retained_source(engine: Any) -> None:
    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_replay_policy="first_groups",
    )
    target = "fakeapp.orders.WidgetCreated"
    metadata, _, _ = broker_schema()
    retained = metadata.tables["broker_retained_message"]
    delivery = metadata.tables["broker_retained_delivery"]

    await broker.subscribe([target], "inventory")
    await broker.subscribe([target], "billing")
    await broker.publish(target, b"payload", {"event_type": target})

    assert len(await broker.claim_batch("inventory", batch_size=10, consumer_name="c1")) == 1
    assert len(await broker.claim_batch("billing", batch_size=10, consumer_name="c2")) == 1
    assert await _row_count(engine, table=retained) == 0
    assert await _row_count(engine, table=delivery) == 0


async def test_store_first_groups_replays_on_first_registration_only(engine: Any) -> None:
    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_replay_policy="first_groups",
    )
    target = "fakeapp.orders.WidgetCreated"
    metadata, _, message = broker_schema()
    retained = metadata.tables["broker_retained_message"]
    delivery = metadata.tables["broker_retained_delivery"]

    await broker.publish(target, b"payload", {"event_type": target})
    assert await _row_count(engine, table=retained) == 1

    await broker.subscribe([target], "inventory")
    first = await broker.claim_batch("inventory", batch_size=10, consumer_name="c1")
    assert len(first) == 1
    assert await _row_count(engine, table=retained) == 0
    assert await _row_count(engine, table=delivery) == 0

    await broker.subscribe([target], "inventory")
    assert await broker.claim_batch("inventory", batch_size=10, consumer_name="c2") == []
    assert await _row_count(engine, table=message) == 1


async def test_store_expected_groups_materializes_before_subscription(engine: Any) -> None:
    target = "fakeapp.orders.WidgetCreated"
    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_replay_policy="expected_groups",
        expected_consumer_groups={target: ["inventory", "billing"]},
    )
    metadata, _, _ = broker_schema()
    retained = metadata.tables["broker_retained_message"]
    delivery = metadata.tables["broker_retained_delivery"]

    await broker.publish(target, b"payload", {"event_type": target})

    assert len(await broker.claim_batch("inventory", batch_size=10, consumer_name="c1")) == 1
    assert len(await broker.claim_batch("billing", batch_size=10, consumer_name="c2")) == 1
    assert await _row_count(engine, table=retained) == 0
    assert await _row_count(engine, table=delivery) == 0


async def test_store_expected_groups_requires_target_configuration(engine: Any) -> None:
    from modulith import ConfigurationError

    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_replay_policy="expected_groups",
        expected_consumer_groups={"another.target": ["inventory"]},
    )

    with pytest.raises(ConfigurationError, match="expected_consumer_groups"):
        await broker.publish("missing.target", b"payload")

    assert await _row_count(engine) == 0


async def test_prune_removes_expired_retained_messages_and_ledgers(engine: Any) -> None:
    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_retention_seconds=0.02,
    )
    target = "fakeapp.orders.WidgetCreated"
    metadata, _, _ = broker_schema()
    retained = metadata.tables["broker_retained_message"]
    delivery = metadata.tables["broker_retained_delivery"]

    await broker.subscribe([target], "inventory")
    await broker.publish(target, b"payload", {"event_type": target})
    await asyncio.sleep(0.05)

    assert await broker.prune() == 1
    assert await _row_count(engine, table=retained) == 0
    assert await _row_count(engine, table=delivery) == 0


async def test_retained_expiry_uses_database_clock(
    engine: Any,
    monkeypatch: Any,
) -> None:
    import modulith.adapters.db_broker as db_broker_module

    broker = DatabaseBroker(
        engine=engine,
        no_subscriber_policy="store",
        orphan_retention_seconds=60,
    )
    target = "fakeapp.orders.WidgetCreated"
    await broker.publish(target, b"payload", {"event_type": target})

    real_datetime = datetime

    class SkewedDateTime:
        fromisoformat = staticmethod(real_datetime.fromisoformat)

        @staticmethod
        def now(tz: Any = None) -> datetime:
            return real_datetime.now(tz) + timedelta(days=365)

    monkeypatch.setattr(db_broker_module, "datetime", SkewedDateTime)
    await broker.subscribe([target], "late")

    assert len(await broker.claim_batch("late", batch_size=10, consumer_name="c1")) == 1


def test_no_subscriber_and_orphan_policy_defaults() -> None:
    broker = DatabaseBroker(engine=object())

    assert broker._no_subscriber_policy == "error"
    assert broker._no_subscriber_wait_timeout_s == 30.0
    assert broker._no_subscriber_wait_poll_interval_s == 0.1
    assert broker._orphan_replay_policy == "ttl_all_groups"
    assert broker._orphan_retention_seconds == 86400.0
    assert broker._expected_consumer_groups == {}


def test_broker_schema_keeps_public_shape_and_adds_replay_tables() -> None:
    schema = broker_schema()

    assert len(schema) == 3
    metadata, _, _ = schema
    retained_message = metadata.tables["broker_retained_message"]
    retained_delivery = metadata.tables["broker_retained_delivery"]

    assert set(retained_message.c.keys()) == {
        "id",
        "target",
        "event_type",
        "payload",
        "headers",
        "created_at",
        "expires_at",
    }
    assert set(retained_delivery.c.keys()) == {
        "retained_message_id",
        "consumer_group",
        "broker_message_id",
        "delivered_at",
    }
    assert [column.name for column in retained_delivery.primary_key.columns] == [
        "retained_message_id",
        "consumer_group",
    ]


@pytest.mark.parametrize(
    ("kwargs", "option_name"),
    [
        ({"no_subscriber_policy": "drop"}, "no_subscriber_policy"),
        ({"orphan_replay_policy": "all"}, "orphan_replay_policy"),
        ({"no_subscriber_wait_timeout_seconds": 0}, "no_subscriber_wait_timeout_seconds"),
        (
            {"no_subscriber_wait_timeout_seconds": float("inf")},
            "no_subscriber_wait_timeout_seconds",
        ),
        (
            {"no_subscriber_wait_poll_interval_ms": float("nan")},
            "no_subscriber_wait_poll_interval_ms",
        ),
        ({"no_subscriber_wait_poll_interval_ms": -1}, "no_subscriber_wait_poll_interval_ms"),
        ({"orphan_retention_seconds": True}, "orphan_retention_seconds"),
        ({"orphan_retention_seconds": 0}, "orphan_retention_seconds"),
    ],
)
def test_broker_rejects_invalid_policy_options(kwargs: dict[str, Any], option_name: str) -> None:
    from modulith import ConfigurationError

    with pytest.raises(ConfigurationError, match=option_name):
        DatabaseBroker(engine=object(), **kwargs)


@pytest.mark.parametrize(
    "expected_groups",
    [
        [],
        {"": ["group-a"]},
        {"target-a": []},
        {"target-a": [""]},
        {"target-a": ["group-a", 3]},
    ],
)
def test_broker_rejects_invalid_expected_consumer_groups(expected_groups: Any) -> None:
    from modulith import ConfigurationError

    with pytest.raises(ConfigurationError, match="expected_consumer_groups"):
        DatabaseBroker(engine=object(), expected_consumer_groups=expected_groups)


def _consumer_with_options(**options: Any) -> DatabaseConsumer:
    return DatabaseConsumer(
        broker=DatabaseBroker(engine=object()),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
        **options,
    )


@pytest.mark.parametrize(
    ("options", "option_name"),
    [
        ({"poll_interval_s": 0}, "poll_interval_s"),
        ({"poll_interval_s": float("inf")}, "poll_interval_s"),
        ({"poll_interval_s": True}, "poll_interval_s"),
        ({"reclaim_stale_seconds": 0}, "reclaim_stale_seconds"),
        ({"reclaim_stale_seconds": float("nan")}, "reclaim_stale_seconds"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_size": True}, "batch_size"),
        ({"batch_size": 1.0}, "batch_size"),
        ({"dispatch_concurrency": 0}, "dispatch_concurrency"),
        ({"dispatch_concurrency": True}, "dispatch_concurrency"),
        ({"dispatch_concurrency": 1.0}, "dispatch_concurrency"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"max_attempts": True}, "max_attempts"),
        ({"max_attempts": 1.0}, "max_attempts"),
        ({"retention_age_seconds": 0}, "retention_age_seconds"),
        ({"retention_age_seconds": float("inf")}, "retention_age_seconds"),
        ({"retention_count": -1}, "retention_count"),
        ({"retention_count": True}, "retention_count"),
        ({"prune_interval_s": -1}, "prune_interval_s"),
        ({"prune_interval_s": float("nan")}, "prune_interval_s"),
        ({"prune_interval_s": True}, "prune_interval_s"),
    ],
)
def test_consumer_rejects_invalid_numeric_options(
    options: dict[str, Any], option_name: str
) -> None:
    from modulith import ConfigurationError

    with pytest.raises(ConfigurationError, match=option_name):
        _consumer_with_options(**options)


def test_consumer_accepts_documented_numeric_boundaries() -> None:
    consumer = _consumer_with_options(
        batch_size=1,
        dispatch_concurrency=1,
        max_attempts=1,
        retention_count=0,
        prune_interval_s=0,
    )

    assert consumer._batch_size == 1
    assert consumer._dispatch_concurrency == 1
    assert consumer._max_attempts == 1
    assert consumer._retention_count == 0
    assert consumer._prune_interval_s == 0


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


# ---------------------------------------------------------------------------
# Claim renewal (in-flight heartbeat): batch size decoupled from the reclaim
# window. Owner-guarded like ack/fail; renewal keeps a slow batch from being
# reclaimed and double-dispatched by a peer.
# ---------------------------------------------------------------------------


async def test_renew_claims_is_owner_guarded(engine: Any) -> None:
    """Only the claiming consumer can extend its own claims; a peer's renewal
    is a no-op (returns 0), exactly like the ack/fail owner-guards."""
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    await broker.subscribe([target], "modulith-inventory")
    await broker.publish(target, b"{}", {"event_type": target})

    rows = await broker.claim_batch("modulith-inventory", batch_size=10, consumer_name="c1")
    assert len(rows) == 1
    ids = [rows[0]["id"]]

    assert await broker.renew_claims(ids, consumer_name="c2") == 0  # not the owner
    assert await broker.renew_claims(ids, consumer_name="c1") == 1  # owner extends
    assert await broker.renew_claims([], consumer_name="c1") == 0  # empty = no-op


async def test_renewed_claim_is_not_reclaimed_by_peer(engine: Any) -> None:
    """A renewal re-stamps claimed_at to server-now, so a peer claiming with a
    stale window that WOULD have reclaimed the original claim gets nothing."""
    broker = DatabaseBroker(engine=engine)
    target = "fakeapp.orders.WidgetCreated"
    await broker.subscribe([target], "modulith-inventory")
    await broker.publish(target, b"{}", {"event_type": target})

    first = await broker.claim_batch(
        "modulith-inventory", batch_size=10, consumer_name="c1", reclaim_stale_seconds=100.0
    )
    assert len(first) == 1

    # Age the claim past the peer's stale window, then renew it. Generous
    # margins (0.5s slept vs a 0.25s peer window = 2x, and the renewal stamp
    # only gets FRESHER if the runner stalls) — no upper-bound race.
    await asyncio.sleep(0.5)
    assert await broker.renew_claims([first[0]["id"]], consumer_name="c1") == 1

    # The peer's cutoff (now - 0.25s) predates the renewal stamp (~now), so the
    # row is NOT handed out — without the renewal this claim would win (see
    # test_orphaned_claim_is_reclaimed_after_visibility_timeout).
    stolen = await broker.claim_batch(
        "modulith-inventory", batch_size=10, consumer_name="c2", reclaim_stale_seconds=0.25
    )
    assert stolen == []


async def test_heartbeat_keeps_slow_batch_from_peer_reclaim(engine: Any) -> None:
    """End to end through the consumer loop: a listener slower than the
    reclaim window does NOT get its row stolen (and double-dispatched),
    because the batch heartbeat renews the claim at reclaim/3 cadence."""
    delivered: list[str] = []
    release = asyncio.Event()

    async def slow_handler(evt: WidgetCreated) -> None:
        await asyncio.wait_for(release.wait(), timeout=5.0)
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, slow_handler)
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
        reclaim_stale_seconds=1.0,  # heartbeat ticks every ~0.33s
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        await broker.publish(target, payload, {"event_type": target})

        # Hold the listener well past the 1.0s reclaim window. The heartbeat
        # (ticking ~every 0.33s) must keep the claim fresh the whole time; a
        # runner stall would have to exceed ~0.67s between two renewals to
        # produce a false steal.
        await asyncio.sleep(1.5)
        stolen = await broker.claim_batch(
            "modulith-inventory",
            batch_size=10,
            consumer_name="peer",
            reclaim_stale_seconds=1.0,
        )
        assert stolen == []  # never reclaimed mid-dispatch

        release.set()
        await _until_async(lambda: _delivered(delivered))
        assert delivered == ["w1"]  # exactly once — no duplicate dispatch
        await _until_async(lambda: _zero_rows(engine))  # acked + deleted
    finally:
        release.set()
        await consumer.stop()


async def test_lease_cap_releases_rows_of_wedged_listener(engine: Any) -> None:
    """A listener that never returns must NOT pin its rows forever: after
    ``_MAX_LEASE_EXTENSION_FACTOR`` reclaim windows the heartbeat stops, the
    claim goes stale, and a peer reclaims the row (at-least-once liveness).
    Lower-bound timing only — a slow runner just waits longer."""
    wedged = asyncio.Event()  # never set — the listener hangs forever

    async def wedged_handler(evt: WidgetCreated) -> None:
        await wedged.wait()

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, wedged_handler)
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
        reclaim_stale_seconds=0.1,  # cap = 10 x 0.1s = ~1s of renewal
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        await broker.publish(target, payload, {"event_type": target})

        async def _peer_reclaims() -> bool:
            rows = await broker.claim_batch(
                "modulith-inventory",
                batch_size=10,
                consumer_name="peer",
                reclaim_stale_seconds=0.1,
            )
            return len(rows) == 1

        # Renewal keeps the row for ~1s (the cap), then stops; once the claim
        # ages past 0.1s the peer wins. 10s timeout >> 1.1s expected.
        await _until_async(_peer_reclaims, timeout=10.0, interval=0.1)
    finally:
        wedged.set()
        await consumer.stop()


async def test_dispatch_concurrency_fans_out_within_batch(engine: Any) -> None:
    """With dispatch_concurrency=4, four claimed rows dispatch concurrently:
    every handler blocks until all four have STARTED — sequential dispatch
    would deadlock here, so completion proves the fan-out."""
    started = 0
    all_started = asyncio.Event()
    delivered: list[str] = []

    async def rendezvous_handler(evt: WidgetCreated) -> None:
        nonlocal started
        started += 1
        if started == 4:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=5.0)
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, rendezvous_handler)
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
        batch_size=10,
        dispatch_concurrency=4,
    )
    # Publish all four rows BEFORE the loop starts: the first claim then takes
    # them in ONE batch, which the rendezvous below requires (a row claimed in
    # an earlier batch could never see the other three start).
    await broker.subscribe([target], "modulith-inventory")
    for i in range(4):
        payload = serializer.serialize(WidgetCreated(name=f"w{i}"))
        await broker.publish(target, payload, {"event_type": target})

    await consumer.start()
    try:

        async def _all_delivered() -> bool:
            return sorted(delivered) == ["w0", "w1", "w2", "w3"]

        await _until_async(_all_delivered)
        await _until_async(lambda: _zero_rows(engine))
    finally:
        await consumer.stop()


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

    # Past the window: the orphaned row is reclaimed (same id) by another
    # consumer. Generous margin (0.2s slept vs a 0.02s stale window = 10x) so a
    # slow/loaded runner can't make this race — there is no upper bound on the
    # elapsed time, only a lower one.
    await asyncio.sleep(0.2)
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
    assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    await consumer.stop()


# ---------------------------------------------------------------------------
# Consumer health
# ---------------------------------------------------------------------------


async def test_database_consumer_health_tracks_start_ready_and_stop(engine: Any) -> None:
    class BlockingSubscribeBroker(DatabaseBroker):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.starting = asyncio.Event()
            self.release = asyncio.Event()

        async def subscribe(self, targets: Any, consumer_group: str) -> None:
            self.starting.set()
            await self.release.wait()
            await super().subscribe(targets, consumer_group)

    broker = BlockingSubscribeBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
        poll_interval_s=0.01,
    )
    assert consumer.health() == ConsumerHealth(ready=False, status="stopped")

    start_task = asyncio.create_task(consumer.start())
    await broker.starting.wait()
    assert consumer.health() == ConsumerHealth(ready=False, status="starting")

    broker.release.set()
    await start_task
    assert consumer.health() == ConsumerHealth(ready=True, status="ready")

    await consumer.stop()
    assert consumer.health() == ConsumerHealth(ready=False, status="stopped")


async def test_database_consumer_health_reports_startup_failure(engine: Any) -> None:
    class FailingSubscribeBroker(DatabaseBroker):
        async def subscribe(self, targets: Any, consumer_group: str) -> None:
            raise RuntimeError("subscription setup failed")

    consumer = DatabaseConsumer(
        broker=FailingSubscribeBroker(engine=engine),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
    )

    with pytest.raises(RuntimeError, match="subscription setup failed"):
        await consumer.start()

    health = consumer.health()
    assert health.ready is False
    assert health.status == "failed"
    assert health.detail == "subscription setup failed"


async def test_database_consumer_health_reports_unexpected_task_exit(engine: Any) -> None:
    consumer = DatabaseConsumer(
        broker=DatabaseBroker(engine=engine),
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["fakeapp.orders.WidgetCreated"],
    )

    async def crash() -> None:
        raise RuntimeError("database poll loop crashed")

    consumer._run = crash  # type: ignore[method-assign]
    await consumer.start()

    async def _failed() -> bool:
        return consumer.health().status == "failed"

    await _until_async(_failed)
    health = consumer.health()
    assert health.ready is False
    assert health.detail == "database poll loop crashed"
    await consumer.stop()


async def test_database_consumer_write_failures_recover_independently(engine: Any) -> None:
    class CompletionFlakyBroker(DatabaseBroker):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.failures = {"ack", "fail", "dead_letter"}
            self.claim_started = asyncio.Event()
            self.allow_claim = asyncio.Event()

        async def claim_batch(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            self.claim_started.set()
            await self.allow_claim.wait()
            return await super().claim_batch(*args, **kwargs)

        async def ack(self, row_id: str, *, consumer_name: str) -> None:
            if "ack" in self.failures:
                raise RuntimeError("ack unavailable")
            await super().ack(row_id, consumer_name=consumer_name)

        async def fail(
            self,
            row_id: str,
            error: str,
            *,
            consumer_name: str,
            max_attempts: int,
        ) -> None:
            if "fail" in self.failures:
                raise RuntimeError("fail unavailable")
            await super().fail(
                row_id,
                error,
                consumer_name=consumer_name,
                max_attempts=max_attempts,
            )

        async def dead_letter(
            self,
            row_id: str,
            reason: str,
            *,
            consumer_name: str,
        ) -> None:
            if "dead_letter" in self.failures:
                raise RuntimeError("dead-letter unavailable")
            await super().dead_letter(row_id, reason, consumer_name=consumer_name)

    class ObservedConsumer(DatabaseConsumer):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.claim_recovered = asyncio.Event()

        def _mark_broker_recovered(self, operation: str, target: str) -> None:
            super()._mark_broker_recovered(operation, target)
            if operation == "claim":
                self.claim_recovered.set()

    listener_fails = False

    async def handler(_event: WidgetCreated) -> None:
        if listener_fails:
            raise RuntimeError("listener unavailable")

    broker = CompletionFlakyBroker(engine=engine)
    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    target = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = ObservedConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[target],
    )
    valid_row = {
        "id": "ack-row",
        "target": target,
        "event_type": target,
        "payload": serializer.serialize(WidgetCreated(name="w1")),
        "attempts": 0,
    }
    poison_row = {
        "id": "dead-row",
        "target": target,
        "event_type": None,
        "payload": b"{}",
        "attempts": 0,
    }

    await consumer.start()
    try:
        await broker.claim_started.wait()
        await consumer._dispatch_one(valid_row)

        listener_fails = True
        with pytest.raises(RuntimeError, match="fail unavailable"):
            await consumer._dispatch_one({**valid_row, "id": "fail-row"})
        with pytest.raises(RuntimeError, match="dead-letter unavailable"):
            await consumer._dispatch_one(poison_row)
        assert consumer.health().status == "degraded"

        # A healthy claim is unrelated to the failed completion writes.
        broker.allow_claim.set()
        await consumer.claim_recovered.wait()
        assert consumer.health().status == "degraded"

        broker.failures.remove("ack")
        listener_fails = False
        await consumer._dispatch_one({**valid_row, "id": "ack-recovery"})
        assert consumer.health().status == "degraded"

        broker.failures.remove("fail")
        listener_fails = True
        await consumer._dispatch_one({**valid_row, "id": "fail-recovery"})
        assert consumer.health().status == "degraded"

        broker.failures.remove("dead_letter")
        await consumer._dispatch_one({**poison_row, "id": "dead-recovery"})
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


async def test_database_consumer_health_recovery_is_scoped_to_target(engine: Any) -> None:
    class TargetFlakyBroker(DatabaseBroker):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.fail_acks = True

        async def ack(self, row_id: str, *, consumer_name: str) -> None:
            if self.fail_acks:
                raise RuntimeError("ack unavailable")
            await super().ack(row_id, consumer_name=consumer_name)

    async def handler(_event: WidgetCreated) -> None:
        pass

    broker = TargetFlakyBroker(engine=engine)
    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    event_type = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=["target-a", "target-b"],
    )
    run_blocker = asyncio.Event()

    async def blocked_run() -> None:
        await run_blocker.wait()

    consumer._run = blocked_run  # type: ignore[method-assign]

    def row(row_id: str, target: str) -> dict[str, Any]:
        return {
            "id": row_id,
            "target": target,
            "event_type": event_type,
            "payload": serializer.serialize(WidgetCreated(name=row_id)),
            "attempts": 0,
        }

    await consumer.start()
    try:
        await consumer._dispatch_one(row("a-failure", "target-a"))
        assert consumer.health().status == "degraded"

        # A successful ACK for target B must not hide target A's failure.
        broker.fail_acks = False
        await consumer._dispatch_one(row("b-success", "target-b"))
        assert consumer.health().status == "degraded"

        await consumer._dispatch_one(row("a-recovery", "target-a"))
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
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


def test_register_broker_reads_policy_options(make_fake_app: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure

    expected_groups = {"fakeapp.orders.WidgetCreated": ["inventory", "billing"]}
    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            "no_subscriber_policy": "wait",
            "no_subscriber_wait_timeout_seconds": 2.5,
            "no_subscriber_wait_poll_interval_ms": 25,
            "orphan_replay_policy": "first_groups",
            "orphan_retention_seconds": 90,
            "expected_consumer_groups": expected_groups,
        },
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    broker = _runtime.broker_registry.get("database")
    assert isinstance(broker, DatabaseBroker)
    assert broker._no_subscriber_policy == "wait"
    assert broker._no_subscriber_wait_timeout_s == 2.5
    assert broker._no_subscriber_wait_poll_interval_s == 0.025
    assert broker._orphan_replay_policy == "first_groups"
    assert broker._orphan_retention_seconds == 90.0
    assert broker._expected_consumer_groups == expected_groups


def test_policy_env_options_override_broker_options(make_fake_app: Any, monkeypatch: Any) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure

    monkeypatch.setenv("MODULITH_BROKER_NO_SUBSCRIBER_POLICY", "wait")
    monkeypatch.setenv("MODULITH_BROKER_NO_SUBSCRIBER_WAIT_TIMEOUT_SECONDS", "3.5")
    monkeypatch.setenv("MODULITH_BROKER_NO_SUBSCRIBER_WAIT_POLL_INTERVAL_MS", "40")
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_REPLAY_POLICY", "expected_groups")
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_RETENTION_SECONDS", "120")
    monkeypatch.setenv(
        "MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS",
        '{"fakeapp.orders.WidgetCreated":["inventory"]}',
    )
    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            "no_subscriber_policy": "error",
            "no_subscriber_wait_timeout_seconds": 30,
            "no_subscriber_wait_poll_interval_ms": 100,
            "orphan_replay_policy": "ttl_all_groups",
            "orphan_retention_seconds": 86400,
            "expected_consumer_groups": {"ignored": ["ignored"]},
        },
    )
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    broker = _runtime.broker_registry.get("database")
    assert isinstance(broker, DatabaseBroker)
    assert broker._no_subscriber_policy == "wait"
    assert broker._no_subscriber_wait_timeout_s == 3.5
    assert broker._no_subscriber_wait_poll_interval_s == 0.04
    assert broker._orphan_replay_policy == "expected_groups"
    assert broker._orphan_retention_seconds == 120.0
    assert broker._expected_consumer_groups == {"fakeapp.orders.WidgetCreated": ["inventory"]}


@pytest.mark.parametrize(
    "invalid_option",
    [
        {"no_subscriber_policy": "drop"},
        {"no_subscriber_wait_timeout_seconds": 0},
        {"no_subscriber_wait_poll_interval_ms": 0},
        {"orphan_replay_policy": "all"},
        {"orphan_retention_seconds": 0},
        {"expected_consumer_groups": {"target": []}},
    ],
)
def test_register_broker_rejects_invalid_policy_config(
    make_fake_app: Any, invalid_option: dict[str, Any]
) -> None:
    make_fake_app({"orders": ""})
    from modulith import ConfigurationError, configure

    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            **invalid_option,
        },
    )

    with pytest.raises(ConfigurationError):
        _runtime.ensure_bootstrapped()


def test_expected_consumer_groups_env_requires_valid_json(
    make_fake_app: Any, monkeypatch: Any
) -> None:
    make_fake_app({"orders": ""})
    from modulith import ConfigurationError, configure

    monkeypatch.setenv("MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS", "not-json")
    configure(
        package="fakeapp",
        broker="database",
        broker_options={"url": "sqlite+aiosqlite:///:memory:"},
    )

    with pytest.raises(ConfigurationError, match="EXPECTED_CONSUMER_GROUPS"):
        _runtime.ensure_bootstrapped()


@pytest.mark.parametrize(
    "url",
    [
        "sqlite+aiosqlite://",
        "sqlite://",
        "sqlite+aiosqlite:///:memory:",
        "sqlite+aiosqlite:///file::memory:?cache=shared&uri=true",
        "sqlite+aiosqlite:///file:shared_mem?mode=memory&cache=shared&uri=true",
    ],
)
def test_process_topology_rejects_all_in_memory_sqlite_urls(make_fake_app: Any, url: str) -> None:
    make_fake_app({"orders": ""})
    from modulith import ConfigurationError, configure

    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": url},
    )

    with pytest.raises(
        ConfigurationError,
        match=r"in-memory SQLite.*topology='processes'",
    ):
        _runtime.ensure_bootstrapped()


@pytest.mark.parametrize(
    "url",
    [
        "sqlite+aiosqlite:///./broker.db",
        "sqlite+aiosqlite:////tmp/modulith-broker.db",
        "sqlite+aiosqlite:///file:broker.db?mode=rwc&uri=true",
    ],
)
def test_process_topology_allows_file_backed_sqlite_urls(make_fake_app: Any, url: str) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure

    configure(
        package="fakeapp",
        topology="processes",
        broker="database",
        broker_options={"url": url},
    )

    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    assert "database" in _runtime.broker_registry.schemes()


@pytest.mark.parametrize(
    "url",
    [
        "sqlite+aiosqlite://",
        "sqlite+aiosqlite:///:memory:",
        "sqlite+aiosqlite:///file::memory:?cache=shared&uri=true",
        "sqlite+aiosqlite:///file:shared_mem?mode=memory&cache=shared&uri=true",
    ],
)
def test_single_topology_allows_in_memory_sqlite_urls(make_fake_app: Any, url: str) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure

    configure(
        package="fakeapp",
        topology="single",
        broker="database",
        broker_options={"url": url},
    )

    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    assert "database" in _runtime.broker_registry.schemes()


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
    """No cadence/retention keys -> library defaults; prune ON via the 3-day
    terminal-row retention default (dead letters must not grow unboundedly)."""
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.adapters.db_broker import (
        _DEFAULT_BATCH_SIZE,
        _DEFAULT_POLL_INTERVAL_S,
        _DEFAULT_RECLAIM_STALE_S,
        _DEFAULT_RETENTION_AGE_S,
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
    assert consumer._retention_age_seconds == _DEFAULT_RETENTION_AGE_S
    assert consumer._prune_enabled() is True


@pytest.mark.parametrize(
    "invalid_option",
    [
        {"poll_interval_ms": 0},
        {"poll_interval_ms": True},
        {"reclaim_stale_seconds": float("inf")},
        {"reclaim_stale_seconds": True},
        {"batch_size": True},
        {"batch_size": 1.5},
        {"dispatch_concurrency": True},
        {"dispatch_concurrency": 1.5},
        {"max_delivery_attempts": True},
        {"retention_age_seconds": 0},
        {"retention_age_seconds": True},
        {"retention_count": True},
        {"prune_interval_seconds": -1},
        {"prune_interval_seconds": True},
    ],
)
def test_make_db_consumer_rejects_invalid_numeric_config(
    make_fake_app: Any, invalid_option: dict[str, Any]
) -> None:
    make_fake_app({"orders": ""})
    from modulith import ConfigurationError, configure

    configure(
        package="fakeapp",
        broker="database",
        broker_options={
            "url": "sqlite+aiosqlite:///:memory:",
            **invalid_option,
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

    with pytest.raises(ConfigurationError):
        _make_db_consumer(spec)


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
            "dispatch_concurrency": 7,
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
    assert consumer._dispatch_concurrency == 7



def test_registration_defaults_to_sqlite_file_url(
    make_fake_app: Any, monkeypatch: Any, tmp_path: Any
) -> None:
    """When topology='processes' with no broker_options.url, the registration
    hook auto-selects an embedded SQLite file ABSOLUTIZED from the process cwd
    (supervisor and workers must converge on ONE file; a relative URL would
    silently split the queue on a later chdir). The engine is lazy (no file
    created at this point), so only the URL string is checked."""
    monkeypatch.chdir(tmp_path)  # keep default broker file out of the repo
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.config import DEFAULT_BROKER_DB_FILENAME

    configure(package="fakeapp", topology="processes")  # no broker, no url
    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    broker = _runtime.broker_registry.get("database")
    assert isinstance(broker, DatabaseBroker)
    assert (
        str(broker.engine.url)
        == f"sqlite+aiosqlite:///{tmp_path / DEFAULT_BROKER_DB_FILENAME}"
    )


# ---------------------------------------------------------------------------
# MEDIUM-3: _ensure_schema tolerates a cross-process CREATE TABLE race
# ---------------------------------------------------------------------------


class _RaceConn:
    def __init__(self, error_text: str | None, *, persistent: bool = False) -> None:
        self._error_text = error_text
        self._persistent = persistent
        self.run_sync_calls = 0

    async def run_sync(self, _fn: Any) -> None:
        self.run_sync_calls += 1
        if self._error_text is not None and (self._persistent or self.run_sync_calls == 1):
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

    def __init__(self, *, error_text: str | None, persistent: bool = False) -> None:
        self._conn = _RaceConn(error_text, persistent=persistent)

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
    a second create_all pass verifies every required table before marking ready."""
    broker = DatabaseBroker(
        engine=_SchemaRaceEngine(error_text="table broker_message already exists")
    )
    await broker._ensure_schema()  # must not raise
    assert broker._engine._conn.run_sync_calls == 2
    assert broker._schema_ready is True


async def test_ensure_schema_never_marks_ready_when_reconciliation_fails() -> None:
    from sqlalchemy.exc import OperationalError

    broker = DatabaseBroker(
        engine=_SchemaRaceEngine(
            error_text="table broker_message already exists",
            persistent=True,
        )
    )

    with pytest.raises(OperationalError):
        await broker._ensure_schema()

    assert broker._engine._conn.run_sync_calls == 2
    assert broker._schema_ready is False


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


# ---------------------------------------------------------------------------
# CRITICAL-7: the poll loop survives a transient claim_batch failure
# MEDIUM-10:  the prune loop survives a transient prune failure
# ---------------------------------------------------------------------------


class _ClaimFlakyBroker(DatabaseBroker):
    """DatabaseBroker whose first ``fail_claims`` claim_batch calls raise, then
    delegate to the real implementation — to prove the poll loop backs off and
    retries rather than dying on a transient backend error."""

    fail_claims = 1

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.claim_calls = 0

    async def claim_batch(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.claim_calls += 1
        if self.claim_calls <= self.fail_claims:
            raise RuntimeError("transient claim failure")
        return await super().claim_batch(*args, **kwargs)


async def test_run_survives_claim_failure_and_retries(engine: Any) -> None:
    delivered: list[str] = []

    async def handler(evt: WidgetCreated) -> None:
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    broker = _ClaimFlakyBroker(engine=engine)
    target = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="c1",
        group="modulith-inventory",
        targets=[target],
        poll_interval_s=0.01,
    )
    await consumer.start()
    try:

        async def _degraded() -> bool:
            return consumer.health().status == "degraded"

        await _until_async(_degraded)
        assert consumer.health().detail == "transient claim failure"

        await broker.publish(
            target, serializer.serialize(WidgetCreated(name="w1")), {"event_type": target}
        )
        await _until_async(lambda: _delivered(delivered))
        assert delivered == ["w1"]
        assert broker.claim_calls >= 2  # failed once, retried, then delivered
        assert consumer.health() == ConsumerHealth(ready=True, status="ready")
    finally:
        await consumer.stop()


class _PruneFlakyBroker(DatabaseBroker):
    """DatabaseBroker whose first prune call raises, then no-ops — to prove the
    background prune loop logs and continues rather than dying."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.prune_calls = 0

    async def prune(self, **kwargs: Any) -> int:
        self.prune_calls += 1
        if self.prune_calls == 1:
            raise RuntimeError("transient prune failure")
        return 0


async def test_prune_loop_survives_prune_failure(engine: Any) -> None:
    broker = _PruneFlakyBroker(engine=engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=InMemoryEventBus(),
        serializer=JsonEventSerializer(),
        consumer_name="c1",
        group="g",
        targets=["t"],
        poll_interval_s=0.01,
        prune_interval_s=0.01,
        retention_count=100,  # a retention knob so the prune loop starts
    )
    await consumer.start()
    try:
        assert consumer._prune_task is not None  # prune loop is running

        async def _pruned_twice() -> bool:
            return broker.prune_calls >= 2

        await _until_async(_pruned_twice)  # survived the first failure, kept pruning
    finally:
        await consumer.stop()
