"""Integration tests for the database broker against real Postgres + MySQL.

The db_broker unit tests (test_db_broker.py) run on aiosqlite, which cannot
exercise the dialect-specific paths the adapter relies on in production:

  * ``FOR UPDATE SKIP LOCKED`` — SQLite has no row locking, so the
    competing-consumer partitioning is only real on Postgres/MySQL.
  * the by-count prune's window function + derived-table ``DELETE`` — MySQL
    rejects a subquery that references the delete target directly (error 1093),
    a hazard SQLite never surfaces.
  * the native upsert (``ON CONFLICT`` / ``ON DUPLICATE KEY``) under concurrent
    subscribers — the first-deploy replicated-module race the check-then-insert
    loop used to lose.

Every test runs on BOTH backends via the ``broker_engine`` fixture
(parametrized ``postgres`` / ``mysql``); each backend skips independently when
its container/URL is unavailable. This is the ground truth for "supports
mysql, postgres and sqlite".
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from modulith import event
from modulith.adapters.db_broker import DatabaseBroker, DatabaseConsumer, broker_schema
from modulith.event_bus import InMemoryEventBus
from modulith.serializers import JsonEventSerializer

pytestmark = [pytest.mark.integration]

# The broker routing key (target) is arbitrary; the ``event_type`` header must
# be the real FQN so the consumer's serializer can resolve the class on
# deserialize. They differ on purpose here to keep the two roles distinct.
_TARGET = "app.orders.WidgetCreated"


@event
@dataclass(frozen=True)
class WidgetCreated:
    name: str


_EVENT_TYPE = f"{WidgetCreated.__module__}.{WidgetCreated.__qualname__}"


@pytest.fixture(params=["postgres", "mysql"])
async def broker_engine(request: Any) -> Any:
    """A real server-DB async engine (Postgres or MySQL, parametrized) with a
    freshly-created broker schema, dropped again on teardown.

    Each param pulls its session-scoped URL fixture via ``getfixturevalue`` so
    an unavailable backend skips only its own parametrization rather than the
    whole test.
    """
    url = request.getfixturevalue(f"{request.param}_url")
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    metadata, _, _ = broker_schema()
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)
    try:
        yield engine
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await engine.dispose()


async def _row_count(engine: Any, table: Any) -> int:
    from sqlalchemy import func, select

    async with engine.connect() as conn:
        result = await conn.execute(select(func.count()).select_from(table))
        return int(result.scalar_one())


async def _all_ids(engine: Any) -> set[str]:
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(select(message.c.id))
        return {row[0] for row in result}


async def _insert(
    engine: Any,
    *,
    id: str,
    target: str,
    group: str,
    status: str,
    age_seconds: float,
) -> None:
    """Insert one broker_message row directly with a chosen status and a
    created_at ``age_seconds`` in the past (prune fixture)."""
    from sqlalchemy import insert

    _, _, message = broker_schema()
    created = datetime.now(UTC) - timedelta(seconds=age_seconds)
    async with engine.begin() as conn:
        await conn.execute(
            insert(message).values(
                id=id,
                target=target,
                consumer_group=group,
                event_type=_TARGET,
                payload=b"{}",
                headers=None,
                status=status,
                attempts=0,
                available_at=created,
                claimed_at=None,
                claimed_by=None,
                created_at=created,
                last_error=None,
            )
        )


async def _until(predicate: Any, *, timeout: float = 10.0, interval: float = 0.05) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await predicate():
            return
        await asyncio.sleep(interval)
    if not await predicate():
        raise AssertionError("condition not met within timeout")


# ---------------------------------------------------------------------------
# End-to-end roundtrip through the real consumer loop
# ---------------------------------------------------------------------------


async def test_roundtrip_publish_claim_dispatch_ack(broker_engine: Any) -> None:
    delivered: list[str] = []

    async def handler(evt: WidgetCreated) -> None:
        delivered.append(evt.name)

    bus = InMemoryEventBus()
    bus.register(WidgetCreated, handler)
    serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
    broker = DatabaseBroker(engine=broker_engine)
    consumer = DatabaseConsumer(
        broker=broker,
        bus=bus,
        serializer=serializer,
        consumer_name="inventory:1",
        group="modulith-inventory",
        targets=[_TARGET],
        poll_interval_s=0.05,
    )
    await consumer.start()
    try:
        payload = serializer.serialize(WidgetCreated(name="w1"))
        await broker.publish(_TARGET, payload, {"event_type": _EVENT_TYPE})
        await _until(lambda: _got(delivered))
        assert delivered == ["w1"]
    finally:
        await consumer.stop()


async def _got(delivered: list[str]) -> bool:
    return delivered == ["w1"]


# ---------------------------------------------------------------------------
# FOR UPDATE SKIP LOCKED — competing consumers partition the backlog
# ---------------------------------------------------------------------------


async def test_skip_locked_partitions_backlog_across_competing_consumers(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET], "g")
    serializer = JsonEventSerializer()
    n = 20
    for i in range(n):
        await broker.publish(
            _TARGET, serializer.serialize(WidgetCreated(name=f"w{i}")), {"event_type": _TARGET}
        )

    # Two consumers claim the whole backlog concurrently. SKIP LOCKED must let
    # each grab a disjoint slice instead of blocking or double-claiming.
    rows1, rows2 = await asyncio.gather(
        broker.claim_batch("g", batch_size=n, consumer_name="c1"),
        broker.claim_batch("g", batch_size=n, consumer_name="c2"),
    )
    ids1 = {r["id"] for r in rows1}
    ids2 = {r["id"] for r in rows2}

    assert ids1.isdisjoint(ids2)  # no row claimed by both
    assert len(ids1) + len(ids2) == n  # every row claimed exactly once


# ---------------------------------------------------------------------------
# Prune on the real dialect (window function + derived-table DELETE, age)
# ---------------------------------------------------------------------------


async def test_prune_by_count_keeps_newest_per_partition(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker._ensure_schema()
    for i, age in enumerate((40, 30, 20, 10)):
        await _insert(
            broker_engine, id=f"a{i}", target="A", group="g", status="done", age_seconds=age
        )
    for i, age in enumerate((25, 15, 5)):
        await _insert(
            broker_engine, id=f"b{i}", target="B", group="g", status="dead", age_seconds=age
        )
    await _insert(
        broker_engine, id="pending", target="A", group="g", status="pending", age_seconds=99
    )

    deleted = await broker.prune(retention_count=2)

    assert deleted == 3  # A: 4 -> 2, B: 3 -> 2
    assert await _all_ids(broker_engine) == {"a2", "a3", "b1", "b2", "pending"}


async def test_prune_by_age_deletes_old_terminal_only(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker._ensure_schema()
    await _insert(
        broker_engine, id="old_done", target="A", group="g", status="done", age_seconds=100
    )
    await _insert(
        broker_engine, id="old_dead", target="A", group="g", status="dead", age_seconds=100
    )
    await _insert(
        broker_engine, id="old_pending", target="A", group="g", status="pending", age_seconds=100
    )
    await _insert(broker_engine, id="new_done", target="A", group="g", status="done", age_seconds=1)

    deleted = await broker.prune(retention_age_seconds=50)

    assert deleted == 2
    assert await _all_ids(broker_engine) == {"old_pending", "new_done"}


# ---------------------------------------------------------------------------
# Native upsert — concurrent subscribers can't collide on the PK
# ---------------------------------------------------------------------------


async def test_subscribe_upsert_concurrent_no_collision(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    _, subscription, _ = broker_schema()

    # Three "workers" of the same replicated module registering the same
    # (target, group) at once. The check-then-insert loop this replaced would
    # race here — two see no row, both insert, one dies on the PK.
    await asyncio.gather(
        broker.subscribe([_TARGET], "g"),
        broker.subscribe([_TARGET], "g"),
        broker.subscribe([_TARGET], "g"),
    )

    assert await _row_count(broker_engine, subscription) == 1


# ---------------------------------------------------------------------------
# Crash recovery — an orphaned claim is reclaimed after the visibility timeout
# ---------------------------------------------------------------------------


async def test_reclaim_after_visibility_timeout(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET], "g")
    serializer = JsonEventSerializer()
    await broker.publish(
        _TARGET, serializer.serialize(WidgetCreated(name="w1")), {"event_type": _TARGET}
    )

    first = await broker.claim_batch(
        "g", batch_size=10, consumer_name="c1", reclaim_stale_seconds=100.0
    )
    assert len(first) == 1

    within = await broker.claim_batch(
        "g", batch_size=10, consumer_name="c2", reclaim_stale_seconds=100.0
    )
    assert within == []  # still inside the window — not handed out again

    await asyncio.sleep(0.2)
    reclaimed = await broker.claim_batch(
        "g", batch_size=10, consumer_name="c2", reclaim_stale_seconds=0.05
    )
    assert len(reclaimed) == 1
    assert reclaimed[0]["id"] == first[0]["id"]


# ---------------------------------------------------------------------------
# MEDIUM-5: skew-sensitive timestamps come from the DB server clock
# ---------------------------------------------------------------------------


def _aware(dt: datetime) -> datetime:
    """Read-back timestamps are tz-aware on Postgres (timestamptz) but naive on
    MySQL (DATETIME); normalize to UTC-aware for comparison."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


async def test_now_uses_db_server_clock_and_is_tz_aware(broker_engine: Any) -> None:
    """``_now`` must execute the dialect-correct query (``now()`` on Postgres,
    ``UTC_TIMESTAMP(6)`` on MySQL) against the real driver and return a
    tz-aware instant on the DB's own clock — the foundation of skew immunity."""
    from sqlalchemy import text

    broker = DatabaseBroker(engine=broker_engine)
    dialect = broker_engine.dialect.name
    db_now_sql = "SELECT now()" if dialect == "postgresql" else "SELECT UTC_TIMESTAMP(6)"

    async with broker_engine.connect() as conn:
        got = await broker._now(conn)
        raw = (await conn.execute(text(db_now_sql))).scalar_one()

    assert got.tzinfo is not None  # tz-aware on BOTH dialects
    assert abs((got - _aware(raw)).total_seconds()) < 5.0  # the DB's clock, not the app's


async def test_publish_and_claim_stamp_from_server_clock(broker_engine: Any) -> None:
    """The producer (``created_at``/``available_at``) and the claim
    (``claimed_at``) stamp from the DB clock — assert each lands within a small
    window of the DB's own ``_now``, proving server-clock sourcing is wired
    through ``publish`` and ``claim_batch``, not just the helper."""
    from sqlalchemy import select

    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET], "g")
    serializer = JsonEventSerializer()
    await broker.publish(
        _TARGET, serializer.serialize(WidgetCreated(name="w1")), {"event_type": _TARGET}
    )
    rows = await broker.claim_batch("g", batch_size=10, consumer_name="c1")
    assert len(rows) == 1

    _, _, message = broker_schema()
    async with broker_engine.connect() as conn:
        ref = await broker._now(conn)
        row = (
            await conn.execute(
                select(message.c.created_at, message.c.available_at, message.c.claimed_at).where(
                    message.c.id == rows[0]["id"]
                )
            )
        ).first()

    assert row is not None
    assert abs((_aware(row.created_at) - ref).total_seconds()) < 10.0
    assert abs((_aware(row.available_at) - ref).total_seconds()) < 10.0
    assert abs((_aware(row.claimed_at) - ref).total_seconds()) < 10.0
