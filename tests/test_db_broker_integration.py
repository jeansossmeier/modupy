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
from typing import Any, cast
from uuid import uuid4

import pytest

from modulith import event
from modulith.adapters.db_broker import (
    DatabaseBroker,
    DatabaseConsumer,
    _postgres_target_lock_key,
    broker_schema,
)
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
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import CreateSchema, DropSchema

    if request.param == "mysql":
        database = make_url(url).database
        assert database is not None and database.startswith("modupy_test_")

    admin_engine = create_async_engine(url)
    schema = f"modupy_broker_{uuid4().hex}" if request.param == "postgres" else None
    engine = (
        admin_engine.execution_options(schema_translate_map={None: schema})
        if schema is not None
        else admin_engine
    )
    metadata, _, _ = broker_schema()
    if schema is not None:
        try:
            async with admin_engine.begin() as conn:
                await conn.execute(CreateSchema(schema))
        except SQLAlchemyError as exc:
            await admin_engine.dispose()
            pytest.skip(
                "PostgreSQL broker tests require CREATE SCHEMA privilege to protect "
                f"the supplied database ({type(exc).__name__})"
            )
    else:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield engine
    finally:
        try:
            if schema is not None:
                async with admin_engine.begin() as conn:
                    await conn.execute(DropSchema(schema, if_exists=True, cascade=True))
            else:
                async with engine.begin() as conn:
                    await conn.run_sync(metadata.drop_all)
        finally:
            await engine.dispose()
            if admin_engine is not engine:
                await admin_engine.dispose()


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


async def test_postgres_concurrent_first_bootstrap_creates_complete_schema(
    postgres_url: str,
) -> None:
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import DropSchema

    schema = f"modupy_bootstrap_{uuid4().hex}"
    metadata, _, _ = broker_schema()
    expected_tables = {table.name for table in metadata.sorted_tables}
    brokers = [
        DatabaseBroker(url=postgres_url, engine_options={"schema": schema}) for _ in range(8)
    ]
    admin_engine = create_async_engine(postgres_url)

    try:
        results = await asyncio.gather(
            *(
                broker.subscribe([f"app.orders.Event{index}"], f"group-{index}")
                for index, broker in enumerate(brokers)
            ),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]

        assert failures == []
        async with admin_engine.connect() as conn:
            tables = set(
                await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).get_table_names(schema=schema)
                )
            )
        assert tables == expected_tables
    finally:
        await asyncio.gather(*(broker.close() for broker in brokers))
        async with admin_engine.begin() as conn:
            await conn.execute(DropSchema(schema, if_exists=True, cascade=True))
        await admin_engine.dispose()


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


async def _message_row(engine: Any, row_id: str) -> Any:
    """Return ``(status, attempts, available_at)`` for one message row."""
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(
            select(message.c.status, message.c.attempts, message.c.available_at).where(
                message.c.id == row_id
            )
        )
        return result.first()


async def _subscription_updated_at(engine: Any, target: str, group: str) -> datetime:
    from sqlalchemy import select

    _, subscription, _ = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(
            select(subscription.c.updated_at).where(
                subscription.c.target == target, subscription.c.consumer_group == group
            )
        )
        return cast(datetime, result.scalar_one())


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


async def test_schema_engine_option_isolates_broker_tables_on_postgres(
    postgres_url: str,
) -> None:
    """schema routes the broker's tables into a dedicated Postgres schema,
    leaving public untouched, while publish/claim/dispatch still round-trips
    end to end through that schema."""
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import CreateSchema, DropSchema

    schema = f"mod_broker_{uuid4().hex}"
    sentinel_schema = f"mod_sentinel_{uuid4().hex}"
    expected_tables = {
        "broker_message",
        "broker_retained_delivery",
        "broker_retained_message",
        "broker_subscription",
    }
    admin_engine = create_async_engine(postgres_url)

    def _table_names(sync_conn: Any, schema_name: str) -> set[str]:
        return set(inspect(sync_conn).get_table_names(schema=schema_name))

    async with admin_engine.begin() as conn:
        await conn.execute(CreateSchema(sentinel_schema))
    try:
        broker = DatabaseBroker(url=postgres_url, engine_options={"schema": schema})
        try:
            await broker._ensure_schema()

            async with admin_engine.connect() as conn:
                scoped = await conn.run_sync(_table_names, schema)
                public = await conn.run_sync(_table_names, "public")
            assert scoped == expected_tables
            assert public.isdisjoint(expected_tables)

            await asyncio.gather(
                broker.subscribe([_TARGET], "modulith-inventory"),
                broker.subscribe([_TARGET], "modulith-inventory"),
                broker.subscribe([_TARGET], "modulith-inventory"),
            )
            _, subscription, _ = broker_schema()
            assert await _row_count(broker.engine, subscription) == 1

            delivered: list[str] = []

            async def handler(evt: WidgetCreated) -> None:
                delivered.append(evt.name)

            bus = InMemoryEventBus()
            bus.register(WidgetCreated, handler)
            serializer = JsonEventSerializer(allowed_event_types=[WidgetCreated])
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
        finally:
            await broker.close()
    finally:
        try:
            async with admin_engine.begin() as conn:
                await conn.execute(DropSchema(schema, if_exists=True, cascade=True))
            async with admin_engine.connect() as conn:
                schemas = await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).get_schema_names()
                )
            assert sentinel_schema in schemas
        finally:
            async with admin_engine.begin() as conn:
                await conn.execute(DropSchema(sentinel_schema, if_exists=True, cascade=True))
            await admin_engine.dispose()


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


async def test_overlapping_claims_split_a_backlog_larger_than_the_batch(
    broker_engine: Any,
) -> None:
    """While one consumer's claim transaction is still open, a second claim of
    the same group, and a claim of another group, each still get a full batch.
    InnoDB locks every row an unindexed OR / ORDER BY scan visits, so a claim
    that scans the whole backlog starves the overlapping one on MySQL."""
    from sqlalchemy import text

    _, _, message = broker_schema()
    groups = ["g", "other"]
    base = datetime.now(UTC) - timedelta(hours=1)
    async with broker_engine.begin() as conn:
        for group in groups:
            await conn.execute(
                message.insert(),
                [
                    {
                        "id": str(uuid4()),
                        "target": _TARGET,
                        "consumer_group": group,
                        "event_type": _EVENT_TYPE,
                        "payload": b"{}",
                        "headers": None,
                        "status": "pending",
                        "attempts": 0,
                        "available_at": base + timedelta(milliseconds=i),
                        "claimed_at": None,
                        "claimed_by": None,
                        "created_at": base,
                        "last_error": None,
                        "dispatch_started": False,
                    }
                    for i in range(3000)
                ],
            )
    if broker_engine.dialect.name == "mysql":
        async with broker_engine.connect() as conn:
            await conn.execute(text("ANALYZE TABLE broker_message"))

    first_claimed, release = asyncio.Event(), asyncio.Event()

    class HoldingBroker(DatabaseBroker):
        async def _write(self, operation: Any) -> Any:
            async def held(conn: Any) -> Any:
                result = await operation(conn)
                first_claimed.set()
                await release.wait()
                return result

            return await super()._write(held)

    holder, other = HoldingBroker(engine=broker_engine), DatabaseBroker(engine=broker_engine)
    await holder._ensure_schema()
    first = asyncio.create_task(holder.claim_batch("g", batch_size=10, consumer_name="c1"))
    try:
        await asyncio.wait_for(first_claimed.wait(), 30)
        overlapping = [
            asyncio.create_task(other.claim_batch(group, batch_size=10, consumer_name="c2"))
            for group in groups
        ]
        # Their SELECTs run while the first claim's locks are held; the
        # row UPDATEs may then wait for that commit, so release it after a
        # pause rather than after they return.
        await asyncio.sleep(1.0)
    finally:
        release.set()
    rows = await asyncio.wait_for(first, 30)
    same_group, other_group = await asyncio.wait_for(asyncio.gather(*overlapping), 30)

    assert (len(rows), len(same_group), len(other_group)) == (10, 10, 10)
    assert {r["id"] for r in rows}.isdisjoint({r["id"] for r in same_group})


@pytest.mark.parametrize("broker_engine", ["postgres"], indirect=True)
async def test_postgres_claim_orders_a_stale_claim_ahead_of_newer_pending_rows(
    broker_engine: Any,
) -> None:
    """Postgres keeps one select ordered by ``available_at``: an orphaned claim
    older than a full batch of pending rows is reclaimed in that batch instead
    of waiting for the backlog to drain."""
    _, _, message = broker_schema()
    long_ago = datetime.now(UTC) - timedelta(hours=2)
    recent = datetime.now(UTC) - timedelta(hours=1)

    def row(row_id: str, status: str, available_at: datetime) -> dict[str, Any]:
        claimed = status == "claimed"
        return {
            "id": row_id,
            "target": _TARGET,
            "consumer_group": "g",
            "event_type": _EVENT_TYPE,
            "payload": b"{}",
            "headers": None,
            "status": status,
            "attempts": 0,
            "available_at": available_at,
            "claimed_at": long_ago if claimed else None,
            "claimed_by": "crashed" if claimed else None,
            "created_at": long_ago,
            "last_error": None,
            "dispatch_started": False,
        }

    async with broker_engine.begin() as conn:
        await conn.execute(
            message.insert(),
            [row("stale", "claimed", long_ago)]
            + [row(f"p{i}", "pending", recent + timedelta(seconds=i)) for i in range(5)],
        )

    broker = DatabaseBroker(engine=broker_engine)
    rows = await broker.claim_batch(
        "g", batch_size=5, consumer_name="c1", reclaim_stale_seconds=60.0
    )

    assert [r["id"] for r in rows] == ["stale", "p0", "p1", "p2", "p3"]


async def test_target_filtered_claim_skips_locked_rows_only_within_its_targets(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET, "t.Stale"], "g")
    for i in range(4):
        await broker.publish("t.Stale", b"stale", {"event_type": "t.Stale"})
        await broker.publish(_TARGET, f"live{i}".encode(), {"event_type": _TARGET})

    rows1, rows2 = await asyncio.gather(
        broker.claim_batch("g", batch_size=10, consumer_name="c1", targets=[_TARGET]),
        broker.claim_batch("g", batch_size=10, consumer_name="c2", targets=[_TARGET]),
    )

    claimed = rows1 + rows2
    assert {row["target"] for row in claimed} == {_TARGET}
    assert len({row["id"] for row in claimed}) == len(claimed) == 4
    assert await broker.stale_targets("g", [_TARGET]) == {"t.Stale": 4}
    assert await broker.drop_group("g", targets=["t.Stale"]) == (1, 4)
    assert await broker.stale_targets("g", [_TARGET]) == {}


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


async def test_store_concurrent_reregistration_replays_once(broker_engine: Any) -> None:
    broker = DatabaseBroker(
        engine=broker_engine,
        no_subscriber_policy="store",
    )
    target = f"{_TARGET}.reregister"
    await broker.publish(target, b"payload", {"event_type": _EVENT_TYPE})

    await asyncio.gather(
        broker.subscribe([target], "g"),
        broker.subscribe([target], "g"),
        broker.subscribe([target], "g"),
    )

    rows = await broker.claim_batch("g", batch_size=10, consumer_name="c1")
    assert len(rows) == 1


async def test_store_publish_subscribe_race_never_misses_delivery(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(
        engine=broker_engine,
        no_subscriber_policy="store",
    )

    for index in range(12):
        target = f"{_TARGET}.race-{index}"
        group = f"g-{index}"
        await asyncio.gather(
            broker.publish(target, b"payload", {"event_type": _EVENT_TYPE}),
            broker.subscribe([target], group),
        )

        rows = await broker.claim_batch(group, batch_size=10, consumer_name="c1")
        assert len(rows) == 1, f"publish/subscribe race lost {target}"


async def test_publish_during_a_slow_restart_replay_is_not_blocked_for_the_whole_replay(
    broker_engine: Any,
    monkeypatch: Any,
) -> None:
    import modulith.adapters.db_broker as db_broker_module

    monkeypatch.setattr(db_broker_module, "_RETAINED_ID_CHUNK", 2)
    broker = DatabaseBroker(engine=broker_engine, no_subscriber_policy="store")
    _, subscription, _ = broker_schema()
    target = f"{_TARGET}.slow-replay"
    backlog = [f"backlog-{index}".encode() for index in range(8)]
    for payload in backlog:
        await broker.publish(target, payload, {"event_type": _EVENT_TYPE})

    events: list[str] = []
    first_page_running = asyncio.Event()
    fan_out = broker._fan_out_retained

    async def slow_page(conn: Any, sources: list[Any], groups: list[str]) -> int:
        if len(sources) == 1:  # a publish's own fan-out, not a replay page
            return int(await fan_out(conn, sources, groups))
        written = int(await fan_out(conn, sources, groups))
        first_page_running.set()
        await asyncio.sleep(0.5)
        events.append("page-done")
        return written

    monkeypatch.setattr(broker, "_fan_out_retained", slow_page)

    replay = asyncio.create_task(broker.subscribe([target], "g"))
    await asyncio.wait_for(first_page_running.wait(), timeout=20)
    assert await _row_count(broker_engine, subscription) == 1  # committed before the pages

    await asyncio.sleep(0.2)  # the publish queues behind the page that holds the lock
    await asyncio.wait_for(
        broker.publish(target, b"live", {"event_type": _EVENT_TYPE}),
        timeout=20,
    )
    events.append("publish-done")
    await asyncio.wait_for(replay, timeout=60)

    assert events.count("page-done") == 4
    assert events[-1] == "page-done", f"the publish waited for the whole replay: {events}"
    rows = await broker.claim_batch("g", batch_size=20, consumer_name="c1")
    assert sorted(bytes(row["payload"]) for row in rows) == sorted([*backlog, b"live"])


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


async def test_release_claims_hands_back_only_unstarted_claims_on_the_real_dialect(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET], "g")
    serializer = JsonEventSerializer()
    for name in ("w1", "w2"):
        await broker.publish(
            _TARGET, serializer.serialize(WidgetCreated(name=name)), {"event_type": _TARGET}
        )
    [started, unstarted] = [
        row["id"] for row in await broker.claim_batch("g", batch_size=10, consumer_name="c1")
    ]
    await broker.renew_claims([started], consumer_name="c1", start_dispatch=True)

    assert await broker.release_claims([started, unstarted], consumer_name="c1") == 1

    peer = await broker.claim_batch(
        "g", batch_size=10, consumer_name="c2", reclaim_stale_seconds=3600.0
    )
    assert [(row["id"], row["attempts"]) for row in peer] == [(unstarted, 0)]


# ---------------------------------------------------------------------------
# HIGH-8: the subscription upsert's DO UPDATE refreshes updated_at (real DDL)
# ---------------------------------------------------------------------------


async def test_subscribe_upsert_refreshes_updated_at(broker_engine: Any) -> None:
    """A re-subscribe must take the ON CONFLICT DO UPDATE / ON DUPLICATE KEY
    UPDATE branch and refresh ``updated_at`` (not silently DO NOTHING) — proven
    on the real dialect, where the unit suite's SQLite can't exercise the
    MySQL ``ON DUPLICATE KEY`` form."""
    broker = DatabaseBroker(engine=broker_engine)
    _, subscription, _ = broker_schema()

    await broker.subscribe([_TARGET], "g")
    first = await _subscription_updated_at(broker_engine, _TARGET, "g")

    await asyncio.sleep(0.01)  # guarantee a strictly later microsecond stamp
    await broker.subscribe([_TARGET], "g")  # re-subscribe -> DO UPDATE
    second = await _subscription_updated_at(broker_engine, _TARGET, "g")

    assert await _row_count(broker_engine, subscription) == 1  # not duplicated
    # timestamps are tz-aware on Postgres, naive on MySQL — normalize.
    first_aware = first if first.tzinfo is not None else first.replace(tzinfo=UTC)
    second_aware = second if second.tzinfo is not None else second.replace(tzinfo=UTC)
    assert second_aware > first_aware  # DO UPDATE fired, updated_at refreshed


# ---------------------------------------------------------------------------
# MEDIUM-9: mark completion / dead-letter / fail+backoff on the real dialect
# ---------------------------------------------------------------------------


async def _publish_and_claim(broker: Any, consumer_name: str = "c1") -> str:
    """Publish one message and claim it, returning its row id."""
    await broker.subscribe([_TARGET], "g")
    serializer = JsonEventSerializer()
    await broker.publish(
        _TARGET, serializer.serialize(WidgetCreated(name="w1")), {"event_type": _TARGET}
    )
    rows = await broker.claim_batch("g", batch_size=10, consumer_name=consumer_name)
    assert len(rows) == 1
    return cast(str, rows[0]["id"])


async def test_completion_mode_mark_keeps_row_done(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine, completion_mode="mark")
    rid = await _publish_and_claim(broker)

    await broker.ack(rid, consumer_name="c1")

    row = await _message_row(broker_engine, rid)
    assert row is not None and row.status == "done"  # kept, marked done (not deleted)


async def test_dead_letter_marks_row_dead(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    rid = await _publish_and_claim(broker)

    await broker.dead_letter(rid, "poison", consumer_name="c1")

    row = await _message_row(broker_engine, rid)
    assert row is not None and row.status == "dead"


async def test_fail_increments_attempts_backs_off_then_dead(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    rid = await _publish_and_claim(broker)

    # First failure: below the cap -> pending, attempts=1, redelivery delayed.
    await broker.fail(rid, "boom", consumer_name="c1", max_attempts=2)
    async with broker_engine.connect() as conn:
        ref = await broker._now(conn)
    row = await _message_row(broker_engine, rid)
    assert row is not None and row.status == "pending" and row.attempts == 1
    avail = (
        row.available_at
        if row.available_at.tzinfo is not None
        else row.available_at.replace(tzinfo=UTC)
    )
    assert avail > ref  # backoff (server-clock) pushed redelivery into the future

    # After the backoff window, reclaim and fail again -> hits the cap -> dead.
    await asyncio.sleep(0.2)
    again = await broker.claim_batch("g", batch_size=10, consumer_name="c1")
    assert len(again) == 1 and again[0]["id"] == rid
    await broker.fail(rid, "boom again", consumer_name="c1", max_attempts=2)

    row = await _message_row(broker_engine, rid)
    assert row is not None and row.status == "dead" and row.attempts == 2


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


async def test_publish_from_the_sync_api_loop_uses_the_engine_owning_loop(
    broker_engine: Any,
) -> None:
    """``publish_sync`` runs ``publish`` on the sync API's daemon-thread loop
    while the worker's consumer has filled the pool from the app loop. asyncpg
    and aiomysql connections are bound to the loop that opened them, so the
    daemon-loop publish must never check one of them out on its own loop."""
    from modulith.sync import _get_or_create_loop

    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET], "g")
    await broker.claim_batch("g", batch_size=10, consumer_name="app")
    payload = JsonEventSerializer().serialize(WidgetCreated(name="w"))

    def publish_from_sync_view() -> None:
        future = asyncio.run_coroutine_threadsafe(
            broker.publish(_TARGET, payload, {"event_type": _EVENT_TYPE}), _get_or_create_loop()
        )
        future.result(timeout=10)

    for _ in range(3):
        await asyncio.to_thread(publish_from_sync_view)
        rows = await broker.claim_batch("g", batch_size=10, consumer_name="app")
        assert [row["payload"] for row in rows] == [payload]


async def test_calls_succeed_after_the_owning_loop_closed_and_this_loop_took_over(
    broker_engine: Any,
) -> None:
    """The first loop's pooled connections are bound to it; once it has closed,
    the loop that takes over must not check one of them out."""
    await broker_engine.dispose()
    broker = DatabaseBroker(engine=broker_engine)
    payload = JsonEventSerializer().serialize(WidgetCreated(name="w"))

    async def first_owner() -> None:
        await broker.subscribe([_TARGET], "g")
        await broker.publish(_TARGET, payload, {"event_type": _EVENT_TYPE})

    await asyncio.to_thread(asyncio.run, first_owner())

    for _ in range(2):
        await broker.publish(_TARGET, payload, {"event_type": _EVENT_TYPE})
    rows = await broker.claim_batch("g", batch_size=10, consumer_name="taker")
    assert [row["payload"] for row in rows] == [payload] * 3


@pytest.mark.parametrize("broker_engine", ["mysql"], indirect=True)
@pytest.mark.parametrize(
    ("reported", "version", "minimum"),
    [
        ("5.7.44-log", (5, 7, 44), "8.0.1"),
        ("10.5.29-MariaDB-1:10.5.29+maria~ubu2004", (10, 5, 29), "10.6"),
    ],
)
async def test_mysql_server_without_skip_locked_is_rejected_at_startup(
    broker_engine: Any,
    reported: str,
    version: tuple[int, ...],
    minimum: str,
) -> None:
    from sqlalchemy import event

    from modulith import ConfigurationError

    def report_an_older_server(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> tuple[str, Any]:
        if statement == "SELECT VERSION()":
            return f"SELECT '{reported}'", parameters
        return statement, parameters

    sync_engine = broker_engine.sync_engine
    event.listen(sync_engine, "before_cursor_execute", report_an_older_server, retval=True)
    try:
        broker = DatabaseBroker(engine=broker_engine)
        with pytest.raises(ConfigurationError) as excinfo:
            await broker.subscribe([_TARGET], "g")
    finally:
        event.remove(sync_engine, "before_cursor_execute", report_an_older_server)

    message = str(excinfo.value)
    assert ".".join(map(str, version)) in message
    assert minimum in message


# ---------------------------------------------------------------------------
# Group retirement: liveness, sole subscribers and the schema probe
# ---------------------------------------------------------------------------


async def _age_subscriptions(engine: Any, *, days: int) -> None:
    from sqlalchemy import update

    _, subscription, _ = broker_schema()
    async with engine.begin() as conn:
        await conn.execute(
            update(subscription).values(updated_at=datetime.now(UTC) - timedelta(days=days))
        )


async def test_active_groups_reads_fresh_subscriptions_only(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    for group in ("g-subscribed", "g-claiming", "g-aged"):
        await broker.subscribe([_TARGET], group)
    await broker.publish(_TARGET, b"x", {"event_type": _EVENT_TYPE})
    assert await broker.claim_batch("g-claiming", batch_size=1, consumer_name="c1")
    await _age_subscriptions(broker_engine, days=30)
    await broker.subscribe([_TARGET], "g-subscribed")

    assert await broker.active_groups(within_seconds=3600) == {"g-subscribed"}


async def test_touch_subscriptions_skips_rows_a_concurrent_transaction_holds_locked(
    broker_engine: Any,
) -> None:
    from sqlalchemy import select

    broker = DatabaseBroker(engine=broker_engine)
    held, free = f"{_TARGET}.held", f"{_TARGET}.free"
    await broker.subscribe([held, free], "g")
    await _age_subscriptions(broker_engine, days=30)
    _, subscription, _ = broker_schema()

    # A replica's subscribe holds its upserted rows until it commits.
    async with broker_engine.connect() as holder:
        await holder.execute(
            select(subscription.c.target)
            .where(subscription.c.target == held, subscription.c.consumer_group == "g")
            .with_for_update()
        )
        await asyncio.wait_for(broker.touch_subscriptions([held, free], "g"), timeout=1.0)
        await holder.rollback()

    touched = _aware(await _subscription_updated_at(broker_engine, free, "g"))
    skipped = _aware(await _subscription_updated_at(broker_engine, held, "g"))
    assert touched > datetime.now(UTC) - timedelta(hours=1)
    assert skipped < datetime.now(UTC) - timedelta(days=1)


async def test_touch_subscriptions_brings_an_aged_group_back_into_the_window(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe([_TARGET, "t.Other"], "g")
    await _age_subscriptions(broker_engine, days=30)
    assert await broker.active_groups(within_seconds=3600) == set()

    await broker.touch_subscriptions([_TARGET], "g")

    assert await broker.active_groups(within_seconds=3600) == {"g"}
    touched = _aware(await _subscription_updated_at(broker_engine, _TARGET, "g"))
    untouched = _aware(await _subscription_updated_at(broker_engine, "t.Other", "g"))
    assert untouched < touched - timedelta(days=1)


async def test_sole_subscriber_targets_on_the_real_dialect(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    await broker.subscribe(["t.Only", "t.Shared", "t.Also"], "g-retired")
    await broker.subscribe(["t.Shared"], "g-orders")

    assert await broker.sole_subscriber_targets("g-retired") == ["t.Also", "t.Only"]
    assert await broker.sole_subscriber_targets("g-orders") == []


async def _message_states(engine: Any) -> dict[str, tuple[str, str]]:
    """Map every broker_message id to its ``(consumer_group, status)``."""
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with engine.connect() as conn:
        result = await conn.execute(
            select(message.c.id, message.c.consumer_group, message.c.status)
        )
        return {row[0]: (row[1], row[2]) for row in result}


async def test_drop_group_removes_a_groups_undelivered_state_on_the_real_dialect(
    broker_engine: Any,
) -> None:
    from sqlalchemy import select

    broker = DatabaseBroker(engine=broker_engine, completion_mode="mark")
    await broker.subscribe([_TARGET, "t.Other"], "g")
    await broker.subscribe([_TARGET], "g-kept")
    for target in (_TARGET, "t.Other"):
        for _ in range(3):
            await broker.publish(target, b"x", {"event_type": _EVENT_TYPE})
    claimed = [row["id"] for row in await broker.claim_batch("g", batch_size=4, consumer_name="c1")]
    assert len(claimed) == 4
    done_id, dead_id = claimed[:2]
    await broker.ack(done_id, consumer_name="c1")
    await broker.dead_letter(dead_id, "boom", consumer_name="c1")
    kept = {
        row_id: state
        for row_id, state in (await _message_states(broker_engine)).items()
        if state[0] == "g-kept"
    }
    assert len(kept) == 3
    assert await broker.group_backlog() == {"g": 4, "g-kept": 3}

    assert await broker.drop_group("g") == (2, 4)

    assert await broker.group_backlog() == {"g-kept": 3}
    assert await _message_states(broker_engine) == {
        done_id: ("g", "done"),
        dead_id: ("g", "dead"),
        **kept,
    }
    _, subscription, _ = broker_schema()
    async with broker_engine.connect() as conn:
        result = await conn.execute(select(subscription.c.target, subscription.c.consumer_group))
        assert {tuple(row) for row in result} == {(_TARGET, "g-kept")}


async def test_group_backlog_with_targets_counts_what_drop_group_with_those_targets_deletes_on_the_real_dialect(
    broker_engine: Any,
) -> None:
    broker = DatabaseBroker(engine=broker_engine, completion_mode="mark")
    await broker.subscribe([_TARGET, "t.Other"], "g")
    await broker.subscribe([_TARGET], "g-kept")
    for target in (_TARGET, "t.Other"):
        for _ in range(3):
            await broker.publish(target, b"x", {"event_type": _EVENT_TYPE})
    claimed = [
        row["id"]
        for row in await broker.claim_batch(
            "g", batch_size=3, consumer_name="c1", targets=[_TARGET]
        )
    ]
    assert len(claimed) == 3
    done_id, dead_id = claimed[:2]
    await broker.ack(done_id, consumer_name="c1")
    await broker.dead_letter(dead_id, "boom", consumer_name="c1")

    assert await broker.group_backlog() == {"g": 4, "g-kept": 3}
    assert await broker.group_backlog(targets=[_TARGET]) == {"g": 1, "g-kept": 3}
    assert await broker.group_backlog(targets=("t.Other",)) == {"g": 3}
    assert await broker.group_backlog(targets=["t.none"]) == {}
    assert await broker.group_backlog(targets=[]) == {}

    counted = (await broker.group_backlog(targets=[_TARGET]))["g"]
    assert await broker.drop_group("g", targets=[_TARGET]) == (1, counted)
    assert await broker.group_backlog() == {"g": 3, "g-kept": 3}


async def test_has_schema_reports_whether_the_broker_tables_exist(broker_engine: Any) -> None:
    broker = DatabaseBroker(engine=broker_engine)
    assert await broker.has_schema() is True

    metadata, _, _ = broker_schema()
    async with broker_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)

    assert await DatabaseBroker(engine=broker_engine).has_schema() is False


# ---------------------------------------------------------------------------
# Per-target write lock — serialization on real Postgres (pg_advisory_xact_lock)
# ---------------------------------------------------------------------------


@pytest.fixture
async def pg_broker(postgres_url: str) -> Any:
    """A ``store``-policy broker on its own Postgres schema, dropped on teardown."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import DropSchema

    schema = f"modupy_lock_{uuid4().hex}"
    broker = DatabaseBroker(
        url=postgres_url,
        engine_options={"schema": schema},
        no_subscriber_policy="store",
    )
    await broker._ensure_schema()
    admin_engine = create_async_engine(postgres_url)
    try:
        yield broker
    finally:
        await broker.close()
        async with admin_engine.begin() as conn:
            await conn.execute(DropSchema(schema, if_exists=True, cascade=True))
        await admin_engine.dispose()


async def _advisory_waiters(broker: DatabaseBroker) -> list[int]:
    """Signed advisory-lock keys that some backend of this database is waiting on."""
    from sqlalchemy import text

    async with broker._engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT classid::bigint, objid::bigint FROM pg_locks "
                "WHERE locktype = 'advisory' AND NOT granted "
                "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        )
        keys = []
        for classid, objid in result:
            unsigned = (classid << 32) | objid
            keys.append(unsigned - (1 << 64) if unsigned >= 1 << 63 else unsigned)
        return sorted(keys)


async def test_target_lock_blocks_same_target_callers_but_not_other_targets(
    pg_broker: DatabaseBroker,
) -> None:
    target, other = f"{_TARGET}.lock-held", f"{_TARGET}.lock-free"
    holding, release = asyncio.Event(), asyncio.Event()
    events: list[str] = []
    active = 0
    max_active = 0

    async def hold(conn: Any) -> None:
        holding.set()
        await release.wait()
        events.append("holder-done")

    def contender(name: str) -> Any:
        async def op(conn: Any) -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            events.append(f"{name}-entered")
            await asyncio.sleep(0.05)
            active -= 1

        return op

    holder = asyncio.create_task(pg_broker._write_target_locked([target], hold))
    await asyncio.wait_for(holding.wait(), timeout=10)
    waiters = [
        asyncio.create_task(pg_broker._write_target_locked([target], contender(f"w{index}")))
        for index in range(3)
    ]
    key = _postgres_target_lock_key(target)

    async def three_blocked() -> bool:
        return await _advisory_waiters(pg_broker) == [key] * 3

    await _until(three_blocked)
    await asyncio.wait_for(pg_broker._write_target_locked([other], contender("other")), timeout=10)

    assert events == ["other-entered"]  # same-target callers still parked on the lock

    release.set()
    await asyncio.wait_for(asyncio.gather(holder, *waiters), timeout=20)

    assert events[:2] == ["other-entered", "holder-done"]
    assert sorted(events[2:]) == ["w0-entered", "w1-entered", "w2-entered"]
    assert max_active == 1
    assert await _advisory_waiters(pg_broker) == []


async def test_target_lock_overlapping_target_sets_serialize_without_deadlock(
    pg_broker: DatabaseBroker,
) -> None:
    a, b, c = (f"{_TARGET}.multi-{name}" for name in "abc")
    active: dict[str, int] = dict.fromkeys((a, b, c), 0)
    overlaps: list[str] = []
    # Opposite orderings of the same pair would deadlock without sorted acquisition.
    name_targets = {"ab": [a, b], "ba": [b, a], "bca": [b, c, a], "c": [c]}

    def op_over(name: str) -> Any:
        async def op(conn: Any) -> str:
            targets = name_targets[name]
            for t in targets:
                active[t] += 1
                if active[t] > 1:
                    overlaps.append(t)
            await asyncio.sleep(0.02)
            for t in targets:
                active[t] -= 1
            return name

        return op

    for _ in range(5):
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    pg_broker._write_target_locked(targets, op_over(name))
                    for name, targets in name_targets.items()
                )
            ),
            timeout=20,
        )
        assert sorted(results) == sorted(name_targets)

    assert overlaps == []


async def _delivered_payloads(broker: DatabaseBroker) -> dict[str, list[bytes]]:
    from sqlalchemy import select

    _, _, message = broker_schema()
    async with broker._engine.connect() as conn:
        result = await conn.execute(select(message.c.consumer_group, message.c.payload))
        grouped: dict[str, list[bytes]] = {}
        for group, payload in result:
            grouped.setdefault(group, []).append(bytes(payload))
        return {group: sorted(payloads) for group, payloads in grouped.items()}


async def test_concurrent_publishers_and_subscribers_deliver_each_message_once_per_group(
    pg_broker: DatabaseBroker,
) -> None:
    publishers, groups = 4, [f"g-{index}" for index in range(4)]
    for round_index in range(60):
        target = f"{_TARGET}.fanout-{round_index}"
        payloads = [f"{round_index}:{index}".encode() for index in range(publishers)]

        await asyncio.wait_for(
            asyncio.gather(
                *(pg_broker.publish(target, p, {"event_type": _EVENT_TYPE}) for p in payloads),
                *(pg_broker.subscribe([target], group) for group in groups),
                *(pg_broker.subscribe([target], group) for group in groups),
            ),
            timeout=30,
        )

        delivered = await _delivered_payloads(pg_broker)
        for group in groups:
            mine = [p for p in delivered.get(group, []) if p.startswith(f"{round_index}:".encode())]
            assert mine == sorted(payloads), f"{group} round {round_index}"


async def test_concurrent_retry_dead_letters_redelivers_each_dead_row_once(
    pg_broker: DatabaseBroker,
) -> None:
    dead_ids = {f"dead-{index}" for index in range(6)}
    for index, row_id in enumerate(sorted(dead_ids)):
        await _insert(
            pg_broker._engine,
            id=row_id,
            target=_TARGET,
            group=f"g-{index % 2}",
            status="dead",
            age_seconds=60,
        )
    await _insert(
        pg_broker._engine, id="done-0", target=_TARGET, group="g-0", status="done", age_seconds=60
    )
    claimed: list[str] = []

    async def claimer(group: str, name: str) -> None:
        for _ in range(10):
            rows = await pg_broker.claim_batch(group, batch_size=10, consumer_name=name)
            claimed.extend(cast(str, row["id"]) for row in rows)
            await asyncio.sleep(0.01)

    retried, *_ = await asyncio.wait_for(
        asyncio.gather(
            asyncio.gather(*(pg_broker.retry_dead_letters() for _ in range(4))),
            claimer("g-0", "c0"),
            claimer("g-1", "c1"),
            claimer("g-0", "c2"),
        ),
        timeout=30,
    )
    for group, name in (("g-0", "c0"), ("g-1", "c1")):
        rows = await pg_broker.claim_batch(group, batch_size=10, consumer_name=name)
        claimed.extend(cast(str, row["id"]) for row in rows)

    assert sum(retried) == len(dead_ids)  # no row counted by two retry calls
    assert sorted(claimed) == sorted(dead_ids)  # each re-delivered exactly once
    done = await _message_row(pg_broker._engine, "done-0")
    assert done is not None and done.status == "done"
