"""End-to-end transactional-outbox lifecycle against a real Postgres.

The default adapter suite (test_postgres_adapter.py) runs on aiosqlite and the
plugin suite (test_outbox.py) runs on an in-memory stub. Both miss the behaviour
that only emerges on the real engine the outbox is *for*: the after-commit
dispatch on real MVCC, the background retry loop sweeping ``FOR UPDATE SKIP
LOCKED`` rows, dead-letter promotion persisting the ``is_dead_lettered`` BYTEA/
boolean column, the ``delete``/``archive`` completion modes through real
transactions, and crash-recovery of committed-but-undelivered rows.

These tests exercise the whole lifecycle on Postgres provisioned by the shared
``pg_engine`` fixture (a throwaway testcontainers Postgres, or
``MODULITH_TEST_POSTGRES_URL`` when set). They skip without Docker, keeping the
default suite green.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from modulith import EventPublication, event
from modulith.adapters import postgres_outbox
from modulith.adapters.postgres_outbox import (
    EventPublicationArchiveRow,
    EventPublicationRow,
    PostgresPublicationStore,
    bind_session,
)
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

pytestmark = [pytest.mark.integration]


@event
@dataclass(frozen=True)
class OutboxEvent:
    value: int


@pytest.fixture(autouse=True)
def _reset():
    """Isolate global outbox/runtime/adapter state around every test."""
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


def _bootstrap(package: str = "obxe2e"):
    """Bring up a discovery-free runtime and return its event bus."""
    _runtime.configure(package=package, auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    return _runtime.event_bus


def _pub(value: int, handler, **overrides) -> EventPublication:
    """Build a publication addressed to ``handler`` (matched by listener id)."""
    serializer = JsonEventSerializer()
    defaults = dict(
        id=uuid4(),
        payload=serializer.serialize(OutboxEvent(value=value)),
        event_type=f"{OutboxEvent.__module__}.{OutboxEvent.__qualname__}",
        listener=outbox._listener_id(handler),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


async def _until(predicate, *, timeout: float = 8.0, interval: float = 0.02) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if predicate():
            return
        await asyncio.sleep(interval)
    if not predicate():
        raise AssertionError("condition not met within timeout")


async def _until_async(coro_predicate, *, timeout: float = 10.0, interval: float = 0.05) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await coro_predicate():
            return
        await asyncio.sleep(interval)
    if not await coro_predicate():
        raise AssertionError("condition not met within timeout")


# ---------------------------------------------------------------------------
# Happy path: durable dispatch through the real after-commit hook
# ---------------------------------------------------------------------------


async def test_durable_dispatch_marks_complete_on_real_postgres(pg_engine) -> None:
    """A publication saved inside a committed transaction is dispatched by the
    after-commit hook and marked complete — on real Postgres, completed_at is a
    tz-aware TIMESTAMPTZ and the row survives in the table (update mode)."""
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    sessionmaker = async_sessionmaker(pg_engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(7, handler))
            await session.commit()
        finally:
            outbox._current_session.reset(token)
    await store.wait_for_dispatch()

    assert delivered == [7]
    async with sessionmaker() as s:
        rows = (await s.execute(select(EventPublicationRow))).scalars().all()
        assert len(rows) == 1
        assert rows[0].completed_at is not None  # update mode keeps the completed row
        assert rows[0].is_dead_lettered is False
    await store.dispose()


# ---------------------------------------------------------------------------
# The background retry loop, firing against real Postgres
# ---------------------------------------------------------------------------


async def test_retry_loop_delivers_after_transient_failure(pg_engine) -> None:
    """A listener that fails once then succeeds is retried by the background
    loop and eventually completed — the loop really sweeps ``find_incomplete``
    (FOR UPDATE SKIP LOCKED) on Postgres and re-dispatches."""
    delivered: list[int] = []
    calls = {"n": 0}

    async def handler(evt: OutboxEvent) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient failure")
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(
        store,
        JsonEventSerializer(),
        retry_interval_seconds=0.05,
        retry_stale_seconds=0.0,
        start_loop=True,
    )
    # A committed-but-undelivered row (standalone save schedules no after-commit
    # dispatch); the loop's sweep must find and deliver it.
    await store.save(_pub(11, handler))

    # Wait on the durable completion state (set just after a successful dispatch),
    # not the in-memory list — the list is appended a beat before mark_complete.
    async def completed() -> bool:
        async with async_sessionmaker(pg_engine)() as s:
            row = (await s.execute(select(EventPublicationRow))).scalar_one_or_none()
            return row is not None and row.completed_at is not None

    await _until_async(completed)
    assert delivered == [11]
    assert calls["n"] >= 2  # failed once, then a retry succeeded

    async with async_sessionmaker(pg_engine)() as s:
        row = (await s.execute(select(EventPublicationRow))).scalar_one()
        assert row.completed_at is not None
        assert row.attempt_count >= 1  # the failed attempt was recorded

    await outbox.shutdown()
    await store.dispose()


# ---------------------------------------------------------------------------
# Dead-lettering: the exact column (is_dead_lettered) the schema bug broke
# ---------------------------------------------------------------------------


async def test_dead_letters_after_exhausting_attempts(pg_engine) -> None:
    """Repeated failures promote a publication to dead-lettered on real
    Postgres: the boolean flag persists (round-trips through BYTEA/boolean),
    ``find_incomplete`` excludes it, ``find_dead_lettered`` includes it, and
    ``status()`` counts it."""

    async def handler(evt: OutboxEvent) -> None:
        raise RuntimeError("permanent failure")

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    store = PostgresPublicationStore(engine=pg_engine, dead_letter_after_attempts=2)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)

    pub = _pub(99, handler)
    await store.save(pub)  # committed, attempt_count 0

    # Drive dispatch directly (no backoff wait): two failed attempts reach the
    # dead-letter threshold.
    await outbox._dispatch_publication(pub)
    fresh = (await store.find_incomplete(timedelta(0)))[0]
    await outbox._dispatch_publication(fresh)

    # Excluded from the retry window, present in the dead-letter set.
    assert await store.find_incomplete(timedelta(0)) == []
    dead = await store.find_dead_lettered()
    assert len(dead) == 1
    assert dead[0].attempt_count >= 2
    assert JsonEventSerializer().deserialize(dead[0].payload, dead[0].event_type).value == 99

    # The boolean flag is persisted as real TRUE on Postgres.
    async with async_sessionmaker(pg_engine)() as s:
        row = (await s.execute(select(EventPublicationRow))).scalar_one()
        assert row.is_dead_lettered is True
        assert row.completed_at is None

    counts = await outbox.status()
    assert counts == {"incomplete": 0, "completed": 0, "dead_lettered": 1}
    await store.dispose()


# ---------------------------------------------------------------------------
# Completion modes through real transactions
# ---------------------------------------------------------------------------


async def test_completion_mode_delete_removes_row(pg_engine) -> None:
    """``completion_mode='delete'`` hard-deletes the row on successful delivery."""
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), completion_mode="delete", start_loop=False)

    sessionmaker = async_sessionmaker(pg_engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(1, handler))
            await session.commit()
        finally:
            outbox._current_session.reset(token)
    await store.wait_for_dispatch()

    assert delivered == [1]
    async with sessionmaker() as s:
        remaining = (
            await s.execute(select(func.count()).select_from(EventPublicationRow))
        ).scalar_one()
        assert remaining == 0  # deleted on completion
    await store.dispose()


async def test_completion_mode_archive_moves_row(pg_engine) -> None:
    """``completion_mode='archive'`` moves the row to the archive table on
    success — one transaction inserts the archive row and deletes the primary."""
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), completion_mode="archive", start_loop=False)

    sessionmaker = async_sessionmaker(pg_engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(2, handler))
            await session.commit()
        finally:
            outbox._current_session.reset(token)
    await store.wait_for_dispatch()

    assert delivered == [2]
    async with sessionmaker() as s:
        primary = (
            await s.execute(select(func.count()).select_from(EventPublicationRow))
        ).scalar_one()
        archived = (
            await s.execute(select(func.count()).select_from(EventPublicationArchiveRow))
        ).scalar_one()
        assert primary == 0
        assert archived == 1
    await store.dispose()


# ---------------------------------------------------------------------------
# Maintenance APIs on real Postgres
# ---------------------------------------------------------------------------


async def test_purge_completed_removes_old_rows(pg_engine) -> None:
    """``purge_completed`` bulk-deletes completed rows older than the threshold,
    leaving recent completed and incomplete rows intact."""

    async def handler(evt: OutboxEvent) -> None:
        return None

    _bootstrap().register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    old = datetime(2020, 1, 1, tzinfo=UTC)
    recent = datetime.now(UTC)
    # One old completed, one recent completed, one incomplete.
    await store.save(_pub(1, handler, published_at=old, completed_at=old))
    await store.save(_pub(2, handler, published_at=recent, completed_at=recent))
    await store.save(_pub(3, handler, published_at=recent))

    purged = await store.purge_completed(timedelta(days=365))
    assert purged == 1  # only the 2020 completed row

    async with async_sessionmaker(pg_engine)() as s:
        remaining = (
            await s.execute(select(func.count()).select_from(EventPublicationRow))
        ).scalar_one()
        assert remaining == 2
    await store.dispose()


async def test_purge_completed_trims_the_archive_table(pg_engine) -> None:
    """``completion_mode='archive'`` MOVES the row out of the primary table, so
    a purge that only swept the primary table could never trim the archive: it
    would report a truthful-looking 0 while the archive grew without bound."""

    async def handler(evt: OutboxEvent) -> None:
        return None

    _bootstrap().register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), completion_mode="archive", start_loop=False)

    pub = _pub(1, handler)
    await store.save(pub)
    await store.archive(pub.id)  # stamps completed_at and moves the row

    sessionmaker = async_sessionmaker(pg_engine)

    async def archived_rows() -> int:
        async with sessionmaker() as s:
            return int(
                (
                    await s.execute(select(func.count()).select_from(EventPublicationArchiveRow))
                ).scalar_one()
            )

    assert await archived_rows() == 1
    assert await store.purge_completed(timedelta(days=365)) == 0  # still inside retention
    assert await archived_rows() == 1

    assert await store.purge_completed(timedelta(0)) == 1
    assert await archived_rows() == 0
    await store.dispose()


@pytest.mark.parametrize("fenced_write", ["complete_claim", "fail_claim"])
async def test_fenced_write_loses_to_a_peer_reclaim_it_did_not_see(
    pg_engine, fenced_write: str
) -> None:
    """A fenced write must lose to a claim a peer took over WHILE the write was
    in flight — not only to one that was already stale when it started.

    The interleave is forced deterministically: a third transaction holds the
    row lock, the fenced write blocks on it, and the peer's takeover commits
    before the lock is released. A read-then-check-then-write pair passes its
    token check on the pre-takeover snapshot and then clobbers the peer's live
    lease; a conditional write (or a locked read) sees the new claim.
    """

    async def handler(evt: OutboxEvent) -> None:
        return None

    _bootstrap().register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine)
    await store.save(_pub(1, handler))
    (claim,) = await store.claim_batch(
        owner="worker-a", batch_size=1, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert claim.claim_token is not None

    sessionmaker = async_sessionmaker(pg_engine)
    async with sessionmaker() as holder:
        locked = await holder.get(EventPublicationRow, claim.id, with_for_update=True)
        assert locked is not None

        if fenced_write == "complete_claim":
            write = asyncio.create_task(store.complete_claim(claim.id, claim.claim_token, "update"))
        else:
            claim.attempt_count = 1
            claim.last_error = "boom"
            write = asyncio.create_task(store.fail_claim(claim, claim.claim_token))

        await _wait_until_blocked_on_a_lock(pg_engine)
        # A peer sweeper takes the row over while the fenced write is blocked.
        locked.claim_owner = "worker-b"
        locked.claim_token = "peer-token"
        locked.claim_until = datetime.now(UTC) + timedelta(hours=1)
        await holder.commit()

    assert await write is False
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, claim.id)
        assert row is not None
        assert row.claim_token == "peer-token"  # the peer's lease survived
        assert row.completed_at is None
        assert row.attempt_count == 0
    await store.dispose()


async def _wait_until_blocked_on_a_lock(engine) -> None:
    """Poll pg_stat_activity until some backend is waiting on a lock.

    Bounded, and never a bare sleep-and-hope: the fenced write must have
    actually reached the database and blocked before the peer takes over, or
    the interleave under test would not happen at all.
    """
    async with engine.connect() as conn:
        for _ in range(500):
            waiting = (
                await conn.execute(
                    text("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'")
                )
            ).scalar_one()
            if waiting:
                return
            await asyncio.sleep(0.01)
    raise AssertionError("no backend ever blocked on the row lock")


async def test_force_retry_redelivers_dead_lettered(pg_engine) -> None:
    """After fixing a broken listener, ``force_retry`` redelivers a publication
    even once it has been dead-lettered (excluded from the retry window)."""
    delivered: list[int] = []
    fail = {"on": True}

    async def handler(evt: OutboxEvent) -> None:
        if fail["on"]:
            raise RuntimeError("listener broken")
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine, dead_letter_after_attempts=1)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=1, start_loop=False)

    pub = _pub(5, handler)
    await store.save(pub)
    await outbox._dispatch_publication(pub)  # fails once -> dead-lettered (threshold 1)
    assert len(await store.find_dead_lettered()) == 1

    # Operator fixes the listener and forces a retry.
    fail["on"] = False
    await outbox.force_retry(pub.id)

    assert delivered == [5]
    assert await store.find_dead_lettered() == []
    await store.dispose()


async def test_retry_all_dead_lettered_resets_and_redelivers(pg_engine) -> None:
    """``retry_all_dead_lettered`` resets the attempt budget and redelivers every
    dead-lettered publication once the underlying fault is resolved."""
    delivered: list[int] = []
    fail = {"on": True}

    async def handler(evt: OutboxEvent) -> None:
        if fail["on"]:
            raise RuntimeError("listener broken")
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine, dead_letter_after_attempts=1)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=1, start_loop=False)

    for v in (10, 20):
        pub = _pub(v, handler)
        await store.save(pub)
        await outbox._dispatch_publication(pub)
    assert len(await store.find_dead_lettered()) == 2

    fail["on"] = False
    resubmitted = await outbox.retry_all_dead_lettered()

    assert resubmitted == 2
    assert sorted(delivered) == [10, 20]
    assert await store.find_dead_lettered() == []
    await store.dispose()


# ---------------------------------------------------------------------------
# retry-all under claim_strategy="advisory_lock": real pg_try_advisory_lock
# ---------------------------------------------------------------------------


def _advisory_key(publication: EventPublication) -> int:
    """The key ``PostgresPublicationStore.try_lock_publication`` locks for a row."""
    return publication.id.int & 0x7FFFFFFFFFFFFFFF


async def _advisory_locks(engine, *, granted: bool) -> list[int]:
    """Sorted advisory-lock keys of this database that are held (``granted``) or awaited."""
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT classid::bigint, objid::bigint FROM pg_locks "
                "WHERE locktype = 'advisory' AND granted = :granted "
                "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            ),
            {"granted": granted},
        )
        return sorted((classid << 32) | objid for classid, objid in result)


async def _dead_lettered_store(pg_engine, values, handler, fail) -> list[EventPublication]:
    """Configure an advisory-lock outbox and dead-letter one row per value."""
    bus = _bootstrap()
    bus.register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine, dead_letter_after_attempts=1)
    outbox.configure(
        store,
        JsonEventSerializer(),
        dead_letter_after_attempts=1,
        start_loop=False,
        claim_strategy="advisory_lock",
    )
    fail["on"] = True
    pubs = [_pub(value, handler) for value in values]
    for pub in pubs:
        await store.save(pub)
        await outbox._dispatch_publication(pub)
    assert len(await store.find_dead_lettered()) == len(pubs)
    fail["on"] = False
    return pubs


async def test_retry_all_holds_the_rows_advisory_lock_while_it_delivers(pg_engine) -> None:
    fail = {"on": True}
    inside, release = asyncio.Event(), asyncio.Event()
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        if fail["on"]:
            raise RuntimeError("listener broken")
        inside.set()
        await release.wait()
        delivered.append(evt.value)

    (pub,) = await _dead_lettered_store(pg_engine, [7], handler, fail)
    store = outbox._store
    assert await _advisory_locks(pg_engine, granted=True) == []

    retry = asyncio.create_task(outbox.retry_all_dead_lettered())
    await asyncio.wait_for(inside.wait(), timeout=10)

    assert await _advisory_locks(pg_engine, granted=True) == [_advisory_key(pub)]
    assert delivered == []

    release.set()
    assert await asyncio.wait_for(retry, timeout=10) == 1

    assert delivered == [7]
    assert await _advisory_locks(pg_engine, granted=True) == []
    await store.dispose()


async def test_retry_all_leaves_a_row_locked_by_another_session_to_the_sweep(pg_engine) -> None:
    fail = {"on": True}
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        if fail["on"]:
            raise RuntimeError("listener broken")
        delivered.append(evt.value)

    held, _ = await _dead_lettered_store(pg_engine, [1, 2], handler, fail)
    store = outbox._store

    async with pg_engine.connect() as raw:
        holder = await raw.execution_options(isolation_level="AUTOCOMMIT")
        await holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _advisory_key(held)})
        try:
            resubmitted = await outbox.retry_all_dead_lettered()

            assert resubmitted == 2  # the held row still counts as resubmitted
            assert delivered == [2]  # but only the unlocked row was delivered
            reopened = await store.find_by_id(held.id)
            assert reopened is not None
            assert reopened.completed_at is None and reopened.attempt_count == 0
            assert await store.find_dead_lettered() == []
        finally:
            await holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _advisory_key(held)})

    await outbox._sweep(timedelta(0))

    assert sorted(delivered) == [1, 2]
    await store.dispose()


async def test_concurrent_retry_all_and_sweep_deliver_every_dead_row_exactly_once(
    pg_engine,
) -> None:
    fail = {"on": True}
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        if fail["on"]:
            raise RuntimeError("listener broken")
        await asyncio.sleep(0.02)
        delivered.append(evt.value)

    values = list(range(12))
    pubs = await _dead_lettered_store(pg_engine, values, handler, fail)
    value_of = {pub.id: value for pub, value in zip(pubs, values, strict=True)}
    store = outbox._store
    retries_done = asyncio.Event()

    async def sweeper() -> None:
        while not retries_done.is_set():
            await outbox._sweep(timedelta(0))
            await asyncio.sleep(0.005)

    peer_delivered: list[int] = []

    async def peer_dispatcher() -> None:
        """A dispatcher in another process: its own session, the same lock protocol."""
        async with pg_engine.connect() as raw:
            conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
            while not retries_done.is_set():
                for pub_id, value in value_of.items():
                    key = pub_id.int & 0x7FFFFFFFFFFFFFFF
                    got = await conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key})
                    if not got.scalar():
                        continue
                    try:
                        open_row = await conn.execute(
                            select(EventPublicationRow.id).where(
                                EventPublicationRow.id == pub_id,
                                EventPublicationRow.completed_at.is_(None),
                                EventPublicationRow.is_dead_lettered.is_(False),
                            )
                        )
                        if open_row.first() is not None:
                            peer_delivered.append(value)
                            await asyncio.sleep(0.05)
                            await conn.execute(
                                update(EventPublicationRow)
                                .where(EventPublicationRow.id == pub_id)
                                .values(completed_at=datetime.now(UTC))
                            )
                    finally:
                        await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
                await asyncio.sleep(0.005)

    sweep_task = asyncio.create_task(sweeper())
    peer_task = asyncio.create_task(peer_dispatcher())
    counts = await asyncio.wait_for(
        asyncio.gather(*(outbox.retry_all_dead_lettered() for _ in range(3))), timeout=60
    )
    retries_done.set()
    await asyncio.wait_for(asyncio.gather(sweep_task, peer_task), timeout=60)
    await outbox._sweep(timedelta(0))

    assert all(count <= len(values) for count in counts)
    assert sorted(delivered + peer_delivered) == values  # each dead row delivered exactly once
    assert await store.find_dead_lettered() == []
    assert await store.count_open() == 0
    assert await _advisory_locks(pg_engine, granted=True) == []
    await store.dispose()


# ---------------------------------------------------------------------------
# Crash recovery: committed-but-undelivered rows swept on restart
# ---------------------------------------------------------------------------


async def test_crash_recovery_sweep_delivers_committed_backlog(pg_engine) -> None:
    """Rows committed by a previous process but never dispatched (no after-commit
    task survived the crash) are delivered by the retry loop's entry sweep on
    the next start — at-least-once delivery on real Postgres."""
    delivered: list[int] = []

    async def handler(evt: OutboxEvent) -> None:
        delivered.append(evt.value)

    bus = _bootstrap()
    bus.register(OutboxEvent, handler)

    # First "process": persist committed rows, never dispatch them.
    seed = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(seed, JsonEventSerializer(), start_loop=False)
    for v in range(5):
        await seed.save(_pub(v, handler))
    await seed.dispose()

    # Second "process": a fresh store + retry loop runs the crash sweep on entry.
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()
    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(
        store, JsonEventSerializer(), retry_interval_seconds=0.05, retry_stale_seconds=0.0
    )

    await _until(lambda: sorted(delivered) == [0, 1, 2, 3, 4])
    assert len(delivered) == 5  # delivered once each, no duplication storm

    await outbox.shutdown()
    await store.dispose()


# ---------------------------------------------------------------------------
# Concurrent dispatchers: FOR UPDATE SKIP LOCKED partitions the backlog
# ---------------------------------------------------------------------------


async def test_for_update_skip_locked_partitions_concurrent_sweeps(pg_engine) -> None:
    """The Postgres-only row-claim primitive the multi-replica outbox depends on.

    ``find_incomplete`` issues ``SELECT ... FOR UPDATE SKIP LOCKED``: when one
    worker is already holding a claim on some rows, a second worker's sweep skips
    exactly those rows rather than blocking on them or re-claiming them. That is
    what lets two dispatchers partition the backlog (at-least-once, never a
    blocked herd). SQLite silently no-ops the clause, so this behaviour can only
    be verified on a real Postgres engine.
    """

    async def handler(evt: OutboxEvent) -> None:
        return None

    _bootstrap().register(OutboxEvent, handler)
    store = PostgresPublicationStore(engine=pg_engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    for v in range(6):
        await store.save(_pub(v, handler))  # 6 committed, incomplete rows

    sessionmaker = async_sessionmaker(pg_engine)
    # Worker A opens a transaction and claims (locks) the 4 oldest rows, holding
    # the lock open for the duration of the `async with` block.
    async with sessionmaker() as a:
        claimed = (
            (
                await a.execute(
                    select(EventPublicationRow)
                    .order_by(EventPublicationRow.published_at)
                    .limit(4)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        assert len(claimed) == 4  # A holds a live row lock on 4 publications
        claimed_ids = {r.id for r in claimed}

        # Worker B sweeps concurrently while A still holds its lock: SKIP LOCKED
        # makes B see only the 2 rows A did not claim — disjoint, no overlap.
        visible = await store.find_incomplete(timedelta(0))
        assert len(visible) == 2
        assert {p.id for p in visible}.isdisjoint(claimed_ids)

    # Once A's transaction ends and the lock drops, B's sweep sees all 6 again.
    assert len(await store.find_incomplete(timedelta(0))) == 6
    await store.dispose()
