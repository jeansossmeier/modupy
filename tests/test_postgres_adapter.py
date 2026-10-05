"""Behavioral tests for the SQLAlchemy outbox adapter.

The adapter is named for Postgres (it ships in ``modupy[postgres]`` with
asyncpg) but is built on portable SQLAlchemy 2.0 Core/ORM, so these tests run
it against aiosqlite — no Docker: a tmp-file DB with per-session connections
(see the ``engine`` fixture for why NOT StaticPool + :memory:). The same code
path runs on Postgres in production; only the dialect (and the partial-index
variant) differs.

Covered:
  * schema creates cleanly and round-trips an EventPublication;
  * save() inside a bound transaction enlists the row + queues it on the
    session, and the after-commit hook dispatches it (rollback drops it);
  * save() outside a transaction upserts in its own short transaction;
  * mark_complete / find_incomplete / archive / delete contracts;
  * the duck-typed maintenance helpers count_completed / purge_completed.

The cross-process crash-recovery test (the outbox's forcing function) lives
in tests/test_outbox_crash_recovery.py and uses a file-based SQLite DB.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy.pool import NullPool

from modulith import EventPublication, event
from modulith.adapters import postgres_outbox
from modulith.adapters.postgres_outbox import (
    Base,
    EventPublicationRow,
    PostgresPublicationStore,
    bind_session,
)
from modulith.builtin import outbox
from modulith.config import ConfigurationError
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class PgEvent:
    value: int


received: list[int] = []


async def record(event: PgEvent) -> None:
    received.append(event.value)


@pytest.fixture
async def engine(tmp_path: Path) -> Any:
    """A file-backed aiosqlite engine with one connection PER session (NullPool).

    Deliberately NOT StaticPool + ``sqlite+aiosqlite://``: that hands every
    session the same single DBAPI connection, and SQLite has exactly one
    transaction per connection — so under real concurrency (e.g.
    test_after_commit_and_sweep_race_is_bounded) one session's close (ROLLBACK)
    can clobber another session's in-flight INSERT->COMMIT, a topology
    impossible on per-connection Postgres. Same hazard fixed in
    tests/test_postgres_outbox_adapter.py; see its
    test_concurrent_reader_close_does_not_roll_back_inflight_save for the
    deterministic repro. A tmp-file DB also survives connection invalidation
    (a StaticPool reconnect produced a brand-new empty :memory: database
    mid-test)."""
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'adapter.db'}",
        poolclass=NullPool,
    )
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture(autouse=True)
def _reset():
    received.clear()
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


def _bootstrap_with_listener() -> None:
    _runtime.configure(package="pgtest", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(PgEvent, record)


def _pub(value: int, **overrides) -> EventPublication:
    serializer = JsonEventSerializer()
    defaults = dict(
        id=uuid4(),
        payload=serializer.serialize(PgEvent(value=value)),
        event_type=f"{PgEvent.__module__}.{PgEvent.__qualname__}",
        # Module-qualified identity, mirroring persist() — so the after-commit
        # dispatch's _resolve_listener matches the registered handler.
        listener=outbox._listener_id(record),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


async def test_schema_creates_cleanly(engine) -> None:
    # The fixture already created the schema; verify the table is mapped.
    assert EventPublicationRow.__tablename__ == "event_publications"
    cols = {c.name for c in EventPublicationRow.__table__.columns}
    assert {
        "id",
        "event_type",
        "payload",
        "listener",
        "published_at",
        "completed_at",
        "attempt_count",
        "last_error",
        "last_attempt_at",
        "is_dead_lettered",
    } <= cols


# ---------------------------------------------------------------------------
# T59 — bind_session / unbind_session round trip
# ---------------------------------------------------------------------------


async def test_bind_unbind_session_round_trip_restores_previous_binding(engine) -> None:
    """T59: unbind_session(token) is the public counterpart to bind_session —
    it restores whatever was bound before, including a nested binding (e.g.
    an inner request-scoped session shadowing an outer one), mirroring the
    ``_current_session.reset(token)`` semantics callers previously had to
    reach for directly (the private contextvar) per bind_session's own
    docstring example."""
    from modulith.adapters.postgres_outbox import unbind_session

    assert outbox._bound_session() is None

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as outer, sessionmaker() as inner:
        outer_token = bind_session(outer)
        assert outbox._bound_session() is outer

        inner_token = bind_session(inner)
        assert outbox._bound_session() is inner

        unbind_session(inner_token)
        assert outbox._bound_session() is outer

        unbind_session(outer_token)
        assert outbox._bound_session() is None


def test_bind_session_is_exported_from_outbox_and_postgres_adapter() -> None:
    """bind_session and unbind_session are stably exported from the outbox
    wiring module, with the adapter re-exporting them as aliases."""
    from modulith.adapters import postgres_outbox
    from modulith.builtin import outbox as outbox_module

    assert hasattr(outbox_module, "bind_session")
    assert hasattr(outbox_module, "unbind_session")
    assert hasattr(postgres_outbox, "bind_session")
    assert hasattr(postgres_outbox, "unbind_session")

    assert postgres_outbox.bind_session is outbox_module.bind_session
    assert postgres_outbox.unbind_session is outbox_module.unbind_session

    assert "bind_session" in outbox_module.__all__
    assert "unbind_session" in outbox_module.__all__
    assert "bind_session" in postgres_outbox.__all__
    assert "unbind_session" in postgres_outbox.__all__


async def test_unbind_session_with_raw_session_binding(engine) -> None:
    """unbind_session correctly handles a raw session binding (not wrapped
    in _SessionBinding), restoring the previous binding."""
    from modulith.adapters.postgres_outbox import unbind_session

    assert outbox._bound_session() is None

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as outer, sessionmaker() as inner:
        outer_token = bind_session(outer)
        assert outbox._bound_session() is outer

        raw_inner_token = outbox._current_session.set(inner)
        assert outbox._bound_session() is inner

        unbind_session(raw_inner_token)
        assert outbox._bound_session() is outer

        unbind_session(outer_token)
        assert outbox._bound_session() is None


# ---------------------------------------------------------------------------
# save() inside a transaction + after-commit dispatch
# ---------------------------------------------------------------------------


async def test_after_commit_dispatches(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener()

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            pub = _pub(5)
            await store.save(pub)
            await session.commit()
        finally:
            outbox._current_session.reset(token)

    await store.wait_for_dispatch()

    assert received == [5]
    # update completion mode → row marked complete, still present.
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, pub.id)
        assert row is not None
        assert row.completed_at is not None


async def test_rollback_drops_pending(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener()

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(6))
            await session.rollback()
        finally:
            outbox._current_session.reset(token)

    await store.wait_for_dispatch()

    assert received == []  # rolled back → never dispatched
    async with sessionmaker() as s:
        rows = (await s.execute(EventPublicationRow.__table__.select())).all()
        assert rows == []


async def test_rollback_does_not_leak_pending_ids_into_the_next_commit(engine, caplog) -> None:
    """A reused session must not dispatch what its rolled-back sibling queued.

    SQLAlchemy does not reset ``Session.info`` on rollback, and reusing a
    Session across transactions is ordinary. Without an explicit discard the
    rolled-back id survives in the queue and the *next* commit dispatches it,
    finding no row and logging the "deleted before delivery?" warning that is
    supposed to mean a committed row went missing.
    """
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener()

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(6))
            await session.rollback()
            # Same session, second transaction — the queue must start empty.
            await store.save(_pub(7))
            with caplog.at_level(logging.WARNING, logger="modulith.adapters.postgres"):
                await session.commit()
                await store.wait_for_dispatch()
        finally:
            outbox._current_session.reset(token)

    assert received == [7]
    assert "deleted before delivery" not in caplog.text


def test_after_commit_without_running_loop_defers_to_retry_sweep(tmp_path, caplog) -> None:
    """Degraded path (postgres_outbox._schedule_after_commit_dispatch): a
    *sync* SQLAlchemy session
    committing from a thread with no running event loop fires the global
    after_commit hook, but dispatch tasks can't be scheduled. The hook must NOT
    crash — it logs and leaves the committed row for the retry sweep.

    Proven end-to-end: the sync commit persists the row (with no loop running,
    so the get_running_loop()→RuntimeError branch is genuinely taken), and the
    async store's find_incomplete recovers it. This test is intentionally
    synchronous so that no event loop is running at commit time.
    """
    db = tmp_path / "noloop.db"
    sync_engine = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(sync_engine)

    # Constructing the store is synchronous: it registers the global after_commit
    # hook and becomes the _active_store the hook routes to.
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(11)

    with caplog.at_level(logging.WARNING, logger="modulith.adapters.postgres"):
        with SyncSession(sync_engine) as session:
            # Mirror what save() does inside a bound transaction.
            session.add(
                EventPublicationRow(
                    id=pub.id,
                    event_type=pub.event_type,
                    payload=pub.payload,
                    listener=pub.listener,
                    published_at=pub.published_at,
                    completed_at=None,
                    attempt_count=0,
                    last_error=None,
                    last_attempt_at=None,
                    is_dead_lettered=False,
                )
            )
            session.info.setdefault("_modulith_pending", []).append(pub.id)
            session.commit()  # fires after_commit with NO running loop

    # The degraded branch was taken: warning logged, no dispatch task created.
    assert any("without a running loop" in r.getMessage() for r in caplog.records)
    assert store._inflight == set()

    # And the committed row is recoverable by the retry sweep (the safety net).
    found = asyncio.run(store.find_incomplete(timedelta(0)))
    assert [p.id for p in found] == [pub.id]

    asyncio.run(store.dispose())
    asyncio.run(engine.dispose())
    sync_engine.dispose()


# ---------------------------------------------------------------------------
# the 5 store methods
# ---------------------------------------------------------------------------


async def test_save_standalone_upserts(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)  # no bound session → standalone insert

    found = await store.find_incomplete(timedelta(0))
    assert len(found) == 1
    assert found[0].id == pub.id
    assert found[0].payload == pub.payload
    assert found[0].event_type == pub.event_type
    assert found[0].listener == pub.listener

    # Re-save with a mutated attempt_count → upsert (no duplicate row).
    pub.attempt_count = 4
    pub.last_error = "boom"
    await store.save(pub)
    found = await store.find_incomplete(timedelta(0))
    assert len(found) == 1
    assert found[0].attempt_count == 4
    assert found[0].last_error == "boom"


async def test_mark_complete_excludes_from_incomplete(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    await store.mark_complete(pub.id)

    assert await store.find_incomplete(timedelta(0)) == []
    assert await store.count_completed() == 1


async def test_find_incomplete_respects_staleness(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    fresh = _pub(1, published_at=datetime.now(UTC))
    stale = _pub(2, published_at=datetime.now(UTC) - timedelta(seconds=120))
    await store.save(fresh)
    await store.save(stale)

    found = await store.find_incomplete(timedelta(seconds=30))
    ids = {p.id for p in found}
    assert stale.id in ids
    assert fresh.id not in ids


async def test_delete_removes_row(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    await store.delete(pub.id)
    assert await store.find_incomplete(timedelta(0)) == []


async def test_archive_moves_row_out_of_primary(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    await store.archive(pub.id)
    assert await store.find_incomplete(timedelta(0)) == []


async def test_purge_completed_removes_old(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    await store.mark_complete(pub.id)
    # Backdate completion so it's past the purge threshold.
    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, pub.id)
        row.completed_at = datetime.now(UTC) - timedelta(days=40)
        await s.commit()

    removed = await store.purge_completed(timedelta(days=30))
    assert removed == 1
    assert await store.count_completed() == 0


# ---------------------------------------------------------------------------
# regression: reopen guard, dead-letter exclusion, unbounded counts
# ---------------------------------------------------------------------------


async def test_standalone_save_never_reopens_completed_row(engine) -> None:
    # A completed row must not be resurrected by a stale failed re-save (the
    # crash sweep racing the after-commit task). merge() used to blank
    # completed_at; the guarded upsert refuses to touch a completed row.
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    await store.mark_complete(pub.id)
    assert await store.find_incomplete(timedelta(0)) == []

    # Stale in-memory copy still has completed_at=None and a fresh failure.
    pub.completed_at = None
    pub.attempt_count = 1
    pub.last_error = "late failure after completion"
    await store.save(pub)

    assert await store.find_incomplete(timedelta(0)) == []  # not reopened
    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, pub.id)
        assert row.completed_at is not None
        assert row.attempt_count == 0  # stale failure was dropped


async def test_find_incomplete_excludes_dead_lettered(engine) -> None:
    # Dead-lettered rows are filtered at the SQL level so the LIMIT-100 retry
    # window is never starved by exhausted records.
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=3)
    live = _pub(1, attempt_count=1)
    dead = _pub(2, attempt_count=3)  # >= threshold → dead-lettered
    await store.save(live)
    await store.save(dead)

    found_ids = {p.id for p in await store.find_incomplete(timedelta(0))}
    assert live.id in found_ids
    assert dead.id not in found_ids

    dl = await store.find_dead_lettered()
    assert [p.id for p in dl] == [dead.id]


async def test_count_open_and_dead_lettered_are_unbounded_and_accurate(engine) -> None:
    # count_* are the operational source of truth (find_incomplete is capped).
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)
    for i in range(3):
        await store.save(_pub(i, attempt_count=0))  # open
    for i in range(2):
        await store.save(_pub(100 + i, attempt_count=2))  # dead-lettered
    done = _pub(999)
    await store.save(done)
    await store.mark_complete(done.id)

    assert await store.count_open() == 3
    assert await store.count_dead_lettered() == 2
    assert await store.count_completed() == 1


async def test_status_distinguishes_open_completed_dead_via_store_counts(engine) -> None:
    # status() must use the unbounded store counts — find_incomplete now
    # excludes dead-letters, so the old partition-by-attempt path would report 0.
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)
    for i in range(3):
        await store.save(_pub(i, attempt_count=0))
    for i in range(2):
        await store.save(_pub(100 + i, attempt_count=2))
    done = _pub(999)
    await store.save(done)
    await store.mark_complete(done.id)

    assert await outbox.status() == {"incomplete": 3, "completed": 1, "dead_lettered": 2}


async def test_configure_rejects_conflicting_dead_letter_thresholds(engine) -> None:
    """Dead-letter thresholds are unified — constructing the store with an
    explicit threshold that disagrees with the one passed to
    outbox.configure() must fail loudly instead of leaving the store's
    is_dead_lettered flag (written from ITS OWN threshold in save()) out of
    sync with the plugin's own skip-check."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=3)

    with pytest.raises(ConfigurationError, match="dead_letter_after_attempts"):
        outbox.configure(
            store, JsonEventSerializer(), dead_letter_after_attempts=5, start_loop=False
        )


async def test_configure_unifies_store_threshold_with_matching_value(engine) -> None:
    """Matching explicit values on both sides are not a conflict — the
    long-standing pattern every other adapter test in this file already uses."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)

    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)

    assert outbox._dead_letter_after_attempts == 2
    assert store.dead_letter_after_attempts == 2


async def test_force_retry_reaches_dead_lettered_publication(engine) -> None:
    # find_incomplete excludes dead-letters, so force_retry must reach a
    # dead-lettered row via the dedicated dead-letter lookup instead.
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)
    _bootstrap_with_listener()

    dead = _pub(7, attempt_count=2)  # >= threshold → dead-lettered
    await store.save(dead)
    assert await store.find_incomplete(timedelta(0)) == []  # not in retry window

    await outbox.force_retry(dead.id)

    assert received == [7]  # delivered despite being dead-lettered


async def test_find_by_id_reaches_publication_outside_capped_windows(engine) -> None:
    """force_retry must not depend on the capped find_incomplete
    (LIMIT 100) / find_dead_lettered (LIMIT 100) scans to locate a
    target row — a direct point lookup reaches it regardless of backlog size."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=2, start_loop=False)
    _bootstrap_with_listener()

    target = _pub(42, attempt_count=2)  # dead-lettered
    await store.save(target)

    found = await store.find_by_id(target.id)
    assert found is not None
    assert found.id == target.id

    assert await store.find_by_id(uuid4()) is None  # unknown id


async def test_find_dead_lettered_keyset_pagination_reaches_101_plus(engine) -> None:
    """101+ dead-lettered rows must all be reachable — the old
    find_dead_lettered() (LIMIT 100, no cursor) silently hid every row past
    the 100th from list_dead_lettered() / retry_all_dead_lettered()."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=1)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=1, start_loop=False)

    total = 105
    for i in range(total):
        await store.save(_pub(i, attempt_count=1))  # >= threshold(1) → dead-lettered

    first_page = await store.find_dead_lettered()
    assert len(first_page) == 100  # single-page call keeps its old capped contract

    all_dead = await outbox.list_dead_lettered()
    assert len(all_dead) == total  # the plugin pages through every row
    assert len({p.id for p in all_dead}) == total  # no duplicates across pages


async def test_find_failing_returns_only_attempted_undelivered_not_dead_rows(engine) -> None:
    """Failing = attempt_count > 0, not completed, and below the dead-letter
    threshold: never-attempted, delivered and dead-lettered rows are excluded."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=3)
    fresh = _pub(0, attempt_count=0)
    first_failure = _pub(1, attempt_count=1, last_error="boom")
    last_chance = _pub(2, attempt_count=2, last_error="boom again")
    dead = _pub(3, attempt_count=3, last_error="gave up")
    done = _pub(4, attempt_count=1, completed_at=datetime.now(UTC))
    for pub in (fresh, first_failure, last_chance, dead, done):
        await store.save(pub)

    failing = await store.find_failing()

    assert {p.id for p in failing} == {first_failure.id, last_chance.id}
    assert {p.last_error for p in failing} == {"boom", "boom again"}


async def test_find_failing_keyset_pagination_reaches_101_plus(engine) -> None:
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=10)
    total = 105
    base = datetime.now(UTC) - timedelta(hours=1)
    for i in range(total):
        await store.save(_pub(i, attempt_count=1, published_at=base + timedelta(seconds=i)))

    first_page = await store.find_failing()
    assert len(first_page) == 100
    last = first_page[-1]
    assert last.published_at is not None
    second_page = await store.find_failing(after=(last.published_at, last.id), limit=100)

    assert len(second_page) == total - 100
    assert len({p.id for p in first_page + second_page}) == total


async def test_outbox_list_failing_pages_through_the_sqlite_store(engine) -> None:
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=10)
    outbox.configure(store, JsonEventSerializer(), dead_letter_after_attempts=10, start_loop=False)
    total = 103
    for i in range(total):
        await store.save(_pub(i, attempt_count=2))

    failing = await outbox.list_failing()

    assert len({p.id for p in failing}) == total


async def test_last_attempt_at_persists_and_round_trips(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    ts = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    pub = _pub(1, attempt_count=1, last_attempt_at=ts)
    await store.save(pub)

    (found,) = await store.find_incomplete(timedelta(0))
    assert found.last_attempt_at == ts


# ---------------------------------------------------------------------------
# regression: after-commit hook + active-store lifecycle
# ---------------------------------------------------------------------------


async def test_dispose_unregisters_after_commit_hook(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    assert postgres_outbox._hook_installed is True
    assert postgres_outbox._active_store is store

    await store.dispose()

    assert postgres_outbox._hook_installed is False
    assert postgres_outbox._active_store is None


async def test_second_store_restores_previous_on_dispose(engine) -> None:
    # Constructing a second store must not permanently hijack dispatch routing;
    # disposing it restores the first (LIFO), rather than blanking it.
    first = PostgresPublicationStore(engine=engine)
    second = PostgresPublicationStore(engine=engine)
    assert postgres_outbox._active_store is second

    await second.dispose()
    assert postgres_outbox._active_store is first  # restored, not None
    assert postgres_outbox._hook_installed is True  # first still needs the hook

    await first.dispose()
    assert postgres_outbox._active_store is None
    assert postgres_outbox._hook_installed is False


# ---------------------------------------------------------------------------
# regression: FOR UPDATE SKIP LOCKED row-claiming (concurrent-sweep safety)
# ---------------------------------------------------------------------------


async def test_skip_locked_enabled_only_on_postgres() -> None:
    """The row-claim optimization is gated on the Postgres dialect: SQLite has
    no row locking and would reject the clause. The flag drives whether
    find_incomplete adds it (#37).

    The Postgres case uses a stub engine exposing only ``dialect.name`` — the
    constructor reads exactly that to set the flag, and the asyncpg driver isn't
    installed in this (driverless) test environment.
    """

    class _StubDialect:
        name = "postgresql"

    class _StubEngine:
        dialect = _StubDialect()

    assert PostgresPublicationStore(engine=_StubEngine())._supports_skip_locked is True

    sqlite = create_async_engine("sqlite+aiosqlite://")
    try:
        assert PostgresPublicationStore(engine=sqlite)._supports_skip_locked is False
    finally:
        await sqlite.dispose()


def test_find_incomplete_statement_emits_skip_locked_on_postgres() -> None:
    """The locking clause find_incomplete attaches compiles to the exact
    Postgres SQL that lets concurrent sweepers partition rows instead of both
    grabbing the same ones. Pins the SQL so a regression dropping the clause
    (silently reintroducing cross-worker double-dispatch) is caught in CI
    without a Postgres server."""
    stmt = (
        select(EventPublicationRow)
        .where(EventPublicationRow.completed_at.is_(None))
        .with_for_update(skip_locked=True)
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE SKIP LOCKED" in compiled


async def test_after_commit_and_sweep_race_is_bounded(engine) -> None:
    """The after-commit dispatch task and the retry sweep can race for the SAME
    freshly-committed rows. The reopen-guard + mark_complete must keep that
    bounded: every event delivered (at-least-once), but no row delivered more
    than once per racing path (#41). A regression that re-delivers in a loop
    would blow past the bound."""
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener()

    sessionmaker = async_sessionmaker(engine)
    n = 20
    pubs = [_pub(i) for i in range(n)]
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            for pub in pubs:
                await store.save(pub)
            await session.commit()  # schedules after-commit tasks; rows now sweepable
        finally:
            outbox._current_session.reset(token)

    # Race a manual sweep (the retry-loop path) against the in-flight
    # after-commit dispatch tasks for the very same rows.
    async def sweep() -> None:
        for pub in await store.find_incomplete(timedelta(0)):
            await store._dispatch_after_commit(pub.id)

    await asyncio.gather(store.wait_for_dispatch(), sweep())

    # At-least-once: nothing lost.
    assert set(received) == set(range(n))
    # Bounded: two racing delivery paths → at most 2 deliveries per event, never
    # an unbounded storm.
    counts = Counter(received)
    assert max(counts.values()) <= 2, f"unbounded re-delivery: {counts}"


# ---------------------------------------------------------------------------
# Outbox claims — lease-mode claim_batch/renew_claim/complete_claim/
# fail_claim, and advisory-lock capability gating
# ---------------------------------------------------------------------------


async def test_claim_batch_atomically_claims_and_commits(engine) -> None:
    """claim_batch must commit the claim BEFORE the caller dispatches: a crash
    between claim and dispatch must leave a durable, independently-visible
    claim (that simply expires and gets reclaimed), never an uncommitted one."""
    store = PostgresPublicationStore(engine=engine)
    await store.save(_pub(1))
    await store.save(_pub(2))

    claimed = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=30.0, older_than=timedelta(0)
    )

    assert len(claimed) == 2
    assert all(p.claim_token for p in claimed)
    # Committed independently of any dispatch — a fresh session sees the claim.
    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        for p in claimed:
            row = await s.get(EventPublicationRow, p.id)
            assert row.claim_owner == "worker-a"
            assert row.claim_token == p.claim_token
            assert row.claim_until is not None


async def test_claim_batch_excludes_rows_with_an_active_lease(engine) -> None:
    """A row claimed by one sweeper with a not-yet-expired lease must not be
    handed to a second sweeper's claim_batch — this is the mechanism that
    prevents two concurrent sweepers from double-dispatching under lease mode."""
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)

    first = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert [p.id for p in first] == [pub.id]

    second = await store.claim_batch(
        owner="worker-b", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert second == []  # worker-a's lease is still active


async def test_claim_batch_reclaims_expired_lease(engine) -> None:
    """A lease that already expired (the claiming sweeper crashed, or its
    renewal loop starved) must be reclaimable by the next sweeper — otherwise
    a crashed sweeper's claims block the row forever."""
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)

    await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=-1.0, older_than=timedelta(0)
    )  # already-expired lease (negative seconds)

    reclaimed = await store.claim_batch(
        owner="worker-b", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert [p.id for p in reclaimed] == [pub.id]
    assert reclaimed[0].claim_token != None  # noqa: E711 — new token, not worker-a's


async def test_renew_claim_extends_lease_and_rejects_stale_token(engine) -> None:
    store = PostgresPublicationStore(engine=engine)
    await store.save(_pub(1))
    (claim,) = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=5.0, older_than=timedelta(0)
    )

    assert await store.renew_claim(claim.id, claim.claim_token, 60.0) is True
    assert await store.renew_claim(claim.id, "not-the-real-token", 60.0) is False

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, claim.id)
        claim_until = row.claim_until.replace(tzinfo=UTC)  # SQLite drops tz on read
        assert claim_until > datetime.now(UTC) + timedelta(seconds=30)  # the real renewal applied


async def test_complete_claim_is_fenced_by_token(engine) -> None:
    """A stale/mismatched token must not be able to complete a row that a
    different (newer) claimant now owns."""
    store = PostgresPublicationStore(engine=engine)
    pub = _pub(1)
    await store.save(pub)
    (claim,) = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )

    assert await store.complete_claim(claim.id, "stale-token", "update") is False
    assert await store.find_incomplete(
        timedelta(0)
    )  # still incomplete — rejected write applied nothing

    assert await store.complete_claim(claim.id, claim.claim_token, "update") is True
    assert await store.find_incomplete(timedelta(0)) == []


async def test_fail_claim_is_fenced_by_token(engine) -> None:
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=5)
    pub = _pub(1)
    await store.save(pub)
    (claim,) = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    claim.attempt_count = 1
    claim.last_error = "boom"

    assert await store.fail_claim(claim, "stale-token") is False

    assert await store.fail_claim(claim, claim.claim_token) is True
    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, claim.id)
        assert row.attempt_count == 1
        assert row.last_error == "boom"
        assert row.claim_token is None  # released so the next sweep can reclaim immediately


async def test_fail_claim_does_not_touch_a_completed_row(engine) -> None:
    """Completing a row in ``update`` mode leaves its claim token in place, so a
    failure reported late under that token must not be written onto the
    delivered row: it would stamp an error and a dead-letter flag on it."""
    store = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=2)
    await store.save(_pub(1))
    (claim,) = await store.claim_batch(
        owner="worker-a", batch_size=10, lease_seconds=60.0, older_than=timedelta(0)
    )
    assert await store.complete_claim(claim.id, claim.claim_token, "update") is True
    claim.attempt_count = 2
    claim.last_error = "late failure"

    assert await store.fail_claim(claim, claim.claim_token) is False

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        row = await s.get(EventPublicationRow, claim.id)
        assert row.completed_at is not None
        assert (row.attempt_count, row.last_error, row.is_dead_lettered) == (0, None, False)


async def test_try_lock_publication_requires_postgres_engine(engine) -> None:
    """advisory_lock mode is Postgres-only (pg_try_advisory_lock); a
    SQLite-backed store must refuse rather than silently no-op."""
    store = PostgresPublicationStore(engine=engine)
    assert store.supports_advisory_lock is False

    with pytest.raises(ConfigurationError, match=r"[Pp]ostgres"):
        await store.try_lock_publication(uuid4())


async def test_try_lock_publication_closes_connection_on_execute_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """try_lock_publication had no try/finally around the ``SELECT
    pg_try_advisory_lock`` execute(): a transient lock-query error (connection
    blip, failover, statement timeout) leaked the checked-out connection back
    to the pool on every retry. The connection must be released even when the
    lock query itself raises, and invalidated: whether the lock was taken is
    unknown."""

    class _StubDialect:
        name = "postgresql"

    class _RaisingConn:
        def __init__(self) -> None:
            self.closed = False
            self.invalidated = False

        async def execution_options(self, **opts: Any) -> _RaisingConn:
            return self

        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("boom")

        async def invalidate(self) -> None:
            self.invalidated = True

        async def close(self) -> None:
            self.closed = True

    class _StubEngine:
        dialect = _StubDialect()

        def __init__(self) -> None:
            self.conn = _RaisingConn()

        async def connect(self) -> Any:
            return self.conn

    engine_stub = _StubEngine()
    store = PostgresPublicationStore(engine=engine_stub)
    monkeypatch.setattr(store, "_lock_connection_engine", lambda: engine_stub)

    with pytest.raises(RuntimeError, match="boom"):
        await store.try_lock_publication(uuid4())

    assert engine_stub.conn.closed is True
    assert engine_stub.conn.invalidated is True


async def test_try_lock_publication_does_not_leave_an_open_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``conn.execute()`` autobegins a transaction that then stays open for
    the whole dispatch ``_sweep_advisory`` holds the lock connection across —
    idle-in-transaction on real Postgres, blocking VACUUM and risking
    idle_in_transaction_session_timeout killing the backend mid-dispatch. The
    lock connection must be put in autocommit mode before the lock query so no
    transaction is ever opened."""

    class _StubDialect:
        name = "postgresql"

    class _FakeConn:
        def __init__(self) -> None:
            self.autocommit = False

        async def execution_options(self, **opts: Any) -> _FakeConn:
            if opts.get("isolation_level") == "AUTOCOMMIT":
                self.autocommit = True
            return self

        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            class _Result:
                def scalar(self_inner) -> Any:
                    return 1

            return _Result()

        def in_transaction(self) -> bool:
            return not self.autocommit

        async def close(self) -> None:
            pass

    class _StubEngine:
        dialect = _StubDialect()

        def __init__(self) -> None:
            self.conn = _FakeConn()

        async def connect(self) -> Any:
            return self.conn

    engine_stub = _StubEngine()
    store = PostgresPublicationStore(engine=engine_stub)
    monkeypatch.setattr(store, "_lock_connection_engine", lambda: engine_stub)

    handle = await store.try_lock_publication(uuid4())

    assert handle is not None
    assert handle.in_transaction() is False, (
        "advisory-lock connection left an open transaction across the dispatch it holds"
    )


_OUTBOX_TIMESTAMP_COLUMNS = {
    "event_publications": ("published_at", "completed_at", "last_attempt_at", "claim_until"),
    "event_publications_archive": ("published_at", "completed_at", "last_attempt_at"),
}


def _outbox_timestamp_types(dialect: Any) -> dict[str, str]:
    from sqlalchemy import DateTime

    from modulith.adapters.postgres_outbox import EventPublicationArchiveRow, EventPublicationRow

    compiled = {}
    for model in (EventPublicationRow, EventPublicationArchiveRow):
        table = model.__table__
        datetime_columns = {c.name for c in table.columns if isinstance(c.type, DateTime)}
        assert datetime_columns == set(_OUTBOX_TIMESTAMP_COLUMNS[table.name])
        for name in datetime_columns:
            compiled[f"{table.name}.{name}"] = table.c[name].type.compile(dialect=dialect)
    return compiled


def test_outbox_timestamp_columns_keep_microseconds_on_mysql_and_mariadb() -> None:
    """MySQL's bare DATETIME rounds to whole seconds, so a lease or backoff
    stamp would be stored up to half a second away from the value written."""
    from sqlalchemy.dialects.mysql import dialect as mysql_dialect
    from sqlalchemy.dialects.mysql import mariadb

    for dialect in (mysql_dialect(), mariadb.MariaDBDialect()):
        assert set(_outbox_timestamp_types(dialect).values()) == {"DATETIME(6)"}


def test_outbox_timestamp_columns_are_unchanged_on_postgres_and_sqlite() -> None:
    from sqlalchemy.dialects import sqlite

    assert set(_outbox_timestamp_types(postgresql.dialect()).values()) == {
        "TIMESTAMP WITH TIME ZONE"
    }
    assert set(_outbox_timestamp_types(sqlite.dialect()).values()) == {"DATETIME"}


def _offline_ddl(url: str, capsys: pytest.CaptureFixture[str]) -> str:
    """The SQL ``alembic upgrade 0008:head --sql`` renders for ``url`` (no connection)."""
    from alembic import command
    from alembic.config import Config

    import modulith.adapters as adapters_pkg

    cfg = Config()
    cfg.set_main_option("script_location", str(Path(adapters_pkg.__file__).parent / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "0008_outbox_trace_context:head", sql=True)
    return capsys.readouterr().out


@pytest.mark.parametrize("scheme", ["mysql+pymysql", "mariadb+pymysql"])
def test_migration_widens_outbox_timestamps_to_microseconds_on_mysql_and_mariadb(
    scheme: str, capsys: pytest.CaptureFixture[str]
) -> None:
    ddl = _offline_ddl(f"{scheme}://user:pass@offline-host-never-contacted/outbox", capsys)

    for table, columns in _OUTBOX_TIMESTAMP_COLUMNS.items():
        for column in columns:
            nullability = "NOT NULL" if column == "published_at" else "NULL"
            assert f"ALTER TABLE {table} CHANGE {column} {column} DATETIME(6) {nullability};" in ddl
    assert ddl.count("DATETIME(6)") == 7


@pytest.mark.parametrize("url", ["postgresql+psycopg://u:p@host/db", "sqlite:///offline.db"])
def test_migration_leaves_outbox_timestamps_alone_off_mysql(
    url: str, capsys: pytest.CaptureFixture[str]
) -> None:
    ddl = _offline_ddl(url, capsys)

    assert "published_at" not in ddl
    assert "claim_until" not in ddl
