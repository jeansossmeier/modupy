"""Regression tests for the Postgres outbox adapter.

Each test's docstring states the contract it pins. The
adapter is portable SQLAlchemy 2.0, so these run against aiosqlite (no Docker)
— a tmp-file DB with per-session connections (see the ``engine`` fixture for
why NOT StaticPool + :memory:). Async/timing behavior is synchronized with
events and bounded DB polls, never with bare sleeps.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
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
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


@event
@dataclass(frozen=True)
class G04Event:
    value: int


received: list[int] = []


async def record(event: G04Event) -> None:
    received.append(event.value)


@pytest.fixture
async def engine(tmp_path: Path) -> Any:
    """A file-backed aiosqlite engine with one connection PER session (NullPool).

    Deliberately NOT StaticPool + ``sqlite+aiosqlite://``: that hands every
    session the same single DBAPI connection, and SQLite has exactly one
    transaction per connection — so under real concurrency (this file starts
    the retry loop) one session's close (ROLLBACK) clobbered another session's
    in-flight INSERT->COMMIT, a topology impossible on per-connection Postgres.
    See test_concurrent_reader_close_does_not_roll_back_inflight_save for the
    deterministic repro of the retry-loop flake this caused.
    A tmp-file DB also survives connection invalidation (a StaticPool reconnect
    produced a brand-new empty :memory: database mid-test)."""
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'g04.db'}",
        poolclass=NullPool,
    )
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture(autouse=True)
def _reset() -> Any:
    received.clear()
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


def _bootstrap_with_listener(handler: Any) -> None:
    _runtime.configure(package="g04test", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(G04Event, handler)


def _pub(value: int, handler: Any = record, **overrides: Any) -> EventPublication:
    serializer = JsonEventSerializer()
    defaults: dict[str, Any] = dict(
        id=uuid4(),
        payload=serializer.serialize(G04Event(value=value)),
        event_type=f"{G04Event.__module__}.{G04Event.__qualname__}",
        listener=outbox._listener_id(handler),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


# ---------------------------------------------------------------------------
# Pending publications must never be dropped silently
# ---------------------------------------------------------------------------


async def test_after_commit_with_pending_but_no_store_warns_loudly(engine: Any, caplog) -> None:
    """Publications queued on session.info while no store is active
    were silently discarded by the after-commit hook (bare early return, zero
    log output) — unlike the no-running-loop branch, which warns. The committed
    row is durable and recoverable by a later store's retry sweep, but the
    dropped dispatch must be observable."""
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener(record)

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            pub = _pub(1)
            await store.save(pub)
            # Simulate the dispose()-during-shutdown race: the store is
            # deactivated after save() queued the id but before this session
            # committed, while the after-commit hook is still registered.
            postgres_outbox._active_store = None
            with caplog.at_level(logging.WARNING, logger="modulith.adapters.postgres"):
                await session.commit()
        finally:
            outbox._current_session.reset(token)

    assert any(
        "no active" in r.getMessage() and r.levelno >= logging.WARNING for r in caplog.records
    ), f"expected a loud warning about the dropped dispatch, got: {caplog.records}"
    assert received == []  # nothing dispatched...
    # ...but the publication is durably committed and sweep-recoverable.
    found = await store.find_incomplete(timedelta(0))
    assert [p.id for p in found] == [pub.id]


# ---------------------------------------------------------------------------
# Retry-loop-driven failures must advance attempt_count in the DB
# ---------------------------------------------------------------------------


async def test_retry_loop_driven_failures_advance_attempt_count_in_db(engine: Any) -> None:
    """The retry task copies the creating call site's contextvars;
    when it is (re)created while a request session is bound, save() inside a
    retry-driven _record_failure read that frozen, closed session and enlisted
    the bookkeeping row there — never committed, so attempt_count stayed stuck
    in the DB, backoff never grew, and dead-lettering could never fire. The
    retry task must run session-less so failure re-saves take the
    standalone-commit path and attempt_count really advances."""
    fail_count = 0

    async def always_fails(event: G04Event) -> None:
        nonlocal fail_count
        fail_count += 1
        raise RuntimeError("permanent listener failure")

    _bootstrap_with_listener(always_fails)
    store = PostgresPublicationStore(engine=engine)

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            # The retry task is created while this request session is bound —
            # the exact precondition that froze the session into its context.
            outbox.configure(
                store,
                JsonEventSerializer(),
                retry_interval_seconds=0.05,
                retry_stale_seconds=0.0,
                start_loop=True,
            )
        finally:
            outbox._current_session.reset(token)

    # A committed-but-undelivered row: the crash sweep + retry sweeps must
    # dispatch it, fail, and durably record each attempt.
    await store.save(_pub(9, always_fails))

    async def db_attempts() -> int:
        found = await store.find_incomplete(timedelta(0))
        return found[0].attempt_count if found else 0

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 8.0
    while loop.time() < deadline and await db_attempts() < 2:
        await asyncio.sleep(0.02)

    await outbox.shutdown()
    assert fail_count >= 2, "retry loop never re-dispatched the failing publication"
    assert await db_attempts() >= 2, (
        "retry-loop-driven failures were not persisted: attempt_count is stuck, "
        "so backoff and dead-lettering can never engage"
    )


# ---------------------------------------------------------------------------
# Concurrent session close must not clobber an in-flight save
# ---------------------------------------------------------------------------


async def test_concurrent_reader_close_does_not_roll_back_inflight_save(engine: Any) -> None:
    """Regression for the ~7%-under-load flake in
    test_retry_loop_driven_failures_advance_attempt_count_in_db:
    the old engine fixture (StaticPool + ``sqlite+aiosqlite://``) handed EVERY
    session the same single DBAPI connection, and SQLite has exactly one
    transaction per connection. When the retry loop's crash-sweep read session
    closed (Session close issues ROLLBACK) inside a standalone ``save()``'s
    INSERT->COMMIT await gap, it rolled back the in-flight INSERT on the shared
    connection; save's COMMIT then no-opped and returned success, the row never
    existed, and attempt_count stayed 0 forever (``assert 0 >= 2``). Impossible
    on real Postgres, where each session has its own connection — so the
    fixture must give sessions independent connections too.

    This test forces the racing interleave deterministically: ``commit()`` is
    widened to flush -> wait -> commit (exposing the natural await gap, same
    barrier technique as the g03 race tests) while a sweep-shaped reader
    session closes inside the gap. The really-saved row must survive."""
    store = PostgresPublicationStore(engine=engine)
    insert_flushed = asyncio.Event()
    reader_closed = asyncio.Event()

    class GapCommitSession(AsyncSession):
        async def commit(self) -> None:
            await self.flush()  # INSERT hits the connection, inside the open tx
            insert_flushed.set()
            await reader_closed.wait()  # the sweep-shaped reader closes here
            await super().commit()

    store._sessionmaker = cast(
        "async_sessionmaker[AsyncSession]",
        async_sessionmaker(engine, class_=GapCommitSession, expire_on_commit=False),
    )
    plain_sessionmaker = async_sessionmaker(engine)

    async def sweep_like_reader() -> None:
        # Mirrors _retry_loop's crash sweep -> store.find_incomplete: a
        # read-only session whose exit closes it (ROLLBACK on its connection).
        async with plain_sessionmaker() as s:
            await s.execute(select(EventPublicationRow))
            await insert_flushed.wait()  # close AFTER the INSERT, BEFORE COMMIT
        reader_closed.set()

    reader = asyncio.create_task(sweep_like_reader())
    pub = _pub(11)
    await store.save(pub)  # the REAL standalone insert+commit path
    await reader

    found = await store.find_incomplete(timedelta(0))
    assert [p.id for p in found] == [pub.id], (
        "a concurrent session close rolled back the in-flight standalone save(): "
        "the row vanished without any error, so retry bookkeeping can never engage"
    )
    await store.dispose()


# ---------------------------------------------------------------------------
# A retrying backlog must not starve newer publications forever
# ---------------------------------------------------------------------------


async def test_find_incomplete_rotates_past_retrying_backlog(engine: Any) -> None:
    """find_incomplete's LIMIT 100 + raw published_at ordering let a
    backlog of >100 legitimately-retrying rows occupy the window on every
    sweep forever (retries never change published_at), so newer publications
    were never surfaced. Ordering by the last attempt instead (falling back to
    published_at for never-attempted rows) rotates attempted rows to the back
    of the queue, so the whole backlog — including fresh rows — is surfaced
    within a bounded number of sweeps."""
    store = PostgresPublicationStore(engine=engine)
    base = datetime.now(UTC) - timedelta(hours=2)

    # 120 old rows already under retry (attempt_count 1, still retryable).
    stuck = [
        _pub(
            i,
            published_at=base + timedelta(seconds=i),
            attempt_count=1,
            last_attempt_at=base + timedelta(seconds=i),
        )
        for i in range(120)
    ]
    for p in stuck:
        await store.save(p)
    # 5 newer, healthy publications behind the backlog.
    fresh = [_pub(1000 + i, published_at=base + timedelta(minutes=30 + i)) for i in range(5)]
    for p in fresh:
        await store.save(p)
    fresh_ids = {p.id for p in fresh}

    surfaced: set[Any] = set()
    for _ in range(3):  # three simulated retry sweeps
        batch = await store.find_incomplete(timedelta(0))
        assert len(batch) == 100  # the window stays full
        surfaced |= {p.id for p in batch}
        for p in batch:  # mirror _record_failure for every attempted row
            p.attempt_count += 1
            p.last_attempt_at = datetime.now(UTC)
            await store.save(p)

    assert fresh_ids <= surfaced, (
        f"newer publications starved behind the retrying backlog: "
        f"{len(fresh_ids - surfaced)} of {len(fresh_ids)} never surfaced in 3 sweeps"
    )


# ---------------------------------------------------------------------------
# Out-of-order dispose must not resurrect a disposed store
# ---------------------------------------------------------------------------


async def test_out_of_order_dispose_does_not_resurrect_disposed_store(engine: Any) -> None:
    """dispose() restored ``_active_store`` from a single
    ``_prev_store`` back-pointer, which only unwinds correctly in strict LIFO
    order. Disposing first-then-second (creation order) resurrected the
    already-disposed first store — possibly with a closed engine — as the live
    after-commit dispatch target, and left the global hook installed forever."""
    first = PostgresPublicationStore(engine=engine)
    second = PostgresPublicationStore(engine=engine)
    assert postgres_outbox._active_store is second

    await first.dispose()  # out of creation order: first goes first
    assert postgres_outbox._active_store is second  # routing unaffected

    await second.dispose()
    # No live store remains: the disposed `first` must NOT be resurrected as
    # the dispatch target, and the global hook must be unregistered.
    assert postgres_outbox._active_store is None
    assert postgres_outbox._hook_installed is False


# ---------------------------------------------------------------------------
# save() must accept a bound plain (sync) SQLAlchemy Session
# ---------------------------------------------------------------------------


def test_save_with_bound_sync_session_persists_and_defers_to_sweep(tmp_path) -> None:
    """save() unconditionally accessed ``session.sync_session.info``
    — an AsyncSession-only attribute — so binding the plain (sync) Session the
    degraded no-running-loop path is documented for crashed with a confusing
    AttributeError. The real save() must enlist the row and queue the pending
    id on a sync session too; the commit (no running loop) then takes the
    warning branch and the retry sweep recovers the committed row."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SyncSession

    db = tmp_path / "syncsave.db"
    sync_engine = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(sync_engine)
    store = PostgresPublicationStore(engine=create_async_engine(f"sqlite+aiosqlite:///{db}"))
    pub = _pub(21)

    with SyncSession(sync_engine) as session:
        token = bind_session(session)
        try:
            # The REAL save(), not hand-rolled session.info bookkeeping —
            # this is exactly the access that raised AttributeError.
            asyncio.run(store.save(pub))
            assert session.info.get("_modulith_pending") == [pub.id]
            session.commit()  # fires after_commit with NO running loop
        finally:
            outbox._current_session.reset(token)

    # The committed row is durable and recoverable by the retry sweep.
    found = asyncio.run(store.find_incomplete(timedelta(0)))
    assert [p.id for p in found] == [pub.id]
    asyncio.run(store.dispose())
    sync_engine.dispose()


# ---------------------------------------------------------------------------
# _reset_for_testing must not leak in-flight dispatch tasks
# ---------------------------------------------------------------------------


async def test_reset_for_testing_cancels_inflight_dispatch_tasks(engine: Any) -> None:
    """postgres_outbox._reset_for_testing() cleared the module
    globals but left in-flight after-commit dispatch tasks running on the
    orphaned store. Once the sibling outbox._reset_for_testing() nulled the
    plugin's _store, the orphaned task resumed against torn-down module state
    and crashed with an AssertionError swallowed by _dispatch_after_commit's
    bare except — the publication was left incomplete forever with nothing
    surfaced. The reset must cancel the tracked tasks instead."""
    release = asyncio.Event()
    started = asyncio.Event()

    async def blocked(event: G04Event) -> None:
        started.set()
        await release.wait()  # held in flight until the test decides

    _bootstrap_with_listener(blocked)
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(3, blocked))
            await session.commit()  # schedules the after-commit dispatch task
        finally:
            outbox._current_session.reset(token)

    await asyncio.wait_for(started.wait(), timeout=2)  # listener is in flight
    (task,) = store._inflight

    # Exactly the fixture-teardown order used across the suite.
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()

    done, _pending = await asyncio.wait({task}, timeout=1.0)
    assert task in done, (
        "in-flight dispatch task leaked past _reset_for_testing and is still "
        "running against torn-down module state"
    )
    assert task.cancelled()


# ---------------------------------------------------------------------------
# Non-UTC-aware timestamps must round-trip as the same instant
# ---------------------------------------------------------------------------


async def test_non_utc_aware_timestamps_round_trip_as_same_instant(engine: Any) -> None:
    """On the SQLite dialect the driver stores the wall-clock digits
    and drops the offset; _aware() then reattached UTC on read, silently
    shifting any non-UTC-aware published_at/last_attempt_at/completed_at by its
    offset (10:00+05:00 came back as 10:00+00:00 — a 5-hour corruption).
    Normalizing to UTC on WRITE makes the read-side
    assumption hold on every dialect."""
    store = PostgresPublicationStore(engine=engine)
    plus5 = timezone(timedelta(hours=5))
    published = datetime(2026, 1, 1, 10, 0, tzinfo=plus5)  # == 05:00:00 UTC
    attempted = datetime(2026, 1, 1, 11, 30, tzinfo=plus5)  # == 06:30:00 UTC
    pub = _pub(1, published_at=published, attempt_count=1, last_attempt_at=attempted)
    await store.save(pub)

    (found,) = await store.find_incomplete(timedelta(0))
    # Aware datetimes compare by instant: same moment, regardless of offset.
    assert found.published_at == published, (
        f"published_at corrupted: {found.published_at!r} != {published!r} "
        f"({published.astimezone(UTC)!r} as a UTC instant)"
    )
    assert found.last_attempt_at == attempted

    # completed_at takes the same write path via the standalone upsert.
    completed = datetime(2026, 1, 2, 9, 0, tzinfo=plus5)
    done_pub = _pub(2, completed_at=completed)
    await store.save(done_pub)
    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        from modulith.adapters.postgres_outbox import EventPublicationRow

        row = await s.get(EventPublicationRow, done_pub.id)
        assert row is not None and row.completed_at is not None
        stored = (
            row.completed_at if row.completed_at.tzinfo else row.completed_at.replace(tzinfo=UTC)
        )
        assert stored == completed


# ---------------------------------------------------------------------------
# Missing SQLAlchemy must fail with an actionable ImportError
# ---------------------------------------------------------------------------


def test_missing_sqlalchemy_raises_helpful_import_error() -> None:
    """postgres_outbox imports sqlalchemy at module level with no
    guard (unlike redis_broker's lazy import / cli.py's try-except), so a base
    install importing it got a bare 'No module named sqlalchemy'. The import
    must fail with an ImportError that names the ``modupy[postgres]`` extra.
    Run in a subprocess with sqlalchemy blocked, since this venv has it."""
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(
        """
        import builtins
        import sys

        real_import = builtins.__import__

        def block_sqlalchemy(name, *args, **kwargs):
            if name == "sqlalchemy" or name.startswith("sqlalchemy."):
                raise ImportError(f"No module named {name!r}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = block_sqlalchemy
        try:
            import modulith.adapters.postgres_outbox
        except ImportError as exc:
            assert "modupy[postgres]" in str(exc), f"unhelpful message: {exc}"
            sys.exit(0)
        sys.exit(1)  # imported despite sqlalchemy being unavailable?!
        """
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
