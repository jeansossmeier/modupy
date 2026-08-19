"""Real-Postgres integration tests for the outbox adapter.

The default adapter suite (tests/test_postgres_adapter.py) runs on aiosqlite —
fast, no Docker — but SQLite's semantics diverge from Postgres on exactly the
points this adapter is tuned for: ``FOR UPDATE SKIP LOCKED`` row-claiming,
tz-aware ``TIMESTAMPTZ``, ``BYTEA`` payloads, and the ``WHERE completed_at IS
NULL`` partial index. A regression in any of those ships green on SQLite.

The shared ``postgres_url`` and ``pg_engine`` fixtures create a disposable
database and schema from either ``MODULITH_TEST_POSTGRES_URL`` or a
testcontainers instance, so these tests never alter the supplied database.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from modulith import EventPublication, event
from modulith.adapters import postgres_outbox
from modulith.adapters.postgres_outbox import (
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
class PgIntegrationEvent:
    value: int


_received: list[int] = []


async def _record(evt: PgIntegrationEvent) -> None:
    _received.append(evt.value)


@pytest.fixture
async def engine(pg_engine):
    """Alias the shared real-Postgres engine fixture (fresh outbox schema)."""
    yield pg_engine


@pytest.fixture(autouse=True)
def _reset():
    _received.clear()
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()
    yield
    _runtime._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


def _pub(value: int, **overrides) -> EventPublication:
    serializer = JsonEventSerializer()
    defaults = dict(
        id=uuid4(),
        payload=serializer.serialize(PgIntegrationEvent(value=value)),
        event_type=f"{PgIntegrationEvent.__module__}.{PgIntegrationEvent.__qualname__}",
        listener=outbox._listener_id(_record),
        published_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return EventPublication(**defaults)


async def test_postgres_url_uses_uuid_disposable_database(postgres_url: str) -> None:
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    database = make_url(postgres_url).database
    assert database is not None
    prefix = "modupy_test_"
    assert database.startswith(prefix)
    assert UUID(hex=database.removeprefix(prefix)).hex == database.removeprefix(prefix)

    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT current_database()")) == database
    finally:
        await engine.dispose()


def _bootstrap_with_listener() -> None:
    _runtime.configure(package="pgint", auto_discover=False)
    _runtime.ensure_bootstrapped()
    assert _runtime.event_bus is not None
    _runtime.event_bus.register(PgIntegrationEvent, _record)


# ---------------------------------------------------------------------------
# tz-aware timestamps + BYTEA round-trip (SQLite cannot validate these) — #38
# ---------------------------------------------------------------------------


async def test_tz_aware_timestamp_and_bytea_round_trip(engine) -> None:
    """TIMESTAMPTZ preserves the timezone and BYTEA preserves arbitrary bytes —
    neither is faithfully exercised on SQLite (which drops tz and has no real
    binary type)."""
    store = PostgresPublicationStore(engine=engine)
    ts = datetime(2026, 6, 1, 12, 30, tzinfo=UTC)
    # A payload with non-UTF8 bytes — JSONB could not store this; BYTEA must.
    binary_payload = b"\x00\x01\x02\xff\xfe non-utf8 \x80"
    pub = _pub(1, published_at=ts, last_attempt_at=ts, attempt_count=1, payload=binary_payload)
    await store.save(pub)

    (found,) = await store.find_incomplete(timedelta(0))
    assert found.payload == binary_payload
    # Comes back tz-aware (not naive) and equal to the original instant.
    assert found.published_at == ts
    assert found.published_at.tzinfo is not None
    assert found.last_attempt_at == ts


async def test_schema_translate_map_recipe_isolates_outbox_tables(postgres_url: str) -> None:
    """An app-built engine carrying ``execution_options(schema_translate_map=
    ...)`` routes the outbox's Core tables into a target schema without any
    change to this adapter — ``postgres_outbox.py`` never references a schema
    itself, so the translation is entirely the caller's responsibility."""
    from sqlalchemy import inspect
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.schema import CreateSchema, DropSchema

    schema = f"orders_{uuid4().hex}"
    sentinel_schema = f"orders_sentinel_{uuid4().hex}"
    expected_tables = {"event_publications", "event_publications_archive"}
    admin_engine = create_async_engine(postgres_url)
    engine = create_async_engine(postgres_url).execution_options(
        schema_translate_map={None: schema}
    )

    def _table_names(sync_conn, schema_name: str):
        return set(inspect(sync_conn).get_table_names(schema=schema_name))

    async with admin_engine.begin() as conn:
        await conn.execute(CreateSchema(sentinel_schema))
    try:
        async with engine.begin() as conn:
            await conn.execute(CreateSchema(schema))
            await conn.run_sync(postgres_outbox.Base.metadata.create_all)

        store = PostgresPublicationStore(engine=engine)
        pub = _pub(99)
        await store.save(pub)
        (found,) = await store.find_incomplete(timedelta(0))
        assert found.id == pub.id

        async with admin_engine.connect() as conn:
            scoped = await conn.run_sync(_table_names, schema)
            public = await conn.run_sync(_table_names, "public")
        assert scoped == expected_tables
        assert public.isdisjoint(expected_tables)
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
            await engine.dispose()
            await admin_engine.dispose()


# ---------------------------------------------------------------------------
# transactional atomicity on the real dialect — #59
# ---------------------------------------------------------------------------


async def test_business_rollback_discards_publication_on_real_postgres(engine) -> None:
    """The outbox's reason to exist: a publication committed in the business
    transaction is durable; one whose transaction rolls back is discarded. The
    plugin-level tests assert this against a stub that cannot truly roll back;
    here it is exercised against real Postgres MVCC."""
    store = PostgresPublicationStore(engine=engine)
    outbox.configure(store, JsonEventSerializer(), start_loop=False)
    _bootstrap_with_listener()
    sessionmaker = async_sessionmaker(engine)

    # Committed → durable and dispatched.
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(10))
            await session.commit()
        finally:
            outbox._current_session.reset(token)
    await store.wait_for_dispatch()

    # Rolled back → row discarded, never dispatched.
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            await store.save(_pub(20))
            await session.rollback()
        finally:
            outbox._current_session.reset(token)
    await store.wait_for_dispatch()

    assert _received == [10]
    async with sessionmaker() as s:
        rows = (await s.execute(select(EventPublicationRow))).scalars().all()
        assert [r.payload for r in rows]  # the committed row survived
        values = {JsonEventSerializer().deserialize(r.payload, r.event_type).value for r in rows}
        assert values == {10}  # the rolled-back publication is absent
    await store.dispose()


# ---------------------------------------------------------------------------
# FOR UPDATE SKIP LOCKED partitions concurrent sweepers — #37 / #38
# ---------------------------------------------------------------------------


async def test_skip_locked_partitions_concurrent_sweepers(engine) -> None:
    """Two concurrent sweepers must NOT both claim the same rows. With FOR
    UPDATE SKIP LOCKED, the second transaction skips rows the first has locked,
    so their claimed sets are disjoint and together cover the backlog. On a
    plain SELECT both would grab everything (double-dispatch); on plain FOR
    UPDATE the second would block. This is the property SQLite cannot test."""
    store = PostgresPublicationStore(engine=engine)
    n = 6
    for i in range(n):
        await store.save(_pub(i))

    locking_stmt = (
        select(EventPublicationRow)
        .where(
            EventPublicationRow.completed_at.is_(None),
            EventPublicationRow.is_dead_lettered.is_(False),
        )
        .order_by(EventPublicationRow.published_at)
        .with_for_update(skip_locked=True)
        .limit(n)
    )

    sessionmaker = async_sessionmaker(engine)
    sa = sessionmaker()
    sb = sessionmaker()
    try:
        await sa.begin()
        await sb.begin()
        # A locks its claimed rows; B then skips them.
        rows_a = (await sa.execute(locking_stmt)).scalars().all()
        rows_b = (await sb.execute(locking_stmt)).scalars().all()
        ids_a = {r.id for r in rows_a}
        ids_b = {r.id for r in rows_b}

        assert ids_a, "first sweeper should claim some rows"
        assert ids_a.isdisjoint(ids_b), "skip-locked must prevent both claiming the same row"
        # A claimed everything (LIMIT n >= backlog), so B sees nothing left.
        assert ids_b == set()
        assert len(ids_a) == n
    finally:
        await sa.rollback()
        await sb.rollback()
        await sa.close()
        await sb.close()
    await store.dispose()


# ---------------------------------------------------------------------------
# find_incomplete: dead-letter exclusion + LIMIT-100 window
# ---------------------------------------------------------------------------


def _row(*, published_at, dead: bool = False, attempts: int = 0) -> EventPublicationRow:
    return EventPublicationRow(
        id=uuid4(),
        event_type=f"{PgIntegrationEvent.__module__}.{PgIntegrationEvent.__qualname__}",
        payload=b'{"value":1}',
        listener="pgint.listener",
        published_at=published_at,
        completed_at=None,
        attempt_count=attempts,
        last_error=None,
        last_attempt_at=None,
        is_dead_lettered=dead,
    )


async def test_find_incomplete_dead_letter_backlog_does_not_starve_live_rows(engine) -> None:
    """``find_incomplete``'s documented starvation scenario on real Postgres —
    a backlog of >100 dead-lettered rows, all OLDER than the live rows, would
    fill the LIMIT-100 window and hide every live retryable row if the
    ``is_dead_lettered IS FALSE`` filter regressed out of the WHERE clause.
    The SQL-level exclusion must surface every live row regardless."""
    base = datetime.now(UTC) - timedelta(hours=1)
    dead_rows = [
        _row(published_at=base + timedelta(seconds=i), dead=True, attempts=5) for i in range(150)
    ]
    live_rows = [_row(published_at=base + timedelta(minutes=30, seconds=i)) for i in range(5)]
    live_ids = {r.id for r in live_rows}  # captured pre-commit (commit expires instances)

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        s.add_all([*dead_rows, *live_rows])
        await s.commit()

    store = PostgresPublicationStore(engine=engine)
    try:
        found = await store.find_incomplete(timedelta(0))
    finally:
        await store.dispose()

    assert {p.id for p in found} == live_ids


async def test_find_incomplete_caps_the_sweep_window_at_100_rows(engine) -> None:
    """The sweep window is capped at 100 rows on real Postgres, and
    the cap admits the least-recently-attempted rows first (never-attempted
    rows sort by ``published_at``)."""
    base = datetime.now(UTC) - timedelta(hours=1)
    live_rows = [_row(published_at=base + timedelta(seconds=i)) for i in range(120)]
    oldest_100_ids = {r.id for r in live_rows[:100]}  # captured pre-commit

    sessionmaker = async_sessionmaker(engine)
    async with sessionmaker() as s:
        s.add_all(live_rows)
        await s.commit()

    store = PostgresPublicationStore(engine=engine)
    try:
        found = await store.find_incomplete(timedelta(0))
    finally:
        await store.dispose()

    assert len(found) == 100
    assert {p.id for p in found} == oldest_100_ids
