"""SQLAlchemy PublicationStore implementation for the transactional outbox.

This is the reference adapter. It demonstrates the complete pattern for a
storage-backed PublicationStore: schema definition, session-aware saves, and
after-commit dispatch hooking.

Distributed as ``modupy[postgres]`` (asyncpg). It is
named for Postgres and tuned for it (the pending-rows partial index), but is
built on portable SQLAlchemy 2.0 so it also runs on any async dialect
(aiosqlite in tests). Only the dialect-specific index variant differs.

Critical correctness pattern:
  1. ``save`` inside a bound transaction enlists the row in *that* session
     (not a fresh one), so the publication commits atomically with the
     business work. The pending id is queued on ``session.info`` so the
     after-commit hook can find it.
  2. The after-commit hook runs synchronously inside ``await session.commit()``
     and *schedules* (does not await) async dispatch tasks — commit completes
     before any listener runs.
  3. A rollback never fires after_commit, but ``session.info`` is *not* reset
     by SQLAlchemy — a reused session would carry the dead ids into its next
     commit. An after_transaction_end listener discards the queue explicitly
     when a transaction ends uncommitted (rollback or close), so
     queued-but-uncommitted publications are never dispatched, and logs each
     such discard at WARNING with the event types.

Two deliberate deviations from the literal SPEC §10.1 schema, both forced by
the ``PublicationStore`` Protocol (the authoritative contract):

  * ``payload`` is ``BYTEA``/``LargeBinary``, not ``JSONB``. The Protocol
    types payload as ``bytes`` precisely so binary serializers (Avro,
    Protobuf) work without re-encoding — JSONB cannot store arbitrary bytes.
  * The pending partial index is ``WHERE completed_at IS NULL`` (the
    ``is_dead_lettered`` predicate is dropped). ``find_incomplete`` returns
    all incomplete rows so the plugin can both retry-skip and *count*
    dead-letters by ``attempt_count``; the index still excludes the millions
    of completed rows. ``is_dead_lettered`` is kept as a derived column for
    external dashboards.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

try:
    # Module-level (not lazy like the broker adapters' in-__init__ imports):
    # the ORM schema classes below need SQLAlchemy at import time. Guarded so
    # a base install fails with the actionable extra, not a bare ModuleNotFound.
    from sqlalchemy import (
        Boolean,
        CursorResult,
        DateTime,
        Index,
        Integer,
        LargeBinary,
        String,
        Text,
        Uuid,
        and_,
        delete,
        false,
        func,
        or_,
        select,
        text,
        update,
    )
    from sqlalchemy import event as sa_event
    from sqlalchemy.dialects.mysql import LONGBLOB as MySQLLongBlob
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
    from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
except ImportError as exc:  # pragma: no cover — exercised in a subprocess test
    raise ImportError(
        "modulith.adapters.postgres_outbox requires SQLAlchemy (async). "
        "Install the extra: pip install 'modupy[postgres]'"
    ) from exc

from modulith import EventPublication
from modulith.builtin import outbox
from modulith.builtin.outbox import _bound_session, _current_session, _SessionBinding
from modulith.config import ConfigurationError

logger = logging.getLogger("modulith.adapters.postgres")


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Declarative base for the outbox tables."""


# Payload type that can hold a real serialized event on EVERY dialect.
# LargeBinary compiles to MySQL/MariaDB BLOB, which caps at 65,535 bytes — the
# outbox imposes no size limit of its own, so a moderately large event is
# accepted by ``save`` and then dies at flush with MySQL error 1406 ("Data too
# long for column"), taking the *business* transaction down with it because the
# publication row is enlisted in that same transaction. LONGBLOB (4 GiB)
# removes the cliff; Postgres BYTEA and SQLite BLOB are already unbounded so the
# variant is inert there. The migrations mirror this exactly.
_PAYLOAD = LargeBinary().with_variant(MySQLLongBlob(), "mysql", "mariadb")


class EventPublicationRow(Base):
    """The primary outbox table: one row per (event, listener) publication."""

    __tablename__ = "event_publications"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    # Text, not an unbounded String — MySQL's VARCHAR requires an explicit
    # length, so a plain String() column fails to even compile the CREATE
    # TABLE on that dialect. Text renders as TEXT/LONGTEXT there (and
    # PostgreSQL/SQLite treat Text and unbounded String identically), so this
    # keeps the "arbitrarily long dotted path" semantics portable everywhere.
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    # BYTEA, not JSONB — payload is bytes (binary-serializer support).
    payload: Mapped[bytes] = mapped_column(_PAYLOAD, nullable=False)
    listener: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_dead_lettered: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    # Lease columns for the claim protocol (see ``claim_pending``). Nullable
    # because an unclaimed row carries no lease, and because the dispatcher's
    # ``in_process`` claim mode never writes them at all.
    claim_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    claim_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claim_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Partial index keeps the pending-rows scan small even with millions
        # of completed rows. The predicate applies on Postgres; on other
        # dialects SQLAlchemy emits a plain index on published_at.
        Index("idx_pending", "published_at", postgresql_where=text("completed_at IS NULL")),
        # Not the whole picture on Postgres: the sweep's
        # ``coalesce(last_attempt_at, published_at)`` ordering is served by an
        # expression index that only migration 0005 creates, because a
        # functional partial index does not compile on MySQL or MariaDB and so
        # cannot live in dialect-portable metadata.
    )


class EventPublicationArchiveRow(Base):
    """Archive table for the ``archive`` completion mode."""

    __tablename__ = "event_publications_archive"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[bytes] = mapped_column(_PAYLOAD, nullable=False)
    listener: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # server_default mirrors the migration and the primary table exactly so the
    # ORM and the DDL never diverge on the archive table either.
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Nothing deletes from this table except ``purge_completed``, which
        # selects by completed_at — so this is the only index it needs, and
        # without it the purge scans an unbounded table.
        Index("ix_event_publications_archive_completed_at", "completed_at"),
    )


# ---------------------------------------------------------------------------
# After-commit dispatch wiring (registered once, routed to the active store)
# ---------------------------------------------------------------------------

# The active store. The after-commit hook is global (registered on the sync
# Session class so it sees the application's own sessions), so it routes
# pending dispatches to whichever store is currently configured.
_active_store: PostgresPublicationStore | None = None
_hook_installed = False

# Every live (constructed, not-yet-disposed) store, in creation order; the
# active store is always the top. An explicit stack — not a per-store
# back-pointer — so dispose() can remove a store from *anywhere* in it:
# a single ``_prev_store`` link only unwound correctly in strict LIFO order
# and would resurrect an already-disposed (possibly engine-closed) store as
# the dispatch target when stores were disposed in creation order.
_store_stack: list[PostgresPublicationStore] = []


def _schedule_after_commit_dispatch(session: Session) -> None:
    """Sync after-commit callback: schedule dispatch of queued publications.

    Runs inside ``await session.commit()`` (so a loop is running) and pops the
    pending ids queued by ``save``. Each is dispatched as a scheduled task —
    fire-and-forget; the retry loop is the safety net if a task is lost.
    """
    pending = session.info.pop("_modulith_pending", [])
    session.info.pop("_modulith_pending_types", None)
    if not pending:
        return
    store = _active_store
    if store is None:
        # dispose() deactivated the store after save() queued these ids but
        # before this session committed. The rows ARE durably committed
        # (completed_at NULL), so the next configured store's retry sweep
        # delivers them — but the skipped after-commit dispatch must be
        # observable, exactly like the no-running-loop branch below.
        logger.warning(
            "after_commit fired with %d queued publication(s) but no active "
            "PostgresPublicationStore (disposed before this session committed?); "
            "the committed row(s) will be delivered by the retry sweep of the "
            "next configured store instead",
            len(pending),
        )
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning(
            "after_commit fired without a running loop; %d publication(s) "
            "will be delivered by the retry loop instead",
            len(pending),
        )
        return
    for publication_id in pending:
        task = loop.create_task(store._dispatch_after_commit(publication_id))
        store._inflight.add(task)
        task.add_done_callback(store._inflight.discard)


def _discard_uncommitted_pending(session: Session, transaction: Any) -> None:
    """Drop, and report, queued ids whose transaction ended without committing.

    Fires when a session transaction ends. After a commit, after_commit has
    already popped the queue, so anything left belongs to a transaction that
    rolled back or was closed uncommitted: those rows are gone, and so is
    their dispatch. A bound session that outlives its last ``commit()`` —
    FastAPI BackgroundTasks run before a ``get_db`` dependency's teardown —
    loses its later publishes this way, so each loss is logged at WARNING
    with the event types rather than passing silently.

    SQLAlchemy does not reset ``Session.info`` on rollback, and a Session is
    routinely reused for a second transaction. Without the pop, the next
    commit's after_commit would dispatch ids whose rows were never committed,
    each logging the "found no row … deleted before delivery?" warning that
    is supposed to mean something has gone wrong with a *committed* row.

    Only the root transaction counts. A flush runs in its own inner
    transaction, which ends before the root commits. A SAVEPOINT end is left
    alone too: the queue is flat, so it cannot tell which ids belong to the
    savepoint and which to the enclosing transaction, and dropping the
    enclosing ones would silently downgrade them from after-commit dispatch
    to retry-sweep latency.
    """
    if transaction.parent is not None:
        return
    pending = session.info.pop("_modulith_pending", None)
    types = session.info.pop("_modulith_pending_types", {})
    if not pending:
        return
    logger.warning(
        "discarded %d outbox publication(s) of %s: the bound session's "
        "transaction ended without committing (rolled back, or closed after "
        "its last commit). Publish before the final commit, or commit after "
        "the work that publishes (see the bind_session docs).",
        len(pending),
        ", ".join(sorted({types.get(pid, "<unknown>") for pid in pending})),
    )


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


async def _discard_connection(conn: Any) -> None:
    """Close an advisory-lock connection without returning it to its pool."""
    try:
        await conn.invalidate()
    finally:
        await conn.close()


def _aware(value: datetime | None) -> datetime | None:
    """Normalize a possibly-naive timestamp (SQLite loses tz) to UTC-aware."""
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _to_utc(value: datetime | None) -> datetime | None:
    """Normalize an aware timestamp to UTC before persisting.

    Dialect-agnostic write-side guarantee: the SQLite driver stores the
    wall-clock digits and drops the offset, so a non-UTC-aware value written
    as-is would come back from ``_aware`` shifted by its offset (silent
    corruption of the instant). Converting on write makes the read-side
    "naive means UTC" assumption hold on every dialect. Naive values are
    passed through unchanged — they are already interpreted as UTC on read,
    and guessing a zone for them here would corrupt rather than fix.
    """
    if value is not None and value.tzinfo is not None:
        return value.astimezone(UTC)
    return value


def _pub_to_row(publication: EventPublication, *, dead: bool) -> EventPublicationRow:
    """Build the ORM row for a publication, normalizing timestamps to UTC."""
    return EventPublicationRow(
        id=publication.id,
        event_type=publication.event_type,
        payload=publication.payload,
        listener=publication.listener,
        published_at=_to_utc(publication.published_at),
        completed_at=_to_utc(publication.completed_at),
        attempt_count=publication.attempt_count,
        last_error=publication.last_error,
        last_attempt_at=_to_utc(publication.last_attempt_at),
        is_dead_lettered=dead,
    )


def _row_to_pub(row: EventPublicationRow) -> EventPublication:
    return EventPublication(
        id=row.id,
        payload=row.payload,
        event_type=row.event_type,
        listener=row.listener,
        published_at=_aware(row.published_at),
        completed_at=_aware(row.completed_at),
        attempt_count=row.attempt_count,
        last_error=row.last_error,
        last_attempt_at=_aware(row.last_attempt_at),
    )


async def _try_claim_row(
    s: AsyncSession,
    publication_id: UUID,
    *,
    owner: str,
    token: str,
    now: datetime,
    until: datetime,
) -> bool:
    """Claim one row with a conditional UPDATE that re-checks claimability
    (incomplete, not dead-lettered, no live lease). Only a rowcount of 1
    counts as claimed; the caller commits."""
    result = await s.execute(
        update(EventPublicationRow)
        .where(
            EventPublicationRow.id == publication_id,
            EventPublicationRow.completed_at.is_(None),
            EventPublicationRow.is_dead_lettered.is_(False),
            or_(
                EventPublicationRow.claim_until.is_(None),
                EventPublicationRow.claim_until <= now,
            ),
        )
        .values(claim_owner=owner, claim_token=token, claim_until=until)
        .execution_options(synchronize_session=False)
    )
    return cast(CursorResult[Any], result).rowcount == 1


# ---------------------------------------------------------------------------
# The PublicationStore implementation
# ---------------------------------------------------------------------------


class PostgresPublicationStore:
    """SQLAlchemy backend for the transactional outbox.

    Conforms structurally to ``modulith.PublicationStore`` (duck-typed via
    Protocol — no inheritance required).

    Usage::

        store = PostgresPublicationStore(engine=async_engine)
        outbox.configure(
            store=store,
            # The outbox serializer round-trips: it deserializes rows the
            # retry sweep reads back, and ``deserialize`` imports the module
            # named in the row's event_type. Anything that can write to the
            # outbox table can therefore choose which module gets imported,
            # so pass the allowlist of event types this application actually
            # publishes.
            serializer=JsonEventSerializer(
                allowed_event_types=[OrderPlaced, ShipmentDispatched],
            ),
        )
        configure(outbox="postgres")
    """

    def __init__(
        self, engine: AsyncEngine, *, dead_letter_after_attempts: int | None = None
    ) -> None:
        global _active_store
        self._engine = engine
        self._sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        # Public (no leading underscore) and paired with an explicit-ness
        # marker: outbox.configure() duck-types both to unify this store's
        # threshold with the plugin's own (see _resolve_dead_letter_threshold
        # in modulith/builtin/outbox.py) and to detect conflicting explicit
        # settings instead of silently letting the store's is_dead_lettered
        # flag disagree with the plugin's skip-check.
        self.dead_letter_after_attempts_explicit = dead_letter_after_attempts is not None
        self.dead_letter_after_attempts = (
            dead_letter_after_attempts if dead_letter_after_attempts is not None else 10
        )
        self._inflight: set[asyncio.Task[None]] = set()
        # Cross-loop usage detector for _open_session(), the chokepoint every
        # read/write method routes through: a weakref.ref to the first loop
        # seen, mirroring db_broker.py's DatabaseBroker._used_loop_ref.
        self._used_loop_ref: weakref.ReferenceType[asyncio.AbstractEventLoop] | None = None
        self._cross_loop_warned = False
        # FOR UPDATE SKIP LOCKED is a Postgres row-claim optimization; SQLite
        # (tests) has no row locking and would reject the clause, so gate on it.
        self._supports_skip_locked = engine.dialect.name == "postgresql"
        # The advisory_lock claim mode needs pg_try_advisory_lock, which
        # only exists on Postgres. Public (no leading underscore) so
        # outbox.configure() can check it via getattr without reaching into
        # this store's SQLAlchemy engine directly (keeps the storage-agnostic
        # plugin from depending on dialect internals).
        self.supports_advisory_lock = engine.dialect.name == "postgresql"
        self._lock_engine: AsyncEngine | None = None
        # Push onto the live-store stack so dispose() can restore whichever
        # live store remains, rather than blanking dispatch routing — and warn
        # loudly instead of silently hijacking a still-live store's
        # after-commit dispatches.
        if _active_store is not None and _active_store is not self:
            logger.warning(
                "another PostgresPublicationStore is already active; the global "
                "after-commit hook now routes to this new store. Two live stores "
                "on different engines will contend — dispose the previous one first."
            )
        _store_stack.append(self)
        _active_store = self
        self._install_session_hooks()

    def _install_session_hooks(self) -> None:
        """Register the global session listeners exactly once."""
        global _hook_installed
        if _hook_installed:
            return
        sa_event.listen(Session, "after_commit", _schedule_after_commit_dispatch)
        sa_event.listen(Session, "after_transaction_end", _discard_uncommitted_pending)
        _hook_installed = True

    def _check_cross_loop_usage(self) -> None:
        """Warn once when this store's engine is used from a second loop.

        Behaviour is unchanged either way. asyncpg and aiomysql connections
        only work on the loop that opened them, so a pooled connection checked
        out by another loop fails its first query with ``RuntimeError: ...
        attached to a different loop``, idle pool or not. On SQLite the
        ``AsyncAdaptedQueue`` binds to whichever loop first blocks on it, and
        another loop that later waits for a free connection raises
        ``RuntimeError: <Queue ...> is bound to a different event loop``. This surfaces the hazard
        early instead of leaving it to those opaque failures.
        """
        loop = asyncio.get_running_loop()
        if self._used_loop_ref is None:
            self._used_loop_ref = weakref.ref(loop)
            return
        if self._cross_loop_warned:
            return
        bound_loop = self._used_loop_ref()
        if bound_loop is not None and bound_loop is not loop:
            logger.warning(
                "PostgresPublicationStore engine first used on one event loop is "
                "now used from another. Unlike the database broker, the store "
                "does not hand calls to the loop that owns its engine. On "
                "Postgres and MySQL, the next query on a connection another "
                "loop opened raises 'attached to a different loop'; on SQLite, "
                "a loop that has to wait for a free connection after another "
                "loop did raises 'Queue is bound to a different event loop'. "
                "Keep every publish and dispatch for one store on one loop "
                "(await publish() rather than publish_sync())."
            )
            self._cross_loop_warned = True

    def _open_session(self) -> Any:
        """The single chokepoint every read/write method opens a session
        through — see ``_check_cross_loop_usage``."""
        self._check_cross_loop_usage()
        return self._sessionmaker()

    async def save(self, publication: EventPublication) -> None:
        """Persist a publication.

        Inside a bound transaction: enlist the row in that session so it
        commits atomically, and queue its id for after-commit dispatch.
        Outside a transaction (retry/failure re-save): a *guarded partial
        upsert* — insert if new, otherwise update only the mutable
        attempt/error/dead-letter fields. It never reopens an
        already-completed row: a stale failed re-save (e.g. the crash sweep
        racing the after-commit task) must not resurrect a delivered
        publication by blanking ``completed_at``.
        """
        session = _bound_session()
        dead = publication.attempt_count >= self.dead_letter_after_attempts

        if session is not None:
            session.add(_pub_to_row(publication, dead=dead))
            # ``.info`` lives on the sync Session; an AsyncSession exposes it
            # via ``.sync_session``, while a plain (sync) Session — legitimate
            # on the documented no-running-loop degraded path — IS the sync
            # session already. bind_session() accepts either.
            sync_session = getattr(session, "sync_session", session)
            sync_session.info.setdefault("_modulith_pending", []).append(publication.id)
            sync_session.info.setdefault("_modulith_pending_types", {})[publication.id] = (
                publication.event_type
            )
            return

        async with self._open_session() as s:
            existing = await s.get(EventPublicationRow, publication.id)
            if existing is None:
                s.add(_pub_to_row(publication, dead=dead))
                await s.commit()
                return
            if existing.completed_at is not None:
                # Already delivered — never reopen. Drop this stale re-save.
                return
            existing.attempt_count = publication.attempt_count
            existing.last_error = publication.last_error
            existing.last_attempt_at = _to_utc(publication.last_attempt_at)
            existing.is_dead_lettered = dead
            await s.commit()

    async def mark_complete(self, publication_id: UUID) -> None:
        async with self._open_session() as s:
            row = await s.get(EventPublicationRow, publication_id)
            if row is not None:
                row.completed_at = datetime.now(UTC)
                await s.commit()

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        """Return up to 100 *retryable* incomplete publications, least-recently-
        attempted first (never-attempted rows sort by ``published_at``).

        Dead-lettered rows are excluded at the SQL level so the LIMIT 100 window
        is never starved by exhausted records that the sweep would only skip —
        a backlog of dead-letters could otherwise hide live retryable rows past
        row 100. Operational counts come from the unbounded ``count_*`` helpers,
        not this capped query.

        The ordering key is ``coalesce(last_attempt_at, published_at)``, not raw
        ``published_at``: retries update ``last_attempt_at`` but never
        ``published_at``, so a >100-row backlog of legitimately-retrying rows
        would otherwise occupy the capped window on every sweep and starve newer
        publications indefinitely. Sorting by the last attempt rotates each
        attempted row to the back of the queue, bounding how long any row —
        old or new — can wait for a slot.

        ``FOR UPDATE SKIP LOCKED`` (Postgres; a no-op on SQLite) only keeps two
        concurrent sweeps from blocking on each other's row locks — it does NOT
        partition the work between them. The locks live and die with this
        session's transaction, which ends when the ``async with`` below exits,
        i.e. BEFORE the caller has dispatched anything: a second sweeper running
        a moment later finds the very same rows unlocked, returns them too, and
        both dispatch the same publication. ``claim_batch`` is what actually
        partitions work — it COMMITs a per-row lease before returning, and only
        rows whose lease has expired are claimable. This method is the
        unfenced path (``claim_strategy="none"``), which is why delivery is
        at-least-once and listeners must be idempotent (see the outbox
        plugin's correctness properties).
        """
        cutoff = datetime.now(UTC) - older_than
        async with self._open_session() as s:
            stmt = (
                select(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(False),
                    EventPublicationRow.published_at <= cutoff,
                )
                .order_by(
                    func.coalesce(
                        EventPublicationRow.last_attempt_at,
                        EventPublicationRow.published_at,
                    )
                )
                .limit(100)
            )
            if self._supports_skip_locked:
                stmt = stmt.with_for_update(skip_locked=True)
            rows = (await s.execute(stmt)).scalars().all()
            return [_row_to_pub(r) for r in rows]

    async def archive(self, publication_id: UUID) -> None:
        async with self._open_session() as s:
            row = await s.get(EventPublicationRow, publication_id)
            if row is None:
                return
            s.add(
                EventPublicationArchiveRow(
                    id=row.id,
                    event_type=row.event_type,
                    payload=row.payload,
                    listener=row.listener,
                    published_at=row.published_at,
                    completed_at=datetime.now(UTC),
                    attempt_count=row.attempt_count,
                    last_error=row.last_error,
                    last_attempt_at=row.last_attempt_at,
                )
            )
            await s.delete(row)
            await s.commit()

    async def delete(self, publication_id: UUID) -> None:
        async with self._open_session() as s:
            row = await s.get(EventPublicationRow, publication_id)
            if row is not None:
                await s.delete(row)
                await s.commit()

    # ----- Duck-typed maintenance extensions used by the outbox plugin -----

    async def count_completed(self) -> int:
        async with self._open_session() as s:
            stmt = (
                select(func.count())
                .select_from(EventPublicationRow)
                .where(EventPublicationRow.completed_at.is_not(None))
            )
            return int((await s.execute(stmt)).scalar_one())

    async def count_archived(self) -> int:
        """Count rows moved to the archive table (``completion_mode="archive"``).

        The archive table holds only delivered publications (see
        ``EventPublicationArchiveRow`` — nothing writes to it except
        ``archive``/``complete_claim(mode="archive")``), so no
        ``completed_at`` filter is needed, matching ``count_completed``'s
        unfiltered count of the primary table.
        """
        async with self._open_session() as s:
            stmt = select(func.count()).select_from(EventPublicationArchiveRow)
            return int((await s.execute(stmt)).scalar_one())

    async def count_open(self) -> int:
        """Count incomplete, *retryable* publications (not dead-lettered).

        Unbounded — unlike ``find_incomplete`` (LIMIT 100), this is the source
        of truth for operational dashboards/doctor, which must see the real
        backlog (e.g. 50k stuck rows) rather than a capped sample.
        """
        async with self._open_session() as s:
            stmt = (
                select(func.count())
                .select_from(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(False),
                )
            )
            return int((await s.execute(stmt)).scalar_one())

    async def count_dead_lettered(self) -> int:
        """Count incomplete publications that have exhausted their retry budget."""
        async with self._open_session() as s:
            stmt = (
                select(func.count())
                .select_from(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(True),
                )
            )
            return int((await s.execute(stmt)).scalar_one())

    async def find_dead_lettered(
        self, *, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> list[EventPublication]:
        """Return dead-lettered publications, oldest first, one page at a time.

        ``find_incomplete`` excludes dead-letters, so this is the dedicated
        source for ``list_dead_lettered`` / ``retry_all_dead_lettered``. A
        single unpaginated call (the contract every existing caller relies
        on) is just the first page: ``after=None, limit=100``.

        Keyset pagination keeps a backlog past the 100-row page from
        silently hiding from those callers: ``after`` is the
        ``(published_at, id)`` of the last row of the previous page, and rows
        are ordered by that same pair so an exact tie on ``published_at``
        still produces a stable, gap-free, duplicate-free cursor (a plain
        OFFSET would double up or skip rows if a dead-letter table changes
        between pages; keyset pagination does not).
        """
        async with self._open_session() as s:
            stmt = select(EventPublicationRow).where(
                EventPublicationRow.completed_at.is_(None),
                EventPublicationRow.is_dead_lettered.is_(True),
            )
            if after is not None:
                after_at, after_id = after
                # after_at is required in the keyset cursor; _to_utc preserves non-None.
                normalized = _to_utc(after_at)
                assert normalized is not None
                after_at = normalized
                stmt = stmt.where(
                    or_(
                        EventPublicationRow.published_at > after_at,
                        and_(
                            EventPublicationRow.published_at == after_at,
                            EventPublicationRow.id > after_id,
                        ),
                    )
                )
            stmt = stmt.order_by(EventPublicationRow.published_at, EventPublicationRow.id).limit(
                limit
            )
            rows = (await s.execute(stmt)).scalars().all()
            return [_row_to_pub(r) for r in rows]

    async def find_by_id(self, publication_id: UUID) -> EventPublication | None:
        """Direct point lookup by id, used by ``force_retry``.

        ``find_incomplete``/``find_dead_lettered`` are both capped windows —
        a manually-targeted retry must reach a row regardless of how far back
        in a large backlog it sits, rather than only the rows that happen to
        fall in the current page.
        """
        async with self._open_session() as s:
            row = await s.get(EventPublicationRow, publication_id)
            return _row_to_pub(row) if row is not None else None

    # ----- claim_strategy="lease" — atomic claim + token fencing ----------

    async def claim_batch(
        self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
    ) -> list[EventPublication]:
        """Atomically claim up to ``batch_size`` claimable rows and COMMIT
        before returning — the caller (the outbox retry loop) dispatches only
        after this transaction lands, so a crash between claim and dispatch
        just leaves the lease to expire and be reclaimed by the next sweeper,
        never an uncommitted phantom claim.

        A row is claimable when it is incomplete, not dead-lettered, past
        ``older_than``, AND its current lease (if any) has already expired
        (``claim_until IS NULL OR claim_until <= now``) — this last predicate
        is what stops two concurrent sweepers from both claiming the same row
        while a lease is still active. Ordering and ``FOR UPDATE SKIP LOCKED``
        mirror ``find_incomplete`` for the same reasons documented there.

        Returned publications carry a fresh ``claim_token`` (bearer for
        ``renew_claim``/``complete_claim``/``fail_claim``); ``claim_owner`` is
        stored for operator diagnostics only — fencing is always by token.
        """
        now = datetime.now(UTC)
        cutoff = now - older_than
        until = now + timedelta(seconds=lease_seconds)
        async with self._open_session() as s:
            stmt = (
                select(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(False),
                    EventPublicationRow.published_at <= cutoff,
                    or_(
                        EventPublicationRow.claim_until.is_(None),
                        EventPublicationRow.claim_until <= now,
                    ),
                )
                .order_by(
                    func.coalesce(
                        EventPublicationRow.last_attempt_at,
                        EventPublicationRow.published_at,
                    )
                )
                .limit(batch_size)
            )
            if self._supports_skip_locked:
                stmt = stmt.with_for_update(skip_locked=True)
            rows = (await s.execute(stmt)).scalars().all()
            if not self._supports_skip_locked:
                return await self._claim_unlocked(s, rows, owner=owner, now=now, until=until)
            claimed: list[EventPublication] = []
            for row in rows:
                token = uuid4().hex
                row.claim_owner = owner
                row.claim_token = token
                row.claim_until = until
                pub = _row_to_pub(row)
                pub.claim_token = token
                claimed.append(pub)
            await s.commit()
            return claimed

    async def _claim_unlocked(
        self,
        s: AsyncSession,
        rows: Sequence[EventPublicationRow],
        *,
        owner: str,
        now: datetime,
        until: datetime,
    ) -> list[EventPublication]:
        """Claim candidates read without row locks (MySQL, SQLite): each row
        is taken by a conditional UPDATE that re-checks claimability, and only
        a rowcount of 1 counts as claimed. A peer that claimed the row after
        our SELECT leaves ``claim_until`` in the future, so our UPDATE matches
        nothing and the row is dropped from the batch. Rows are updated in
        primary-key order so concurrent claimers take row locks in the same
        order and cannot deadlock each other."""
        tokens: dict[UUID, str] = {}
        for row in sorted(rows, key=lambda r: str(r.id)):
            token = uuid4().hex
            if await _try_claim_row(s, row.id, owner=owner, token=token, now=now, until=until):
                tokens[row.id] = token
        await s.commit()
        claimed: list[EventPublication] = []
        for row in rows:
            if row.id in tokens:
                pub = _row_to_pub(row)
                pub.claim_token = tokens[row.id]
                claimed.append(pub)
        return claimed

    async def _claim_publication(
        self, publication_id: UUID, *, owner: str, lease_seconds: float
    ) -> EventPublication | None:
        """Claim one row with the same lease-conditional UPDATE as
        ``claim_batch`` and commit. Returns the claimed publication carrying
        its ``claim_token``, or None when the row is completed, dead-lettered,
        gone, or under another claimant's live lease."""
        now = datetime.now(UTC)
        until = now + timedelta(seconds=lease_seconds)
        token = uuid4().hex
        async with self._open_session() as s:
            claimed = await _try_claim_row(
                s, publication_id, owner=owner, token=token, now=now, until=until
            )
            await s.commit()
            if not claimed:
                return None
            row = await s.get(EventPublicationRow, publication_id)
            if row is None:
                return None
            pub = _row_to_pub(row)
            pub.claim_token = token
            return pub

    async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
        """Extend a still-held claim's lease. Returns False (no write applied)
        if ``token`` no longer matches the row's current claim — the lease
        already expired and/or a peer sweeper reclaimed it; the caller must
        stop dispatching and let the new claimant own it. A completed row
        also returns False, so the sweep's pre-dispatch re-arm doubles as a
        completion check: an unfenced completion (``mark_complete``) keeps
        the token in place.

        Also used by the outbox retry loop to voluntarily release a claim
        early (``lease_seconds=0.0``) when a claimed row turns out not to be
        due for retry yet (backoff), instead of holding it idle for the full
        lease and blocking every other sweeper from picking it up sooner.
        """
        until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        async with self._open_session() as s:
            stmt = (
                update(EventPublicationRow)
                .where(
                    EventPublicationRow.id == publication_id,
                    EventPublicationRow.claim_token == token,
                    EventPublicationRow.completed_at.is_(None),
                )
                .values(claim_until=until)
            )
            result = await s.execute(stmt)
            await s.commit()
            return bool(cast(CursorResult[Any], result).rowcount)

    async def complete_claim(self, publication_id: UUID, token: str, mode: str) -> bool:
        """Fenced completion write: applies ``mode`` (update/delete/archive)
        ONLY if ``token`` still matches the row's claim. Returns False without
        touching the row if it doesn't — a stale claimant must never complete
        a row a newer claimant now owns.

        The row is read ``FOR UPDATE`` so the token check and the write it
        gates happen inside one locked window. Unlocked, a peer's
        ``claim_batch`` could commit a fresh claim in between, and this write —
        keyed on the primary key alone — would then clobber the new claimant's
        lease. Holding the lock instead makes that peer skip the row, because
        ``claim_batch`` selects ``FOR UPDATE SKIP LOCKED``."""
        async with self._open_session() as s:
            row = await s.get(EventPublicationRow, publication_id, with_for_update=True)
            if row is None or row.claim_token != token:
                return False
            if mode == "delete":
                await s.delete(row)
            elif mode == "archive":
                s.add(
                    EventPublicationArchiveRow(
                        id=row.id,
                        event_type=row.event_type,
                        payload=row.payload,
                        listener=row.listener,
                        published_at=row.published_at,
                        completed_at=datetime.now(UTC),
                        attempt_count=row.attempt_count,
                        last_error=row.last_error,
                        last_attempt_at=row.last_attempt_at,
                    )
                )
                await s.delete(row)
            else:
                row.completed_at = datetime.now(UTC)
            await s.commit()
            return True

    async def fail_claim(self, publication: EventPublication, token: str) -> bool:
        """Fenced failure-record write: persists ``attempt_count``/
        ``last_error`` ONLY if ``token`` still matches. On success, the claim
        is released (``claim_owner``/``claim_token``/``claim_until`` cleared)
        so the row is immediately reclaimable on the next sweep rather than
        sitting idle for the remainder of the lease.

        The token predicate lives in the UPDATE itself (like ``renew_claim``),
        not in a preceding SELECT: a read-then-write pair leaves a window in
        which a peer's ``claim_batch`` commits a new claim that the write then
        clobbers. Rowcount 0 means the row is gone or the peer owns it."""
        dead = publication.attempt_count >= self.dead_letter_after_attempts
        async with self._open_session() as s:
            stmt = (
                update(EventPublicationRow)
                .where(
                    EventPublicationRow.id == publication.id,
                    EventPublicationRow.claim_token == token,
                )
                .values(
                    attempt_count=publication.attempt_count,
                    last_error=publication.last_error,
                    last_attempt_at=_to_utc(publication.last_attempt_at),
                    is_dead_lettered=dead,
                    claim_owner=None,
                    claim_token=None,
                    claim_until=None,
                )
            )
            result = await s.execute(stmt)
            await s.commit()
            return bool(cast(CursorResult[Any], result).rowcount)

    # ----- claim_strategy="advisory_lock" — Postgres-only -----------------

    async def try_lock_publication(self, publication_id: UUID) -> object | None:
        """Attempt to acquire a session-level Postgres advisory lock keyed by
        ``publication_id``, held on a DEDICATED connection for the duration
        of dispatch (advisory locks are per-session, not per-row/transaction).

        Returns the open connection (the handle ``unlock_publication`` needs)
        on success, or None if another connection already holds it — the
        caller's contract is try-lock-and-skip, never block-and-wait.

        The lock connection comes from a pool of its own, sized like the
        engine's. The listener and this store's reads and writes during the
        dispatch draw on the engine's pool, so a burst of held locks cannot
        exhaust the pool they wait on. A connection returns to the lock pool
        only when it provably holds no lock: the lock attempt returned false,
        or ``unlock_publication`` released the lock. Any other outcome
        invalidates it, so a lock can never outlive its handle.
        """
        if not self.supports_advisory_lock:
            raise ConfigurationError(
                "claim_strategy='advisory_lock' requires a Postgres engine "
                "(pg_try_advisory_lock); this store is backed by "
                f"{self._engine.dialect.name!r}"
            )
        lock_key = publication_id.int & 0x7FFFFFFFFFFFFFFF  # fit signed bigint
        conn = await self._lock_connection_engine().connect()
        try:
            # AUTOCOMMIT: the lock query would otherwise autobegin a
            # transaction that stays open for the whole dispatch this handle
            # is held across, sitting idle-in-transaction on real Postgres.
            conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            got = (
                await conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key})
            ).scalar()
        except BaseException:
            await _discard_connection(conn)
            raise
        if not got:
            await conn.close()
            return None
        return conn

    def _lock_connection_engine(self) -> AsyncEngine:
        """The engine advisory-lock connections come from: the store engine's
        URL, dialect and connection factory over a separate pool."""
        if self._lock_engine is None:
            sync_engine = self._engine.sync_engine
            self._lock_engine = AsyncEngine(
                Engine(sync_engine.pool.recreate(), sync_engine.dialect, sync_engine.url)
            )
        return self._lock_engine

    async def unlock_publication(self, handle: object, publication_id: UUID) -> None:
        """Release a lock handle returned by ``try_lock_publication``.

        The connection returns to the lock pool when ``pg_advisory_unlock``
        confirms the release; when the unlock raises, is cancelled or returns
        false, the session may still hold a lock, so it is invalidated."""
        lock_key = publication_id.int & 0x7FFFFFFFFFFFFFFF
        conn = cast(Any, handle)
        released = False
        try:
            released = bool(
                (
                    await conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
                ).scalar()
            )
        finally:
            if released:
                await conn.close()
            else:
                await _discard_connection(conn)

    async def purge_completed(self, older_than: timedelta) -> int:
        """Delete completed publications older than ``older_than`` from BOTH
        tables; returns the total number of rows removed.

        ``completion_mode="archive"`` MOVES the row into
        ``event_publications_archive``, so under that mode the primary table
        holds no completed rows at all: purging only the primary table would
        report a truthful-looking zero every night while the archive grows
        without bound.
        """
        cutoff = datetime.now(UTC) - older_than
        purged = 0
        async with self._open_session() as s:
            for table in (EventPublicationRow, EventPublicationArchiveRow):
                stmt = delete(table).where(
                    table.completed_at.is_not(None),
                    table.completed_at <= cutoff,
                )
                result = await s.execute(stmt)
                purged += int(cast(CursorResult[Any], result).rowcount or 0)
            await s.commit()
        return purged

    # ----- Dispatch + lifecycle -------------------------------------------

    async def _dispatch_after_commit(self, publication_id: UUID) -> None:
        """Load a committed publication and dispatch it to its listener.

        Runs with no bound session (cleared below) so any failure re-save in
        the plugin takes the standalone-transaction path rather than reusing
        the now-closed business session.

        A row a sweep already completed is skipped. Under
        ``claim_strategy="lease"`` the row is claimed first, exactly as a
        sweep claims it, and delivered under that lease with renewal and
        fenced completion; a row a sweep holds is left to that sweep. Under
        ``"advisory_lock"`` it is delivered under the row's advisory lock,
        through the same lock/re-read/unlock path the advisory sweep uses.

        Crash recovery: the claim is taken after commit, not in ``save()``, so
        a process that dies between commit and claim leaves its rows for the
        restart sweep. A row it was already delivering under a lease stays
        claimed until that lease expires, so the first sweep after expiry
        recovers it: up to ``claim_lease_seconds`` plus
        ``retry_interval_seconds`` after the crash. An advisory lock dies with
        its connection, so under ``"advisory_lock"`` the restart sweep
        recovers such rows at once.
        """
        token = _current_session.set(None)
        try:
            async with self._open_session() as s:
                row = await s.get(EventPublicationRow, publication_id)
                pub = _row_to_pub(row) if row is not None else None
            if pub is not None and pub.completed_at is not None:
                logger.debug("after-commit dispatch: publication %s already completed", pub.id)
            elif pub is not None and outbox._claim_strategy == "lease":
                claimed = await self._claim_publication(
                    pub.id,
                    owner=outbox._claim_owner,
                    lease_seconds=outbox._claim_lease_seconds,
                )
                if claimed is None:
                    logger.debug(
                        "after-commit dispatch: publication %s is claimed elsewhere", pub.id
                    )
                else:
                    await outbox._dispatch_with_lease_renewal(claimed)
            elif pub is not None and outbox._claim_strategy == "advisory_lock":
                await outbox._dispatch_under_advisory_lock(pub)
            elif pub is not None:
                await outbox._dispatch_publication(pub)
            elif outbox._completion_mode in ("delete", "archive"):
                # Completing the row removes it under these modes, so a sweep
                # that delivered it first is the ordinary way to find it gone.
                logger.debug(
                    "after-commit dispatch: publication %s already completed and removed",
                    publication_id,
                )
            else:
                # The row was committed (we were queued from after_commit) yet is
                # gone now — deleted out from under us, or never actually
                # committed. Surface it rather than silently no-op'ing.
                logger.warning(
                    "after-commit dispatch found no row for publication %s "
                    "(deleted before delivery?) — skipping",
                    publication_id,
                )
        except Exception:
            logger.exception("after-commit dispatch failed for %s", publication_id)
        finally:
            _current_session.reset(token)

    async def wait_for_dispatch(self) -> None:
        """Await all in-flight after-commit dispatch tasks (test/shutdown aid).

        Cross-loop safe: a task in ``_inflight`` may live on a different
        event loop than the one this coroutine runs on —
        ``_schedule_after_commit_dispatch`` creates the after-commit task on
        whatever loop is running at commit time, which can be sync.py's
        persistent daemon-thread loop. Handing such a task to
        ``asyncio.gather()`` raises ("Task ... attached to a different
        loop"), so a same-loop task is gathered normally and a foreign-loop
        one is drained by polling ``task.done()`` instead — the same
        technique ``modulith.builtin.outbox.shutdown()`` uses for the retry
        task.
        """
        while self._inflight:
            try:
                running: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            pending = list(self._inflight)
            same_loop = [task for task in pending if task.get_loop() is running]
            foreign_loop = [task for task in pending if task.get_loop() is not running]
            awaitables: list[Any] = []
            if same_loop:
                awaitables.append(asyncio.gather(*same_loop, return_exceptions=True))
            awaitables.extend(_poll_until_done(task) for task in foreign_loop)
            if awaitables:
                await asyncio.gather(*awaitables)

    async def dispose(self) -> None:
        """Drain in-flight dispatches and deactivate this store.

        Removes this store from the live-store stack — wherever it sits, so
        disposal in any order is safe (nested construct/dispose restores the
        previous store; creation-order disposal never resurrects an
        already-disposed one). When no live store remains, unregisters the
        global after-commit listener so it stops firing on every commit in the
        host application and doesn't leak across process/test lifetimes.

        Ordering hazard: dispose only after every session that publications
        were saved through has committed. A session that commits *after* the
        last store is disposed fires no after-commit dispatch (the hook is
        gone) — its committed rows are not lost (they are durable and the next
        configured store's retry sweep delivers them), but nothing is
        dispatched or logged at that commit.
        """
        global _active_store, _hook_installed
        await self.wait_for_dispatch()
        if self._lock_engine is not None:
            await self._lock_engine.dispose()
            self._lock_engine = None
        if self in _store_stack:
            _store_stack.remove(self)
        if _active_store is self:
            _active_store = _store_stack[-1] if _store_stack else None
            if _active_store is None and _hook_installed:
                sa_event.remove(Session, "after_commit", _schedule_after_commit_dispatch)
                sa_event.remove(Session, "after_transaction_end", _discard_uncommitted_pending)
                _hook_installed = False


# ---------------------------------------------------------------------------
# Helpers exported for application setup
# ---------------------------------------------------------------------------


def bind_session(session: Any) -> Any:
    """Bind a SQLAlchemy session to the current context.

    Call at the start of a request/transaction so publish() finds the session
    via ``_current_session``. Returns the contextvar token; pass it to
    ``unbind_session`` when the request ends::

        async def get_db_with_outbox():
            async with async_session_maker() as session:
                token = bind_session(session)
                try:
                    yield session
                    await session.commit()
                finally:
                    unbind_session(token)

    A publish while bound joins the session's open transaction (autobegun if
    needed) and is delivered only when a later ``commit()`` covers it. The
    ``commit()`` after ``yield`` covers publishes made after the route's own
    commit, such as FastAPI ``BackgroundTasks``, which run before the
    dependency's teardown. A transaction that ends uncommitted (rollback, or
    the session closing) discards its publications and logs a WARNING naming
    their event types.

    A task created with ``asyncio.create_task`` inside the bound scope shares
    the binding until ``unbind_session``: its publishes before then enlist in
    this session under the same rule, and its publishes after then take the
    unbound path (direct dispatch, no outbox row).
    """
    return _current_session.set(_SessionBinding(session))


def unbind_session(token: Any) -> None:
    """Undo a ``bind_session`` call, restoring whatever was bound before it.

    Call with the token ``bind_session`` returned, once the request/
    transaction it was bound for ends. Restores the *previous* binding
    (``None`` at the outermost scope, or an outer session if this bind was
    nested inside one) rather than unconditionally clearing it — the same
    guarantee ``contextvars.ContextVar.reset()`` gives, which this wraps.
    The binding also ends for every task that inherited it.

    Unbinding neither commits nor discards: publications enlisted since the
    last commit are still delivered if the session commits afterwards, and
    are discarded, with a WARNING, if it closes or rolls back instead.
    """
    binding = _current_session.get()
    _current_session.reset(token)
    if isinstance(binding, _SessionBinding):
        binding.session = None


# ---------------------------------------------------------------------------
# Test support
# ---------------------------------------------------------------------------


def _reset_for_testing() -> None:
    """Clear adapter module globals and unregister the after-commit listener.

    ONLY for tests. The after-commit hook is registered on the global sync
    ``Session`` class, so without this it leaks across tests/process lifetime
    (the outbox plugin's own ``_reset_for_testing`` can't reach these globals).

    In-flight after-commit dispatch tasks are cancelled (best-effort,
    thread-safely — mirroring ``outbox._cancel_retry_task``) before the
    globals are cleared: an orphaned task would otherwise resume against
    already-reset outbox module state and die on a swallowed AssertionError,
    leaving its publication silently incomplete. Production code paths use
    ``dispose()``, which *drains* in-flight tasks instead.
    """
    global _active_store, _hook_installed
    for store in _store_stack:
        for task in list(store._inflight):
            _cancel_task_threadsafe(task)
    _store_stack.clear()
    _active_store = None
    if _hook_installed:
        sa_event.remove(Session, "after_commit", _schedule_after_commit_dispatch)
        sa_event.remove(Session, "after_transaction_end", _discard_uncommitted_pending)
        _hook_installed = False


async def _poll_until_done(task: asyncio.Task[None]) -> None:
    """Wait for a foreign-loop task without awaiting it directly (that raises
    "Task ... attached to a different loop"). Used by ``wait_for_dispatch``."""
    while not task.done():
        await asyncio.sleep(0.01)


def _cancel_task_threadsafe(task: asyncio.Task[None]) -> None:
    """Request cancellation of ``task`` from any thread. Best-effort: a task
    whose loop is already closed has nothing left to cancel."""
    if task.done():
        return
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if task.get_loop() is running:
        task.cancel()
    else:
        try:
            task.get_loop().call_soon_threadsafe(task.cancel)
        except RuntimeError:
            pass  # the task's loop is already closed


__all__ = [
    "Base",
    "EventPublicationArchiveRow",
    "EventPublicationRow",
    "PostgresPublicationStore",
    "bind_session",
    "unbind_session",
]
