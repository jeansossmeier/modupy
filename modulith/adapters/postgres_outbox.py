"""SQLAlchemy PublicationStore implementation for the transactional outbox.

This is the reference adapter. It demonstrates the complete pattern for a
storage-backed PublicationStore: schema definition, session-aware saves, and
after-commit dispatch hooking.

Distributed as ``modulith-postgres`` / ``modulith[postgres]`` (asyncpg). It is
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
  3. On rollback the session never fires after_commit and its ``info`` is
     discarded, so queued-but-uncommitted publications are never dispatched.

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
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CursorResult,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    String,
    Uuid,
    delete,
    false,
    func,
    select,
    text,
)
from sqlalchemy import event as sa_event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from modulith import EventPublication
from modulith.builtin import outbox
from modulith.builtin.outbox import _current_session

logger = logging.getLogger("modulith.adapters.postgres")


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """Declarative base for the outbox tables."""


class EventPublicationRow(Base):
    """The primary outbox table: one row per (event, listener) publication."""

    __tablename__ = "event_publications"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    # BYTEA, not JSONB — payload is bytes (binary-serializer support).
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    listener: Mapped[str] = mapped_column(String, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_dead_lettered: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )

    __table_args__ = (
        # Partial index keeps the pending-rows scan small even with millions
        # of completed rows. The predicate applies on Postgres; on other
        # dialects SQLAlchemy emits a plain index on published_at.
        Index("idx_pending", "published_at", postgresql_where=text("completed_at IS NULL")),
    )


class EventPublicationArchiveRow(Base):
    """Archive table for the ``archive`` completion mode."""

    __tablename__ = "event_publications_archive"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    listener: Mapped[str] = mapped_column(String, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # server_default mirrors the migration and the primary table exactly so the
    # ORM and the DDL never diverge on the archive table either.
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# After-commit dispatch wiring (registered once, routed to the active store)
# ---------------------------------------------------------------------------

# The active store. The after-commit hook is global (registered on the sync
# Session class so it sees the application's own sessions), so it routes
# pending dispatches to whichever store is currently configured.
_active_store: PostgresPublicationStore | None = None
_hook_installed = False


def _schedule_after_commit_dispatch(session: Session) -> None:
    """Sync after-commit callback: schedule dispatch of queued publications.

    Runs inside ``await session.commit()`` (so a loop is running) and pops the
    pending ids queued by ``save``. Each is dispatched as a scheduled task —
    fire-and-forget; the retry loop is the safety net if a task is lost.
    """
    pending = session.info.pop("_modulith_pending", [])
    store = _active_store
    if not pending or store is None:
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


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def _aware(value: datetime | None) -> datetime | None:
    """Normalize a possibly-naive timestamp (SQLite loses tz) to UTC-aware."""
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


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


# ---------------------------------------------------------------------------
# The PublicationStore implementation
# ---------------------------------------------------------------------------


class PostgresPublicationStore:
    """SQLAlchemy backend for the transactional outbox.

    Conforms structurally to ``modulith.PublicationStore`` (duck-typed via
    Protocol — no inheritance required).

    Usage::

        store = PostgresPublicationStore(engine=async_engine)
        outbox.configure(store=store, serializer=JsonEventSerializer())
        configure(outbox="postgres")
    """

    def __init__(self, engine: AsyncEngine, *, dead_letter_after_attempts: int = 10) -> None:
        global _active_store
        self._engine = engine
        self._sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        self._dead_letter_after_attempts = dead_letter_after_attempts
        self._inflight: set[asyncio.Task[None]] = set()
        # FOR UPDATE SKIP LOCKED is a Postgres row-claim optimization; SQLite
        # (tests) has no row locking and would reject the clause, so gate on it.
        self._supports_skip_locked = engine.dialect.name == "postgresql"
        # Remember whoever was active so dispose() can restore it (LIFO), rather
        # than blanking dispatch routing — and warn loudly instead of silently
        # hijacking a still-live store's after-commit dispatches.
        self._prev_store = _active_store
        if _active_store is not None and _active_store is not self:
            logger.warning(
                "another PostgresPublicationStore is already active; the global "
                "after-commit hook now routes to this new store. Two live stores "
                "on different engines will contend — dispose the previous one first."
            )
        _active_store = self
        self._install_session_hooks()

    def _install_session_hooks(self) -> None:
        """Register the global after-commit listener exactly once."""
        global _hook_installed
        if _hook_installed:
            return
        sa_event.listen(Session, "after_commit", _schedule_after_commit_dispatch)
        _hook_installed = True

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
        session = _current_session.get()
        dead = publication.attempt_count >= self._dead_letter_after_attempts

        if session is not None:
            row = EventPublicationRow(
                id=publication.id,
                event_type=publication.event_type,
                payload=publication.payload,
                listener=publication.listener,
                published_at=publication.published_at,
                completed_at=publication.completed_at,
                attempt_count=publication.attempt_count,
                last_error=publication.last_error,
                last_attempt_at=publication.last_attempt_at,
                is_dead_lettered=dead,
            )
            session.add(row)
            session.sync_session.info.setdefault("_modulith_pending", []).append(publication.id)
            return

        async with self._sessionmaker() as s:
            existing = await s.get(EventPublicationRow, publication.id)
            if existing is None:
                s.add(
                    EventPublicationRow(
                        id=publication.id,
                        event_type=publication.event_type,
                        payload=publication.payload,
                        listener=publication.listener,
                        published_at=publication.published_at,
                        completed_at=publication.completed_at,
                        attempt_count=publication.attempt_count,
                        last_error=publication.last_error,
                        last_attempt_at=publication.last_attempt_at,
                        is_dead_lettered=dead,
                    )
                )
                await s.commit()
                return
            if existing.completed_at is not None:
                # Already delivered — never reopen. Drop this stale re-save.
                return
            existing.attempt_count = publication.attempt_count
            existing.last_error = publication.last_error
            existing.last_attempt_at = publication.last_attempt_at
            existing.is_dead_lettered = dead
            await s.commit()

    async def mark_complete(self, publication_id: UUID) -> None:
        async with self._sessionmaker() as s:
            row = await s.get(EventPublicationRow, publication_id)
            if row is not None:
                row.completed_at = datetime.now(UTC)
                await s.commit()

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        """Return up to 100 *retryable* incomplete publications, oldest first.

        Dead-lettered rows are excluded at the SQL level so the LIMIT 100 window
        is never starved by exhausted records that the sweep would only skip —
        a backlog of dead-letters could otherwise hide live retryable rows past
        row 100. Operational counts come from the unbounded ``count_*`` helpers,
        not this capped query.

        ``FOR UPDATE SKIP LOCKED`` (Postgres; a no-op on SQLite) lets concurrent
        sweeps in different workers partition the rows instead of both grabbing
        the same ones. It narrows — but does not eliminate — cross-process
        double-dispatch, which is why delivery is at-least-once and listeners
        must be idempotent (see the outbox plugin's correctness properties).
        """
        cutoff = datetime.now(UTC) - older_than
        async with self._sessionmaker() as s:
            stmt = (
                select(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(False),
                    EventPublicationRow.published_at <= cutoff,
                )
                .order_by(EventPublicationRow.published_at)
                .limit(100)
            )
            if self._supports_skip_locked:
                stmt = stmt.with_for_update(skip_locked=True)
            rows = (await s.execute(stmt)).scalars().all()
            return [_row_to_pub(r) for r in rows]

    async def archive(self, publication_id: UUID) -> None:
        async with self._sessionmaker() as s:
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
        async with self._sessionmaker() as s:
            row = await s.get(EventPublicationRow, publication_id)
            if row is not None:
                await s.delete(row)
                await s.commit()

    # ----- Duck-typed maintenance extensions used by the outbox plugin -----

    async def count_completed(self) -> int:
        async with self._sessionmaker() as s:
            stmt = (
                select(func.count())
                .select_from(EventPublicationRow)
                .where(EventPublicationRow.completed_at.is_not(None))
            )
            return int((await s.execute(stmt)).scalar_one())

    async def count_open(self) -> int:
        """Count incomplete, *retryable* publications (not dead-lettered).

        Unbounded — unlike ``find_incomplete`` (LIMIT 100), this is the source
        of truth for operational dashboards/doctor, which must see the real
        backlog (e.g. 50k stuck rows) rather than a capped sample.
        """
        async with self._sessionmaker() as s:
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
        async with self._sessionmaker() as s:
            stmt = (
                select(func.count())
                .select_from(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(True),
                )
            )
            return int((await s.execute(stmt)).scalar_one())

    async def find_dead_lettered(self) -> list[EventPublication]:
        """Return dead-lettered publications (oldest first, capped at 100).

        ``find_incomplete`` now excludes dead-letters, so this is the dedicated
        source for ``list_dead_lettered`` / ``retry_all_dead_lettered``.
        """
        async with self._sessionmaker() as s:
            stmt = (
                select(EventPublicationRow)
                .where(
                    EventPublicationRow.completed_at.is_(None),
                    EventPublicationRow.is_dead_lettered.is_(True),
                )
                .order_by(EventPublicationRow.published_at)
                .limit(100)
            )
            rows = (await s.execute(stmt)).scalars().all()
            return [_row_to_pub(r) for r in rows]

    async def purge_completed(self, older_than: timedelta) -> int:
        cutoff = datetime.now(UTC) - older_than
        async with self._sessionmaker() as s:
            stmt = delete(EventPublicationRow).where(
                EventPublicationRow.completed_at.is_not(None),
                EventPublicationRow.completed_at <= cutoff,
            )
            result = await s.execute(stmt)
            await s.commit()
            return int(cast(CursorResult[Any], result).rowcount or 0)

    # ----- Dispatch + lifecycle -------------------------------------------

    async def _dispatch_after_commit(self, publication_id: UUID) -> None:
        """Load a committed publication and dispatch it to its listener.

        Runs with no bound session (cleared below) so any failure re-save in
        the plugin takes the standalone-transaction path rather than reusing
        the now-closed business session.
        """
        token = _current_session.set(None)
        try:
            async with self._sessionmaker() as s:
                row = await s.get(EventPublicationRow, publication_id)
                pub = _row_to_pub(row) if row is not None else None
            if pub is not None:
                await outbox._dispatch_publication(pub)
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
        """Await all in-flight after-commit dispatch tasks (test/shutdown aid)."""
        while self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)

    async def dispose(self) -> None:
        """Drain in-flight dispatches and deactivate this store.

        Restores the previously-active store (so nested construct/dispose is
        non-destructive). When no store remains active, unregisters the global
        after-commit listener so it stops firing on every commit in the host
        application and doesn't leak across process/test lifetimes.
        """
        global _active_store, _hook_installed
        await self.wait_for_dispatch()
        if _active_store is self:
            _active_store = self._prev_store
            if _active_store is None and _hook_installed:
                sa_event.remove(Session, "after_commit", _schedule_after_commit_dispatch)
                _hook_installed = False


# ---------------------------------------------------------------------------
# Helpers exported for application setup
# ---------------------------------------------------------------------------


def bind_session(session: Any) -> Any:
    """Bind a SQLAlchemy session to the current context.

    Call at the start of a request/transaction so publish() finds the session
    via ``_current_session``. Returns the contextvar token; reset it when the
    request ends::

        async def get_db_with_outbox():
            async with async_session_maker() as session:
                token = bind_session(session)
                try:
                    yield session
                finally:
                    _current_session.reset(token)
    """
    return _current_session.set(session)


# ---------------------------------------------------------------------------
# Test support
# ---------------------------------------------------------------------------


def _reset_for_testing() -> None:
    """Clear adapter module globals and unregister the after-commit listener.

    ONLY for tests. The after-commit hook is registered on the global sync
    ``Session`` class, so without this it leaks across tests/process lifetime
    (the outbox plugin's own ``_reset_for_testing`` can't reach these globals).
    """
    global _active_store, _hook_installed
    _active_store = None
    if _hook_installed:
        sa_event.remove(Session, "after_commit", _schedule_after_commit_dispatch)
        _hook_installed = False


__all__ = [
    "Base",
    "EventPublicationArchiveRow",
    "EventPublicationRow",
    "PostgresPublicationStore",
    "bind_session",
]
