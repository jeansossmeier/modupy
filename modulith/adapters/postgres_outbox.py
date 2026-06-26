"""Postgres PublicationStore implementation via SQLAlchemy.

This is the reference adapter. It demonstrates the complete pattern for
a storage-backed PublicationStore: schema definition, session-aware
saves, after-commit dispatch hooking.

Implementation status: SKELETON. ~180 lines when complete; will likely
need to split into postgres_outbox.py + sqlalchemy_integration.py.

Distributed as `modulith-postgres` or as `modulith[postgres]` extra.
Optional dependencies: sqlalchemy>=2.0, asyncpg>=0.29.

Critical correctness pattern:
  1. save() uses the session from the contextvar (not a fresh one).
     This makes the publication record commit atomically with the
     business transaction.
  2. After-commit hook runs synchronously (SQLAlchemy event), schedules
     async dispatch tasks. The schedule-then-fire pattern means commit
     succeeds before any listener runs.
  3. On commit failure (rollback), the pending dispatch list is dropped
     because session.info pops with the rollback. No orphan dispatches.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any
from uuid import UUID

from modulith import EventPublication
from modulith.builtin.outbox import _current_session

logger = logging.getLogger("modulith.adapters.postgres")


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

# IMPLEMENTATION TODO: define SQLAlchemy table.
# from sqlalchemy import Column, String, DateTime, Integer, Boolean, Index
# from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB
# from sqlalchemy.orm import declarative_base
#
# Base = declarative_base()
#
# class EventPublicationRow(Base):
#     __tablename__ = "event_publications"
#     id = Column(PGUUID(as_uuid=True), primary_key=True)
#     event_type = Column(String, nullable=False)
#     payload = Column(JSONB, nullable=False)
#     listener = Column(String, nullable=False)
#     published_at = Column(DateTime(timezone=True), nullable=False)
#     completed_at = Column(DateTime(timezone=True), nullable=True)
#     attempt_count = Column(Integer, default=0)
#     last_error = Column(String, nullable=True)
#     is_dead_lettered = Column(Boolean, default=False)
#
#     __table_args__ = (
#         # Partial index: queries for incomplete events are common; a
#         # partial index keeps it small even with millions of completed rows.
#         Index(
#             "idx_pending",
#             "published_at",
#             postgresql_where=(completed_at.is_(None)) & (is_dead_lettered.is_(False)),
#         ),
#     )

# Migration: ship as alembic migration in modulith-postgres package.


# ---------------------------------------------------------------------------
# The PublicationStore implementation
# ---------------------------------------------------------------------------


class PostgresPublicationStore:
    """SQLAlchemy + Postgres backend for the transactional outbox.

    Conforms structurally to modulith.PublicationStore. Implementations
    don't inherit from a base class — duck typing via Protocol.

    Usage:
        # In application setup:
        from modulith import configure
        from modulith.adapters.postgres_outbox import PostgresPublicationStore
        from modulith.builtin import outbox

        store = PostgresPublicationStore(engine=async_engine)
        outbox.configure(store=store, serializer=JsonEventSerializer())
        configure(outbox="postgres")
    """

    def __init__(self, engine: Any) -> None:
        """Engine is an AsyncEngine from sqlalchemy.ext.asyncio.

        IMPLEMENTATION TODO:
        - Store engine reference.
        - Hook into the engine's session_factory's after_commit event.
          See _install_session_hooks() below.
        """
        self._engine = engine
        self._install_session_hooks()

    def _install_session_hooks(self) -> None:
        """Register SQLAlchemy event listeners for after_commit dispatch.

        IMPLEMENTATION TODO:
        Use sqlalchemy.event.listens_for on the AsyncSession's
        sync_session_class:

            from sqlalchemy import event as sa_event
            from sqlalchemy.ext.asyncio import async_sessionmaker

            session_class = async_sessionmaker(self._engine).sync_session_class

            @sa_event.listens_for(session_class, "after_commit")
            def _on_commit(session):
                pending = session.info.pop("_modulith_pending", [])
                for publication_id in pending:
                    asyncio.create_task(_dispatch_after_commit(publication_id))

        The dispatch happens via the outbox plugin's _dispatch_publication
        function. We schedule on the running loop because we're in a sync
        callback that may not have access to the original loop.
        """
        ...

    async def save(self, publication: EventPublication) -> None:
        """Save in the current session so it commits with the business txn.

        IMPLEMENTATION TODO:
        1. session = _current_session.get()
        2. if session is None: raise RuntimeError — outbox publish without
           a transaction context is a misuse.
        3. row = EventPublicationRow(**publication.__dict__)
        4. session.add(row)
        5. session.info.setdefault("_modulith_pending", []).append(publication.id)

        After business commits, the after_commit hook fires the dispatch.
        """
        ...

    async def mark_complete(self, publication_id: UUID) -> None:
        """Set completed_at = now, in a new short transaction.

        IMPLEMENTATION TODO:
        Use a fresh AsyncSession (not the current one — by the time
        completion fires, the original session is closed). UPDATE
        event_publications SET completed_at = now() WHERE id = :id.
        """
        ...

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        """SELECT pending publications older than the threshold.

        IMPLEMENTATION TODO:
        SELECT * FROM event_publications
        WHERE completed_at IS NULL
          AND is_dead_lettered = FALSE
          AND published_at < (now() - :older_than)
        ORDER BY published_at ASC
        LIMIT 100  -- batch to avoid pulling millions of rows

        Convert each row to EventPublication dataclass.
        """
        ...

    async def archive(self, publication_id: UUID) -> None:
        """Move to event_publications_archive, delete from primary."""
        ...

    async def delete(self, publication_id: UUID) -> None:
        """Hard-delete by id."""
        ...


# ---------------------------------------------------------------------------
# Helpers exported for application setup
# ---------------------------------------------------------------------------


def bind_session(session: Any) -> None:
    """Bind a SQLAlchemy session to the current context.

    Call this at the start of a request/transaction so publish() can
    find the session via _current_session.

    Typical FastAPI dependency:

        async def get_db_with_outbox():
            async with async_session_maker() as session:
                token = bind_session(session)
                try:
                    yield session
                finally:
                    _current_session.reset(token)

    IMPLEMENTATION TODO: just _current_session.set(session); return token.
    """
    return _current_session.set(session)


__all__ = [
    "PostgresPublicationStore",
    "bind_session",
]
