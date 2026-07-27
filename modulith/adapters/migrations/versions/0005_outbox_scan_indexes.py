"""index the outbox claim ordering and the archive purge scan

Revision ID: 0005_outbox_scan_indexes
Revises: 0004_broker_retained_messages
Create Date: 2026-07-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_outbox_scan_indexes"
down_revision: str | None = "0004_broker_retained_messages"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CLAIM_INDEX = "ix_event_publications_claim_order"
_ARCHIVE_INDEX = "ix_event_publications_archive_completed_at"

# The exact ORDER BY expression and the leading filter shared by
# ``find_incomplete`` and ``claim_batch`` (adapters/postgres_outbox.py). The
# predicate is written to match the rendered WHERE clause term for term
# (``is_dead_lettered IS false``, not ``= false``) so Postgres can prove the
# query implies it and use the partial index.
_CLAIM_ORDER = "coalesce(last_attempt_at, published_at)"
_CLAIM_PREDICATE = "completed_at IS NULL AND is_dead_lettered IS false"


def _is_postgres() -> bool:
    """Whether this run targets Postgres.

    ``get_context().dialect`` is populated in offline (``--sql``) mode too,
    where there is no connection to ask.
    """
    return op.get_context().dialect.name == "postgresql"


def upgrade() -> None:
    # Expression index: both sweep queries end in ``ORDER BY <expr> LIMIT n``,
    # and no index on a plain column can serve that ordering, so without this
    # Postgres reads every pending row — payload bytes and all — and sorts the
    # whole set on every sweep of every worker process. Postgres-only: MariaDB
    # has no functional indexes and MySQL only gained them in 8.0.13, neither
    # supports a partial index, and SQLite's planner is not the one the sweep
    # contends with in production.
    if _is_postgres():
        op.create_index(
            _CLAIM_INDEX,
            "event_publications",
            [sa.text(_CLAIM_ORDER)],
            postgresql_where=sa.text(_CLAIM_PREDICATE),
        )

    # ``purge_completed`` deletes from the archive by ``completed_at``. The
    # archive is append-only until that purge runs, so it is the one outbox
    # table with no bound on its size — the scan needs an index on every
    # dialect, not just Postgres.
    op.create_index(_ARCHIVE_INDEX, "event_publications_archive", ["completed_at"])


def downgrade() -> None:
    op.drop_index(_ARCHIVE_INDEX, table_name="event_publications_archive")
    if _is_postgres():
        op.drop_index(_CLAIM_INDEX, table_name="event_publications")
