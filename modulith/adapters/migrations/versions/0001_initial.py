"""initial outbox schema

Creates ``event_publications`` and ``event_publications_archive`` plus the
pending-rows partial index. Mirrors
``modulith.adapters.postgres_outbox.Base.metadata``.

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-26
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import LONGBLOB as MySQLLongBlob

# revision identifiers, used by Alembic.
revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Payload type that holds a real serialized event on every dialect — plain
# LargeBinary compiles to MySQL/MariaDB BLOB (65,535 bytes), so a moderately
# large event fails at flush with error 1406 and takes the business transaction
# with it. Inert on Postgres/SQLite. Mirrors postgres_outbox._PAYLOAD exactly.
_PAYLOAD = sa.LargeBinary().with_variant(MySQLLongBlob(), "mysql", "mariadb")


def upgrade() -> None:
    op.create_table(
        "event_publications",
        sa.Column("id", sa.Uuid(), nullable=False),
        # Text, not an unbounded String — MySQL's VARCHAR requires an explicit
        # length, so this column type must render as TEXT/LONGTEXT there (a
        # no-op on Postgres/SQLite, which treat Text and unbounded String
        # identically). Mirrors postgres_outbox.EventPublicationRow.
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", _PAYLOAD, nullable=False),
        sa.Column("listener", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_dead_lettered", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint("id"),
    )
    # Partial index on Postgres (small even with millions of completed rows);
    # a plain index on other dialects.
    op.create_index(
        "idx_pending",
        "event_publications",
        ["published_at"],
        postgresql_where=sa.text("completed_at IS NULL"),
    )

    op.create_table(
        "event_publications_archive",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", _PAYLOAD, nullable=False),
        sa.Column("listener", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("event_publications_archive")
    op.drop_index("idx_pending", table_name="event_publications")
    op.drop_table("event_publications")
