"""database broker schema

Creates ``broker_subscription`` and ``broker_message`` for the database-backed
broker adapter (``modulith.adapters.db_broker``). Mirrors the SQLAlchemy Core
tables built by ``db_broker.broker_schema()`` — keep the two in lockstep (the
migration tests diff the migrated schema against that metadata).

The second link in the migration chain (0001 -> 0002): the first revision that
actually exercises a multi-revision ``upgrade head`` / ``downgrade base`` on the
packaged migrations.

Revision ID: 0002_broker_message
Revises: 0001_initial
Create Date: 2026-07-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime

# revision identifiers, used by Alembic.
revision: str = "0002_broker_message"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# VARCHAR lengths — kept IN LOCKSTEP with db_broker.broker_schema()
# (_ID_LEN / _TARGET_LEN / _GROUP_LEN / _EVENT_TYPE_LEN / _STATUS_LEN /
# _CLAIMED_BY_LEN). MySQL rejects an unbounded VARCHAR, so every String column
# is bounded; the broker column-metadata drift tests diff this against the
# adapter's Core schema on both SQLite and real Postgres.
_ID_LEN = 64
_TARGET_LEN = 255
_GROUP_LEN = 255
_EVENT_TYPE_LEN = 255
_STATUS_LEN = 32
_CLAIMED_BY_LEN = 255

# Microsecond-precision timestamp on every dialect — MySQL and MariaDB default
# DATETIME to whole-second precision (fsp=0), losing the broker's sub-second
# timing; fsp=6 fixes both. Inert on Postgres/SQLite. Mirrors
# db_broker.broker_schema()'s `ts` type exactly (drift tests enforce it).
_TS = sa.DateTime(timezone=True).with_variant(MySQLDateTime(fsp=6), "mysql", "mariadb")


def upgrade() -> None:
    op.create_table(
        "broker_subscription",
        sa.Column("target", sa.String(_TARGET_LEN), nullable=False),
        sa.Column("consumer_group", sa.String(_GROUP_LEN), nullable=False),
        sa.Column("updated_at", _TS, nullable=False),
        sa.PrimaryKeyConstraint("target", "consumer_group"),
    )

    op.create_table(
        "broker_message",
        sa.Column("id", sa.String(_ID_LEN), nullable=False),
        sa.Column("target", sa.String(_TARGET_LEN), nullable=False),
        sa.Column("consumer_group", sa.String(_GROUP_LEN), nullable=False),
        # Nullable so a publish with no event_type header (or a poison row)
        # dead-letters at the consumer rather than failing at INSERT.
        sa.Column("event_type", sa.String(_EVENT_TYPE_LEN), nullable=True),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("headers", sa.Text(), nullable=True),
        sa.Column("status", sa.String(_STATUS_LEN), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", _TS, nullable=False),
        sa.Column("claimed_at", _TS, nullable=True),
        sa.Column("claimed_by", sa.String(_CLAIMED_BY_LEN), nullable=True),
        sa.Column("created_at", _TS, nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # The competing-consumer claim path (status + due-time scan per group).
    op.create_index(
        "ix_broker_message_claim",
        "broker_message",
        ["consumer_group", "status", "available_at"],
    )
    # The age-based prune scan.
    op.create_index(
        "ix_broker_message_prune",
        "broker_message",
        ["status", "created_at"],
    )
    # Diagnostics / by-target inspection.
    op.create_index(
        "ix_broker_message_target",
        "broker_message",
        ["target"],
    )


def downgrade() -> None:
    op.drop_index("ix_broker_message_target", table_name="broker_message")
    op.drop_index("ix_broker_message_prune", table_name="broker_message")
    op.drop_index("ix_broker_message_claim", table_name="broker_message")
    op.drop_table("broker_message")
    op.drop_table("broker_subscription")
