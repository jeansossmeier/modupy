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

# revision identifiers, used by Alembic.
revision: str = "0002_broker_message"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "broker_subscription",
        sa.Column("target", sa.String(), nullable=False),
        sa.Column("consumer_group", sa.String(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("target", "consumer_group"),
    )

    op.create_table(
        "broker_message",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("target", sa.String(), nullable=False),
        sa.Column("consumer_group", sa.String(), nullable=False),
        # Nullable so a publish with no event_type header (or a poison row)
        # dead-letters at the consumer rather than failing at INSERT.
        sa.Column("event_type", sa.String(), nullable=True),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("headers", sa.Text(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
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
