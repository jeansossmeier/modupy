"""database broker retained-message tables

Revision ID: 0004_broker_retained_messages
Revises: 0003_outbox_claim_leases
Create Date: 2026-07-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime

revision: str = "0004_broker_retained_messages"
down_revision: str | None = "0003_outbox_claim_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ID_LEN = 64
_TARGET_LEN = 255
_GROUP_LEN = 255
_EVENT_TYPE_LEN = 255
_TS = sa.DateTime(timezone=True).with_variant(MySQLDateTime(fsp=6), "mysql", "mariadb")


def upgrade() -> None:
    op.create_table(
        "broker_retained_message",
        sa.Column("id", sa.String(_ID_LEN), nullable=False),
        sa.Column("target", sa.String(_TARGET_LEN), nullable=False),
        sa.Column("event_type", sa.String(_EVENT_TYPE_LEN), nullable=True),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("headers", sa.Text(), nullable=True),
        sa.Column("created_at", _TS, nullable=False),
        sa.Column("expires_at", _TS, nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_broker_retained_message_expiry",
        "broker_retained_message",
        ["expires_at"],
    )
    op.create_index(
        "ix_broker_retained_message_target_expiry",
        "broker_retained_message",
        ["target", "expires_at"],
    )

    op.create_table(
        "broker_retained_delivery",
        sa.Column("retained_message_id", sa.String(_ID_LEN), nullable=False),
        sa.Column("consumer_group", sa.String(_GROUP_LEN), nullable=False),
        sa.Column("broker_message_id", sa.String(_ID_LEN), nullable=False),
        sa.Column("delivered_at", _TS, nullable=False),
        sa.ForeignKeyConstraint(
            ["retained_message_id"],
            ["broker_retained_message.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("retained_message_id", "consumer_group"),
        sa.UniqueConstraint(
            "broker_message_id",
            name="uq_broker_retained_delivery_message",
        ),
    )


def downgrade() -> None:
    op.drop_table("broker_retained_delivery")
    op.drop_index(
        "ix_broker_retained_message_target_expiry",
        table_name="broker_retained_message",
    )
    op.drop_index(
        "ix_broker_retained_message_expiry",
        table_name="broker_retained_message",
    )
    op.drop_table("broker_retained_message")
