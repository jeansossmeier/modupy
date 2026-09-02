"""database broker retained-message tables

Revision ID: 0004_broker_retained_messages
Revises: 0003_outbox_claim_leases
Create Date: 2026-07-14
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime
from sqlalchemy.dialects.mysql import LONGBLOB as MySQLLongBlob

revision: str = "0004_broker_retained_messages"
down_revision: str | None = "0003_outbox_claim_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ID_LEN = 64
_TARGET_LEN = 255
_GROUP_LEN = 255
_EVENT_TYPE_LEN = 255
_TS = sa.DateTime(timezone=True).with_variant(MySQLDateTime(fsp=6), "mysql", "mariadb")

# Payload type that holds a full-size publish on every dialect — plain
# LargeBinary compiles to MySQL/MariaDB BLOB (65,535 bytes), far below the
# broker's payload cap, so an oversized INSERT fails with error 1406. Inert on
# Postgres/SQLite. Mirrors db_broker.broker_schema()'s `payload_type` exactly.
_PAYLOAD = sa.LargeBinary().with_variant(MySQLLongBlob(), "mysql", "mariadb")


def _existing() -> tuple[Any, str | None]:
    """Reflection handle for the schema this migration is actually writing to,
    or ``None`` offline (``--sql`` renders DDL with no connection to inspect).

    Same reason as 0002: ``db_broker.DatabaseBroker._ensure_schema`` may have
    already created these tables with ``metadata.create_all`` on a database
    that never ran a migration, and an unguarded ``create_table`` would abort
    ``upgrade head`` there permanently.
    """
    context = op.get_context()
    if context.as_sql:
        return None, None
    return sa.inspect(op.get_bind()), context.version_table_schema


def _has_table(inspector: Any, name: str, schema: str | None) -> bool:
    return inspector is not None and inspector.has_table(name, schema=schema)


def upgrade() -> None:
    inspector, schema = _existing()
    retained_exists = _has_table(inspector, "broker_retained_message", schema)
    retained_indexes = (
        {index["name"] for index in inspector.get_indexes("broker_retained_message", schema=schema)}
        if retained_exists
        else set()
    )

    if not retained_exists:
        op.create_table(
            "broker_retained_message",
            sa.Column("id", sa.String(_ID_LEN), nullable=False),
            sa.Column("target", sa.String(_TARGET_LEN), nullable=False),
            sa.Column("event_type", sa.String(_EVENT_TYPE_LEN), nullable=True),
            sa.Column("payload", _PAYLOAD, nullable=False),
            sa.Column("headers", sa.Text(), nullable=True),
            sa.Column("created_at", _TS, nullable=False),
            sa.Column("expires_at", _TS, nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
    if "ix_broker_retained_message_expiry" not in retained_indexes:
        op.create_index(
            "ix_broker_retained_message_expiry",
            "broker_retained_message",
            ["expires_at"],
        )
    if "ix_broker_retained_message_target_expiry" not in retained_indexes:
        op.create_index(
            "ix_broker_retained_message_target_expiry",
            "broker_retained_message",
            ["target", "expires_at"],
        )

    if not _has_table(inspector, "broker_retained_delivery", schema):
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
