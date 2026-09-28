"""record whether a claimed broker message started dispatching

Revision ID: 0006_broker_dispatch_started
Revises: 0005_outbox_scan_indexes
Create Date: 2026-09-28
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_broker_dispatch_started"
down_revision: str | None = "0005_outbox_scan_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_exists() -> bool:
    """Whether the column is already there, or False offline (``--sql``).

    ``DatabaseBroker._ensure_schema`` adds this column itself on a database
    it bootstrapped, so adopting migrations later must skip it rather than
    abort the chain on a duplicate column.
    """
    context = op.get_context()
    if context.as_sql:
        return False
    inspector = sa.inspect(op.get_bind())
    schema = context.version_table_schema
    if not inspector.has_table("broker_message", schema=schema):
        return False
    columns = inspector.get_columns("broker_message", schema=schema)
    return any(column["name"] == "dispatch_started" for column in columns)


def upgrade() -> None:
    # Mirrors the ``dispatch_started`` column of db_broker.broker_schema();
    # the drift tests diff the two.
    if _column_exists():
        return
    op.add_column(
        "broker_message",
        sa.Column(
            "dispatch_started",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("broker_message", "dispatch_started")
