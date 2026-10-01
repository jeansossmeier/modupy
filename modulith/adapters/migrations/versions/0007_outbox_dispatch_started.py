"""record whether a claimed outbox publication started dispatching

Revision ID: 0007_outbox_dispatch_started
Revises: 0006_broker_dispatch_started
Create Date: 2026-09-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_outbox_dispatch_started"
down_revision: str | None = "0006_broker_dispatch_started"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_exists() -> bool:
    """Whether the column is already there, or False offline (``--sql``).

    Tables created from the ORM metadata (``Base.metadata.create_all``)
    already carry it, so adopting migrations later must skip it rather than
    abort the chain on a duplicate column.
    """
    context = op.get_context()
    if context.as_sql:
        return False
    inspector = sa.inspect(op.get_bind())
    schema = context.version_table_schema
    if not inspector.has_table("event_publications", schema=schema):
        return False
    columns = inspector.get_columns("event_publications", schema=schema)
    return any(column["name"] == "dispatch_started" for column in columns)


def upgrade() -> None:
    # Mirrors EventPublicationRow.dispatch_started; the drift tests diff the two.
    if _column_exists():
        return
    op.add_column(
        "event_publications",
        sa.Column(
            "dispatch_started",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("event_publications", "dispatch_started")
