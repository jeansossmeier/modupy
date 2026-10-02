"""store the W3C trace context of the publish that created an outbox publication

Revision ID: 0008_outbox_trace_context
Revises: 0007_outbox_dispatch_started
Create Date: 2026-10-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008_outbox_trace_context"
down_revision: str | None = "0007_outbox_dispatch_started"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("event_publications", "event_publications_archive")


def _column_exists(table: str) -> bool:
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
    if not inspector.has_table(table, schema=schema):
        return False
    columns = inspector.get_columns(table, schema=schema)
    return any(column["name"] == "trace_context" for column in columns)


def upgrade() -> None:
    # Mirrors the trace_context column of EventPublicationRow and
    # EventPublicationArchiveRow; the drift tests diff them.
    for table in _TABLES:
        if not _column_exists(table):
            op.add_column(table, sa.Column("trace_context", sa.Text(), nullable=True))


def downgrade() -> None:
    for table in _TABLES:
        op.drop_column(table, "trace_context")
