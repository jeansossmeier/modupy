"""keep microseconds in the outbox timestamp columns on MySQL and MariaDB

Revision ID: 0009_outbox_ts_microseconds
Revises: 0008_outbox_trace_context
Create Date: 2026-10-05
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime

revision: str = "0009_outbox_ts_microseconds"
down_revision: str | None = "0008_outbox_trace_context"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Column name -> nullable, for each table. Mirrors the _TIMESTAMP columns of
# EventPublicationRow and EventPublicationArchiveRow; 0001 and 0003 created
# them as a bare DATETIME, which MySQL and MariaDB keep to whole seconds.
_COLUMNS: dict[str, dict[str, bool]] = {
    "event_publications": {
        "published_at": False,
        "completed_at": True,
        "last_attempt_at": True,
        "claim_until": True,
    },
    "event_publications_archive": {
        "published_at": False,
        "completed_at": True,
        "last_attempt_at": True,
    },
}


def _is_mysql_family() -> bool:
    """Whether this run targets MySQL or MariaDB (also true offline, in ``--sql`` mode)."""
    return op.get_context().dialect.name in {"mysql", "mariadb"}


def _retype(*, existing: MySQLDateTime, new: MySQLDateTime) -> None:
    for table, columns in _COLUMNS.items():
        for column, nullable in columns.items():
            op.alter_column(
                table,
                column,
                existing_type=existing,
                type_=new,
                existing_nullable=nullable,
            )


def upgrade() -> None:
    if _is_mysql_family():
        _retype(existing=MySQLDateTime(), new=MySQLDateTime(fsp=6))


def downgrade() -> None:
    # Narrowing makes MySQL round every stored fraction to a whole second; the
    # column holds a timestamp, so the value stays within a second of the original.
    if _is_mysql_family():
        _retype(existing=MySQLDateTime(fsp=6), new=MySQLDateTime())
