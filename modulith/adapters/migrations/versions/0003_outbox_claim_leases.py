"""reserve nullable outbox claim lease columns; make event_type/listener/
last_error MySQL-safe on deployments that already ran 0001

Revision ID: 0003_outbox_claim_leases
Revises: 0002_broker_message
Create Date: 2026-07-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import LONGBLOB as MySQLLongBlob
from sqlalchemy.sql.schema import SchemaItem

revision: str = "0003_outbox_claim_leases"
down_revision: str | None = "0002_broker_message"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Mirrors postgres_outbox._PAYLOAD and 0001's payload column: plain
# LargeBinary compiles to MySQL/MariaDB BLOB (65,535 bytes), which truncates or
# rejects a moderately large event. Inert on Postgres/SQLite.
_PAYLOAD = sa.LargeBinary().with_variant(MySQLLongBlob(), "mysql", "mariadb")

# Both tables' columns that a deployment whose 0001 created them as an
# unbounded String needs converted — MySQL's VARCHAR requires an explicit
# length, so that type never compiled a CREATE TABLE on that dialect. A
# fresh install gets Text from 0001 directly; this alter converges the
# older deployment onto the same Text schema, so "upgrade from base" and
# "upgrade from 0002" land on identical columns (enforced by the migration
# drift tests).
# nullable=True only for last_error; event_type/listener are NOT NULL in
# both tables (mirrors postgres_outbox.EventPublicationRow/ArchiveRow).
_TEXT_COLUMNS: dict[str, bool] = {"event_type": False, "listener": False, "last_error": True}
_TABLES = ("event_publications", "event_publications_archive")


def _table_as_of_0001(name: str) -> sa.Table:
    """The table exactly as 0001 defines it (entering this revision).

    Batch mode needs this passed explicitly as ``copy_from`` — without it,
    SQLite's "create new table, copy, swap" strategy reflects the live
    table, which requires a real database connection and breaks offline
    (``--sql``) DDL generation. Passing the shape directly makes the batch
    op work identically online and offline.
    """
    columns: list[SchemaItem] = [
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", _PAYLOAD, nullable=False),
        sa.Column("listener", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    ]
    if name == "event_publications":
        columns.append(
            sa.Column("is_dead_lettered", sa.Boolean(), nullable=False, server_default=sa.false())
        )
    columns.append(sa.PrimaryKeyConstraint("id"))
    if name == "event_publications":
        # 0001's idx_pending — must be declared here too, or batch mode's
        # table recreate silently drops it (it only recreates what's in the
        # Table object passed as copy_from).
        columns.append(
            sa.Index(
                "idx_pending", "published_at", postgresql_where=sa.text("completed_at IS NULL")
            )
        )
    return sa.Table(name, sa.MetaData(), *columns)


def upgrade() -> None:
    # batch_alter_table: SQLite has no ALTER COLUMN ... TYPE statement, so
    # Alembic's batch mode recreates the table under the hood there; on
    # Postgres/MySQL it emits a direct ALTER COLUMN (no table rebuild).
    for table in _TABLES:
        with op.batch_alter_table(table, copy_from=_table_as_of_0001(table)) as batch_op:
            for column, nullable in _TEXT_COLUMNS.items():
                batch_op.alter_column(
                    column,
                    existing_type=sa.String(),
                    type_=sa.Text(),
                    existing_nullable=nullable,
                )
            # Same converge-onto-a-fresh-install motive as the Text columns:
            # an install whose 0001 created payload as a plain LargeBinary has
            # a MySQL BLOB payload capped at 65,535 bytes, where a fresh
            # install gets LONGBLOB. Widening is lossless, and on
            # Postgres/SQLite the variant resolves to the type already there.
            batch_op.alter_column(
                "payload",
                existing_type=sa.LargeBinary(),
                type_=_PAYLOAD,
                existing_nullable=False,
            )

    # Lease columns for the claim protocol, added nullable so this schema-only
    # revision cannot change the behavior of a dispatcher that never sets them.
    op.add_column(
        "event_publications",
        sa.Column("claim_owner", sa.String(255), nullable=True),
    )
    op.add_column(
        "event_publications",
        sa.Column("claim_token", sa.String(64), nullable=True),
    )
    op.add_column(
        "event_publications",
        sa.Column("claim_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("event_publications", "claim_until")
    op.drop_column("event_publications", "claim_token")
    op.drop_column("event_publications", "claim_owner")

    # The Text conversion is deliberately NOT reversed. 0001 creates these
    # columns as Text, so Text already IS the 0002 shape for every install
    # that starts from the current tree; narrowing them on the way down would
    # instead DESTROY data — the outbox writes up to 500 characters of
    # ``last_error`` (see builtin/outbox._record_failure), and an
    # ``ALTER ... TYPE VARCHAR(255)`` over an existing longer value aborts the
    # rollback on Postgres and truncates it on non-strict MySQL. On the one
    # deployment shape this revision's upgrade actually converts (a Postgres
    # install whose 0001 created an unbounded String), leaving Text behind is
    # a no-op: Postgres treats unbounded varchar and text identically.
