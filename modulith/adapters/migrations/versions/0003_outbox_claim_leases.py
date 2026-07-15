"""reserve nullable outbox claim lease columns

Revision ID: 0003_outbox_claim_leases
Revises: 0002_broker_message
Create Date: 2026-07-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_outbox_claim_leases"
down_revision: str | None = "0002_broker_message"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Task 4 will activate lease-based claiming. These columns are nullable so
    # this schema-only revision cannot change current outbox behavior.
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
