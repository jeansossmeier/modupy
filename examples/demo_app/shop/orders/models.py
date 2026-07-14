"""SQLAlchemy model for the ``orders`` module's own table.

Only imported when the demo's durable-outbox mode is active (``MODULITH_OUTBOX``
!= "memory" or ``MODULITH_DB_URL`` set) — see ``shop.orders.place_order`` and
``shop.main``'s lifespan. Importing SQLAlchemy at module top-level here is
fine: this module itself is lazily imported, so the zero-config in-memory
default path never pulls SQLAlchemy in.

``orders`` owns this model outright — no shared cross-module DB module — so
module boundary verification (``modulith verify``) stays clean: only the
``contracts`` package is shared between modules.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Float, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for the orders module's own tables."""


class Order(Base):
    """A placed order, persisted atomically with its ``OrderPlaced`` outbox row."""

    __tablename__ = "shop_orders"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str] = mapped_column(String, nullable=False)
    total: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
