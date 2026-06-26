"""Cross-module event definitions for the demo shop.

Each event is a frozen dataclass marked with ``@event``. Frozen dataclasses
give value semantics (equality, hashing) and make events safe to share — the
property the transactional outbox relies on for round-trip fidelity.
"""

from __future__ import annotations

from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderPlaced:
    """A customer placed an order. Published by the ``orders`` module."""

    order_id: str
    customer_id: str
    total: float


@event
@dataclass(frozen=True)
class StockReserved:
    """Stock was reserved for an order. Published by the ``inventory`` module."""

    order_id: str
