"""Cross-module event definitions for the demo shop.

Each event is a frozen dataclass marked with ``@event``. Frozen dataclasses
give value semantics (equality, hashing) and make events safe to share — the
property the transactional outbox relies on for round-trip fidelity.
"""

from __future__ import annotations

from dataclasses import dataclass

from modulith import event, externalized


@event
@externalized
@dataclass(frozen=True)
class OrderPlaced:
    """A customer placed an order. Published by the ``orders`` module.

    Marked ``@externalized`` so process-per-module topology routes it through
    the configured broker to the ``inventory`` worker. Inert in single-process
    topology (``modulith/runtime.py`` skips broker routing when
    ``topology == "single"``).
    """

    order_id: str
    customer_id: str
    total: float


@event
@externalized
@dataclass(frozen=True)
class StockReserved:
    """Stock was reserved for an order. Published by the ``inventory`` module.

    Marked ``@externalized`` so process-per-module topology routes it through
    the configured broker to the ``notifications`` worker.
    """

    order_id: str
