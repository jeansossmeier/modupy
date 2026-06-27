"""Notifications module — the tail of the event chain.

Listens for ``StockReserved`` and "notifies" the customer (here, by recording
the event). It depends on neither orders nor inventory — only on the shared
contract. Add or remove this module and the others are unaffected.
"""

from __future__ import annotations

from modulith import listener
from shop.contracts.events import StockReserved

# Toy "outbox of sent notifications" the demo can inspect.
sent: list[StockReserved] = []


@listener
async def notify_customer(event: StockReserved) -> None:
    """Notify the customer that their order's stock is reserved."""
    sent.append(event)
