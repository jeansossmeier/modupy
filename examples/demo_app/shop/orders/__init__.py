"""Orders module — the public entry point for placing orders.

``place_order`` persists the order (here, an in-memory list for the demo) and
publishes ``OrderPlaced``. It knows nothing about who reacts to that event;
inventory and notifications subscribe independently. This is the core modulith
idea: modules collaborate through events, not direct calls.
"""

from __future__ import annotations

from uuid import uuid4

from modulith import publish
from shop.contracts.events import OrderPlaced

# A toy "database" the demo can inspect. A real module would write to its own
# tables inside a transaction and publish through the outbox.
placed: list[OrderPlaced] = []


async def place_order(customer_id: str, total: float) -> str:
    """Place an order and announce it to the rest of the system."""
    order_id = uuid4().hex[:8]
    evt = OrderPlaced(order_id=order_id, customer_id=customer_id, total=total)
    placed.append(evt)
    await publish(evt)
    return order_id
