"""Inventory module — reserves stock in reaction to orders.

The ``@listener`` registers automatically when modulith discovers this module;
no wiring code is needed. After reserving, it publishes ``StockReserved`` so
downstream modules (notifications) can react in turn — a chain of independent
modules, each ignorant of the others.
"""

from __future__ import annotations

from modulith import listener, publish
from shop.contracts.events import OrderPlaced, StockReserved

# Toy state the demo can inspect.
reserved: list[StockReserved] = []


@listener
async def reserve_stock(event: OrderPlaced) -> None:
    """Reserve stock for a placed order, then announce the reservation."""
    evt = StockReserved(order_id=event.order_id)
    reserved.append(evt)
    await publish(evt)
