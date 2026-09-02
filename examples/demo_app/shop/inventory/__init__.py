"""Inventory module — reserves stock in reaction to orders.

The ``@listener`` registers automatically when modulith discovers this module;
no wiring code is needed. After reserving, it publishes ``StockReserved`` so
downstream modules (notifications) can react in turn — a chain of independent
modules, each ignorant of the others.

Also exposes a read-only ``router`` for inspecting reserved stock, mounted at
``/inventory`` by ``shop.main`` (single-process) and ``modulith._worker``
(process-per-module), so ``GET /inventory/reserved`` works in both topologies.
"""

from __future__ import annotations

from collections import deque

from fastapi import APIRouter

from modulith import listener, publish
from shop.contracts.events import OrderPlaced, StockReserved

reserved: deque[StockReserved] = deque(maxlen=1000)
_reserved_order_ids: set[str] = set()

router = APIRouter()


@listener
async def reserve_stock(event: OrderPlaced) -> None:
    """Reserve stock for a placed order, then announce the reservation.

    Guarded by order_id membership: outbox delivery is at-least-once
    (modulith/builtin/outbox.py:14-16 — a listener may be called more than
    once), so a redelivered OrderPlaced must not reserve stock (or publish
    StockReserved) twice.
    """
    if event.order_id in _reserved_order_ids:
        return
    evt = StockReserved(order_id=event.order_id)
    reserved.append(evt)
    _reserved_order_ids.add(event.order_id)
    await publish(evt)


@router.get("/reserved")
async def list_reserved() -> dict[str, list[str]]:
    """Inspect which orders currently have stock reserved."""
    return {"reserved": [evt.order_id for evt in reserved]}
