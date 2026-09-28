"""Notifications module — the tail of the event chain.

Listens for ``StockReserved`` and "notifies" the customer (here, by recording
the event). It depends on neither orders nor inventory — only on the shared
contract. Add or remove this module and the others are unaffected.

Also exposes a read-only ``router`` for inspecting sent notifications, mounted
at ``/notifications`` by ``shop.main`` (single-process) and
``modulith._worker`` (process-per-module), so ``GET /notifications/sent``
works in both topologies.
"""

from __future__ import annotations

from collections import deque

from fastapi import APIRouter

from modulith import listener
from shop.contracts.events import StockReserved

# Toy "outbox of sent notifications" the demo can inspect. Dev-inspection-
# only: bounded so a long-running process doesn't grow this list unboundedly.
sent: deque[StockReserved] = deque(maxlen=1000)

router = APIRouter()


@listener
async def notify_customer(event: StockReserved) -> None:
    """Notify the customer that their order's stock is reserved.

    Guarded by order_id membership: outbox delivery is at-least-once
    (``modulith/builtin/outbox.py`` module docstring — a listener may be
    called more than once), so a redelivered StockReserved must not notify
    twice.
    """
    if any(evt.order_id == event.order_id for evt in sent):
        return
    sent.append(event)


@router.get("/sent")
async def list_sent() -> dict[str, list[str]]:
    """Inspect which orders have received a stock-reserved notification."""
    return {"sent": [evt.order_id for evt in sent]}
