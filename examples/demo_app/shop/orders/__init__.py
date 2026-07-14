"""Orders module — the public entry point for placing orders.

``place_order`` persists the order and publishes ``OrderPlaced``. It knows
nothing about who reacts to that event; inventory and notifications subscribe
independently. This is the core modulith idea: modules collaborate through
events, not direct calls.

Two modes, selected by whether a session is passed in:

* No session (default, zero-config): the order is recorded in the in-memory
  ``placed`` list and the event is published in-memory — today's behavior,
  unchanged.
* A session is passed (durable-outbox mode, wired by ``shop.main``): the order
  row is added to the session and ``publish`` runs inside that transaction, so
  the runtime's outbox plugin persists the ``OrderPlaced`` publication
  atomically with the order row and dispatches it after commit.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING
from uuid import uuid4

from modulith import publish
from shop.contracts.events import OrderPlaced

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# A toy "database" the demo can inspect in the zero-config (in-memory) mode
# only — see place_order's session-None guard below. Dev-inspection-only:
# bounded so a long-running process doesn't grow this list unboundedly.
placed: deque[OrderPlaced] = deque(maxlen=1000)


async def place_order(
    customer_id: str, total: float, *, session: AsyncSession | None = None
) -> str:
    """Place an order and announce it to the rest of the system.

    ``session`` is ``None`` in the zero-config default (in-memory publish).
    When provided (durable-outbox mode), the order is persisted through it and
    ``OrderPlaced`` is published inside that same bound transaction.
    """
    order_id = uuid4().hex[:8]
    evt = OrderPlaced(order_id=order_id, customer_id=customer_id, total=total)

    if session is not None:
        from shop.orders.models import Order

        session.add(Order(id=order_id, customer_id=customer_id, total=total))
    else:
        # Dual-write hazard: appending to `placed` unconditionally would
        # write to this in-memory list regardless of whether the caller's
        # session transaction later commits or rolls back. In durable mode
        # the Order row (committed atomically with the OrderPlaced outbox
        # row) is the sole source of truth, so `placed` is populated only
        # on the in-memory path, where it IS the source of truth.
        placed.append(evt)

    await publish(evt)
    return order_id


# Re-export the module's HTTP router as ``shop.orders.router`` so the
# process-per-module worker (``modulith._worker.create_app``) can mount it under
# ``/orders`` — the same convention ``inventory``/``notifications`` follow.
# Imported at the bottom to avoid a circular import: ``api`` imports
# ``place_order`` from this module, which must be defined first.
from shop.orders.api import router as router  # noqa: E402
