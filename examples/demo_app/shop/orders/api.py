"""FastAPI router for the orders module.

Mounted by ``shop.main``. A module owns its own HTTP surface; the app just
includes the routers. In process-per-module topology this same router is
served by the orders worker and reached through the reverse proxy at
``/orders``.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from shop.orders import place_order

router = APIRouter()


class PlaceOrderRequest(BaseModel):
    customer_id: str
    total: float


@router.post("/orders")
async def post_order(req: PlaceOrderRequest) -> dict[str, str]:
    """Place an order; the event chain fans out to the other modules."""
    order_id = await place_order(customer_id=req.customer_id, total=req.total)
    return {"order_id": order_id}
