from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shop.orders import DuplicateOrderError, get_order, place_order

router = APIRouter()


class NewOrder(BaseModel):
    order_id: str
    customer_id: str
    total: float


@router.post("")
async def post_order(body: NewOrder) -> dict[str, str]:
    try:
        await place_order(body.order_id, body.customer_id, body.total)
    except DuplicateOrderError:
        raise HTTPException(status_code=409, detail="order already exists") from None
    return {"order_id": body.order_id}


@router.get("/{order_id}")
async def read_order(order_id: str) -> dict[str, str | float]:
    order = await get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="unknown order")
    return {"order_id": order.order_id, "customer_id": order.customer_id, "total": order.total}
