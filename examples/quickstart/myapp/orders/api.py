from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from myapp.orders import create_order, is_fulfilled

router = APIRouter()


class NewOrder(BaseModel):
    customer_id: str


@router.post("")
async def post_order(body: NewOrder) -> dict[str, str]:
    return {"order_id": await create_order(body.customer_id)}


@router.get("/{order_id}/fulfilment")
async def get_fulfilment(order_id: str) -> dict[str, str | bool]:
    if not is_fulfilled(order_id):
        raise HTTPException(status_code=404, detail="order not fulfilled")
    return {"order_id": order_id, "fulfilled": True}
