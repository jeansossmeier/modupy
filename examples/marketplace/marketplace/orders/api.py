from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from marketplace.orders import place_order, status_of

router = APIRouter()


class NewOrder(BaseModel):
    order_id: str
    customer_id: str
    sku: str
    quantity: int
    card_token: str
    country: str


@router.post("")
async def post_order(body: NewOrder) -> dict[str, str]:
    try:
        await place_order(
            body.order_id, body.customer_id, body.sku, body.quantity, body.card_token, body.country
        )
    except LookupError as unknown:
        raise HTTPException(status_code=404, detail=str(unknown)) from unknown
    except IntegrityError as duplicate:
        raise HTTPException(status_code=409, detail="order already exists") from duplicate
    return {"order_id": body.order_id, "status": "placed"}


@router.get("/{order_id}")
async def get_order(order_id: str) -> dict[str, str | None]:
    try:
        order: dict[str, str | None] = await status_of(order_id)
    except LookupError as unknown:
        raise HTTPException(status_code=404, detail=str(unknown)) from unknown
    return order
