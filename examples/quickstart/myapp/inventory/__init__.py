from fastapi import APIRouter, HTTPException
from modulith import listener

from myapp.contracts.events import OrderCreated

_reserved: set[str] = set()

router = APIRouter()


@listener
async def reserve(event: OrderCreated) -> None:
    _reserved.add(event.order_id)  # your real stock reservation goes here


@router.get("/{order_id}")
async def get_reservation(order_id: str) -> dict[str, str | bool]:
    if order_id not in _reserved:
        raise HTTPException(status_code=404, detail="order not reserved")
    return {"order_id": order_id, "reserved": True}
