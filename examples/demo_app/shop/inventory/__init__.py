from fastapi import APIRouter, HTTPException
from modulith import listener, publish
from modulith.builtin.outbox import bind_session, unbind_session

from shop.contracts.events import OrderPlaced, StockReserved
from shop.database import sessionmaker
from shop.inventory.models import Reservation

router = APIRouter()


@listener
async def reserve_stock(event: OrderPlaced) -> None:
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            if await session.get(Reservation, event.order_id) is not None:
                return
            await publish(StockReserved(order_id=event.order_id))
            session.add(Reservation(order_id=event.order_id))
            await session.commit()
        finally:
            unbind_session(token)


@router.get("/reservations/{order_id}")
async def read_reservation(order_id: str) -> dict[str, str | bool]:
    async with sessionmaker() as session:
        if await session.get(Reservation, order_id) is None:
            raise HTTPException(status_code=404, detail="no reservation")
    return {"order_id": order_id, "reserved": True}
