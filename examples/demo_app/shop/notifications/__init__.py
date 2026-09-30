from fastapi import APIRouter, HTTPException
from modulith import listener
from modulith.builtin.outbox import bind_session, unbind_session

from shop.contracts.events import StockReserved
from shop.database import sessionmaker
from shop.notifications.models import Notification

router = APIRouter()


@listener
async def notify_customer(event: StockReserved) -> None:
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            if await session.get(Notification, event.order_id) is not None:
                return
            session.add(Notification(order_id=event.order_id))
            await session.commit()
        finally:
            unbind_session(token)


@router.get("/{order_id}")
async def read_notification(order_id: str) -> dict[str, str | bool]:
    async with sessionmaker() as session:
        if await session.get(Notification, order_id) is None:
            raise HTTPException(status_code=404, detail="no notification")
    return {"order_id": order_id, "notified": True}
