from modulith import listener
from sqlalchemy import exists, insert, literal, select

from marketplace.contracts import OrderCancelled, OrderConfirmed, OrderPlaced, ShipmentBooked
from marketplace.db import transaction
from marketplace.notifications.tables import notification


async def record(order_id: str, kind: str, message: str) -> None:
    async with transaction() as session:
        await session.execute(
            insert(notification).from_select(
                ["order_id", "kind", "message"],
                select(literal(order_id), literal(kind), literal(message)).where(
                    ~exists().where(
                        notification.c.order_id == order_id, notification.c.kind == kind
                    )
                ),
            )
        )


@listener
async def on_order_placed(event: OrderPlaced) -> None:
    await record(event.order_id, "received", "order received")


@listener
async def on_order_confirmed(event: OrderConfirmed) -> None:
    await record(event.order_id, "confirmed", "order confirmed")


@listener
async def on_order_cancelled(event: OrderCancelled) -> None:
    await record(event.order_id, "cancelled", f"order cancelled: {event.reason}")


@listener
async def on_shipment_booked(event: ShipmentBooked) -> None:
    await record(
        event.order_id,
        "shipped",
        f"shipped with {event.carrier}, tracking {event.tracking_number}",
    )
