from modulith import publish
from sqlalchemy import insert, select

from marketplace import catalog
from marketplace.contracts import OrderCancelled, OrderPlaced
from marketplace.db import engine, transaction
from marketplace.orders.tables import order


async def place_order(
    order_id: str, customer_id: str, sku: str, quantity: int, card_token: str, country: str
) -> None:
    total_cents = quantity * await catalog.price_of(sku)
    async with transaction() as session:
        await session.execute(
            insert(order).values(
                order_id=order_id,
                customer_id=customer_id,
                sku=sku,
                quantity=quantity,
                total_cents=total_cents,
                country=country,
                card_token=card_token,
                status="placed",
            )
        )
        await publish(
            OrderPlaced(
                order_id=order_id,
                customer_id=customer_id,
                sku=sku,
                quantity=quantity,
                total_cents=total_cents,
                country=country,
            )
        )


async def status_of(order_id: str) -> dict[str, str | None]:
    async with engine().connect() as connection:
        row = (
            await connection.execute(
                select(order.c.status, order.c.reason).where(order.c.order_id == order_id)
            )
        ).one_or_none()
    if row is None:
        raise LookupError(f"unknown order {order_id!r}")
    return {"order_id": order_id, "status": row.status, "reason": row.reason}


__all__ = ["OrderCancelled", "OrderPlaced", "place_order", "router", "status_of"]

# Process-per-module mode serves HTTP by mounting the `router` attribute of each
# module package, so the router is re-exported here.
from marketplace.orders.api import router as router  # noqa: E402
