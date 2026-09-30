from modulith import listener, publish
from sqlalchemy import update

from marketplace.contracts import OrderCancelled, StockRejected
from marketplace.db import transaction
from marketplace.orders.tables import order


@listener
async def on_stock_rejected(event: StockRejected) -> None:
    async with transaction() as session:
        cancelled = await session.execute(
            update(order)
            .where(order.c.order_id == event.order_id, order.c.status == "placed")
            .values(status="cancelled", reason="out of stock")
            .returning(order.c.customer_id)
        )
        customer_id = cancelled.scalar_one_or_none()
        if customer_id is not None:
            await publish(
                OrderCancelled(
                    order_id=event.order_id, customer_id=customer_id, reason="out of stock"
                )
            )
