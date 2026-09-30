from modulith import listener, publish
from sqlalchemy import update

from marketplace.contracts import (
    OrderCancelled,
    OrderConfirmed,
    PaymentCaptured,
    PaymentRequested,
    StockRejected,
    StockReleased,
    StockReserved,
)
from marketplace.db import transaction
from marketplace.orders.tables import order


@listener
async def on_stock_reserved(event: StockReserved) -> None:
    async with transaction() as session:
        reserved = await session.execute(
            update(order)
            .where(order.c.order_id == event.order_id, order.c.status == "placed")
            .values(status="reserved")
            .returning(order.c.total_cents, order.c.card_token)
        )
        row = reserved.one_or_none()
        if row is not None:
            await publish(
                PaymentRequested(
                    order_id=event.order_id, amount_cents=row.total_cents, card_token=row.card_token
                )
            )


@listener
async def on_payment_captured(event: PaymentCaptured) -> None:
    async with transaction() as session:
        confirmed = await session.execute(
            update(order)
            .where(order.c.order_id == event.order_id, order.c.status == "reserved")
            .values(status="confirmed")
            .returning(
                order.c.customer_id,
                order.c.sku,
                order.c.quantity,
                order.c.total_cents,
                order.c.country,
            )
        )
        row = confirmed.one_or_none()
        if row is not None:
            await publish(
                OrderConfirmed(
                    order_id=event.order_id,
                    customer_id=row.customer_id,
                    sku=row.sku,
                    quantity=row.quantity,
                    total_cents=row.total_cents,
                    country=row.country,
                )
            )


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


# StockReleased means the payment was declined: inventory.handlers.on_payment_declined
# is the only place stock is released.
@listener
async def on_stock_released(event: StockReleased) -> None:
    async with transaction() as session:
        cancelled = await session.execute(
            update(order)
            .where(order.c.order_id == event.order_id, order.c.status == "reserved")
            .values(status="cancelled", reason="payment declined")
            .returning(order.c.customer_id)
        )
        customer_id = cancelled.scalar_one_or_none()
        if customer_id is not None:
            await publish(
                OrderCancelled(
                    order_id=event.order_id, customer_id=customer_id, reason="payment declined"
                )
            )
