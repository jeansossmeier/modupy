from modulith import listener, publish
from sqlalchemy import exists, insert, literal, select, update

from marketplace.contracts import (
    OrderPlaced,
    PaymentDeclined,
    ProductListed,
    StockRejected,
    StockReleased,
    StockReserved,
)
from marketplace.db import transaction
from marketplace.inventory.tables import reservation, stock


@listener
async def on_product_listed(event: ProductListed) -> None:
    async with transaction() as session:
        await session.execute(
            insert(stock).from_select(
                ["sku", "on_hand"],
                select(literal(event.sku), literal(event.stock)).where(
                    ~exists().where(stock.c.sku == event.sku)
                ),
            )
        )


@listener
async def on_payment_declined(event: PaymentDeclined) -> None:
    async with transaction() as session:
        released = await session.execute(
            update(reservation)
            .where(reservation.c.order_id == event.order_id, reservation.c.status == "reserved")
            .values(status="released")
            .returning(reservation.c.sku, reservation.c.quantity)
        )
        row = released.one_or_none()
        if row is None:
            return

        await session.execute(
            update(stock)
            .where(stock.c.sku == row.sku)
            .values(on_hand=stock.c.on_hand + row.quantity)
        )
        await publish(StockReleased(order_id=event.order_id, sku=row.sku, quantity=row.quantity))


@listener
async def on_order_placed(event: OrderPlaced) -> None:
    async with transaction() as session:
        first_delivery = await session.execute(
            insert(reservation)
            .from_select(
                ["order_id", "sku", "quantity", "status"],
                select(
                    literal(event.order_id),
                    literal(event.sku),
                    literal(event.quantity),
                    literal("pending"),
                ).where(~exists().where(reservation.c.order_id == event.order_id)),
            )
            .returning(reservation.c.order_id)
        )
        if first_delivery.first() is None:
            return

        reserved = await session.execute(
            update(stock)
            .where(stock.c.sku == event.sku, stock.c.on_hand >= event.quantity)
            .values(on_hand=stock.c.on_hand - event.quantity)
            .returning(stock.c.sku)
        )
        outcome = "reserved" if reserved.first() is not None else "rejected"
        await session.execute(
            update(reservation)
            .where(reservation.c.order_id == event.order_id)
            .values(status=outcome)
        )
        if outcome == "reserved":
            await publish(
                StockReserved(order_id=event.order_id, sku=event.sku, quantity=event.quantity)
            )
        else:
            available = await session.scalar(
                select(stock.c.on_hand).where(stock.c.sku == event.sku)
            )
            await publish(
                StockRejected(
                    order_id=event.order_id,
                    sku=event.sku,
                    requested=event.quantity,
                    available=available or 0,
                )
            )
