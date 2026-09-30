from modulith import listener
from sqlalchemy import exists, insert, literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from marketplace.contracts import OrderCancelled, OrderConfirmed, ShipmentBooked
from marketplace.db import transaction
from marketplace.reporting.tables import report


async def ensure_row(session: AsyncSession, order_id: str) -> None:
    await session.execute(
        insert(report).from_select(
            ["order_id"],
            select(literal(order_id)).where(~exists().where(report.c.order_id == order_id)),
        )
    )


@listener
async def on_order_confirmed(event: OrderConfirmed) -> None:
    async with transaction() as session:
        await ensure_row(session, event.order_id)
        await session.execute(
            update(report)
            .where(report.c.order_id == event.order_id, report.c.confirmed.is_(False))
            .values(confirmed=True, total_cents=event.total_cents)
        )


@listener
async def on_order_cancelled(event: OrderCancelled) -> None:
    async with transaction() as session:
        await ensure_row(session, event.order_id)
        await session.execute(
            update(report)
            .where(report.c.order_id == event.order_id, report.c.cancelled.is_(False))
            .values(cancelled=True)
        )


@listener
async def on_shipment_booked(event: ShipmentBooked) -> None:
    async with transaction() as session:
        await ensure_row(session, event.order_id)
        await session.execute(
            update(report)
            .where(report.c.order_id == event.order_id, report.c.shipped.is_(False))
            .values(shipped=True)
        )
