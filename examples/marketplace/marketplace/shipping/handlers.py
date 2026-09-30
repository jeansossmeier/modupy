from modulith import listener, publish
from sqlalchemy import exists, insert, literal, select, update

from marketplace.contracts import OrderConfirmed, ShipmentBooked
from marketplace.db import transaction
from marketplace.shipping.events import ShipmentRequested
from marketplace.shipping.tables import shipment, zone


@listener
async def on_order_confirmed(event: OrderConfirmed) -> None:
    async with transaction() as session:
        first_delivery = await session.execute(
            insert(shipment)
            .from_select(
                ["order_id", "customer_id", "country", "status"],
                select(
                    literal(event.order_id),
                    literal(event.customer_id),
                    literal(event.country),
                    literal("requested"),
                ).where(~exists().where(shipment.c.order_id == event.order_id)),
            )
            .returning(shipment.c.order_id)
        )
        if first_delivery.first() is not None:
            await publish(ShipmentRequested(order_id=event.order_id, country=event.country))


@listener
async def book_carrier(event: ShipmentRequested) -> None:
    async with transaction() as session:
        carrier = await session.scalar(
            select(zone.c.carrier).where(zone.c.country == event.country)
        )
        if carrier is None:
            raise LookupError(f"no shipping zone for {event.country!r}")

        booked = await session.execute(
            update(shipment)
            .where(shipment.c.order_id == event.order_id, shipment.c.status == "requested")
            .values(status="booked", carrier=carrier, tracking_number=f"TRK-{event.order_id}")
            .returning(shipment.c.customer_id)
        )
        customer_id = booked.scalar_one_or_none()
        if customer_id is not None:
            await publish(
                ShipmentBooked(
                    order_id=event.order_id,
                    customer_id=customer_id,
                    carrier=carrier,
                    tracking_number=f"TRK-{event.order_id}",
                )
            )
