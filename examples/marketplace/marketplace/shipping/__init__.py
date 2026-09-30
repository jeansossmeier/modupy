from sqlalchemy import select, update

from marketplace.contracts import ShipmentBooked
from marketplace.db import engine, transaction
from marketplace.shipping.events import ShipmentRequested
from marketplace.shipping.tables import shipment, zone


async def set_zone(country: str, carrier: str) -> None:
    async with transaction() as session:
        updated = await session.execute(
            update(zone)
            .where(zone.c.country == country)
            .values(carrier=carrier)
            .returning(zone.c.country)
        )
        if updated.first() is None:
            await session.execute(zone.insert().values(country=country, carrier=carrier))


async def shipment_of(order_id: str) -> dict[str, str | None]:
    async with engine().connect() as connection:
        row = (
            await connection.execute(
                select(shipment.c.status, shipment.c.carrier, shipment.c.tracking_number).where(
                    shipment.c.order_id == order_id
                )
            )
        ).one_or_none()
    if row is None:
        raise LookupError(f"no shipment for order {order_id!r}")
    return {
        "order_id": order_id,
        "status": row.status,
        "carrier": row.carrier,
        "tracking_number": row.tracking_number,
    }


__all__ = ["ShipmentBooked", "ShipmentRequested", "router", "set_zone", "shipment_of"]

# Process-per-module mode serves HTTP by mounting the `router` attribute of each
# module package, so the router is re-exported here.
from marketplace.shipping.api import router as router  # noqa: E402
