import asyncio

from sqlalchemy import exists, insert, literal, select

import marketplace.catalog.tables
import marketplace.inventory.tables
import marketplace.orders.tables
import marketplace.payments.tables  # noqa: F401
from marketplace.db import engine, metadata
from marketplace.shipping.tables import zone

ZONES = {"US": "UPS", "DE": "DHL"}


async def create_all() -> None:
    async with engine().begin() as connection:
        await connection.run_sync(metadata.create_all)
        for country, carrier in ZONES.items():
            await connection.execute(
                insert(zone).from_select(
                    ["country", "carrier"],
                    select(literal(country), literal(carrier)).where(
                        ~exists().where(zone.c.country == country)
                    ),
                )
            )
    await engine().dispose()


if __name__ == "__main__":
    asyncio.run(create_all())
