import asyncio

from shop.database import engine
from shop.inventory.models import Base as InventoryBase
from shop.notifications.models import Base as NotificationsBase
from shop.orders.models import Base as OrdersBase


async def _create_tables() -> None:
    async with engine.begin() as connection:
        for base in (OrdersBase, InventoryBase, NotificationsBase):
            await connection.run_sync(base.metadata.create_all)
    await engine.dispose()


def create_tables() -> None:
    asyncio.run(_create_tables())


if __name__ == "__main__":
    create_tables()
