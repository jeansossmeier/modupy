import asyncio

import marketplace.catalog.tables  # noqa: F401
from marketplace.db import engine, metadata


async def create_all() -> None:
    async with engine().begin() as connection:
        await connection.run_sync(metadata.create_all)
    await engine().dispose()


if __name__ == "__main__":
    asyncio.run(create_all())
