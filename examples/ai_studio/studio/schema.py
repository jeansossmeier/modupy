import asyncio

from studio.database import engine
from studio.reviews.models import Base


async def _create_tables() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    await engine.dispose()


def create_tables() -> None:
    asyncio.run(_create_tables())


if __name__ == "__main__":
    create_tables()
