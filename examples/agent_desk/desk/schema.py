import asyncio

from desk.activity import Base as ActivityBase
from desk.agent import Base as AgentBase
from desk.database import engine
from desk.tickets.models import Base as TicketsBase


async def _create_tables() -> None:
    async with engine.begin() as connection:
        for base in (TicketsBase, ActivityBase, AgentBase):
            await connection.run_sync(base.metadata.create_all)
    await engine.dispose()


def create_tables() -> None:
    asyncio.run(_create_tables())


if __name__ == "__main__":
    create_tables()
