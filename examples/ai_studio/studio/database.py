import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from modulith.builtin.outbox import bind_session, unbind_session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

URL = os.environ.get("MODULITH_OUTBOX_URL", "sqlite+aiosqlite:///studio.db")

engine = create_async_engine(URL, poolclass=NullPool if URL.startswith("sqlite") else None)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncSession]:
    """A session whose published events go to the outbox and commit with its writes."""
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            yield session
            await session.commit()
        finally:
            unbind_session(token)
