import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import cache

from modulith.builtin.outbox import bind_session, unbind_session
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

metadata = MetaData()


@cache
def engine() -> AsyncEngine:
    return create_async_engine(os.environ["MODULITH_OUTBOX_URL"])


@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncSession]:
    async with AsyncSession(engine(), expire_on_commit=False) as session:
        token = bind_session(session)
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
        finally:
            unbind_session(token)
