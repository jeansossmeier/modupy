import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

URL = os.environ.get("MODULITH_OUTBOX_URL", "sqlite+aiosqlite:///shop.db")

engine = create_async_engine(URL, poolclass=NullPool if URL.startswith("sqlite") else None)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
