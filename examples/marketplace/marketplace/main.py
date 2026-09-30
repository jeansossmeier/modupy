from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap
from modulith.builtin import outbox

from marketplace.db import engine
from marketplace.orders import router as orders_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    bootstrap()
    outbox.start()
    yield
    await outbox.shutdown()
    await engine().dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(orders_router, prefix="/orders")
