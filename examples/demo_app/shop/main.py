import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap
from modulith.builtin import outbox

from shop.database import engine
from shop.inventory import router as inventory_router
from shop.notifications import router as notifications_router
from shop.orders import router as orders_router

logging.basicConfig(level=logging.INFO)  # so the startup banner is visible


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    bootstrap()
    outbox.start()
    yield
    await outbox.shutdown()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(orders_router, prefix="/orders")
app.include_router(inventory_router, prefix="/inventory")
app.include_router(notifications_router, prefix="/notifications")
