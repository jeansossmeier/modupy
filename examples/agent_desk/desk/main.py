import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap
from modulith.builtin import outbox

from desk.activity import router as activity_router
from desk.database import engine
from desk.tickets import router as tickets_router

logging.basicConfig(level=logging.INFO)  # so the startup banner is visible


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    bootstrap()
    outbox.start()
    yield
    await outbox.shutdown()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(tickets_router, prefix="/tickets")
app.include_router(activity_router, prefix="/activity")
