import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap
from modulith.builtin import outbox

from studio.database import engine
from studio.reviews import router as reviews_router
from studio.summaries import router as summaries_router

logging.basicConfig(level=logging.INFO)  # so the startup banner is visible


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    bootstrap()
    outbox.start()
    yield
    await outbox.shutdown()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(reviews_router, prefix="/reviews")
app.include_router(summaries_router, prefix="/summaries")
