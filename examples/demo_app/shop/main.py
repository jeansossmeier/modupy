"""FastAPI entrypoint for the demo shop.

Run from ``examples/demo_app``::

    uvicorn shop.main:app --reload
    # or, with the CLI:
    modulith dev shop.main:app

modulith auto-detects the ``shop`` package, discovers the modules, registers
their listeners, and prints a startup banner. No bootstrap call is needed — the
runtime initializes lazily on the first publish.

Env vars this module reads (all optional; the default is zero-config,
single-process, in-memory):

  * ``MODULITH_OUTBOX``            — "memory" (default) or "postgres". Any
                                      value other than "memory" (or setting
                                      ``MODULITH_DB_URL``) turns on the
                                      durable transactional outbox.
  * ``MODULITH_DB_URL``            — SQLAlchemy async URL for the outbox
                                      engine. Defaults to a local SQLite file
                                      when the outbox is on but no URL is set.
  * ``MODULITH_DEMO_OTEL``         — "1" initializes console-exporter OTel
                                      tracing.
  * ``MODULITH_DEMO_SERIALIZER``   — "custom" uses the versioned-JSON outbox
                                      storage serializer instead of the
                                      default JSON one.

``MODULITH_BROKER``, ``MODULITH_TOPOLOGY``, ``MODULITH_BROKER_URL``, and
``REDIS_URL`` are native modulith config env vars read by the runtime itself —
this module does not need to touch them.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from shop.inventory import router as inventory_router
from shop.notifications import router as notifications_router
from shop.orders.api import router as orders_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Wire optional telemetry and the durable outbox from env vars.

    Leaves ``app.state.sessionmaker`` at ``None`` (the zero-config default)
    unless the outbox env vars request the durable path, in which case it
    builds an async engine, creates both the orders module's own tables and
    the outbox's tables, configures the outbox plugin, and exposes the
    sessionmaker for ``shop.orders.api``'s dependency to use.
    """
    if os.getenv("MODULITH_DEMO_OTEL") == "1":
        from shop.telemetry import init_telemetry

        init_telemetry()

    db_url = os.getenv("MODULITH_DB_URL")
    outbox_on = os.getenv("MODULITH_OUTBOX", "memory") != "memory" or bool(db_url)

    engine = None
    store = None
    if outbox_on:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
        from sqlalchemy.pool import NullPool

        import modulith
        from modulith.adapters.postgres_outbox import Base as OutboxBase
        from modulith.adapters.postgres_outbox import PostgresPublicationStore
        from modulith.builtin import outbox
        from modulith.serializers import JsonEventSerializer
        from shop.contracts.events import OrderPlaced, StockReserved
        from shop.orders.models import Base as OrdersBase

        url = db_url or "sqlite+aiosqlite:///./demo.db"
        if url.startswith("sqlite"):
            # NullPool: each request opens (and closes) its own connection rather
            # than borrowing from a pool. aiosqlite runs one worker thread per
            # connection, so a pooled aiosqlite connection would pin a request to
            # whichever thread first opened it — NullPool sidesteps that.
            engine = create_async_engine(url, poolclass=NullPool)
        else:
            # Default async connection pool with pre-ping to detect stale connections.
            engine = create_async_engine(url, pool_pre_ping=True)
        async with engine.begin() as conn:
            await conn.run_sync(OrdersBase.metadata.create_all)
            await conn.run_sync(OutboxBase.metadata.create_all)

        if os.getenv("MODULITH_DEMO_SERIALIZER") == "custom":
            from shop.serialization import VersionedJsonSerializer

            serializer: object = VersionedJsonSerializer()
        else:
            # Scoped to the demo's real event types — see shop/contracts/events.py.
            serializer = JsonEventSerializer(allowed_event_types=[OrderPlaced, StockReserved])

        store = PostgresPublicationStore(engine=engine)
        outbox.configure(store, serializer)
        # Eager bootstrap: the crash-recovery sweep no-ops until the runtime
        # has bootstrapped (it needs a resolved event_bus), which otherwise
        # wouldn't happen until the first publish(). Bootstrapping here lets
        # stranded publications from a previous crash retry immediately.
        modulith.bootstrap()
        app.state.sessionmaker = async_sessionmaker(engine)
    else:
        app.state.sessionmaker = None

    yield

    # Teardown order matters: drain/unregister the store's after-commit hook
    # (store.dispose) BEFORE outbox.shutdown() stops the retry loop, and
    # dispose the engine LAST — both prior steps still need it to flush
    # in-flight dispatches and run the retry loop's final sweep.
    if store is not None:
        await store.dispose()
    from modulith.builtin import outbox

    await outbox.shutdown()
    if engine is not None:
        await engine.dispose()


app = FastAPI(title="modulith demo shop", lifespan=lifespan)
app.include_router(orders_router, prefix="/orders")
app.include_router(inventory_router, prefix="/inventory")
app.include_router(notifications_router, prefix="/notifications")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
