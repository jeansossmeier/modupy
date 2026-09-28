"""Durable-outbox test of the bundled demo application (``examples/demo_app``).

Drives the REAL ``shop`` package (not a synthetic fixture app) with the
transactional outbox wired to a real SQLAlchemy engine, proving the durable
path documented in ``shop.main``'s lifespan actually works: ``place_order``
persists an ``Order`` row atomically with the ``OrderPlaced`` outbox row, and
after commit the event reaches ``inventory``'s listener — exactly the chain
``tests/test_demo_app.py`` proves for the default in-memory path.

The default-lane test uses a temp-file SQLite engine (no Docker); an
``@pytest.mark.integration`` variant repeats the same assertions against real
Postgres via the shared ``postgres_url`` fixture (see tests/conftest.py).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from modulith.adapters import postgres_outbox
from modulith.adapters.postgres_outbox import Base as OutboxBase
from modulith.adapters.postgres_outbox import PostgresPublicationStore, bind_session
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

DEMO_ROOT = Path(__file__).resolve().parent.parent / "examples" / "demo_app"


async def _until_reserved(inventory: Any, order_id: str, timeout: float = 5.0) -> None:
    """Wait for ``inventory``'s listener to record ``order_id``.

    ``wait_for_dispatch()`` awaits only after-commit dispatch tasks. The retry
    loop's startup sweep (``older_than=0``) can claim the just-committed row
    first, and after-commit dispatch then leaves the row to that sweep.
    """
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not any(evt.order_id == order_id for evt in inventory.reserved):
        if loop.time() >= end:
            raise AssertionError(f"order {order_id} was never reserved")
        await asyncio.sleep(0.02)


@pytest.fixture
def demo_shop(monkeypatch):
    """Bootstrap the on-disk ``shop`` package with clean runtime/outbox state.

    Mirrors ``tests/test_demo_app.py``'s ``demo_app`` fixture (sys.path +
    runtime/manifest reset) plus ``tests/test_postgres_outbox_e2e.py``'s
    ``_reset`` fixture (outbox + adapter globals) — this test drives both the
    demo package AND the durable outbox, so it needs both fixtures' cleanup.
    """
    from modulith import manifest as _manifest_mod

    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()

    monkeypatch.syspath_prepend(str(DEMO_ROOT))

    yield

    for name in list(sys.modules):
        if name == "shop" or name.startswith("shop."):
            del sys.modules[name]
    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


async def test_durable_outbox_persists_order_and_dispatches_across_modules(
    demo_shop, tmp_path: Path
) -> None:
    from shop.orders import place_order
    from shop.orders.models import Base as OrdersBase
    from shop.orders.models import Order

    from modulith import configure

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'demo.db'}", poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(OrdersBase.metadata.create_all)
        await conn.run_sync(OutboxBase.metadata.create_all)

    # try/finally so engine.dispose() still runs if an assertion below fails
    # (matches the Postgres sibling test's teardown structure).
    try:
        store = PostgresPublicationStore(engine=engine)
        outbox.configure(store, JsonEventSerializer(), start_loop=True)

        configure(package="shop", auto_discover=True)
        _runtime.ensure_bootstrapped()

        from shop import inventory

        sessionmaker = async_sessionmaker(engine)
        async with sessionmaker() as session:
            token = bind_session(session)
            try:
                order_id = await place_order(customer_id="c-durable", total=42.0, session=session)
                await session.commit()
            finally:
                outbox._current_session.reset(token)

        await store.wait_for_dispatch()

        async with sessionmaker() as s:
            row: Order = (await s.execute(select(Order).where(Order.id == order_id))).scalar_one()
            assert row.customer_id == "c-durable"
            assert row.total == 42.0

        await _until_reserved(inventory, order_id)

        await store.dispose()
        await outbox.shutdown()
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_durable_outbox_persists_order_and_dispatches_across_modules_on_postgres(
    demo_shop, postgres_url: str
) -> None:
    from shop.orders import place_order
    from shop.orders.models import Base as OrdersBase
    from shop.orders.models import Order

    from modulith import configure

    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(OrdersBase.metadata.create_all)
        await conn.run_sync(OutboxBase.metadata.create_all)

    try:
        store = PostgresPublicationStore(engine=engine)
        outbox.configure(store, JsonEventSerializer(), start_loop=True)

        configure(package="shop", auto_discover=True)
        _runtime.ensure_bootstrapped()

        from shop import inventory

        sessionmaker = async_sessionmaker(engine)
        async with sessionmaker() as session:
            token = bind_session(session)
            try:
                order_id = await place_order(customer_id="c-pg", total=7.5, session=session)
                await session.commit()
            finally:
                outbox._current_session.reset(token)

        await store.wait_for_dispatch()

        async with sessionmaker() as s:
            row: Order = (await s.execute(select(Order).where(Order.id == order_id))).scalar_one()
            assert row.customer_id == "c-pg"

        await _until_reserved(inventory, order_id)

        await store.dispose()
        await outbox.shutdown()
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(OutboxBase.metadata.drop_all)
            await conn.run_sync(OrdersBase.metadata.drop_all)
        await engine.dispose()
