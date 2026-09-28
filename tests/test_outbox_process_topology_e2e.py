"""Durable outbox across real process-per-module workers, on SQLite only.

The real ``Supervisor`` spawns one ``uvicorn`` worker per module of a small
app written to ``tmp_path``. Both workers bind their outbox store from
``MODULITH_OUTBOX_URL``, which points at the same SQLite file as the orders
module's business table. An ``OrderPlaced`` published inside the orders
worker's transaction is stored as an outbox row in that transaction and, after
commit, delivered through the SQLite database broker to the billing worker's
listener.

Spawns real uvicorn subprocesses (slow) -> ``@pytest.mark.integration``, like
``test_demo_app_topology.py``; no server or container is needed.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from textwrap import dedent

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from modulith.adapters.db_broker import DatabaseBroker
from modulith.adapters.postgres_outbox import Base
from modulith.supervisor import Supervisor, WorkerSpec

from conftest import _free_port

pytestmark = [pytest.mark.integration]

_REPO_ROOT = Path(__file__).resolve().parent.parent

_APP_FILES = {
    "obx/__init__.py": "",
    "obx/contracts/__init__.py": """
        from dataclasses import dataclass

        from modulith import event, externalized

        @event
        @externalized
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str
    """,
    "obx/orders/__init__.py": """
        import os

        from fastapi import APIRouter
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from modulith import publish
        from modulith.adapters.postgres_outbox import bind_session, unbind_session
        from obx.contracts import OrderPlaced

        router = APIRouter()
        _sessions = async_sessionmaker(
            create_async_engine(os.environ["MODULITH_OUTBOX_URL"]), expire_on_commit=False
        )

        @router.post("/place/{order_id}")
        async def place(order_id: str) -> dict[str, str]:
            async with _sessions() as session:
                token = bind_session(session)
                try:
                    await session.execute(
                        text("INSERT INTO orders (id) VALUES (:id)"), {"id": order_id}
                    )
                    await publish(OrderPlaced(order_id))
                    await session.commit()
                finally:
                    unbind_session(token)
            return {"order_id": order_id}
    """,
    "obx/orders/_manifest.py": """
        from modulith import declare_module

        declare_module(publishes=["OrderPlaced"], declared_dependencies=["contracts"])
    """,
    "obx/billing/__init__.py": """
        from fastapi import APIRouter

        from modulith import listener
        from obx.contracts import OrderPlaced

        router = APIRouter()
        RECEIVED: list[str] = []

        @listener
        async def on_placed(event: OrderPlaced) -> None:
            RECEIVED.append(event.order_id)

        @router.get("/received")
        async def received() -> dict[str, list[str]]:
            return {"received": RECEIVED}
    """,
    "obx/billing/_manifest.py": """
        from modulith import declare_module
        from obx.billing import on_placed

        declare_module(
            consumes=["OrderPlaced"],
            listeners=[on_placed],
            declared_dependencies=["contracts"],
        )
    """,
}


def _write_app(root: Path) -> None:
    for relpath, source in _APP_FILES.items():
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dedent(source))


async def _eventually(check: object, timeout: float = 30.0) -> object:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        result = await check()  # type: ignore[operator]
        if result or loop.time() >= end:
            return result
        await asyncio.sleep(0.2)


async def test_transactional_publish_in_one_worker_is_stored_and_delivered_in_another(
    tmp_path: Path,
) -> None:
    _write_app(tmp_path / "app")
    outbox_url = f"sqlite+aiosqlite:///{tmp_path / 'business.db'}"
    broker_url = f"sqlite+aiosqlite:///{tmp_path / 'broker.db'}"
    engine = create_async_engine(outbox_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE TABLE orders (id TEXT PRIMARY KEY)"))
    broker = DatabaseBroker(url=broker_url)
    await broker._ensure_schema()
    await broker.close()

    env = {
        "PYTHONPATH": f"{tmp_path / 'app'}{os.pathsep}{_REPO_ROOT}",
        "MODULITH_OUTBOX": "postgres",
        "MODULITH_OUTBOX_URL": outbox_url,
        "MODULITH_BROKER": "database",
        "MODULITH_BROKER_URL": broker_url,
        "MODULITH_BROKER_POLL_INTERVAL_MS": "100",
    }
    specs = [
        WorkerSpec(module_name=name, package="obx", port=_free_port(), env=dict(env))
        for name in ("orders", "billing")
    ]
    orders_port, billing_port = (spec.port for spec in specs)
    supervisor = Supervisor(specs)
    await supervisor.start()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:

            async def healthy() -> bool:
                try:
                    responses = [
                        await client.get(f"http://127.0.0.1:{port}/health")
                        for port in (orders_port, billing_port)
                    ]
                except httpx.HTTPError:
                    return False
                return all(r.status_code == 200 for r in responses)

            assert await _eventually(healthy, timeout=40.0)
            placed = await client.post(f"http://127.0.0.1:{orders_port}/orders/place/o-1")

            async def received() -> list[str]:
                resp = await client.get(f"http://127.0.0.1:{billing_port}/billing/received")
                return list(resp.json()["received"])

            delivered = await _eventually(received)
    finally:
        await supervisor.stop()

    async with engine.connect() as conn:
        orders = (await conn.execute(text("SELECT id FROM orders"))).scalars().all()
        rows = (
            await conn.execute(
                text("SELECT listener, completed_at IS NOT NULL FROM event_publications")
            )
        ).all()
    await engine.dispose()

    assert (placed.status_code, delivered, orders) == (200, ["o-1"], ["o-1"])
    assert [(listener.split(":")[0], bool(done)) for listener, done in rows] == [
        ("__modulith.broker_route__", True)
    ]
