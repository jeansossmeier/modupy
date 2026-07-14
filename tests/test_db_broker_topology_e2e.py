"""Process-per-module topology end-to-end over the DATABASE broker.

A faithful mirror of ``test_process_topology_e2e.py`` (which proves the same
stack over Redis) but with ``broker="database"``: the real ``Supervisor``
spawns two real ``uvicorn`` worker subprocesses (one per module), the reverse
proxy routes real HTTP to the right worker by path prefix, and an order placed
through the proxy publishes an event that crosses a real process boundary
*through the database* and fires a listener inside the OTHER worker process.

The db_broker unit and integration tests exercise the broker inside a single
process. This closes the last seam the design actually ships on: two separate
OS processes, each with its OWN SQLAlchemy engine against the SAME database,
using the ``broker_message`` / ``broker_subscription`` tables as the transport.
It also proves the cross-process ordering the fan-out-on-write model depends on
— the consumer worker ``subscribe()``s during its lifespan startup (which
uvicorn completes before ``/health`` serves), so a subscription row exists
before the producer worker ever publishes; without it the publish would fan out
to zero groups and the event would be silently dropped.

Two backends prove the two headline deployments:

  * **Postgres** — the production durable path (a real server DB, a connection
    pool, ``FOR UPDATE SKIP LOCKED`` claims), provisioned by the shared
    ``postgres_url`` testcontainers fixture; skipped without Docker.
  * **an embedded SQLite file** shared across the worker processes — the
    zero-infrastructure "no Redis, no server, just a file" bootstrap the design
    promises. It runs anywhere: no Docker, no external service.

Both spawn real uvicorn subprocesses (slow) → ``@pytest.mark.integration``.
"""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from textwrap import dedent

import httpx
import pytest

import modulith
from modulith.adapters.db_broker import DatabaseBroker, broker_schema
from modulith.proxy import create_proxy_app
from modulith.supervisor import Supervisor, WorkerSpec, _rules_from_specs

pytestmark = [pytest.mark.integration]

# Repo root (the directory containing the ``modulith`` package) so the worker
# subprocesses can import modulith regardless of their cwd.
_REPO_ROOT = Path(modulith.__file__).resolve().parent.parent


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _write_app(root: Path) -> None:
    """Write a minimal 2-module shop app (orders publishes, inventory listens).

    Identical in shape to the Redis topology test's app: orders has no local
    listener for ``OrderPlaced`` so the runtime routes the publish to the broker;
    inventory listens and records receipts, exposing them over HTTP so the test
    can observe cross-process delivery from outside both workers.
    """
    pkg = root / "shopapp"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")

    (pkg / "contracts").mkdir()
    (pkg / "contracts" / "__init__.py").write_text(
        dedent(
            """
            from dataclasses import dataclass
            from modulith import event

            @event
            @dataclass(frozen=True)
            class OrderPlaced:
                order_id: str
            """
        )
    )

    (pkg / "orders").mkdir()
    (pkg / "orders" / "__init__.py").write_text(
        dedent(
            """
            from fastapi import APIRouter
            from modulith import publish
            from shopapp.contracts import OrderPlaced

            router = APIRouter()

            @router.post("/place")
            async def place(order_id: str) -> dict:
                # No local listener here → the runtime routes this to the broker.
                await publish(OrderPlaced(order_id=order_id))
                return {"order_id": order_id}
            """
        )
    )

    (pkg / "inventory").mkdir()
    (pkg / "inventory" / "__init__.py").write_text(
        dedent(
            """
            from fastapi import APIRouter
            from modulith import listener
            from shopapp.contracts import OrderPlaced

            received: list[str] = []
            router = APIRouter()

            @listener
            async def reserve_stock(evt: OrderPlaced) -> None:
                received.append(evt.order_id)

            @router.get("/received")
            async def get_received() -> dict:
                return {"received": received}
            """
        )
    )


async def _until_async(
    coro_predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float = 30.0,
    interval: float = 0.2,
) -> None:
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await coro_predicate():
            return
        await asyncio.sleep(interval)
    if not await coro_predicate():
        raise AssertionError("condition not met within timeout")


async def _wait_healthy(client: httpx.AsyncClient, port: int, module: str) -> None:
    async def ok() -> bool:
        try:
            r = await client.get(f"http://127.0.0.1:{port}/health")
        except httpx.HTTPError:
            return False
        return r.status_code == 200 and r.json().get("module") == module

    await _until_async(ok, timeout=40.0)


async def _create_broker_schema(url: str) -> None:
    """Create the broker tables before the workers start.

    Uses the production ``DatabaseBroker`` engine path (so SQLite gets WAL +
    busy_timeout applied to the file from the first connection), mirroring a
    real deployment that runs the ``0002`` migration before booting workers.
    Doing it here — rather than relying on each worker's lazy ``_ensure_schema``
    — removes any concurrent-``CREATE TABLE`` race between the two processes.
    """
    broker = DatabaseBroker(url=url)
    await broker._ensure_schema()
    await broker.close()


async def _drop_broker_schema(url: str) -> None:
    """Drop the broker tables so a shared (session-scoped) server DB is left
    clean for the next test. A no-op-safe teardown for the throwaway SQLite
    file, but essential for the shared Postgres container."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    metadata, _, _ = broker_schema()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
    finally:
        await engine.dispose()


async def _assert_cross_process_delivery(tmp_path: Path, broker_url: str) -> None:
    """Spin up the full two-worker fleet on ``broker_url`` and assert an order
    placed through the proxy is delivered to the inventory worker's listener
    across the process boundary via the database."""
    _write_app(tmp_path)
    await _create_broker_schema(broker_url)

    orders_port = _free_port()
    inventory_port = _free_port()

    worker_env = {
        "MODULITH_BROKER": "database",
        "MODULITH_BROKER_URL": broker_url,
        # Snappy delivery: poll every 100ms instead of the 1s default so the
        # test doesn't wait a full second per hop.
        "MODULITH_BROKER_POLL_INTERVAL_MS": "100",
        "PYTHONPATH": f"{tmp_path}{os.pathsep}{_REPO_ROOT}",
    }
    specs = [
        WorkerSpec(module_name="orders", package="shopapp", port=orders_port, env=dict(worker_env)),
        WorkerSpec(
            module_name="inventory", package="shopapp", port=inventory_port, env=dict(worker_env)
        ),
    ]

    supervisor = Supervisor(specs)
    await supervisor.start()
    try:
        # Workers come up as real uvicorn processes; wait for both to be ready.
        # inventory being healthy means its lifespan startup finished → its
        # DatabaseConsumer has subscribed → a subscription row exists before we
        # place the order below.
        async with httpx.AsyncClient(timeout=5.0) as direct:
            await _wait_healthy(direct, orders_port, "orders")
            await _wait_healthy(direct, inventory_port, "inventory")

        proxy_app = create_proxy_app(_rules_from_specs(specs))
        transport = httpx.ASGITransport(app=proxy_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://proxy", timeout=10.0
        ) as client:
            # The proxy knows both backends.
            topo = await client.get("/_modulith/topology")
            prefixes = {r["prefix"] for r in topo.json()["routes"]}
            assert prefixes == {"/orders", "/inventory"}

            # Path-prefix routing reaches the right worker (empty to start).
            before = await client.get("/inventory/received")
            assert before.status_code == 200
            assert before.json()["received"] == []

            # Place an order through the proxy → orders worker publishes the
            # event, which the runtime routes to the database broker.
            placed = await client.post("/orders/place", params={"order_id": "o-42"})
            assert placed.status_code == 200
            assert placed.json() == {"order_id": "o-42"}

            # The event crosses the process boundary THROUGH THE DATABASE and
            # fires the listener inside the inventory worker.
            async def delivered() -> bool:
                resp = await client.get("/inventory/received")
                return resp.status_code == 200 and "o-42" in resp.json()["received"]

            await _until_async(delivered, timeout=20.0)
    finally:
        await supervisor.stop()
        await _drop_broker_schema(broker_url)

    # After shutdown the worker processes are gone.
    async with httpx.AsyncClient(timeout=2.0) as direct:
        with pytest.raises(httpx.HTTPError):
            await direct.get(f"http://127.0.0.1:{orders_port}/health")


async def test_two_real_workers_deliver_cross_process_event_over_postgres(
    tmp_path: Path, postgres_url: str
) -> None:
    """The production durable path: two real workers deliver a cross-process
    event over a real Postgres server (connection pool + SKIP LOCKED claims)."""
    await _assert_cross_process_delivery(tmp_path, postgres_url)


async def test_two_real_workers_deliver_cross_process_event_over_sqlite_file(
    tmp_path: Path,
) -> None:
    """The zero-infrastructure path: two real workers deliver a cross-process
    event through a single embedded SQLite *file* shared between them — no
    Redis, no server, no Docker. This is the "easy bootstrap" the design sells."""
    db_path = tmp_path / "broker.db"
    # Absolute path → four slashes after the scheme (``sqlite+aiosqlite:////``).
    await _assert_cross_process_delivery(tmp_path, f"sqlite+aiosqlite:///{db_path}")
