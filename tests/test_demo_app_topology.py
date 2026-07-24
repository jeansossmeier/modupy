"""Process-per-module topology end-to-end against the REAL demo app.

Drives the real ``examples/demo_app/shop`` package (not a throwaway app) over
both cross-process broker backends the demo documents:

* the zero-infrastructure **SQLite database broker** (no Redis, no server, no
  Docker beyond the throwaway file), and
* **Redis Streams** (via a testcontainers Redis).

In each case the real ``Supervisor`` spawns three real ``uvicorn`` worker
subprocesses (one per module — ``orders``, ``inventory``, ``notifications``),
the reverse proxy routes real HTTP to the right worker by path prefix, and an
order placed through the proxy triggers a two-hop chain of events that each
cross a real process boundary *through the broker*:

  1. ``orders`` publishes ``OrderPlaced`` (externalized) -> the ``inventory``
     worker's listener consumes it across the process boundary and publishes
     ``StockReserved`` (externalized).
  2. ``inventory`` publishes ``StockReserved`` -> the ``notifications``
     worker's listener consumes it across ANOTHER process boundary.

In this topology the supervisor runs ``modulith._worker:create_app`` per
module — NOT ``shop.main:app`` — so the demo's durable outbox lifespan never
runs here. ``shop.orders.api.get_session`` reads ``request.app.state.sessionmaker``,
which is unset on the worker app, so it yields ``None`` and
``shop.orders.place_order`` takes the in-memory publish path, which the
runtime routes to the broker because topology is ``"processes"``. These tests
exercise pure broker cross-process delivery, with no outbox involved.

Spawns real uvicorn subprocesses (slow) -> ``@pytest.mark.integration``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

import httpx
import pytest

import modulith
from modulith.adapters.db_broker import DatabaseBroker, broker_schema
from modulith.proxy import create_proxy_app
from modulith.supervisor import Supervisor, WorkerSpec, _rules_from_specs
from conftest import _free_port

pytestmark = [pytest.mark.integration]

# Repo root (the directory containing the ``modulith`` package) so the worker
# subprocesses can import ``modulith`` regardless of their cwd.
_REPO_ROOT = Path(modulith.__file__).resolve().parent.parent

# The real demo app lives at examples/demo_app/shop; its parent directory is
# what needs to be on the worker subprocesses' PYTHONPATH so ``import shop``
# resolves.
DEMO_ROOT = Path(modulith.__file__).resolve().parent.parent / "examples" / "demo_app"


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
    busy_timeout applied to the file from the first connection). Doing it here
    — rather than relying on each worker's lazy ``_ensure_schema`` — removes
    any concurrent-``CREATE TABLE`` race between the three worker processes.
    """
    broker = DatabaseBroker(url=url)
    await broker._ensure_schema()
    await broker.close()


async def _drop_broker_schema(url: str) -> None:
    """Drop the broker tables so the throwaway SQLite file's schema doesn't
    linger (defensive; the file itself is removed with ``tmp_path``)."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    metadata, _, _ = broker_schema()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
    finally:
        await engine.dispose()


def _demo_specs(worker_env: dict[str, str]) -> list[WorkerSpec]:
    """Build the three real demo-shop worker specs sharing one broker env.

    Each worker subprocess needs both the real demo package (``shop``) and
    ``modulith`` itself on its ``PYTHONPATH``.
    """
    env = dict(worker_env)
    env["PYTHONPATH"] = f"{DEMO_ROOT}{os.pathsep}{_REPO_ROOT}"
    return [
        WorkerSpec(module_name="orders", package="shop", port=_free_port(), env=dict(env)),
        WorkerSpec(module_name="inventory", package="shop", port=_free_port(), env=dict(env)),
        WorkerSpec(module_name="notifications", package="shop", port=_free_port(), env=dict(env)),
    ]


async def _assert_two_hop_over_proxy(specs: list[WorkerSpec]) -> None:
    """Start the three real workers, place an order through the proxy, and assert
    the two-hop ``orders -> inventory -> notifications`` chain crosses both
    process boundaries through whatever broker ``specs`` was configured with."""
    orders_port, inventory_port, notifications_port = (s.port for s in specs)

    supervisor = Supervisor(specs)
    await supervisor.start()
    try:
        # Workers come up as real uvicorn processes; wait for all three to be
        # ready. inventory/notifications being healthy means their lifespan
        # startup finished -> their consumers have subscribed -> a subscription
        # exists before orders ever publishes.
        async with httpx.AsyncClient(timeout=5.0) as direct:
            await _wait_healthy(direct, orders_port, "orders")
            await _wait_healthy(direct, inventory_port, "inventory")
            await _wait_healthy(direct, notifications_port, "notifications")

        proxy_app = create_proxy_app(_rules_from_specs(specs))
        transport = httpx.ASGITransport(app=proxy_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://proxy", timeout=10.0
        ) as client:
            # Both downstream inspection endpoints start empty.
            before_reserved = await client.get("/inventory/reserved")
            assert before_reserved.status_code == 200
            assert before_reserved.json()["reserved"] == []

            before_sent = await client.get("/notifications/sent")
            assert before_sent.status_code == 200
            assert before_sent.json()["sent"] == []

            # Place an order through the proxy -> orders worker publishes
            # OrderPlaced, which the runtime routes to the broker
            # (externalized + processes topology).
            placed = await client.post("/orders", json={"customer_id": "c-1", "total": 19.99})
            assert placed.status_code == 200
            order_id = placed.json()["order_id"]
            assert isinstance(order_id, str) and order_id

            # Hop 1: OrderPlaced crosses the process boundary THROUGH THE BROKER
            # and fires inventory's listener, which reserves stock.
            async def reserved_hop() -> bool:
                resp = await client.get("/inventory/reserved")
                return resp.status_code == 200 and order_id in resp.json()["reserved"]

            await _until_async(reserved_hop, timeout=20.0)

            # Hop 2: inventory's StockReserved crosses ANOTHER process boundary
            # through the broker and fires notifications' listener.
            async def sent_hop() -> bool:
                resp = await client.get("/notifications/sent")
                return resp.status_code == 200 and order_id in resp.json()["sent"]

            await _until_async(sent_hop, timeout=20.0)
    finally:
        await supervisor.stop()

    # After shutdown the worker processes are gone.
    async with httpx.AsyncClient(timeout=2.0) as direct:
        with pytest.raises(httpx.HTTPError):
            await direct.get(f"http://127.0.0.1:{orders_port}/health")


async def test_demo_app_two_hop_cross_process_delivery_over_sqlite_broker(
    tmp_path: Path,
) -> None:
    """The real demo shop's three modules, each in its own process, deliver a
    two-hop event chain (orders -> inventory -> notifications) through the
    zero-infrastructure SQLite database broker — no Redis, no server, no
    Docker."""
    db_path = tmp_path / "demo-broker.db"
    # Absolute path -> four slashes after the scheme (``sqlite+aiosqlite:////``).
    broker_url = f"sqlite+aiosqlite:///{db_path}"

    await _create_broker_schema(broker_url)
    specs = _demo_specs(
        {
            "MODULITH_BROKER": "database",
            "MODULITH_BROKER_URL": broker_url,
            # Snappy delivery: poll every 100ms instead of the 1s default so the
            # test doesn't wait a full second per hop.
            "MODULITH_BROKER_POLL_INTERVAL_MS": "100",
        }
    )
    try:
        await _assert_two_hop_over_proxy(specs)
    finally:
        await _drop_broker_schema(broker_url)


async def test_demo_app_worker_routes_are_isolated_per_module(tmp_path: Path) -> None:
    """Each worker mounts ONLY its own module's router under its own prefix.

    ``modulith._worker.create_app`` imports a single module and mounts its
    ``router`` under ``/<module_name>`` — it must not accidentally expose
    another module's routes (e.g. the orders worker serving
    ``/inventory/reserved``). This asserts that isolation directly against
    each real worker process, plus the ``/health`` readiness fields
    (``ready``/``status``) that ``_wait_healthy`` above doesn't inspect.
    """
    db_path = tmp_path / "demo-broker-isolation.db"
    broker_url = f"sqlite+aiosqlite:///{db_path}"

    await _create_broker_schema(broker_url)
    specs = _demo_specs(
        {
            "MODULITH_BROKER": "database",
            "MODULITH_BROKER_URL": broker_url,
            "MODULITH_BROKER_POLL_INTERVAL_MS": "100",
        }
    )
    orders_port, inventory_port, notifications_port = (s.port for s in specs)

    supervisor = Supervisor(specs)
    await supervisor.start()
    try:
        async with httpx.AsyncClient(timeout=5.0) as direct:
            await _wait_healthy(direct, orders_port, "orders")
            await _wait_healthy(direct, inventory_port, "inventory")
            await _wait_healthy(direct, notifications_port, "notifications")

            # Health readiness fields: orders has no listeners (no consumer),
            # so it reports the no-consumer shape; inventory/notifications have
            # listeners, so their consumer must report ready=True once healthy.
            orders_health = await direct.get(f"http://127.0.0.1:{orders_port}/health")
            assert orders_health.json() == {"status": "ok", "module": "orders"}

            for port, module in (
                (inventory_port, "inventory"),
                (notifications_port, "notifications"),
            ):
                health = (await direct.get(f"http://127.0.0.1:{port}/health")).json()
                assert health["module"] == module
                assert health["ready"] is True

            # Route isolation: the orders worker must not serve inventory's or
            # notifications' routes, and vice versa — each process hosts only
            # the router of the single module it imported.
            assert (
                await direct.get(f"http://127.0.0.1:{orders_port}/inventory/reserved")
            ).status_code == 404
            assert (
                await direct.get(f"http://127.0.0.1:{orders_port}/notifications/sent")
            ).status_code == 404
            assert (
                await direct.get(f"http://127.0.0.1:{inventory_port}/orders")
            ).status_code == 404
            assert (
                await direct.get(f"http://127.0.0.1:{notifications_port}/inventory/reserved")
            ).status_code == 404

            # Each worker's own route responds correctly on its own process.
            assert (
                await direct.get(f"http://127.0.0.1:{inventory_port}/inventory/reserved")
            ).json() == {"reserved": []}
            assert (
                await direct.get(f"http://127.0.0.1:{notifications_port}/notifications/sent")
            ).json() == {"sent": []}
    finally:
        await supervisor.stop()
        await _drop_broker_schema(broker_url)


async def test_demo_app_two_hop_cross_process_delivery_over_redis_broker(
    redis_url: str,
    redis_client: object,
) -> None:
    """The same real demo shop, same three processes, same two-hop chain — but
    delivered over **Redis Streams** instead of the SQLite broker (the demo's
    ``docker compose up redis`` / ``MODULITH_BROKER=redis-streams`` mode).

    ``redis_client`` is requested only for its flush-before/after hygiene; the
    workers reach Redis themselves via ``REDIS_URL``."""
    specs = _demo_specs(
        {
            "MODULITH_BROKER": "redis-streams",
            "REDIS_URL": redis_url,
            "MODULITH_STREAM_PREFIX": "modulith.demo.topo",
        }
    )
    await _assert_two_hop_over_proxy(specs)
