"""Process-per-module topology end-to-end: real workers, proxy, and Redis.

Everything below the supervisor is tested in isolation elsewhere — ``create_app``
with a TestClient (test_worker.py), the proxy with an in-process backend
(test_proxy.py), the supervisor with ``sleep``/``exit`` dummies
(test_supervisor.py). Nothing assembled the full stack. This test does:

  * the real ``Supervisor`` spawns two real ``uvicorn`` worker subprocesses
    (``modulith._worker:create_app``), one per module;
  * the reverse proxy routes real HTTP to the correct worker by path prefix;
  * placing an order through the proxy publishes an event that crosses a real
    process boundary over real Redis and fires a listener in the *other* worker;
  * ``Supervisor.stop()`` tears the fleet down.

Provisioned by ``redis_url``/``redis_client`` (testcontainers Redis); skipped
without Docker. Slow (spawns real processes) — hence ``@pytest.mark.integration``.
"""

from __future__ import annotations

import asyncio
import os
import socket
from pathlib import Path
from textwrap import dedent

import httpx
import pytest

import modulith
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
    """Write a minimal 2-module shop app (orders publishes, inventory listens)."""
    pkg = root / "shopapp"
    (pkg).mkdir()
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


async def _until_async(coro_predicate, *, timeout: float = 30.0, interval: float = 0.2) -> None:
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


async def test_two_real_workers_route_http_and_deliver_cross_process_event(
    tmp_path, redis_url, redis_client
) -> None:
    _write_app(tmp_path)
    orders_port = _free_port()
    inventory_port = _free_port()

    worker_env = {
        "MODULITH_BROKER": "redis-streams",
        "REDIS_URL": redis_url,
        "MODULITH_STREAM_PREFIX": "modulith.topo",
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

            # Path-prefix routing reaches the right worker: /inventory/received is
            # served by the inventory worker (empty to start).
            before = await client.get("/inventory/received")
            assert before.status_code == 200
            assert before.json()["received"] == []

            # Place an order through the proxy → orders worker publishes the event.
            placed = await client.post("/orders/place", params={"order_id": "o-42"})
            assert placed.status_code == 200
            assert placed.json() == {"order_id": "o-42"}

            # The event crosses the process boundary over real Redis and fires the
            # listener inside the inventory worker.
            async def delivered() -> bool:
                resp = await client.get("/inventory/received")
                return resp.status_code == 200 and "o-42" in resp.json()["received"]

            await _until_async(delivered, timeout=20.0)
    finally:
        await supervisor.stop()

    # After shutdown the worker processes are gone.
    async with httpx.AsyncClient(timeout=2.0) as direct:
        with pytest.raises(httpx.HTTPError):
            await direct.get(f"http://127.0.0.1:{orders_port}/health")
