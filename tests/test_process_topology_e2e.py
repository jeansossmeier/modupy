"""Process-per-module topology end-to-end with real workers and brokers.

Everything below the supervisor is tested in isolation elsewhere — ``create_app``
with a TestClient (test_worker.py), the proxy with an in-process backend
(test_proxy.py), the supervisor with ``sleep``/``exit`` dummies
(test_supervisor.py). Nothing assembled the full stack. This test does:

  * the real ``Supervisor`` spawns two real ``uvicorn`` worker subprocesses
    (``modulith._worker:create_app``), one per module;
  * the reverse proxy routes real HTTP to the correct worker by path prefix;
  * placing an order through the proxy publishes an event that crosses a real
    process boundary and fires a listener in the *other* worker;
  * ``Supervisor.stop()`` tears the fleet down.

The Redis variant uses testcontainers and skips without Docker. The local SHM
variant uses only private files under ``tmp_path``. Both are slow because they
spawn real processes, hence ``@pytest.mark.integration``.
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

from conftest import _free_port

pytestmark = [pytest.mark.integration]

# Repo root (the directory containing the ``modulith`` package) so the worker
# subprocesses can import modulith regardless of their cwd.
_REPO_ROOT = Path(modulith.__file__).resolve().parent.parent


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
            import os

            from fastapi import APIRouter
            from modulith import publish
            from modulith.runtime import _runtime
            from shopapp.contracts import OrderPlaced

            router = APIRouter()

            @router.post("/place")
            async def place(order_id: str) -> dict:
                # No local listener here → the runtime routes this to the broker.
                await publish(OrderPlaced(order_id=order_id))
                return {"order_id": order_id}

            @router.get("/broker-paths")
            async def broker_paths() -> dict:
                registry = _runtime.broker_registry
                assert registry is not None
                broker = registry.get("shm")
                return {
                    "broker": _runtime.config.broker,
                    "state_dir": os.environ["MODULITH_BROKER_STATE_DIR"],
                    "sqlite_path": os.environ["MODULITH_BROKER_SQLITE_PATH"],
                    "hint_path": os.environ["MODULITH_BROKER_HINT_PATH"],
                    "ring_path": str(broker._ring.path),
                }
            """
        )
    )

    (pkg / "inventory").mkdir()
    (pkg / "inventory" / "__init__.py").write_text(
        dedent(
            """
            import os

            from fastapi import APIRouter
            from modulith import listener
            from modulith.runtime import _runtime
            from shopapp.contracts import OrderPlaced

            received: list[str] = []
            router = APIRouter()

            @listener
            async def reserve_stock(evt: OrderPlaced) -> None:
                received.append(evt.order_id)

            @router.get("/received")
            async def get_received() -> dict:
                return {"received": received}

            @router.get("/broker-paths")
            async def broker_paths() -> dict:
                registry = _runtime.broker_registry
                assert registry is not None
                broker = registry.get("shm")
                return {
                    "broker": _runtime.config.broker,
                    "state_dir": os.environ["MODULITH_BROKER_STATE_DIR"],
                    "sqlite_path": os.environ["MODULITH_BROKER_SQLITE_PATH"],
                    "hint_path": os.environ["MODULITH_BROKER_HINT_PATH"],
                    "ring_path": str(broker._ring.path),
                }
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


def _port_is_closed(port: int) -> bool:
    """Return whether no process accepts TCP connections on the worker port."""
    with socket.socket() as probe:
        probe.settimeout(0.1)
        return probe.connect_ex(("127.0.0.1", port)) != 0


async def _stop_and_assert_workers_gone(supervisor: Supervisor, ports: tuple[int, ...]) -> None:
    """Bound supervisor shutdown and reap every process after partial startup."""
    try:
        await asyncio.wait_for(supervisor.stop(), timeout=15.0)
    finally:
        processes = tuple(supervisor._processes.values())

        # stop() normally reaps every process. This fallback also covers a
        # cancelled or partially failed stop without leaving worker ports open.
        for process in processes:
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
        if processes:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(process.wait() for process in processes)),
                    timeout=5.0,
                )
            except TimeoutError:
                for process in processes:
                    if process.returncode is None:
                        try:
                            process.kill()
                        except ProcessLookupError:
                            pass
                await asyncio.wait_for(
                    asyncio.gather(*(process.wait() for process in processes)),
                    timeout=5.0,
                )

        # A failed stop may not reach its task cleanup. Cancel and await the
        # remaining test-owned monitor/log tasks before the event loop closes.
        tasks = [*supervisor._monitor_tasks, *supervisor._log_tasks]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=5.0,
            )

        assert all(process.returncode is not None for process in processes)

        async def all_ports_closed() -> bool:
            return all(_port_is_closed(port) for port in ports)

        await _until_async(all_ports_closed, timeout=5.0, interval=0.05)
        assert all(_port_is_closed(port) for port in ports)


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
    try:
        await supervisor.start()
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
        await _stop_and_assert_workers_gone(supervisor, (orders_port, inventory_port))


@pytest.mark.parametrize("explicit_broker", [False, True], ids=["default-shm", "explicit-shm"])
async def test_two_real_workers_share_parent_resolved_shm_and_deliver_exact_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    explicit_broker: bool,
) -> None:
    """Default and explicit SHM workers must converge on the parent's files."""
    _write_app(tmp_path)
    state_dir = (tmp_path / "state").resolve()
    sqlite_path = (state_dir / "broker.db").resolve()
    hint_path = (state_dir / "broker.hints").resolve()
    order_id = f"shm-{'explicit' if explicit_broker else 'default'}"

    # Ambient broker settings have higher precedence than WorkerSpec.env.
    # Remove them so this test proves the parent-resolved values below.
    for key in (
        "MODULITH_BROKER",
        "MODULITH_BROKER_URL",
        "MODULITH_BROKER_DSN",
        "MODULITH_BROKER_STATE_DIR",
        "MODULITH_BROKER_SQLITE_PATH",
        "MODULITH_BROKER_HINT_PATH",
    ):
        monkeypatch.delenv(key, raising=False)

    worker_env = {
        "MODULITH_BROKER_STATE_DIR": str(state_dir),
        "MODULITH_BROKER_SQLITE_PATH": str(sqlite_path),
        "MODULITH_BROKER_HINT_PATH": str(hint_path),
        "MODULITH_BROKER_POLL_INTERVAL_MS": "10",
        "PYTHONPATH": f"{tmp_path}{os.pathsep}{_REPO_ROOT}",
    }
    if explicit_broker:
        worker_env["MODULITH_BROKER"] = "shm"

    orders_port = _free_port()
    inventory_port = _free_port()
    specs = [
        WorkerSpec(module_name="orders", package="shopapp", port=orders_port, env=dict(worker_env)),
        WorkerSpec(
            module_name="inventory",
            package="shopapp",
            port=inventory_port,
            env=dict(worker_env),
        ),
    ]
    supervisor = Supervisor(specs, shutdown_timeout=5.0)
    try:
        await asyncio.wait_for(supervisor.start(), timeout=10.0)
        async with httpx.AsyncClient(timeout=5.0) as direct:
            await asyncio.wait_for(
                asyncio.gather(
                    _wait_healthy(direct, orders_port, "orders"),
                    _wait_healthy(direct, inventory_port, "inventory"),
                ),
                timeout=40.0,
            )

        proxy_app = create_proxy_app(_rules_from_specs(specs))
        transport = httpx.ASGITransport(app=proxy_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://proxy",
            timeout=10.0,
        ) as client:
            expected_paths = {
                "broker": "shm",
                "state_dir": str(state_dir),
                "sqlite_path": str(sqlite_path),
                "hint_path": str(hint_path),
                "ring_path": str(hint_path),
            }
            orders_paths = await client.get("/orders/broker-paths")
            inventory_paths = await client.get("/inventory/broker-paths")
            assert orders_paths.json() == expected_paths
            assert inventory_paths.json() == expected_paths

            before = await client.get("/inventory/received")
            assert before.json() == {"received": []}
            placed = await client.post("/orders/place", params={"order_id": order_id})
            assert placed.json() == {"order_id": order_id}

            async def delivered_exactly_once() -> bool:
                response = await client.get("/inventory/received")
                return response.status_code == 200 and response.json() == {"received": [order_id]}

            await _until_async(delivered_exactly_once, timeout=20.0)
            assert sqlite_path.is_file()
            assert hint_path.is_file()
    finally:
        await _stop_and_assert_workers_gone(supervisor, (orders_port, inventory_port))
