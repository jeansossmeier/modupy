"""``own_process`` end-to-end: one module in its own worker, the rest sharing one.

Real ``uvicorn`` workers on the local SHM broker: ``agent`` runs alone, while
``orders`` and ``billing`` share a worker. An order placed through the proxy
reaches billing in-process and the agent across the process boundary, and the
agent's reply crosses back to orders. A note only billing consumes stays in the
shared worker, so the broker's payload cap does not apply to it. Each listener
runs exactly once, and the proxy checks each worker's identity under the name it
runs as.
"""

from __future__ import annotations

import asyncio
import os
import secrets
from pathlib import Path
from textwrap import dedent

import httpx
import pytest

from modulith.proxy import create_proxy_app
from modulith.supervisor import Supervisor, WorkerSpec, _rules_from_specs
from tests.test_process_topology_e2e import (
    _REPO_ROOT,
    _stop_and_assert_workers_gone,
    _until_async,
    _wait_healthy,
)

from conftest import _free_port_block

pytestmark = [pytest.mark.integration]

_MODULES = {
    "contracts": """
        from dataclasses import dataclass
        from modulith import event

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        @event
        @dataclass(frozen=True)
        class OrderTriaged:
            order_id: str
            pid: int

        @event
        @dataclass(frozen=True)
        class OrderNoted:
            order_id: str
            text: str
    """,
    "orders": """
        import os

        from fastapi import APIRouter
        from modulith import listener, publish
        from deskapp.contracts import OrderNoted, OrderPlaced, OrderTriaged

        router = APIRouter()
        triaged: list[dict] = []

        @router.post("/place")
        async def place(order_id: str) -> dict:
            await publish(OrderPlaced(order_id=order_id))
            await publish(OrderNoted(order_id=order_id, text="x" * 2048))
            return {"order_id": order_id, "pid": os.getpid()}

        @router.get("/triaged")
        async def get_triaged() -> dict:
            return {"triaged": triaged}

        @listener
        async def on_triaged(evt: OrderTriaged) -> None:
            triaged.append({"order_id": evt.order_id, "pid": evt.pid})
    """,
    "billing": """
        import os

        from fastapi import APIRouter
        from modulith import listener
        from deskapp.contracts import OrderNoted, OrderPlaced

        router = APIRouter()
        billed: list[dict] = []
        noted: list[str] = []

        @router.get("/billed")
        async def get_billed() -> dict:
            return {"billed": billed, "noted": noted}

        @listener
        async def bill(evt: OrderPlaced) -> None:
            billed.append({"order_id": evt.order_id, "pid": os.getpid()})

        @listener
        async def note(evt: OrderNoted) -> None:
            noted.append(evt.order_id)
    """,
    "agent": """
        import os

        from modulith import listener, publish
        from deskapp.contracts import OrderPlaced, OrderTriaged

        @listener
        async def triage(evt: OrderPlaced) -> None:
            await publish(OrderTriaged(order_id=evt.order_id, pid=os.getpid()))
    """,
}


def _write_app(root: Path) -> None:
    pkg = root / "deskapp"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    for name, source in _MODULES.items():
        (pkg / name).mkdir()
        (pkg / name / "__init__.py").write_text(dedent(source))


async def test_agent_in_its_own_worker_and_the_rest_sharing_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_app(tmp_path)
    for key in (
        "MODULITH_BROKER",
        "MODULITH_BROKER_URL",
        "MODULITH_BROKER_DSN",
        "MODULITH_BROKER_STATE_DIR",
        "MODULITH_BROKER_SQLITE_PATH",
        "MODULITH_BROKER_HINT_PATH",
    ):
        monkeypatch.delenv(key, raising=False)
    state_dir = (tmp_path / "state").resolve()
    token = secrets.token_hex(16)
    worker_env = {
        "MODULITH_BROKER": "shm",
        "MODULITH_BROKER_STATE_DIR": str(state_dir),
        "MODULITH_BROKER_SQLITE_PATH": str(state_dir / "broker.db"),
        "MODULITH_BROKER_HINT_PATH": str(state_dir / "broker.hints"),
        "MODULITH_BROKER_POLL_INTERVAL_MS": "10",
        "MODULITH_BROKER_MAX_PAYLOAD_BYTES": "1024",
        "PYTHONPATH": f"{tmp_path}{os.pathsep}{_REPO_ROOT}",
        "MODULITH_DEPLOYMENT_TOKEN": token,
    }
    agent_port = _free_port_block(2)
    shared_port = agent_port + 1
    specs = [
        WorkerSpec("agent", "deskapp", agent_port, env=dict(worker_env), own_process=True),
        WorkerSpec(
            "billing,orders",
            "deskapp",
            shared_port,
            # What modulith run derives: agent, outside this worker, consumes OrderPlaced.
            env={**worker_env, "MODULITH_REMOTE_EVENT_TYPES": "deskapp.contracts.OrderPlaced"},
        ),
    ]
    supervisor = Supervisor(specs, shutdown_timeout=5.0)
    try:
        await asyncio.wait_for(supervisor.start(), timeout=10.0)
        async with httpx.AsyncClient(timeout=5.0) as direct:
            await asyncio.wait_for(
                asyncio.gather(
                    _wait_healthy(direct, agent_port, "agent"),
                    _wait_healthy(direct, shared_port, "billing,orders"),
                ),
                timeout=40.0,
            )

        proxy_app = create_proxy_app(
            _rules_from_specs(specs),
            deployment_token=token,
            failed_instances=supervisor.failed_instances,
        )
        async with (
            proxy_app.router.lifespan_context(proxy_app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app),
                base_url="http://proxy",
                timeout=10.0,
            ) as client,
        ):
            readiness = await client.get("/_modulith/health")
            placed = await client.post("/orders/place", params={"order_id": "o-1"})
            assert placed.status_code == 200
            shared_pid = placed.json()["pid"]

            async def replied(order_id: str) -> bool:
                resp = await client.get("/orders/triaged")
                return resp.status_code == 200 and any(
                    reply["order_id"] == order_id for reply in resp.json()["triaged"]
                )

            await _until_async(lambda: replied("o-1"), timeout=20.0)
            # A duplicate of o-1 would reach the consumers ahead of o-2, so it has
            # shown up once o-2 has crossed both ways, however slow the machine.
            await client.post("/orders/place", params={"order_id": "o-2"})
            await _until_async(lambda: replied("o-2"), timeout=20.0)
            await asyncio.sleep(0.5)

            billing = (await client.get("/billing/billed")).json()
            replies = (await client.get("/orders/triaged")).json()["triaged"]

        assert (readiness.status_code, readiness.json()) == (
            200,
            {"status": "ok", "backends": {"/agent": "ok", "/billing": "ok", "/orders": "ok"}},
        )
        assert billing["noted"] == ["o-1", "o-2"]
        assert billing["billed"] == [
            {"order_id": "o-1", "pid": shared_pid},
            {"order_id": "o-2", "pid": shared_pid},
        ]
        assert sorted(reply["order_id"] for reply in replies) == ["o-1", "o-2"]
        assert all(reply["pid"] != shared_pid for reply in replies)
    finally:
        await _stop_and_assert_workers_gone(supervisor, (agent_port, shared_port))
