"""Per-module API docs are reachable through the public reverse-proxy port.

``tests/test_worker.py`` pins the worker-side URLs with a TestClient. That
proves the worker serves ``/<module>/docs`` and ``/<module>/openapi.json``, but
not that they survive the hop that motivated moving them: the supervisor's
reverse proxy forwards ``/<module>/*`` to the worker and answers 404 for
everything else, so FastAPI's app-root defaults were unreachable from outside.

This test runs the real thing — a real uvicorn worker subprocess spawned by the
real ``Supervisor``, and the real proxy bound to a real loopback socket — and
fetches the doc URLs over TCP from the public port, which is the only place the
question is actually decided. Real subprocesses, no external service (the
default SHM broker keeps its files under ``tmp_path``), hence ``real_process``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from textwrap import dedent

import httpx
import pytest
from fastapi import FastAPI

import modulith
from modulith.proxy import create_proxy_app
from modulith.supervisor import Supervisor, WorkerSpec, _rules_from_specs

from conftest import _free_port

pytestmark = [pytest.mark.real_process]

# Repo root (the directory containing the ``modulith`` package) so the worker
# subprocess can import modulith regardless of its cwd.
_REPO_ROOT = Path(modulith.__file__).resolve().parent.parent


def _write_app(root: Path) -> None:
    """Write a one-module app whose router contributes a documented route."""
    pkg = root / "docsapp"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "orders").mkdir()
    (pkg / "orders" / "__init__.py").write_text(
        dedent(
            """
            from fastapi import APIRouter

            router = APIRouter()

            @router.get("/ping")
            async def ping() -> dict:
                return {"pong": True}
            """
        )
    )


async def _serve_over_socket(app: FastAPI, port: int) -> asyncio.Task[None]:
    """Bind ``app`` to a real loopback socket via uvicorn; block until reachable."""
    import uvicorn

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    async with httpx.AsyncClient() as probe:
        for _ in range(300):
            try:
                await probe.get(f"http://127.0.0.1:{port}/_modulith/live", timeout=1.0)
                return task
            except httpx.TransportError:
                assert not task.done(), f"proxy on port {port} died: {task.exception()!r}"
                await asyncio.sleep(0.05)
    raise AssertionError(f"proxy on port {port} never came up")


async def _wait_healthy(port: int) -> None:
    async with httpx.AsyncClient(timeout=5.0) as direct:
        for _ in range(400):
            with contextlib.suppress(httpx.HTTPError):
                response = await direct.get(f"http://127.0.0.1:{port}/health")
                if response.status_code == 200:
                    return
            await asyncio.sleep(0.1)
    raise AssertionError(f"worker on port {port} never became healthy")


async def test_module_docs_are_reachable_through_the_public_proxy_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_app(tmp_path)
    state_dir = (tmp_path / "state").resolve()
    # Ambient broker settings outrank WorkerSpec.env; drop them so this worker
    # uses the default SHM broker with its files under tmp_path.
    for key in (
        "MODULITH_BROKER",
        "MODULITH_BROKER_URL",
        "MODULITH_BROKER_DSN",
        "MODULITH_BROKER_STATE_DIR",
        "MODULITH_BROKER_SQLITE_PATH",
        "MODULITH_BROKER_HINT_PATH",
    ):
        monkeypatch.delenv(key, raising=False)

    worker_port = _free_port()
    proxy_port = _free_port()
    specs = [
        WorkerSpec(
            module_name="orders",
            package="docsapp",
            port=worker_port,
            env={
                "MODULITH_BROKER_STATE_DIR": str(state_dir),
                "MODULITH_BROKER_SQLITE_PATH": str(state_dir / "broker.db"),
                "MODULITH_BROKER_HINT_PATH": str(state_dir / "broker.hints"),
                "PYTHONPATH": f"{tmp_path}{os.pathsep}{_REPO_ROOT}",
            },
        )
    ]

    supervisor = Supervisor(specs, shutdown_timeout=5.0)
    proxy_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(supervisor.start(), timeout=30.0)
        await asyncio.wait_for(_wait_healthy(worker_port), timeout=60.0)
        proxy_task = await _serve_over_socket(
            create_proxy_app(_rules_from_specs(specs)), proxy_port
        )

        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{proxy_port}", timeout=10.0
        ) as client:
            schema = await client.get("/orders/openapi.json")
            statuses = {
                path: (await client.get(path)).status_code
                for path in (
                    "/orders/docs",
                    "/orders/redoc",
                    "/orders/docs/oauth2-redirect",
                    "/docs",
                    "/openapi.json",
                    "/_modulith/live",
                    "/_modulith/topology",
                    "/_modulith/health",
                )
            }
            swagger = (await client.get("/orders/docs")).text

        assert schema.status_code == 200
        # The schema a client generator downloads must describe the paths as the
        # public port exposes them, prefix included.
        assert "/orders/ping" in schema.json()["paths"]
        assert statuses == {
            "/orders/docs": 200,
            "/orders/redoc": 200,
            "/orders/docs/oauth2-redirect": 200,
            # The proxy owns no doc routes; the app-root defaults have no worker.
            "/docs": 404,
            "/openapi.json": 404,
            # Moving the docs under a module prefix must not disturb the
            # actuator, which the proxy keeps for itself.
            "/_modulith/live": 200,
            "/_modulith/topology": 200,
            "/_modulith/health": 200,
        }
        # Swagger UI is served by the browser hitting the public port, so the
        # URLs baked into the page have to resolve there too.
        assert "url: '/orders/openapi.json'" in swagger
        assert "'/orders/docs/oauth2-redirect'" in swagger
    finally:
        if proxy_task is not None:
            proxy_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await proxy_task
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(supervisor.stop(), timeout=15.0)
        for process in supervisor._processes.values():
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
