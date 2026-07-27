"""The process-per-module mount contract for the demo shop's routers.

``modulith._worker.create_app`` mounts ``getattr(module_package, "router")``
under ``/<module_name>`` — it never looks inside the package for a router that
lives one level down. A module whose router is reachable only as
``shop.orders.api.router`` therefore starts healthy and serves 404 on every
route in ``--topology processes``, which is why ``shop/orders/__init__.py``
re-exports it.

This reproduces that mount (without spawning uvicorn) for all three modules and
asserts the URLs the demo's README advertises, so the re-export cannot be
dropped silently. The full process-topology run — real worker subprocesses, real
broker, real proxy — is covered by ``tests/test_demo_app_topology.py``.
"""

from __future__ import annotations

import importlib

import httpx
import pytest
from fastapi import FastAPI

MODULES = ("orders", "inventory", "notifications")


def _worker_style_app(module_name: str) -> FastAPI:
    """Build the single-module app a worker process would serve."""
    module = importlib.import_module(f"shop.{module_name}")
    app = FastAPI()
    router = getattr(module, "router", None)
    if router is not None:
        app.include_router(router, prefix=f"/{module_name}")
    return app


@pytest.mark.parametrize("module_name", MODULES)
async def test_module_package_exposes_router(module_name: str) -> None:
    """Every demo module package exposes ``router`` for the worker to mount."""
    module = importlib.import_module(f"shop.{module_name}")

    assert hasattr(module, "router"), (
        f"shop.{module_name} must expose a 'router' attribute — the "
        "process-per-module worker mounts the module PACKAGE's router and "
        "cannot see one defined only in a submodule"
    )


async def test_orders_worker_serves_post_orders() -> None:
    """The orders worker's mount yields ``POST /orders`` — the README's URL."""
    transport = httpx.ASGITransport(app=_worker_style_app("orders"))
    async with httpx.AsyncClient(transport=transport, base_url="http://worker") as client:
        response = await client.post("/orders", json={"customer_id": "c-1", "total": 19.99})

    assert response.status_code == 200, response.text
    assert response.json()["order_id"]


@pytest.mark.parametrize(
    ("module_name", "path", "key"),
    [
        ("inventory", "/inventory/reserved", "reserved"),
        ("notifications", "/notifications/sent", "sent"),
    ],
)
async def test_inspection_workers_serve_their_routes(module_name: str, path: str, key: str) -> None:
    """The inspection routes the README curls in process mode are mounted."""
    transport = httpx.ASGITransport(app=_worker_style_app(module_name))
    async with httpx.AsyncClient(transport=transport, base_url="http://worker") as client:
        response = await client.get(path)

    assert response.status_code == 200, response.text
    assert response.json() == {key: []}
