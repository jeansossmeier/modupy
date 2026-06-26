"""Tests for the per-module worker factory (``modulith._worker.create_app``).

In process-per-module topology the supervisor spawns one uvicorn process per
module, each calling ``create_app()`` via ``--factory``. The defining property
is *selective import*: a worker imports ONLY its configured module (plus the
shared ``contracts`` module) — never its siblings — which is what gives each
worker its own process, import graph, and GIL.

These tests build a fake app on disk and drive ``create_app`` through a real
FastAPI ``TestClient``, asserting the health endpoint, router mounting, the
required-env contract, and the selective-import guarantee. Cross-process broker
routing is a separate concern (covered with the topology/proxy work).
"""

from __future__ import annotations

import sys

import pytest
from fastapi.testclient import TestClient

from modulith._worker import create_app


def _set_worker_env(monkeypatch, module: str, package: str = "fakeapp") -> None:
    monkeypatch.setenv("MODULITH_MODULE", module)
    monkeypatch.setenv("MODULITH_APP_PACKAGE", package)
    # create_app() configures topology="processes", which now (correctly)
    # requires a real cross-process broker — the in-memory default can't carry
    # events between worker processes. These tests exercise only HTTP app
    # construction (no publishing), so a non-memory broker *name* satisfies the
    # config invariant; no adapter claims this scheme, so it stays unregistered
    # and inert (no connection ever attempted). Cross-process broker routing is
    # covered in test_cross_process.py.
    monkeypatch.setenv("MODULITH_BROKER", "test-noop-broker")


# ---------------------------------------------------------------------------
# env contract
# ---------------------------------------------------------------------------


def test_missing_env_raises(monkeypatch) -> None:
    monkeypatch.delenv("MODULITH_MODULE", raising=False)
    monkeypatch.delenv("MODULITH_APP_PACKAGE", raising=False)

    with pytest.raises(RuntimeError, match="MODULITH_MODULE"):
        create_app()


# ---------------------------------------------------------------------------
# health endpoint
# ---------------------------------------------------------------------------


def test_health_endpoint_reports_module(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "module": "orders"}


# ---------------------------------------------------------------------------
# selective import — the core correctness property
# ---------------------------------------------------------------------------


def test_only_configured_module_is_imported(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    _set_worker_env(monkeypatch, "orders")

    create_app()

    assert "fakeapp.orders" in sys.modules
    assert "fakeapp.inventory" not in sys.modules  # sibling NOT imported


# ---------------------------------------------------------------------------
# router mounting
# ---------------------------------------------------------------------------


def test_module_router_is_mounted_under_module_prefix(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {
            "orders": """
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("/ping")
                async def ping() -> dict[str, bool]:
                    return {"pong": True}
            """
        }
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/orders/ping")

    assert resp.status_code == 200
    assert resp.json() == {"pong": True}


# ---------------------------------------------------------------------------
# contracts module import
# ---------------------------------------------------------------------------


def test_contracts_module_imported_when_present(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {"orders": ""},
        extra_files={"contracts/__init__.py": "SHARED = 'event-types-live-here'\n"},
    )
    _set_worker_env(monkeypatch, "orders")

    create_app()

    assert "fakeapp.contracts" in sys.modules


def test_missing_contracts_module_is_tolerated(make_fake_app, monkeypatch) -> None:
    # No contracts package — create_app must not blow up.
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    app = create_app()  # should not raise
    assert app is not None
