"""Tests for the process-per-module reverse proxy.

``create_proxy_app`` builds a FastAPI app that forwards requests to per-module
worker backends by URL prefix, plus ``/_modulith/*`` actuator endpoints. To
exercise real forwarding without opening sockets, the proxy's ``httpx`` client
is injected with an ``ASGITransport`` pointed at an in-process upstream app —
so the proxy genuinely builds the upstream URL, filters headers, and streams
the response, all in-process.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from modulith.proxy import RoutingRule, _match_rule, create_proxy_app

# ---------------------------------------------------------------------------
# _match_rule (pure)
# ---------------------------------------------------------------------------


def test_match_rule_longest_prefix_wins() -> None:
    rules = [
        RoutingRule("/orders", "http://a"),
        RoutingRule("/orders/internal", "http://b"),
    ]
    assert _match_rule("/orders/internal/x", rules).backend_url == "http://b"
    assert _match_rule("/orders/x", rules).backend_url == "http://a"


def test_match_rule_exact_and_trailing_slash() -> None:
    rules = [RoutingRule("/orders", "http://a")]
    assert _match_rule("/orders", rules).backend_url == "http://a"
    assert _match_rule("/orders/", rules).backend_url == "http://a"


def test_match_rule_no_match_returns_none() -> None:
    rules = [RoutingRule("/orders", "http://a")]
    assert _match_rule("/inventory/x", rules) is None
    # prefix must align on a path boundary, not a substring
    assert _match_rule("/ordersX", rules) is None


# ---------------------------------------------------------------------------
# Proxying against an in-process upstream
# ---------------------------------------------------------------------------


def _upstream_app() -> FastAPI:
    up = FastAPI()

    @up.get("/orders/ping")
    async def ping() -> dict[str, bool]:
        return {"pong": True}

    @up.post("/orders/echo")
    async def echo(body: dict) -> dict:
        return {"got": body}

    @up.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @up.get("/orders/host")
    async def host_header(request: Request) -> dict[str, str]:
        return {"host": request.headers.get("host", "")}

    return up


@pytest.fixture
def proxy_app() -> FastAPI:
    upstream = _upstream_app()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream))
    rules = [RoutingRule(prefix="/orders", backend_url="http://orders-worker")]
    return create_proxy_app(rules, client=client)


def test_proxy_forwards_get_to_backend(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/orders/ping")
    assert resp.status_code == 200
    assert resp.json() == {"pong": True}


def test_proxy_forwards_post_body(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.post("/orders/echo", json={"x": 1})
    assert resp.status_code == 200
    assert resp.json() == {"got": {"x": 1}}


def test_proxy_returns_404_for_unmatched_path(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/inventory/thing")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Actuator endpoints
# ---------------------------------------------------------------------------


def test_topology_actuator_lists_routes(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/_modulith/topology")
    assert resp.status_code == 200
    routes = resp.json()["routes"]
    assert {"prefix": "/orders", "backend": "http://orders-worker"} in routes


def test_health_actuator_reports_backend_status(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/_modulith/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["backends"]["/orders"] == "ok"


def test_health_actuator_flags_unreachable_backend() -> None:
    # No ASGITransport wired to this backend → connection fails → unreachable.
    client = httpx.AsyncClient()
    rules = [RoutingRule("/orders", "http://127.0.0.1:59999")]
    app = create_proxy_app(rules, client=client)
    with TestClient(app) as test_client:
        resp = test_client.get("/_modulith/health")
    assert resp.json()["status"] == "degraded"
    assert resp.json()["backends"]["/orders"] in {"unreachable", "unhealthy"}


# ---------------------------------------------------------------------------
# regression: Host rewrite + broadened transport-error mapping (audit)
# ---------------------------------------------------------------------------


def test_proxy_rewrites_host_to_upstream_authority(proxy_app) -> None:
    # The proxy must NOT forward the client's external Host to a loopback
    # worker — httpx should set it to the upstream authority instead.
    with TestClient(proxy_app) as client:
        resp = client.get("/orders/host", headers={"host": "api.example.com"})
    assert resp.status_code == 200
    assert resp.json()["host"] == "orders-worker"
    assert resp.json()["host"] != "api.example.com"


class _FailingClient:
    """httpx-shaped client whose send() raises a chosen TransportError."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def build_request(self, *, method, url, headers=None, content=None):
        return httpx.Request(method, url, headers=headers, content=content)

    async def send(self, request, *, stream: bool = False):
        raise self._exc

    async def aclose(self) -> None:
        pass


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("refused"),
        httpx.ConnectTimeout("slow connect"),
        httpx.ReadError("worker died mid-handshake"),
        httpx.RemoteProtocolError("malformed response"),
    ],
)
def test_proxy_maps_all_transport_errors_to_502(exc: Exception) -> None:
    # ConnectError used to be the only caught case; reachable-but-unresponsive
    # or mid-handshake-death backends leaked as 500. All TransportError → 502.
    app = create_proxy_app([RoutingRule("/orders", "http://orders-worker")], client=_FailingClient(exc))
    with TestClient(app) as client:
        resp = client.get("/orders/ping")
    assert resp.status_code == 502
    assert resp.json()["detail"] == "backend unreachable"
