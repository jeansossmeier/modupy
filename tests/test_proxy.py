"""Tests for the process-per-module reverse proxy.

``create_proxy_app`` builds a FastAPI app that forwards requests to per-module
worker backends by URL prefix, plus ``/_modulith/*`` actuator endpoints. To
exercise real forwarding without opening sockets, the proxy's ``httpx`` client
is injected with an ``ASGITransport`` pointed at an in-process upstream app —
so the proxy genuinely builds the upstream URL, filters headers, and streams
the response, all in-process.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import warnings
from collections.abc import MutableMapping
from typing import Any
from urllib.parse import unquote

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.testclient import TestClient

from modulith._worker import NONCE_HEADER, identity_proof
from modulith.proxy import RoutingRule, _match_rule, create_proxy_app

from conftest import _free_port, _health_answer, _held_backend, _serve, _wait_for

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

    @up.get("/orders/xfwd")
    async def xfwd_headers(request: Request) -> dict[str, str]:
        return {
            "x-forwarded-for": request.headers.get("x-forwarded-for", ""),
            "x-forwarded-proto": request.headers.get("x-forwarded-proto", ""),
            "x-forwarded-host": request.headers.get("x-forwarded-host", ""),
            "x-forwarded-port": request.headers.get("x-forwarded-port", ""),
            "forwarded": request.headers.get("forwarded", ""),
            "x-real-ip": request.headers.get("x-real-ip", ""),
        }

    # Registered WITH a trailing slash so requesting it without one triggers
    # Starlette's default redirect_slashes — the absolute-URL redirect a real
    # worker emits against the loopback authority it sees as its Host.
    @up.get("/orders/items/")
    async def items() -> dict[str, bool]:
        return {"items": True}

    @up.get("/orders/offsite")
    async def offsite() -> RedirectResponse:
        return RedirectResponse("https://auth.example.com/login", status_code=302)

    @up.get("/orders/slow")
    async def slow_response() -> dict[str, str]:
        import asyncio

        # Delay >5s to exercise the read=None timeout config:
        # without read=None, httpx would kill this with ReadTimeout.
        await asyncio.sleep(6)
        return {"delayed": "response"}

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


def test_proxy_schema_builds_without_duplicate_operation_ids(proxy_app) -> None:
    """Building the proxy's OpenAPI document must not warn.

    FastAPI assigns one operation id per route but emits one operation per
    method, so a multi-method catch-all left in the schema collides with
    itself and warns on every process-per-module boot — noise a first-time
    user sees before their own logs.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        schema = proxy_app.openapi()
    assert "/{path}" not in schema["paths"]


@pytest.mark.parametrize("path", ["/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"])
def test_proxy_does_not_serve_its_own_docs_routes(proxy_app, path) -> None:
    """The proxy must not answer FastAPI's stock schema/docs paths.

    They would be registered ahead of the catch-all, so a schema describing
    only the proxy's own actuator routes — never the application's — would be
    served at ``/openapi.json``, with a Swagger UI rendered over it at
    ``/docs``. A client generator pointed at the public port would emit an
    empty client and report success. These paths must instead fall through to
    the catch-all and answer like any other unrouted path.
    """
    with TestClient(proxy_app) as client:
        resp = client.get(path)
    assert resp.status_code == 404
    assert resp.json() == {"detail": f"no worker route for {path!r}"}


def test_proxy_does_not_shadow_a_module_named_docs() -> None:
    """A module whose prefix is ``/docs`` must still reach its worker.

    ``/docs`` is an ordinary URL prefix a module is free to own. If the proxy
    registers FastAPI's stock docs UI there, the worker never sees the request
    and the module's routes disappear from the public port with no error
    anywhere.
    """
    upstream = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @upstream.get("/docs")
    async def docs_root() -> dict[str, str]:
        return {"served_by": "worker"}

    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream))
    app = create_proxy_app(
        [RoutingRule(prefix="/docs", backend_url="http://docs-worker")], client=client
    )
    with TestClient(app) as test_client:
        resp = test_client.get("/docs")
    assert resp.status_code == 200
    assert resp.json() == {"served_by": "worker"}


# ---------------------------------------------------------------------------
# Actuator endpoints
# ---------------------------------------------------------------------------


def test_topology_actuator_lists_routes(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/_modulith/topology")
    assert resp.status_code == 200
    routes = resp.json()["routes"]
    order_route = next(r for r in routes if r["prefix"] == "/orders")
    assert order_route["prefix"] == "/orders"
    assert order_route["backend"] == "http://orders-worker"


def test_topology_includes_replicas_list(proxy_app) -> None:
    with TestClient(proxy_app) as client:
        resp = client.get("/_modulith/topology")
    assert resp.status_code == 200
    routes = resp.json()["routes"]
    order_route = next(r for r in routes if r["prefix"] == "/orders")
    assert "replicas" in order_route
    assert order_route["replicas"] == ["http://orders-worker"]


def test_topology_lists_all_replicas_for_scaled_module() -> None:
    upstream = _upstream_app()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream))
    rules = [
        RoutingRule(
            prefix="/orders",
            backend_url="http://orders-1",
            backend_urls=("http://orders-1", "http://orders-2"),
        )
    ]
    app = create_proxy_app(rules, client=client)
    with TestClient(app) as test_client:
        resp = test_client.get("/_modulith/topology")
    assert resp.status_code == 200
    routes = resp.json()["routes"]
    order_route = next(r for r in routes if r["prefix"] == "/orders")
    assert order_route["backend"] == "http://orders-1"
    assert order_route["replicas"] == ["http://orders-1", "http://orders-2"]


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


def test_actuator_token_guards_metadata_endpoints(proxy_app) -> None:
    app = create_proxy_app(
        [RoutingRule(prefix="/orders", backend_url="http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_upstream_app())),
        actuator_token="secret-token",
    )
    with TestClient(app) as client:
        denied = client.get("/_modulith/topology")
        allowed = client.get(
            "/_modulith/topology",
            headers={"authorization": "Bearer secret-token"},
        )

    assert denied.status_code == 401
    assert allowed.status_code == 200


# ---------------------------------------------------------------------------
# regression: Host rewrite + broadened transport-error mapping
# ---------------------------------------------------------------------------


def test_proxy_rewrites_host_to_upstream_authority(proxy_app) -> None:
    # The proxy must NOT forward the client's external Host to a loopback
    # worker — httpx should set it to the upstream authority instead.
    with TestClient(proxy_app) as client:
        resp = client.get("/orders/host", headers={"host": "api.example.com"})
    assert resp.status_code == 200
    assert resp.json()["host"] == "orders-worker"
    assert resp.json()["host"] != "api.example.com"


async def test_proxy_overwrites_spoofed_forwarded_headers() -> None:
    # A client-controlled X-Forwarded-* must never reach a worker verbatim —
    # workers spawned by the supervisor trust it from the loopback proxy.
    upstream = _upstream_app()
    upstream_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream))
    rules = [RoutingRule(prefix="/orders", backend_url="http://orders-worker")]
    proxy_app = create_proxy_app(rules, client=upstream_client)

    transport = httpx.ASGITransport(app=proxy_app, client=("203.0.113.5", 51000))
    async with httpx.AsyncClient(
        transport=transport, base_url="https://public.example.com"
    ) as proxy_client:
        resp = await proxy_client.get(
            "/orders/xfwd",
            headers={
                "x-forwarded-for": "1.2.3.4",
                "x-forwarded-proto": "http",
                "x-forwarded-host": "evil.example.com",
                "x-forwarded-port": "9999",
                "forwarded": "for=9.9.9.9",
                "x-real-ip": "9.9.9.9",
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["x-forwarded-for"] == "203.0.113.5"
    assert body["x-forwarded-proto"] == "https"
    assert body["x-forwarded-host"] == "public.example.com"
    assert body["x-forwarded-port"] == "443"
    assert body["forwarded"] == ""
    assert body["x-real-ip"] == ""


def _recording_proxy(calls: list[httpx.Request]) -> FastAPI:
    """A proxy whose backend client records every request it is asked to send."""

    def record(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, stream=httpx.ByteStream(b"ok"))

    return create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.MockTransport(record)),
        connect_retry_attempts=1,
    )


async def _get_with_host(
    app: Any, path: str, host: str, base_url: str = "http://public.example.com:8080"
) -> httpx.Response:
    """GET ``path`` from a client that reaches ``app`` at ``base_url`` but sends
    ``host`` as its Host header."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=base_url
    ) as client:
        return await client.get(path, headers={"host": host})


@pytest.mark.parametrize(
    ("base_url", "host", "port"),
    [
        ("http://public.example.com:8080", "public.example.com:9999", "8080"),
        ("http://public.example.com:8080", "public.example.com", "8080"),
        ("https://public.example.com:8443", "public.example.com:443", "8443"),
        ("http://public.example.com", "public.example.com:9999", "80"),
        ("https://public.example.com", "public.example.com:9999", "443"),
    ],
)
async def test_x_forwarded_port_is_the_port_the_connection_arrived_on(
    base_url: str, host: str, port: str
) -> None:
    """A worker sees the proxy's listening port, whatever port the client's Host
    header names. httpx's ASGI transport reports no port for a scheme's default
    one, so the last two rows also cover the fallback to that default."""
    calls: list[httpx.Request] = []
    response = await _get_with_host(_recording_proxy(calls), "/orders/ping", host, base_url)
    assert response.status_code == 200
    assert [c.headers["x-forwarded-port"] for c in calls] == [port]
    assert [c.headers["x-forwarded-host"] for c in calls] == [host]


@pytest.mark.parametrize(("scheme", "port"), [("http", "80"), ("https", "443")])
@pytest.mark.parametrize("server", [None, ("/run/modulith.sock", None)])
async def test_x_forwarded_port_defaults_to_the_scheme_port_on_a_unix_socket(
    server: tuple[str, None] | None, scheme: str, port: str
) -> None:
    """A Unix socket has no TCP port: the ASGI scope omits ``server`` or gives
    ``(path, None)``. The port falls back to the scheme's default, never to the
    one in the Host header."""
    calls: list[httpx.Request] = []
    proxy = _recording_proxy(calls)

    async def on_unix_socket(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        listening = {key: value for key, value in scope.items() if key != "server"}
        if server is not None:
            listening["server"] = server
        await proxy(listening, receive, send)

    response = await _get_with_host(
        on_unix_socket, "/orders/ping", "public.example.com:9999", f"{scheme}://public.example.com"
    )
    assert response.status_code == 200
    assert [c.headers["x-forwarded-port"] for c in calls] == [port]


@pytest.mark.parametrize(
    "host",
    [
        "example.com:99999",
        "example.com:65536",
        "example.com:abc",
        "example.com:8080x",
        "example.com:-1",
        "[::1]:99999",
        "[::1]:abc",
        "[::1",
    ],
)
async def test_a_host_header_with_a_bad_port_answers_400_and_reaches_no_backend(host: str) -> None:
    """The request is malformed: answering it (500 for an out-of-range port) or
    forwarding the value to a worker would both be wrong."""
    calls: list[httpx.Request] = []
    response = await _get_with_host(_recording_proxy(calls), "/orders/ping", host)
    assert response.status_code == 400
    assert response.json() == {"detail": "invalid Host header"}
    assert calls == []


@pytest.mark.parametrize("path", ["/orders/ping", "/no-such-module", "/_modulith/live"])
async def test_a_malformed_host_header_answers_400_on_every_route(path: str) -> None:
    calls: list[httpx.Request] = []
    response = await _get_with_host(_recording_proxy(calls), path, "example.com:99999")
    assert response.status_code == 400
    assert calls == []


@pytest.mark.parametrize(
    "host",
    [
        "public.example.com",
        "public.example.com:8080",
        "public.example.com:65535",
        "127.0.0.1:8000",
        "[::1]:8080",
        "[::1]",
        "orders_svc.internal:8080",
    ],
)
async def test_a_well_formed_host_header_is_forwarded_as_given(host: str) -> None:
    calls: list[httpx.Request] = []
    response = await _get_with_host(_recording_proxy(calls), "/orders/ping", host)
    assert response.status_code == 200
    assert [c.headers["x-forwarded-host"] for c in calls] == [host]


def _header_echo_app(seen: list[dict[str, str]]) -> Any:
    async def app(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        seen.append({k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]})
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    return app


@pytest.mark.real_process
async def test_x_forwarded_port_under_uvicorn_is_the_port_the_proxy_listens_on() -> None:
    """End to end over real sockets: uvicorn fills the ASGI ``server`` entry from
    the accepted connection, so a worker sees the proxy's own port however the
    Host header names it, and a malformed Host never reaches the worker."""
    worker_port, proxy_port = _free_port(), _free_port()
    seen: list[dict[str, str]] = []
    proxy = create_proxy_app(
        [RoutingRule("/orders", f"http://127.0.0.1:{worker_port}")],
        actuator_enabled=False,
        connect_retry_attempts=1,
    )
    servers = [
        await _serve(_header_echo_app(seen), worker_port, "h11"),
        await _serve(proxy, proxy_port, "h11"),
    ]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{proxy_port}", trust_env=False
        ) as client:
            named = await client.get("/orders/x", headers={"host": "example.com:9999"})
            malformed = await client.get("/orders/x", headers={"host": "example.com:99999"})
    finally:
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))

    assert named.status_code == 200
    assert malformed.status_code == 400
    assert [headers["x-forwarded-port"] for headers in seen] == [str(proxy_port)]
    assert [headers["x-forwarded-host"] for headers in seen] == ["example.com:9999"]


def test_proxy_makes_a_backend_redirect_client_followable(proxy_app) -> None:
    # Because the client's Host is dropped, the worker builds absolute URLs
    # against its own loopback authority — Starlette's trailing-slash redirect
    # being the common one. Forwarded verbatim, that Location is unfollowable
    # by the client and discloses the internal worker. The proxy forwards the
    # full path, so stripping the backend authority is the correct rewrite.
    with TestClient(proxy_app) as client:
        resp = client.get("/orders/items", follow_redirects=False)

    assert resp.status_code == 307
    assert resp.headers["location"] == "/orders/items/"


def test_proxy_leaves_an_external_redirect_alone(proxy_app) -> None:
    # Only the backend's own authority is stripped — rewriting a redirect to
    # a third party (an OAuth provider, a CDN) would break it.
    with TestClient(proxy_app) as client:
        resp = client.get("/orders/offsite", follow_redirects=False)

    assert resp.status_code == 302
    assert resp.headers["location"] == "https://auth.example.com/login"


class _FailingClient:
    """httpx-shaped client whose send() raises a chosen TransportError."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def build_request(self, *, method, url, headers=None, content=None):
        return httpx.Request(method, url, headers=headers, content=content)

    async def send(self, request, *, stream: bool = False, follow_redirects: bool = True):
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
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=_FailingClient(exc),
        connect_retry_attempts=1,
    )
    with TestClient(app) as client:
        resp = client.get("/orders/ping")
    assert resp.status_code == 502
    assert resp.json()["detail"] == "backend unreachable"


class _BuildRequestFailingClient:
    """httpx-shaped client whose build_request() itself raises — the call path
    the passthrough _FailingClient above structurally never exercises: its
    build_request never raised, so no test could reach the proxy's
    build_request guard."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.send_calls = 0

    def build_request(self, *, method, url, headers=None, content=None):
        raise self._exc

    async def send(self, request, *, stream: bool = False, follow_redirects: bool = True):
        self.send_calls += 1
        raise AssertionError("send() must not be reached when build_request raises")

    async def aclose(self) -> None:
        pass


@pytest.mark.parametrize(
    "exc",
    [
        # httpx.InvalidURL subclasses Exception directly (not TransportError) —
        # e.g. percent-encoded non-printable ASCII in the path.
        httpx.InvalidURL("Invalid non-printable ASCII character in URL"),
        # A header carrying a raw non-ASCII octet raises UnicodeEncodeError.
        UnicodeEncodeError("ascii", "h\xe9ader", 1, 2, "ordinal not in range(128)"),
    ],
)
def test_proxy_maps_build_request_failures_to_400(exc: Exception) -> None:
    """A request the client itself cannot forward is answered 400 — never an
    uncaught 500 — and is never sent upstream."""
    client = _BuildRequestFailingClient(exc)
    app = create_proxy_app([RoutingRule("/orders", "http://orders-worker")], client=client)
    with TestClient(app) as test_client:
        resp = test_client.get("/orders/ping")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "invalid request"
    assert client.send_calls == 0  # failed at build time; nothing was forwarded


def test_proxy_rejects_body_over_limit() -> None:
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_upstream_app())),
        max_request_body_bytes=4,
    )
    with TestClient(app) as client:
        resp = client.post("/orders/echo", content=b"too-large")

    assert resp.status_code == 413


def test_proxy_succeeds_with_slow_upstream() -> None:
    """Verify that upstreams taking >5s to produce the first byte succeed.

    Before the fix, httpx applied its 5-second default read timeout, killing
    slow upstreams with ReadTimeout → 502. The fix sets read=None so streaming
    and slow backends work correctly.
    """
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_upstream_app())),
    )
    with TestClient(app) as client:
        # /orders/slow delays 6s before responding — would fail with default
        # httpx 5s read timeout. With read=None in the timeout config, it should
        # succeed.
        resp = client.get("/orders/slow", timeout=10)  # client-side timeout for test
    assert resp.status_code == 200
    assert resp.json() == {"delayed": "response"}


def test_proxy_hands_a_redirect_loop_to_the_client_instead_of_following_it() -> None:
    """The proxy sends with ``follow_redirects=False``, so even an injected
    redirect-following client never follows a backend's redirect: the client
    gets the 302 and decides, and the backend is contacted once."""
    seen: list[httpx.Request] = []

    def _always_redirect(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            302, headers={"location": str(request.url)}, stream=httpx.ByteStream(b"")
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_always_redirect), follow_redirects=True
    )
    app = create_proxy_app([RoutingRule("/orders", "http://orders-worker")], client=client)

    with TestClient(app) as test_client:
        resp = test_client.get("/orders/ping", follow_redirects=False)

    assert resp.status_code == 302
    assert len(seen) == 1


def test_proxy_keeps_cookie_and_set_cookie_across_a_backend_redirect() -> None:
    """An injected ``follow_redirects=True`` client used to drop ``Cookie`` on
    the followed hop and lose the first hop's ``Set-Cookie``; the redirect now
    reaches the client with the backend's own ``Set-Cookie``."""
    seen: list[httpx.Request] = []

    def redirect(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            302,
            headers={"location": "/orders/b", "set-cookie": "hop1=1; Path=/"},
            stream=httpx.ByteStream(b""),
        )

    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.MockTransport(redirect), follow_redirects=True),
    )
    with TestClient(app) as test_client:
        resp = test_client.get("/orders/a", headers={"cookie": "sid=abc"}, follow_redirects=False)

    assert (resp.status_code, resp.headers["location"], resp.headers["set-cookie"]) == (
        302,
        "/orders/b",
        "hop1=1; Path=/",
    )
    assert [r.headers.get("cookie") for r in seen] == ["sid=abc"]


def test_proxy_transport_error_logs_omit_query_string_secrets(caplog) -> None:
    caplog.set_level("WARNING", logger="modulith.proxy")
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=_FailingClient(httpx.ConnectError("refused")),
        connect_retry_attempts=1,
    )

    with TestClient(app) as client:
        resp = client.get("/orders/ping?token=secret")

    assert resp.status_code == 502
    assert "token=secret" not in caplog.text
    assert "/orders/ping" in caplog.text


_FIRST_REPLICA = "http://orders-a"
_SECOND_REPLICA = "http://orders-b"


def _two_replica_rule() -> RoutingRule:
    rule = RoutingRule("/orders", _FIRST_REPLICA, (_FIRST_REPLICA, _SECOND_REPLICA))
    rule.mark_down(_FIRST_REPLICA)
    return rule


def test_proxy_relativizes_a_redirect_against_the_replica_that_served_it() -> None:
    def replica(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "orders-b"
        return httpx.Response(
            307,
            headers={"location": f"{_SECOND_REPLICA}/orders/items/"},
            stream=httpx.ByteStream(b""),
        )

    app = create_proxy_app(
        [_two_replica_rule()],
        client=httpx.AsyncClient(transport=httpx.MockTransport(replica)),
    )
    with TestClient(app) as client:
        resp = client.get("/orders/items", follow_redirects=False)

    assert resp.status_code == 307
    assert resp.headers["location"] == "/orders/items/"


def test_proxy_logs_a_request_error_against_the_replica_that_served_it(caplog) -> None:
    caplog.set_level("WARNING", logger="modulith.proxy")

    app = create_proxy_app(
        [_two_replica_rule()],
        client=_FailingClient(httpx.DecodingError("bad content encoding")),
    )
    with TestClient(app) as client:
        resp = client.get("/orders/ping")

    assert resp.status_code == 502
    assert f"backend {_SECOND_REPLICA} returned no usable response" in caplog.text
    assert _FIRST_REPLICA not in caplog.text


# ---------------------------------------------------------------------------
# Bounded connect retry (worker startup/respawn bind window)
# ---------------------------------------------------------------------------


class _SuccessResponse:
    """Minimal response-like object for successful async responses."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.status_code = 200
        self.headers = httpx.Headers({"content-type": "application/json"})

    async def aiter_raw(self, chunk_size=None):
        # Yield the data in one chunk
        yield self.data

    async def aclose(self) -> None:
        pass


class _EventuallyBindingClient:
    """httpx-shaped client that raises ConnectError for the first N calls,
    then returns a successful response — simulates a worker that eventually
    starts binding its port."""

    def __init__(self, fail_count: int) -> None:
        self._fail_count = fail_count
        self.send_calls = 0

    def build_request(self, *, method, url, headers=None, content=None):
        return httpx.Request(method, url, headers=headers, content=content)

    async def send(self, request, *, stream: bool = False, follow_redirects: bool = True):
        self.send_calls += 1
        if self.send_calls <= self._fail_count:
            raise httpx.ConnectError("worker port not bound yet")
        # Return a successful response once the worker "binds"
        import json

        return _SuccessResponse(json.dumps({"pong": True}).encode())

    async def aclose(self) -> None:
        pass


def test_proxy_retries_connect_error_until_backend_binds() -> None:
    """Verify that ConnectError is retried and the request succeeds once the
    worker's port becomes available (simulating worker startup/respawn)."""
    client = _EventuallyBindingClient(fail_count=2)
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=client,
        connect_retry_attempts=5,
        connect_retry_backoff=0.0,  # instant for test speed
    )

    with TestClient(app) as test_client:
        resp = test_client.get("/orders/ping")

    # Request succeeded on retry
    assert resp.status_code == 200
    assert resp.json() == {"pong": True}
    # Send was called exactly 3 times: 2 failures + 1 success
    assert client.send_calls == 3


class _AlwaysFailingClient:
    """httpx-shaped client whose send() always raises ConnectError."""

    def __init__(self) -> None:
        self.send_calls = 0

    def build_request(self, *, method, url, headers=None, content=None):
        return httpx.Request(method, url, headers=headers, content=content)

    async def send(self, request, *, stream: bool = False, follow_redirects: bool = True):
        self.send_calls += 1
        raise httpx.ConnectError("worker port never bound")

    async def aclose(self) -> None:
        pass


def test_proxy_exhausts_connect_retry_budget_then_502() -> None:
    """Verify that ConnectError retries are bounded — after exhausting the
    retry budget, the request returns 502 instead of retrying forever."""
    client = _AlwaysFailingClient()
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")],
        client=client,
        connect_retry_attempts=3,
        connect_retry_backoff=0.0,  # instant for test speed
    )

    with TestClient(app) as test_client:
        resp = test_client.get("/orders/ping")

    # Request failed with 502 after budget exhausted
    assert resp.status_code == 502
    assert resp.json()["detail"] == "backend unreachable"
    # Send was called exactly 3 times (the budget limit)
    assert client.send_calls == 3


# ---------------------------------------------------------------------------
# mid-stream backend death — _safe_stream
# ---------------------------------------------------------------------------


class _DyingStream(httpx.AsyncByteStream):
    """Body stream that yields one chunk then dies — models a worker sending
    headers + partial body before its socket hard-closes."""

    async def __aiter__(self):
        yield b"partial-"
        raise httpx.RemoteProtocolError(
            "peer closed connection without sending complete message body"
        )

    async def aclose(self) -> None:
        pass


class _DiesMidStreamClient:
    """httpx-shaped client whose send() SUCCEEDS (status + headers delivered)
    but whose response body iterator raises mid-stream — the distinct code
    path ``_safe_stream`` guards, which ``_FailingClient`` (connect-time
    failure only) structurally cannot reach."""

    def build_request(self, *, method, url, headers=None, content=None):
        return httpx.Request(method, url, headers=headers, content=content)

    async def send(self, request, *, stream: bool = False, follow_redirects: bool = True):
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            stream=_DyingStream(),
            request=request,
        )

    async def aclose(self) -> None:
        pass


def test_proxy_aborts_stream_when_backend_dies_mid_response(caplog) -> None:
    """Once headers are sent, a mid-stream
    TransportError can't become a 502 — but silently ending the stream and
    answering 200 with a partial body (the old behavior) fabricates a
    successful response the client has no way to know is truncated. It must
    instead abort the ASGI response so the connection drops without a valid
    terminator — the only way an HTTP client can detect the truncation."""
    caplog.set_level("WARNING", logger="modulith.proxy")
    app = create_proxy_app(
        [RoutingRule("/orders", "http://orders-worker")], client=_DiesMidStreamClient()
    )

    with TestClient(app) as client, pytest.raises(httpx.RemoteProtocolError):
        client.get("/orders/ping")

    assert "backend stream interrupted" in caplog.text


# ---------------------------------------------------------------------------
# Request-target validation: the upstream authority is always the backend's
# ---------------------------------------------------------------------------

_BACKEND = "http://127.0.0.1:9001"


async def _proxy_raw_target(raw_path: bytes) -> tuple[int, list[httpx.URL]]:
    """Feed ``raw_path`` to the proxy as an ASGI server would; return the
    response status and every URL the proxy's real httpx client tried to send.

    A hand-built scope reaches the app with exactly the bytes an HTTP parser
    may hand it (h11 passes a target that does not start with ``/``), so the
    result does not depend on which parser the deployment happens to run.
    """
    sent: list[httpx.URL] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request.url)
        return httpx.Response(200, stream=httpx.ByteStream(b"ok"))

    app = create_proxy_app(
        [RoutingRule("/orders", _BACKEND)],
        client=httpx.AsyncClient(transport=httpx.MockTransport(record)),
        actuator_enabled=False,
        connect_retry_attempts=1,
    )
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": unquote(raw_path.decode("ascii")),
        "raw_path": raw_path,
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"public.example"), (b"authorization", b"Bearer client-token")],
        "client": ("203.0.113.5", 51000),
        "server": ("public.example", 80),
    }
    messages: list[MutableMapping[str, Any]] = []
    request_sent = False

    async def receive() -> MutableMapping[str, Any]:
        # One request message, then block like a client that stays connected:
        # a streaming response listens for disconnect until its body is done.
        nonlocal request_sent
        if request_sent:
            await asyncio.Event().wait()
        request_sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: MutableMapping[str, Any]) -> None:
        messages.append(message)

    await asyncio.wait_for(app(scope, receive, send), timeout=10.0)
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    return status, sent


@pytest.mark.parametrize(
    "raw_path",
    [
        b"%2Forders%2F@127.0.0.1:1",
        b"%2Forders%2F@127.0.0.1:1/admin",
        b"%2forders%2f@other.example:443/x",
        b"orders/x",
        b"*",
    ],
)
async def test_proxy_rejects_request_target_not_starting_with_slash(raw_path: bytes) -> None:
    """A target that does not start with ``/`` is not an origin-form path. The
    proxy answers 400 and contacts nothing: appended to the backend URL, such a
    target can name a different host and port."""
    status, sent = await _proxy_raw_target(raw_path)
    assert status == 400
    assert sent == []


@pytest.mark.parametrize(
    "raw_path",
    [
        b"/orders/../health",
        b"/orders/./../health",
        b"/orders/%2e%2e/health",
        b"/orders/%2E%2E/health",
        b"/orders/.%2e/health",
        b"/orders%2F..%2Fhealth",
        b"/orders/x/..",
        b"/orders/.",
    ],
)
async def test_proxy_rejects_dot_segments(raw_path: bytes) -> None:
    """A ``.`` or ``..`` segment, literal or percent-encoded, gets 400 before
    any backend is contacted, so ``/<module>/../health`` cannot reach a
    worker's internal ``/health``: the rule matched ``/<module>``, but URL
    normalization would have sent ``/health``."""
    status, sent = await _proxy_raw_target(raw_path)
    assert status == 400
    assert sent == []


@pytest.mark.parametrize(
    "raw_path",
    [
        b"/orders/x",
        b"/orders/a%2Fb",
        b"/orders/@127.0.0.1:1/x",
        b"/orders//127.0.0.1:1/x",
        b"/orders/..x/.y",
        b"/orders/a%0Ab",
        b"/orders/a%0A",
    ],
)
async def test_proxy_forwards_accepted_targets_only_to_the_backend(raw_path: bytes) -> None:
    """Every forwarded request goes to the backend's own host and port with
    the client's exact path bytes, whatever those bytes contain."""
    status, sent = await _proxy_raw_target(raw_path)
    assert status == 200
    assert [(u.scheme, u.host, u.port, u.raw_path) for u in sent] == [
        ("http", "127.0.0.1", 9001, raw_path)
    ]


def _recording_app(seen: list[str]) -> Any:
    async def app(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        seen.append(scope["path"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"reached"})

    return app


async def _raw_request_status(port: int, target: str) -> str:
    """Send ``target`` verbatim on the request line; return the status line."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(
        f"GET {target} HTTP/1.1\r\nHost: public.example\r\n"
        "Authorization: Bearer client-token\r\nConnection: close\r\n\r\n".encode()
    )
    await writer.drain()
    response = await asyncio.wait_for(reader.read(), timeout=10.0)
    writer.close()
    await writer.wait_closed()
    return response.split(b"\r\n", 1)[0].decode("latin-1")


@pytest.mark.real_process
@pytest.mark.parametrize(
    "http",
    [
        "h11",
        pytest.param(
            "httptools",
            marks=pytest.mark.skipif(
                importlib.util.find_spec("httptools") is None,
                reason="httptools is not installed",
            ),
        ),
    ],
)
async def test_proxy_over_real_parser_sends_crafted_targets_nowhere(http: str) -> None:
    """End to end over real sockets, with the proxy on the named uvicorn HTTP
    parser (h11 is what a default install uses): crafted targets reach neither
    the module's worker nor any other listening server."""
    worker_port, other_port, proxy_port = _free_port(), _free_port(), _free_port()
    worker_saw: list[str] = []
    other_saw: list[str] = []
    proxy = create_proxy_app(
        [RoutingRule("/orders", f"http://127.0.0.1:{worker_port}")],
        actuator_enabled=False,
        connect_retry_attempts=1,
    )
    servers = [
        await _serve(_recording_app(worker_saw), worker_port, "h11"),
        await _serve(_recording_app(other_saw), other_port, "h11"),
        await _serve(proxy, proxy_port, http),
    ]
    try:
        crafted = [
            await _raw_request_status(proxy_port, target)
            for target in (
                f"%2Forders%2F@127.0.0.1:{other_port}",
                f"%2Forders%2F@127.0.0.1:{other_port}/admin",
                "/orders/../health",
                "/orders/%2e%2e/health",
            )
        ]
        control = await _raw_request_status(proxy_port, "/orders/x")
    finally:
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))

    assert other_saw == []
    assert crafted == ["HTTP/1.1 400 Bad Request"] * 4
    assert control == "HTTP/1.1 200 OK"
    assert worker_saw == ["/orders/x"]


def _body_recording_app(seen: list[tuple[str, bytes]]) -> Any:
    async def app(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        body = b""
        more = True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)
        seen.append((scope["method"], body))
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"reached"})

    return app


@pytest.mark.real_process
async def test_proxy_forwards_a_request_with_transfer_encoding_and_content_length() -> None:
    """A client sending both headers (the length is ignored per RFC 9112 6.3)
    is forwarded with its decoded body, not answered 500 by the upstream
    request writer rejecting the stale client Content-Length."""
    worker_port, proxy_port = _free_port(), _free_port()
    worker_saw: list[tuple[str, bytes]] = []
    proxy = create_proxy_app(
        [RoutingRule("/orders", f"http://127.0.0.1:{worker_port}")],
        actuator_enabled=False,
        connect_retry_attempts=1,
    )
    servers = [
        await _serve(_body_recording_app(worker_saw), worker_port, "h11"),
        await _serve(proxy, proxy_port, "h11"),
    ]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(
            b"POST /orders/x HTTP/1.1\r\nHost: public.example\r\n"
            b"Transfer-Encoding: chunked\r\nContent-Length: 1\r\nConnection: close\r\n\r\n"
            b"a\r\n0123456789\r\n0\r\n\r\n"
        )
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), timeout=10.0)
        writer.close()
        await writer.wait_closed()
    finally:
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))

    assert response.split(b"\r\n", 1)[0] == b"HTTP/1.1 200 OK"
    assert worker_saw == [("POST", b"0123456789")]


# ---------------------------------------------------------------------------
# Connection pool: capacity, exhaustion, readiness isolation, env proxies
# ---------------------------------------------------------------------------


@pytest.mark.real_process
async def test_proxy_serves_150_concurrent_slow_requests_with_default_pool() -> None:
    backend_port, proxy_port = _free_port(), _free_port()
    release, in_flight = asyncio.Event(), [0]
    rule = RoutingRule("/orders", f"http://127.0.0.1:{backend_port}")
    servers = [
        await _serve(_held_backend(release, in_flight), backend_port, "h11"),
        await _serve(create_proxy_app([rule]), proxy_port, "h11"),
    ]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{proxy_port}",
            timeout=30,
            limits=httpx.Limits(max_connections=None),
        ) as client:
            slow = [asyncio.create_task(client.get("/orders/slow")) for _ in range(150)]
            await _wait_for(lambda: in_flight[0] >= 150, 10.0)
            reached = in_flight[0]
            fast = await client.get("/orders/fast")
            health = await client.get("/_modulith/health")
            down = dict(rule._down)
            release.set()
            slow_codes = [r.status_code for r in await asyncio.gather(*slow)]
    finally:
        release.set()
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))

    assert reached == 150
    assert (fast.status_code, fast.json()) == (200, {"ok": "fast"})
    assert (health.status_code, health.json()["backends"]) == (200, {"/orders": "ok"})
    assert down == {}
    assert slow_codes == [200] * 150


@pytest.mark.real_process
async def test_proxy_answers_503_on_pool_exhaustion_without_marking_backend_down(caplog) -> None:
    caplog.set_level("WARNING", logger="modulith.proxy")
    backend_port, proxy_port = _free_port(), _free_port()
    release, in_flight = asyncio.Event(), [0]
    rule = RoutingRule("/orders", f"http://127.0.0.1:{backend_port}")
    proxy = create_proxy_app(
        [rule], max_connections=1, timeout=httpx.Timeout(5.0, read=None, pool=0.3)
    )
    servers = [
        await _serve(_held_backend(release, in_flight), backend_port, "h11"),
        await _serve(proxy, proxy_port, "h11"),
    ]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{proxy_port}", timeout=30) as c:
            held = asyncio.create_task(c.get("/orders/slow"))
            await _wait_for(lambda: in_flight[0] >= 1, 10.0)
            exhausted = await c.get("/orders/fast")
            down = dict(rule._down)
            health = await c.get("/_modulith/health")
            release.set()
            held_resp = await held
            after = await c.get("/orders/fast")
    finally:
        release.set()
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))

    assert exhausted.status_code == 503
    assert exhausted.json() == {"detail": "proxy connection pool exhausted"}
    assert "proxy request pool exhausted (all 1 connections in use)" in caplog.text
    assert down == {}
    assert (health.status_code, health.json()["backends"]) == (200, {"/orders": "ok"})
    assert held_resp.status_code == 200
    assert (after.status_code, after.json()) == (200, {"ok": "fast"})


@pytest.mark.real_process
async def test_proxy_ignores_environment_proxy_settings(monkeypatch) -> None:
    """With HTTP_PROXY set and NO_PROXY=localhost (which does not cover
    127.0.0.1), forwarded requests and readiness probes still go straight to
    the loopback worker; the environment's proxy never sees a byte."""
    recorded: list[bytes] = []

    async def recording_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        recorded.append(await reader.readuntil(b"\r\n\r\n"))
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nfrom-proxy")
        await writer.drain()
        writer.close()

    fake = await asyncio.start_server(recording_proxy, "127.0.0.1", 0)
    for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{fake.sockets[0].getsockname()[1]}")
    monkeypatch.setenv("NO_PROXY", "localhost")

    backend_port, proxy_port = _free_port(), _free_port()
    rule = RoutingRule("/orders", f"http://127.0.0.1:{backend_port}")
    servers = [
        await _serve(_held_backend(asyncio.Event(), [0]), backend_port, "h11"),
        await _serve(create_proxy_app([rule], connect_retry_attempts=1), proxy_port, "h11"),
    ]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{proxy_port}", trust_env=False
        ) as c:
            resp = await c.get(
                "/orders/fast", headers={"Authorization": "Bearer SECRET", "Cookie": "sid=abc"}
            )
            health = await c.get("/_modulith/health")
    finally:
        for server, _ in servers:
            server.should_exit = True
        await asyncio.gather(*(task for _, task in servers))
        fake.close()

    assert (resp.status_code, resp.json()) == (200, {"ok": "fast"})
    assert (health.status_code, health.json()["backends"]) == (200, {"/orders": "ok"})
    assert recorded == []


# ---------------------------------------------------------------------------
# Upstream cookies and the identity probe (real sockets, proxy-owned clients)
# ---------------------------------------------------------------------------


def _cookie_setting_backend(token: str, seen: list[tuple[str, str | None]]) -> FastAPI:
    up = FastAPI()

    @up.get("/orders/login")
    async def login(request: Request) -> Any:
        seen.append(("/orders/login", request.headers.get("cookie")))
        resp = JSONResponse({"ok": True})
        resp.set_cookie("session", "USER_A_SECRET", path="/")
        return resp

    @up.get("/orders/profile")
    async def profile(request: Request) -> dict[str, bool]:
        seen.append(("/orders/profile", request.headers.get("cookie")))
        return {"ok": True}

    @up.get("/health")
    async def health(request: Request) -> Any:
        seen.append(("/health", request.headers.get("cookie")))
        resp = JSONResponse(_health_answer(request, token))
        resp.set_cookie("probe", "PROBE_COOKIE", path="/")
        return resp

    return up


async def test_proxy_never_replays_one_clients_upstream_cookie_to_another() -> None:
    seen: list[tuple[str, str | None]] = []
    port = _free_port()
    server, task = await _serve(_cookie_setting_backend("tok", seen), port, "h11")
    rule = RoutingRule(prefix="/orders", backend_url=f"http://127.0.0.1:{port}")
    proxy_app = create_proxy_app([rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            transport = httpx.ASGITransport(app=proxy_app)
            async with httpx.AsyncClient(transport=transport, base_url="http://proxy") as a:
                login = await a.get("/orders/login")
            async with httpx.AsyncClient(transport=transport, base_url="http://proxy") as b:
                anonymous = await b.get("/orders/profile")
                own = await b.get("/orders/profile", headers={"cookie": "mine=1"})
                rule.mark_down(rule.backend_url)
                await b.get("/_modulith/health")
                after_down = await b.get("/orders/profile")
    finally:
        server.should_exit = True
        await task

    assert "session=USER_A_SECRET" in login.headers["set-cookie"]
    assert [anonymous.status_code, own.status_code, after_down.status_code] == [200, 200, 200]
    assert seen == [
        ("/health", None),
        ("/orders/login", None),
        ("/orders/profile", None),
        ("/orders/profile", "mine=1"),
        ("/health", None),
        ("/orders/profile", None),
    ]


def _slow_health_backend(token: str, delay: float, calls: list[str]) -> FastAPI:
    up = FastAPI()

    @up.get("/orders/x")
    async def x() -> dict[str, bool]:
        return {"ok": True}

    @up.get("/health")
    async def health(request: Request) -> dict[str, str]:
        calls.append("/health")
        await asyncio.sleep(delay)
        return _health_answer(request, token)

    return up


async def test_identity_probe_waits_for_a_slow_but_healthy_backend() -> None:
    calls: list[str] = []
    port = _free_port()
    server, task = await _serve(_slow_health_backend("tok", 2.5, calls), port, "h11")
    url = f"http://127.0.0.1:{port}"
    rule = RoutingRule(prefix="/orders", backend_url=url)
    proxy_app = create_proxy_app([rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
            ) as c:
                resp = await c.get("/orders/x")
    finally:
        server.should_exit = True
        await task

    assert (resp.status_code, resp.json()) == (200, {"ok": True})
    assert (rule.is_verified(url), url in rule._down, calls) == (True, False, ["/health"])


async def test_identity_probe_past_its_deadline_answers_504_without_marking_down() -> None:
    calls: list[str] = []
    port = _free_port()
    server, task = await _serve(_slow_health_backend("tok", 1.5, calls), port, "h11")
    url = f"http://127.0.0.1:{port}"
    rule = RoutingRule(prefix="/orders", backend_url=url)
    proxy_app = create_proxy_app([rule], deployment_token="tok", identity_probe_timeout=0.3)
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
            ) as c:
                started = asyncio.get_running_loop().time()
                resp = await c.get("/orders/x")
                elapsed = asyncio.get_running_loop().time() - started
    finally:
        server.should_exit = True
        await task

    assert (resp.status_code, resp.json()) == (504, {"detail": "worker identity check timed out"})
    assert (url in rule._down, rule.is_verified(url)) == (False, False)
    assert elapsed < 1.2


async def test_identity_probe_refuses_an_oversized_health_body() -> None:
    hits: list[str] = []

    def backend(request: httpx.Request) -> httpx.Response:
        hits.append(request.url.path)
        if request.url.path == "/health":
            proof = identity_proof("tok", request.headers[NONCE_HEADER], "orders", 80)
            return httpx.Response(200, json={"proof": proof, "pad": "x" * (2 * 1024 * 1024)})
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(backend))
    rule = RoutingRule(prefix="/orders", backend_url="http://w")
    proxy_app = create_proxy_app([rule], client=client, deployment_token="tok")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as c:
        resp = await c.get("/orders/x")

    assert resp.status_code == 503
    assert "/orders/x" not in hits


async def test_identity_probe_accepts_a_right_proof_in_a_bounded_unready_answer() -> None:
    def backend(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            proof = identity_proof("tok", request.headers[NONCE_HEADER], "orders", 80)
            answer = {
                "status": "failed",
                "module": "orders",
                "ready": False,
                "detail": "x" * 1024,
                "proof": proof,
            }
            return httpx.Response(503, json=answer)
        return httpx.Response(200, stream=httpx.ByteStream(b"served"))

    client = httpx.AsyncClient(transport=httpx.MockTransport(backend))
    rule = RoutingRule(prefix="/orders", backend_url="http://w")
    proxy_app = create_proxy_app([rule], client=client, deployment_token="tok")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as c:
        resp = await c.get("/orders/x")

    assert (resp.status_code, resp.content) == (200, b"served")
    assert rule.is_verified("http://w")


async def _gated_worker(
    token: str,
    gate: asyncio.Event,
    seen: list[str],
    module: str = "orders",
    delay: float = 0.0,
) -> tuple[asyncio.Server, str]:
    """Loopback worker that accepts every connection but answers nothing until
    ``gate`` is set and ``delay`` seconds have passed since the request was
    read, then answers any path with this deployment's ``/health``."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            seen.append(head.split(b" ", 2)[1].decode())
            nonce = next(
                (
                    line.split(b":", 1)[1].strip().decode()
                    for line in head.split(b"\r\n")
                    if line.lower().startswith(NONCE_HEADER.encode() + b":")
                ),
                None,
            )
            port = writer.get_extra_info("sockname")[1]
            proof = identity_proof(token, nonce, module, port) if nonce else None
            body = json.dumps({"status": "ok", "proof": proof}).encode()
            answer = (
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n"
                b"content-length: %d\r\n\r\n%s" % (len(body), body)
            )
            await asyncio.sleep(delay)
            await gate.wait()
            writer.write(answer)
            await writer.drain()
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=2048)
    return server, f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"


async def _close_workers(*servers: asyncio.Server) -> None:
    for server in servers:
        server.close()
        await server.wait_closed()


async def test_concurrent_requests_to_an_unverified_worker_share_one_identity_probe() -> None:
    gate, seen = asyncio.Event(), list[str]()
    server, url = await _gated_worker("tok", gate, seen)
    rule = RoutingRule("/orders", url)
    proxy_app = create_proxy_app([rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
            ) as c:
                calls = [asyncio.create_task(c.get("/orders/x")) for _ in range(20)]
                await _wait_for(lambda: len(seen) >= 1, 10.0)
                await asyncio.sleep(0.3)
                calls[0].cancel()
                await asyncio.sleep(0.1)
                reads_while_pending = list(seen)
                gate.set()
                answers = await asyncio.gather(*calls[1:])
    finally:
        gate.set()
        await _close_workers(server)

    assert reads_while_pending == ["/health"]
    assert [r.status_code for r in answers] == [200] * 19
    assert seen.count("/health") == 1
    assert rule.is_verified(url)


async def test_a_stalled_unverified_worker_leaves_readiness_and_other_modules_serving() -> None:
    stall, open_gate = asyncio.Event(), asyncio.Event()
    open_gate.set()
    a_seen: list[str] = []
    a_server, a_url = await _gated_worker("tok", stall, a_seen, "a")
    b_server, b_url = await _gated_worker("tok", open_gate, [], "b")
    a_rule, b_rule = RoutingRule("/a", a_url), RoutingRule("/b", b_url)
    proxy_app = create_proxy_app([a_rule, b_rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy", timeout=30
            ) as c:
                stalled = [asyncio.create_task(c.get("/a/x")) for _ in range(100)]
                await _wait_for(lambda: len(a_seen) >= 1, 10.0)
                await asyncio.sleep(0.5)
                readiness = await c.get("/_modulith/health")
                b_rule.forget_identity(b_url)
                b_resp = await c.get("/b/x")
                stall.set()
                a_codes = [r.status_code for r in await asyncio.gather(*stalled)]
    finally:
        stall.set()
        await _close_workers(a_server, b_server)

    assert readiness.json()["backends"] == {"/a": "unreachable", "/b": "ok"}
    assert (b_resp.status_code, b_rule.is_verified(b_url)) == (200, True)
    assert a_codes == [200] * 100


async def test_health_probe_pool_exhaustion_names_the_probe_pool_and_its_limit(
    monkeypatch, caplog
) -> None:
    caplog.set_level("WARNING", logger="modulith.proxy")
    monkeypatch.setattr("modulith.proxy._probe_pool_size", lambda rules: 1)
    stall, open_gate = asyncio.Event(), asyncio.Event()
    open_gate.set()
    a_seen: list[str] = []
    a_server, a_url = await _gated_worker("tok", stall, a_seen, "a")
    b_server, b_url = await _gated_worker("tok", open_gate, [], "b")
    a_rule, b_rule = RoutingRule("/a", a_url), RoutingRule("/b", b_url)
    proxy_app = create_proxy_app([a_rule, b_rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy", timeout=30
            ) as c:
                held = asyncio.create_task(c.get("/a/x"))
                await _wait_for(lambda: len(a_seen) >= 1, 10.0)
                b_resp = await c.get("/b/x")
                stall.set()
                held_resp = await held
    finally:
        stall.set()
        await _close_workers(a_server, b_server)

    assert (b_resp.status_code, b_resp.json()) == (
        503,
        {"detail": "proxy connection pool exhausted"},
    )
    assert f"proxy health-probe pool exhausted (all 1 connections in use) for {b_url}/b/x" in (
        caplog.text
    )
    assert "request pool" not in caplog.text
    assert (b_url in b_rule._down, held_resp.status_code) == (False, 200)


async def test_concurrent_readiness_polls_share_one_probe_per_worker() -> None:
    stall, open_gate = asyncio.Event(), asyncio.Event()
    open_gate.set()
    a_seen: list[str] = []
    a_server, a_url = await _gated_worker("tok", stall, a_seen, "a")
    b_server, b_url = await _gated_worker("tok", open_gate, [], "b")
    proxy_app = create_proxy_app(
        [RoutingRule("/a", a_url), RoutingRule("/b", b_url)], deployment_token="tok"
    )
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy", timeout=30
            ) as c:
                polls = await asyncio.gather(*(c.get("/_modulith/health") for _ in range(150)))
                probes_for_first_batch = len(a_seen)
                await c.get("/_modulith/health")
    finally:
        stall.set()
        await _close_workers(a_server, b_server)

    assert {(p.status_code, json.dumps(p.json()["backends"], sort_keys=True)) for p in polls} == {
        (503, json.dumps({"/a": "unreachable", "/b": "ok"}, sort_keys=True))
    }
    assert probes_for_first_batch == 1
    assert len(a_seen) == 2


async def test_readiness_of_a_fleet_larger_than_the_probe_pool_is_not_queued() -> None:
    gate, seen = asyncio.Event(), list[str]()
    gate.set()
    workers = [await _gated_worker("tok", gate, seen, delay=1.2) for _ in range(120)]
    rule = RoutingRule("/orders", workers[0][1], backend_urls=tuple(url for _, url in workers))
    proxy_app = create_proxy_app([rule], deployment_token="tok")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy", timeout=30
            ) as c:
                readiness = await c.get("/_modulith/health")
    finally:
        gate.set()
        await _close_workers(*(server for server, _ in workers))

    assert (readiness.status_code, readiness.json()["backends"]) == (200, {"/orders": "ok"})
    assert rule._down == {}


# ---------------------------------------------------------------------------
# Identity challenge: a listener proves the deployment token, never echoes it
# ---------------------------------------------------------------------------

_OURS = "http://127.0.0.1:9001"
_OTHER_WORKER = "http://127.0.0.1:9002"


def _worker_app(token: str, module: str = "orders", hits: list[str] | None = None) -> FastAPI:
    """A genuine worker of the deployment holding ``token``."""
    up = FastAPI()

    @up.get(f"/{module}/ping")
    async def ping() -> dict[str, str]:
        if hits is not None:
            hits.append("ping")
        return {"served_by": module}

    @up.get("/health")
    async def health(request: Request) -> dict[str, str]:
        return _health_answer(request, token, module)

    return up


def _listener_app(answer: Any, hits: list[str]) -> FastAPI:
    """A foreign listener on the worker's port; ``answer(request)`` is its ``/health``."""
    up = FastAPI()

    @up.get("/orders/ping")
    async def ping() -> dict[str, str]:
        hits.append("ping")
        return {"served_by": "listener"}

    @up.get("/health")
    async def health(request: Request) -> Any:
        result = answer(request)
        return await result if inspect.isawaitable(result) else result

    return up


async def _through_proxy(mounts: dict[str, FastAPI], *, token: str | None = "tok") -> Any:
    """GET /orders/ping then readiness, with ``_OURS`` as the module's backend."""
    client = httpx.AsyncClient(
        mounts={url: httpx.ASGITransport(app=app) for url, app in mounts.items()}
    )
    rule = RoutingRule(prefix="/orders", backend_url=_OURS)
    proxy_app = create_proxy_app([rule], client=client, deployment_token=token)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as c:
        served = await c.get("/orders/ping")
        readiness = await c.get("/_modulith/health")
    return served, readiness.json()["backends"]["/orders"]


def _proof(request: Request, module: str, port: int, token: str = "tok") -> str:
    return identity_proof(token, request.headers[NONCE_HEADER], module, port)


async def test_a_genuine_worker_answering_the_challenge_is_verified_and_served() -> None:
    hits: list[str] = []
    served, readiness = await _through_proxy({_OURS: _worker_app("tok", hits=hits)})

    assert (served.status_code, served.json(), readiness) == (200, {"served_by": "orders"}, "ok")
    assert hits == ["ping"]


async def _relay_to_a_live_worker(request: Request) -> Any:
    """Forwards the proxy's nonce to a live worker of the same deployment on another
    port and returns that worker's real answer."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_worker_app("tok")), base_url=_OTHER_WORKER
    ) as other:
        resp = await other.get("/health", headers={NONCE_HEADER: request.headers[NONCE_HEADER]})
    return resp.json()


@pytest.mark.parametrize(
    "answer",
    [
        pytest.param(
            lambda r: {"status": "ok", "module": "orders", "deployment": "tok"},
            id="echoes-raw-token",
        ),
        pytest.param(lambda r: {"status": "ok", "module": "orders"}, id="no-proof"),
        pytest.param(lambda r: {"status": "ok", "proof": 12345}, id="proof-not-a-string"),
        pytest.param(lambda r: {"status": "ok", "proof": None}, id="proof-null"),
        pytest.param(lambda r: {"status": "ok", "proof": ["x"]}, id="proof-a-list"),
        pytest.param(lambda r: {"status": "ok", "proof": ""}, id="proof-empty"),
        pytest.param(lambda r: {"status": "ok", "proof": "zz-not-hex"}, id="proof-garbage"),
        pytest.param(lambda r: ["not", "an", "object"], id="body-a-list"),
        pytest.param(lambda r: "not a worker", id="body-a-string"),
        pytest.param(
            lambda r: {
                "status": "ok",
                "proof": identity_proof("other", r.headers[NONCE_HEADER], "orders", 9001),
            },
            id="another-deployments-token",
        ),
        pytest.param(
            lambda r: {"status": "ok", "proof": _proof(r, "inventory", 9001)},
            id="same-deployment-other-module",
        ),
        pytest.param(
            lambda r: {"status": "ok", "proof": _proof(r, "orders", 9002)},
            id="same-deployment-other-port",
        ),
        pytest.param(
            lambda r: {"status": "ok", "proof": _proof(r, "orders", 9001).upper()},
            id="proof-altered-case",
        ),
        pytest.param(_relay_to_a_live_worker, id="relays-nonce-to-a-live-worker-on-another-port"),
    ],
)
async def test_a_listener_that_cannot_compute_this_backends_proof_gets_no_traffic(answer) -> None:
    hits: list[str] = []
    served, readiness = await _through_proxy({_OURS: _listener_app(answer, hits)})

    assert (served.status_code, readiness) == (503, "foreign deployment")
    assert hits == []


async def test_a_proof_replayed_from_an_earlier_probe_is_rejected() -> None:
    answers: list[dict[str, str]] = []
    hits: list[str] = []

    def replaying(request: Request) -> Any:
        if not answers:
            answers.append(_health_answer(request, "tok"))
        return answers[0]

    client = httpx.AsyncClient(
        mounts={_OURS: httpx.ASGITransport(app=_listener_app(replaying, hits))}
    )
    rule = RoutingRule(prefix="/orders", backend_url=_OURS)
    proxy_app = create_proxy_app([rule], client=client, deployment_token="tok")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as c:
        first = await c.get("/orders/ping")
        rule.forget_identity(_OURS)
        second = await c.get("/orders/ping")

    assert (first.status_code, second.status_code) == (200, 503)
    assert hits == ["ping"]


async def test_each_identity_probe_sends_a_fresh_random_nonce() -> None:
    nonces: list[str] = []

    def record(request: Request) -> Any:
        nonces.append(request.headers[NONCE_HEADER])
        return _health_answer(request, "tok")

    client = httpx.AsyncClient(mounts={_OURS: httpx.ASGITransport(app=_listener_app(record, []))})
    rule = RoutingRule(prefix="/orders", backend_url=_OURS)
    proxy_app = create_proxy_app([rule], client=client, deployment_token="tok")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as c:
        await c.get("/orders/ping")
        rule.forget_identity(_OURS)
        await c.get("/orders/ping")
        await c.get("/_modulith/health")

    assert len(nonces) == 3 and len(set(nonces)) == 3
    assert all(len(n) >= 32 and n.isalnum() for n in nonces)


async def test_a_proxy_without_a_token_sends_no_challenge_and_serves_any_listener() -> None:
    health_headers: list[str | None] = []

    def anything(request: Request) -> Any:
        health_headers.append(request.headers.get(NONCE_HEADER))
        return {"status": "ok"}

    hits: list[str] = []
    served, readiness = await _through_proxy({_OURS: _listener_app(anything, hits)}, token=None)

    assert (served.status_code, readiness) == (200, "ok")
    assert health_headers == [None]
    assert hits == ["ping"]
