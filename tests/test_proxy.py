"""Tests for the process-per-module reverse proxy.

``create_proxy_app`` builds a FastAPI app that forwards requests to per-module
worker backends by URL prefix, plus ``/_modulith/*`` actuator endpoints. To
exercise real forwarding without opening sockets, the proxy's ``httpx`` client
is injected with an ``ASGITransport`` pointed at an in-process upstream app —
so the proxy genuinely builds the upstream URL, filters headers, and streams
the response, all in-process.
"""

from __future__ import annotations

import warnings

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
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

    async def send(self, request, *, stream: bool = False):
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


def test_proxy_maps_redirect_loop_to_502(caplog) -> None:
    """httpx.TooManyRedirects is a RequestError sibling of TransportError —
    with an injected follow_redirects=True client (the documented seam) a
    redirect-looping backend escaped the TransportError-only mapping as a raw
    500, violating the never-uncaught-500 contract. It must map to 502."""
    caplog.set_level("WARNING", logger="modulith.proxy")

    def _always_redirect(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": str(request.url)})

    # A REAL httpx client that genuinely follows the loop until its own
    # max_redirects trips — not a stub raising the exception by hand.
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_always_redirect), follow_redirects=True
    )
    app = create_proxy_app([RoutingRule("/orders", "http://orders-worker")], client=client)

    with TestClient(app) as test_client:
        resp = test_client.get("/orders/ping")

    assert resp.status_code == 502  # mapped, never an uncaught 500
    assert "/orders/ping" in caplog.text


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

    async def send(self, request, *, stream: bool = False):
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

    async def send(self, request, *, stream: bool = False):
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

    async def send(self, request, *, stream: bool = False):
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
