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
import warnings
from collections.abc import MutableMapping
from typing import Any
from urllib.parse import unquote

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from modulith.proxy import RoutingRule, _match_rule, create_proxy_app

from conftest import _free_port, _held_backend, _serve, _wait_for

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
async def test_proxy_answers_503_on_pool_exhaustion_without_marking_backend_down() -> None:
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
