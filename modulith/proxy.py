"""Reverse proxy for the process-per-module topology.

A FastAPI ASGI app that routes incoming requests to per-module worker
processes by URL prefix. The supervisor runs this on the public port;
workers run on internal ports.

Routing model:
    /orders/*       → http://127.0.0.1:9001
    /inventory/*    → http://127.0.0.1:9002
    /reports/*      → http://127.0.0.1:9003
    /_modulith/*    → handled by the proxy itself (actuator)

Uses ``httpx.AsyncClient`` for streaming proxying. Hop-by-hop headers are
stripped per RFC 7230. WebSocket support is a v2.1 enhancement; v1 is HTTP only.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

# These are imported at module level (not lazily) so FastAPI's get_type_hints
# can resolve the route handlers' string annotations against this module's
# globals. proxy.py is itself only imported lazily (process-per-module mode),
# so the fastapi/httpx dependency stays opt-in.
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

logger = logging.getLogger("modulith.proxy")
DEFAULT_MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True)
class RoutingRule:
    """One URL-prefix → backend-port mapping."""

    prefix: str  # e.g. "/orders"
    backend_url: str  # e.g. "http://127.0.0.1:9001"


def _match_rule(path: str, rules: list[RoutingRule]) -> RoutingRule | None:
    """Find the rule whose prefix matches the path (longest prefix wins).

    Matching is on path boundaries: ``/orders`` matches ``/orders`` and
    ``/orders/x`` but not ``/ordersX``. Trailing slashes are normalized.
    """
    normalized = path.rstrip("/") or "/"
    for rule in sorted(rules, key=lambda r: len(r.prefix), reverse=True):
        prefix = rule.prefix.rstrip("/")
        if normalized == prefix or normalized.startswith(prefix + "/"):
            return rule
    return None


def create_proxy_app(
    rules: list[RoutingRule],
    *,
    client: Any | None = None,
    max_request_body_bytes: int | None = DEFAULT_MAX_REQUEST_BODY_BYTES,
    actuator_token: str | None = None,
) -> FastAPI:
    """Build the reverse-proxy ASGI app.

    ``client`` (an ``httpx.AsyncClient``) may be injected — for tests, or to
    share a connection pool. When omitted, one is created and closed with the
    app's lifespan.
    """
    owns_client = client is None
    http_client: Any = client if client is not None else httpx.AsyncClient()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        if owns_client:
            await http_client.aclose()

    app = FastAPI(title="modulith-proxy", lifespan=lifespan)

    def _actuator_auth_response(request: Request) -> JSONResponse | None:
        if actuator_token is None:
            return None
        expected = f"Bearer {actuator_token}"
        if request.headers.get("authorization") == expected:
            return None
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    # Actuator routes are registered before the catch-all so they win for
    # /_modulith/* paths.
    @app.get("/_modulith/topology", response_model=None)
    async def topology(request: Request) -> dict[str, Any] | JSONResponse:
        denied = _actuator_auth_response(request)
        if denied is not None:
            return denied
        return {"routes": [{"prefix": r.prefix, "backend": r.backend_url} for r in rules]}

    @app.get("/_modulith/health", response_model=None)
    async def health(request: Request) -> dict[str, Any] | JSONResponse:
        denied = _actuator_auth_response(request)
        if denied is not None:
            return denied
        backends: dict[str, str] = {}
        overall = "ok"
        for rule in rules:
            try:
                resp = await http_client.get(rule.backend_url + "/health", timeout=2.0)
                backends[rule.prefix] = "ok" if resp.status_code == 200 else "unhealthy"
            except Exception:
                backends[rule.prefix] = "unreachable"
            if backends[rule.prefix] != "ok":
                overall = "degraded"
        return {"status": overall, "backends": backends}

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy(request: Request, path: str) -> Response:
        rule = _match_rule(request.url.path, rules)
        if rule is None:
            return JSONResponse(
                {"detail": f"no worker route for {request.url.path!r}"}, status_code=404
            )

        upstream = rule.backend_url + request.url.path
        if request.url.query:
            upstream += "?" + request.url.query

        too_large = _body_too_large(request, max_request_body_bytes)
        if too_large is not None:
            return too_large
        body = await request.body()
        if max_request_body_bytes is not None and len(body) > max_request_body_bytes:
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        fwd_headers = _filter_headers(dict(request.headers))
        # Drop the client's Host so httpx sets it to the loopback worker's
        # authority. Forwarding the external Host (e.g. api.example.com) makes
        # workers behave as if internet-facing for URL generation / vhost /
        # Host-allowlist logic — a reverse-proxy correctness/security smell.
        fwd_headers.pop("host", None)
        upstream_req = http_client.build_request(
            method=request.method,
            url=upstream,
            headers=fwd_headers,
            content=body,
        )
        try:
            upstream_resp = await http_client.send(upstream_req, stream=True)
        except httpx.TransportError as exc:
            # TransportError covers the whole connect/read failure tree —
            # ConnectError (refused/DNS), ConnectTimeout (reachable but
            # unresponsive), ReadError/ReadTimeout/RemoteProtocolError (worker
            # died mid-handshake). All mean "backend unavailable" → 502, never
            # an uncaught 500.
            logger.warning(
                "backend %s unreachable for %s: %s",
                rule.backend_url,
                _without_query(upstream),
                exc,
            )
            return JSONResponse({"detail": "backend unreachable"}, status_code=502)

        return StreamingResponse(
            _safe_stream(upstream_resp, _without_query(upstream)),
            status_code=upstream_resp.status_code,
            headers=_filter_headers(dict(upstream_resp.headers)),
            background=BackgroundTask(upstream_resp.aclose),
        )

    return app


# ---------------------------------------------------------------------------
# Hop-by-hop headers — stripped during proxying
# ---------------------------------------------------------------------------

# Per RFC 7230. These are connection-specific and should not be forwarded.
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)


def _filter_headers(headers: dict[str, str]) -> dict[str, str]:
    """Strip hop-by-hop headers before forwarding."""
    return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}


def _body_too_large(request: Request, limit: int | None) -> JSONResponse | None:
    """Reject oversized requests before buffering the body when possible."""
    if limit is None:
        return None
    content_length = request.headers.get("content-length")
    if content_length is None:
        return None
    try:
        size = int(content_length)
    except ValueError:
        return None
    if size > limit:
        return JSONResponse({"detail": "request body too large"}, status_code=413)
    return None


def _without_query(url: str) -> str:
    """Remove query strings before logging so credentials are not persisted."""
    return url.partition("?")[0]


async def _safe_stream(resp: Any, upstream: str) -> AsyncIterator[bytes]:
    """Stream the upstream body, swallowing a mid-response transport failure.

    Once headers are sent the status can't change, so a worker dying mid-stream
    can't become a 502 — but it must not surface as an unhandled ASGI error
    either. Log it and end the stream cleanly; the BackgroundTask still closes
    the response.
    """
    try:
        async for chunk in resp.aiter_raw():
            yield chunk
    except httpx.TransportError as exc:
        logger.warning("backend stream interrupted for %s: %s", upstream, exc)


__all__ = [
    "RoutingRule",
    "create_proxy_app",
]
