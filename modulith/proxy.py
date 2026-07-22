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

import asyncio
import hmac
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
    actuator_enabled: bool = True,
    timeout: httpx.Timeout | None = None,
) -> FastAPI:
    """Build the reverse-proxy ASGI app.

    ``client`` (an ``httpx.AsyncClient``) may be injected — for tests, or to
    share a connection pool. When omitted, one is created and closed with the
    app's lifespan.

    ``timeout`` configures the httpx client's request timeout. Defaults to
    ``httpx.Timeout(5.0, read=None)`` — finite connect/write/pool timeouts
    prevent hanging on unreachable backends, while ``read=None`` disables the
    read timeout to support long-polling, streaming responses, and slow
    upstreams. Ignored if ``client`` is injected (the caller owns the client's
    configuration).

    ``actuator_enabled=False`` (``actuator_mode="disabled"``, resolved by
    ``run_supervised``) unmounts ``/_modulith/*`` entirely — those paths fall
    through to the catch-all proxy handler and answer the same 404 as any
    other unmatched path, so the actuator's existence isn't even revealed.
    """
    owns_client = client is None
    if timeout is None:
        timeout = httpx.Timeout(5.0, read=None)
    http_client: Any = client if client is not None else httpx.AsyncClient(timeout=timeout)

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
        supplied = request.headers.get("authorization", "")
        # Constant-time comparison: a plain `==` short-circuits on the first
        # mismatched byte, leaking token prefixes through a timing
        # side-channel. Compare as bytes — compare_digest rejects non-ASCII str.
        if hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
            return None
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    # Actuator routes are registered before the catch-all so they win for
    # /_modulith/* paths. Skipped entirely when disabled — see docstring.
    if actuator_enabled:

        @app.get("/_modulith/topology", response_model=None)
        async def topology(request: Request) -> dict[str, Any] | JSONResponse:
            denied = _actuator_auth_response(request)
            if denied is not None:
                return denied
            return {"routes": [{"prefix": r.prefix, "backend": r.backend_url} for r in rules]}

        @app.get("/_modulith/live", response_model=None)
        async def live(request: Request) -> dict[str, Any] | JSONResponse:
            """Liveness: this proxy process is up and serving requests.

            Deliberately independent of backend reachability — an
            orchestrator must not restart the healthy proxy process just
            because one worker is degraded; that's what readiness
            (``/_modulith/health``) is for.
            """
            denied = _actuator_auth_response(request)
            if denied is not None:
                return denied
            return {"status": "ok"}

        @app.get("/_modulith/health", response_model=None)
        async def health(request: Request) -> dict[str, Any] | JSONResponse:
            """Readiness: aggregates every worker's own ``/health``.

            Returns 503 when any backend is unhealthy/unreachable — a
            standard readiness contract (orchestrators stop routing traffic
            to a 503 instance) that a constant 200 could never express.
            """
            denied = _actuator_auth_response(request)
            if denied is not None:
                return denied

            async def check_one(rule: RoutingRule) -> tuple[str, str]:
                try:
                    resp = await http_client.get(rule.backend_url + "/health", timeout=2.0)
                    return rule.prefix, "ok" if resp.status_code == 200 else "unhealthy"
                except Exception:
                    return rule.prefix, "unreachable"

            # Concurrent fan-out: total latency ~max(per-backend latency), not
            # the sum — a sequential loop scales O(N * per-backend timeout).
            backends = dict(await asyncio.gather(*(check_one(rule) for rule in rules)))
            overall = "ok" if all(state == "ok" for state in backends.values()) else "degraded"
            body = {"status": overall, "backends": backends}
            if overall != "ok":
                return JSONResponse(body, status_code=503)
            return body

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

        # scope["raw_path"]/["query_string"] carry the exact bytes the client
        # sent, still percent-encoded. request.url.path/.query are built from
        # scope["path"] — already percent-*decoded* per the ASGI spec — so an
        # encoded separator inside a segment (e.g. "%2F" meaning a literal
        # slash within one segment, not a path boundary) would be silently
        # turned into a real "/" and change how many segments the upstream
        # sees. Forwarding the raw bytes preserves the client's exact request.
        raw_path = request.scope.get("raw_path") or request.url.path.encode("utf-8")
        upstream = rule.backend_url + raw_path.decode("latin-1")
        query_string = request.scope.get("query_string", b"")
        if query_string:
            upstream += "?" + query_string.decode("latin-1")

        too_large = _body_too_large(request, max_request_body_bytes)
        if too_large is not None:
            return too_large
        # The Content-Length check above is only a fast path — chunked and
        # streamed uploads carry no length header. Enforce the cap while
        # consuming the stream so an oversized body is rejected as soon as
        # the running total crosses the limit, never fully buffered first
        # (single-request unbounded-memory DoS otherwise).
        chunks: list[bytes] = []
        received = 0
        async for chunk in request.stream():
            received += len(chunk)
            if max_request_body_bytes is not None and received > max_request_body_bytes:
                return JSONResponse({"detail": "request body too large"}, status_code=413)
            chunks.append(chunk)
        body = b"".join(chunks)
        # Build from .raw (a list, not a dict) so a client sending the same
        # header twice (e.g. two Cookie lines, or multi-valued
        # X-Forwarded-For) forwards both — dict(request.headers) keeps only
        # one of any repeated name and silently drops the rest.
        fwd_headers = _filter_headers(_header_pairs(request.headers.raw))
        # Drop the client's Host so httpx sets it to the loopback worker's
        # authority. Forwarding the external Host (e.g. api.example.com) makes
        # workers behave as if internet-facing for URL generation / vhost /
        # Host-allowlist logic — a reverse-proxy correctness/security smell.
        fwd_headers = [(k, v) for k, v in fwd_headers if k.lower() != "host"]
        try:
            upstream_req = http_client.build_request(
                method=request.method,
                url=upstream,
                headers=fwd_headers,
                content=body,
            )
        except Exception as exc:
            # build_request failures share no httpx base class the send()
            # guard below could catch: httpx.InvalidURL (percent-encoded
            # non-printable ASCII in the path) subclasses Exception directly,
            # and a header carrying a raw non-ASCII octet raises
            # UnicodeEncodeError. Both mean the *client's* request cannot be
            # forwarded — answer 400, honoring the "never an uncaught 500"
            # contract documented on the TransportError handler.
            logger.warning(
                "cannot build upstream request for %s: %s",
                _without_query(upstream),
                exc,
            )
            return JSONResponse({"detail": "invalid request"}, status_code=400)
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
        except httpx.RequestError as exc:
            # RequestError siblings outside the TransportError subtree —
            # httpx.TooManyRedirects (a redirect-looping backend behind an
            # injected follow_redirects=True client, S3-r3-162) and
            # httpx.DecodingError. Both mean "no valid response could be
            # obtained from the backend" → 502, honoring the
            # never-an-uncaught-500 contract documented above.
            logger.warning(
                "backend %s returned no usable response for %s: %s",
                rule.backend_url,
                _without_query(upstream),
                exc,
            )
            return JSONResponse({"detail": "backend error"}, status_code=502)

        response = StreamingResponse(
            _safe_stream(upstream_resp, _without_query(upstream)),
            status_code=upstream_resp.status_code,
        )
        # Passing headers= to StreamingResponse builds a plain dict internally
        # (Response.init_headers), which loses duplicates the same way as on
        # the request side (e.g. multiple Set-Cookie). Setting raw_headers
        # directly after construction preserves every occurrence.
        response.raw_headers = [
            (k.lower().encode("latin-1"), v.encode("latin-1"))
            for k, v in _filter_headers(_header_pairs(upstream_resp.headers.raw))
        ]
        return response

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


def _header_pairs(raw: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    """Decode a raw ASGI/httpx header list into (name, value) pairs.

    Kept as a list (not a dict) so repeated header names survive — building
    a dict from an iterable of pairs keeps only one occurrence per key.
    """
    return [(k.decode("latin-1"), v.decode("latin-1")) for k, v in raw]


def _filter_headers(headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Strip hop-by-hop headers (RFC 7230 §6.1) before forwarding.

    Beyond the fixed well-known set, RFC 7230 requires treating any header
    *named inside a Connection header's value* as connection-specific for
    that hop too (e.g. ``Connection: X-Custom-Header``) — those names are
    only meaningful to this hop and must not be forwarded either.
    """
    connection_named = {
        token.strip().lower()
        for name, value in headers
        if name.lower() == "connection"
        for token in value.split(",")
        if token.strip()
    }
    drop = HOP_BY_HOP_HEADERS | connection_named
    return [(k, v) for k, v in headers if k.lower() not in drop]


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
    """Stream the upstream body; abort rather than fake a complete response.

    Once headers are sent the status code can't change, so a worker dying
    mid-stream can't become a 502 — but ending the stream "cleanly" here
    would let the client believe it received a complete, successful 200 when
    bytes are silently missing. Re-raising aborts the ASGI response instead
    (the connection drops without a valid terminator), which is the only way
    an HTTP client can detect the truncation.

    Closes ``resp`` itself (not via a background task) — a re-raised
    exception unwinds straight out of StreamingResponse.__call__, which only
    runs its background task after the body iterator finishes normally.
    """
    try:
        async for chunk in resp.aiter_raw():
            yield chunk
    except httpx.TransportError as exc:
        logger.warning("backend stream interrupted for %s: %s", upstream, exc)
        raise
    finally:
        await resp.aclose()


__all__ = [
    "RoutingRule",
    "create_proxy_app",
]
