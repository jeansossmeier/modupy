"""Reverse proxy for the process-per-module topology.

A Starlette/FastAPI ASGI app that routes incoming requests to per-module
worker processes by URL prefix. The supervisor runs this on the public
port; workers run on internal ports.

Implementation status: SKELETON. ~120 lines when complete.

Routing model:
    /orders/*       → http://127.0.0.1:9001
    /inventory/*    → http://127.0.0.1:9002
    /reports/*      → http://127.0.0.1:9003
    /_modulith/*    → handled by the proxy itself (actuator)

WebSocket support is v2.1; for v1, only HTTP. Most B2B SaaS apps don't
need WebSocket fan-out across modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("modulith.proxy")


@dataclass(frozen=True)
class RoutingRule:
    """One URL-prefix → backend-port mapping."""

    prefix: str  # e.g. "/orders"
    backend_url: str  # e.g. "http://127.0.0.1:9001"


def create_proxy_app(rules: list[RoutingRule]) -> Any:
    """Build the proxy ASGI app.

    IMPLEMENTATION TODO:

    Use httpx.AsyncClient for streaming proxying. Approach:

        from fastapi import FastAPI, Request, Response
        from fastapi.responses import StreamingResponse
        import httpx

        app = FastAPI()
        client = httpx.AsyncClient()

        @app.middleware("http")
        async def proxy_middleware(request: Request, call_next):
            # Find matching rule
            rule = _match_rule(request.url.path, rules)
            if rule is None:
                # No match — handle locally (for /_modulith/* actuator)
                return await call_next(request)

            # Build the upstream URL
            upstream = rule.backend_url + request.url.path
            if request.url.query:
                upstream += "?" + request.url.query

            # Stream the request body upstream
            req = client.build_request(
                method=request.method,
                url=upstream,
                headers=dict(request.headers),
                content=request.stream(),
            )
            resp = await client.send(req, stream=True)

            # Stream the response back to the original client
            return StreamingResponse(
                content=resp.aiter_raw(),
                status_code=resp.status_code,
                headers=dict(resp.headers),
                background=BackgroundTask(resp.aclose),
            )

        # Add the actuator routes
        @app.get("/_modulith/topology")
        async def topology():
            return {"routes": [{"prefix": r.prefix, "backend": r.backend_url}
                               for r in rules]}

        @app.get("/_modulith/health")
        async def health():
            # Check each backend's /health endpoint
            ...

    Edge cases to handle:
      - Hop-by-hop headers must be stripped (Connection, Upgrade, etc.)
      - Trailing slashes — normalize before matching
      - Backends that are starting up — return 503 with retry-after
      - Backends that are unreachable — log, return 502
    """
    raise NotImplementedError("Phase 3 — see TODO above")


def _match_rule(path: str, rules: list[RoutingRule]) -> RoutingRule | None:
    """Find the rule whose prefix matches the path.

    IMPLEMENTATION TODO:
    1. Strip trailing slash from path for consistency.
    2. Sort rules by prefix length, descending (longer match wins).
    3. Return the first whose prefix matches at the start.
    """
    raise NotImplementedError("Phase 3")


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


__all__ = [
    "RoutingRule",
    "create_proxy_app",
]
