"""Reverse proxy for the process-per-module topology.

A FastAPI ASGI app that routes incoming requests to per-module worker
processes by URL prefix. The supervisor runs this on the public port;
workers run on internal ports.

Routing model:
    /orders/*       → http://127.0.0.1:9001
    /inventory/*    → http://127.0.0.1:9002
    /reports/*      → http://127.0.0.1:9003
    /_modulith/*    → handled by the proxy itself (actuator)

``/_modulith/*`` is the only prefix the proxy keeps for itself; it publishes no
OpenAPI schema and no docs UI, so ``/openapi.json``, ``/docs`` and ``/redoc``
are proxied or 404 like any other path (see ``create_proxy_app``).

Uses ``httpx.AsyncClient`` for streaming proxying. Hop-by-hop headers are
stripped per RFC 7230. WebSocket support is a v2.1 enhancement; v1 is HTTP only.
"""

from __future__ import annotations

import asyncio
import hmac
import http.cookiejar
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from itertools import cycle
from typing import Any
from urllib.parse import quote, unquote

# These are imported at module level (not lazily) so FastAPI's get_type_hints
# can resolve the route handlers' string annotations against this module's
# globals. proxy.py is itself only imported lazily (process-per-module mode),
# so the fastapi/httpx dependency stays opt-in.
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger("modulith.proxy")
DEFAULT_MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_CONNECTIONS = 1000
_DOWN_RETRY_SECONDS = 5.0
DEFAULT_IDENTITY_PROBE_TIMEOUT = 30.0
_MAX_HEALTH_BODY_BYTES = 64 * 1024


def _cookieless_jar() -> http.cookiejar.CookieJar:
    """A cookie jar that refuses every cookie, so none is stored or replayed.

    Returned as a bare ``CookieJar``: httpx adopts one as-is, but copies an
    ``httpx.Cookies`` into a new jar with the default, storing policy.
    """
    return http.cookiejar.CookieJar(policy=http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))


def _empty_backend_cycle() -> Iterator[str]:
    return iter(())


@dataclass(frozen=True)
class RoutingRule:
    """One URL-prefix → backend-port mapping, one entry per module replica.

    ``backend_url`` is always the first replica — kept so single-replica
    construction (``RoutingRule(prefix, backend_url)``) and the
    ``/_modulith/topology`` listing are unaffected. ``backend_urls`` holds
    every replica (defaults to just ``backend_url`` when omitted);
    ``next_backend()`` round-robins across whichever of them aren't
    currently marked down — no weights, no stickiness.
    """

    prefix: str  # e.g. "/orders"
    backend_url: str  # e.g. "http://127.0.0.1:9001" (first replica)
    backend_urls: tuple[str, ...] = ()
    _cycle: Iterator[str] = field(default_factory=_empty_backend_cycle, repr=False, compare=False)
    _down: dict[str, float] = field(default_factory=dict, repr=False, compare=False)
    _foreign: dict[str, float] = field(default_factory=dict, repr=False, compare=False)
    _verified: set[str] = field(default_factory=set, repr=False, compare=False)

    def __post_init__(self) -> None:
        urls = self.backend_urls or (self.backend_url,)
        object.__setattr__(self, "backend_urls", urls)
        object.__setattr__(self, "_cycle", cycle(urls))

    def next_backend(self) -> str | None:
        """Round-robin over every replica not currently marked down or foreign.

        Falls back to the replicas not marked foreign once every one is
        marked down — proxying a doomed request (which still answers 502)
        beats a proxy that permanently refuses a module the moment its whole
        fleet blips. A foreign replica (its port answered for another
        deployment) is never returned while its mark is fresh; ``None`` means
        every replica is foreign. An expired foreign mark only makes the
        replica eligible for the identity check that precedes any request.
        """
        now = time.monotonic()
        for _ in range(len(self.backend_urls)):
            candidate = next(self._cycle)
            if self._is_foreign(candidate, now):
                continue
            if candidate in self._down:
                marked_at = self._down[candidate]
                if now - marked_at < _DOWN_RETRY_SECONDS:
                    continue
                del self._down[candidate]
            return candidate
        for _ in range(len(self.backend_urls)):
            candidate = next(self._cycle)
            if not self._is_foreign(candidate, now):
                return candidate
        return None

    def _is_foreign(self, url: str, now: float) -> bool:
        marked_at = self._foreign.get(url)
        return marked_at is not None and now - marked_at < _DOWN_RETRY_SECONDS

    def mark_down(self, url: str) -> None:
        self._down[url] = time.monotonic()
        # Whatever answers on this port next may be a different process.
        self._verified.discard(url)

    def mark_up(self, url: str) -> None:
        self._down.pop(url, None)

    def mark_foreign(self, url: str) -> None:
        self._foreign[url] = time.monotonic()
        self._verified.discard(url)

    def mark_verified(self, url: str) -> None:
        self._verified.add(url)
        self._foreign.pop(url, None)

    def forget_identity(self, url: str) -> None:
        self._verified.discard(url)

    def is_verified(self, url: str) -> bool:
        return url in self._verified


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
    connect_retry_attempts: int = 5,
    connect_retry_backoff: float = 0.2,
    failed_instances: Callable[[], frozenset[str]] | None = None,
    deployment_token: str | None = None,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    identity_probe_timeout: float = DEFAULT_IDENTITY_PROBE_TIMEOUT,
) -> FastAPI:
    """Build the reverse-proxy ASGI app.

    ``client`` (an ``httpx.AsyncClient``) may be injected — for tests, or to
    share a connection pool — and is then used for every upstream call,
    worker ``/health`` probes included. When omitted, the app creates two
    clients and closes them with its lifespan: one for proxied requests,
    capped at ``max_connections`` concurrent upstream connections (each held
    until its response finishes streaming), and a separate small one for
    ``/health`` probes, so a saturated request pool cannot fail readiness.
    A request that finds the pool full for the pool timeout gets 503
    ``"proxy connection pool exhausted"``; the backend is not marked down.
    Both clients ignore ``HTTP_PROXY``/``ALL_PROXY`` and related environment
    variables and always connect to the worker directly.

    ``timeout`` configures the httpx client's request timeout. Defaults to
    ``httpx.Timeout(5.0, read=None)`` — finite connect/write/pool timeouts
    prevent hanging on unreachable backends, while ``read=None`` disables the
    read timeout to support long-polling, streaming responses, and slow
    upstreams. Ignored if ``client`` is injected (the caller owns the client's
    configuration).

    ``connect_retry_attempts`` and ``connect_retry_backoff`` configure bounded
    retry on ``httpx.ConnectError`` (worker port not yet bound during startup/
    respawn). Total worst-case added latency before a truly-dead backend returns
    502 ≈ ``connect_retry_backoff * (connect_retry_attempts - 1)`` ≈ 0.8s with
    defaults. Retries ONLY ConnectError (TCP connection never completed, safe
    to retry any HTTP method); other TransportError types (ConnectTimeout,
    ReadError, etc.) are not retried. Heavy/slow-starting apps can raise the
    budget.

    ``actuator_enabled=False`` (``actuator_mode="disabled"``, resolved by
    ``run_supervised``) unmounts ``/_modulith/*`` entirely — those paths fall
    through to the catch-all proxy handler and answer the same 404 as any
    other unmatched path, so the actuator's existence isn't even revealed.

    ``failed_instances`` (optional) is a callable returning the instance
    names a supervisor's crash-loop breaker has permanently given up on
    (e.g. ``Supervisor.failed_instances``) — ``/_modulith/health`` uses it to
    report such a module as ``"failed (given up)"`` instead of the generic
    ``"unreachable"`` a worker mid-restart-backoff also produces.

    ``deployment_token`` (set by ``run_supervised``) is the value this
    deployment's workers echo as ``"deployment"`` on their ``/health``. A
    backend answering without it — another deployment's worker, or any other
    process holding the port — is reported ``"foreign deployment"`` and marked
    foreign: skipped by routing, including the all-down fallback, until
    re-verified. Identity is checked before a backend's first request, again
    after it was marked down or foreign, and after ``forget_identity`` (which
    ``run_supervised`` calls whenever the supervisor respawns that worker).
    The token tells this deployment's workers from another deployment's after
    an accidental port collision; it is not a secret and does not defend
    against a hostile local process. Each identity probe is bounded by
    ``identity_probe_timeout`` seconds in total and 64 KiB of body; past the
    deadline the request gets 504 and the backend is not marked down.

    No client stores upstream cookies: both owned clients, and an injected
    ``client`` (whose jar is replaced), refuse every ``Set-Cookie``, so one
    user's cookie is never replayed for another or sent on a probe.
    """
    owns_client = client is None
    if timeout is None:
        timeout = httpx.Timeout(5.0, read=None)
    # trust_env=False: httpx would otherwise route loopback traffic through an
    # HTTP_PROXY/ALL_PROXY from the environment (NO_PROXY=localhost does not
    # cover 127.0.0.1), handing the client's credentials to that proxy.
    http_client: Any = (
        client
        if client is not None
        else httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=max_connections),
            trust_env=False,
        )
    )
    # Worker /health probes get their own pool so request traffic that fills
    # the main pool cannot fail readiness or identity checks.
    probe_client: Any = (
        client if client is not None else httpx.AsyncClient(timeout=2.0, trust_env=False)
    )
    # One jar serves every end user's requests and every probe; a stored
    # upstream Set-Cookie would be replayed for other users. Replacing an
    # injected client's jar is part of the ``client`` contract (docstring).
    http_client.cookies = _cookieless_jar()
    probe_client.cookies = _cookieless_jar()

    async def read_health(url: str) -> httpx.Response:
        """GET ``url``/health, keeping at most ``_MAX_HEALTH_BODY_BYTES`` of it.

        An oversized body is replaced by an empty one, which answers for no
        deployment. The read timeout is off: the caller bounds the total time.
        """
        async with probe_client.stream(
            "GET", url + "/health", timeout=httpx.Timeout(5.0, read=None)
        ) as resp:
            body = bytearray()
            async for chunk in resp.aiter_bytes():
                body += chunk
                if len(body) > _MAX_HEALTH_BODY_BYTES:
                    return httpx.Response(resp.status_code)
            return httpx.Response(resp.status_code, content=bytes(body))

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        if owns_client:
            await http_client.aclose()
            await probe_client.aclose()

    # ``openapi_url=None`` unregisters FastAPI's own /openapi.json, /docs,
    # /docs/oauth2-redirect and /redoc. A reverse proxy must not claim paths it
    # cannot answer for the application: those routes are registered ahead of
    # the catch-all below, so /openapi.json served a schema holding only this
    # proxy's actuator routes (never the application's), /docs rendered a
    # Swagger UI over that empty schema, and a module actually named ``docs``
    # had its own routes shadowed outright. Unregistered, the four paths fall
    # through to the catch-all and behave like any other path — proxied when a
    # rule matches, 404 otherwise. Each worker still serves its own schema on
    # its internal port; the proxy publishes no aggregate one. ``app.openapi()``
    # remains callable, so a caller that wants the proxy's own schema can build
    # it in-process.
    app = FastAPI(title="modulith-proxy", lifespan=lifespan, openapi_url=None)
    app.add_middleware(_RejectUnsafeTargets)

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

    def record_identity(rule: RoutingRule, url: str, health: httpx.Response) -> bool:
        """Mark ``url`` verified or foreign from its ``/health`` answer."""
        if deployment_token is None:
            return True
        if _answers_for(health, deployment_token):
            rule.mark_verified(url)
            return True
        rule.mark_foreign(url)
        logger.warning(
            "%s answers /health for another deployment (or is not a modulith "
            "worker): another process holds this worker port; not routing to it",
            url,
        )
        return False

    async def confirm_identity(rule: RoutingRule, url: str) -> bool:
        """Probe an unverified backend before it is sent any request.

        Transport errors propagate so the caller's connect-retry and 502
        handling apply to the probe exactly as to the request itself. A probe
        still unanswered after ``identity_probe_timeout`` raises
        ``TimeoutError`` and leaves the backend unverified but not down.
        """
        if deployment_token is None or rule.is_verified(url):
            return True
        health = await asyncio.wait_for(read_health(url), identity_probe_timeout)
        return record_identity(rule, url, health)

    def identity_timed_out(url: str) -> JSONResponse:
        logger.warning(
            "%s did not answer /health within %ss; identity unverified",
            url,
            identity_probe_timeout,
        )
        return JSONResponse({"detail": "worker identity check timed out"}, status_code=504)

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
            """Readiness: aggregates every replica's own ``/health``.

            A module is ``ok`` if at least one of its replicas answers
            healthy (the proxy already round-robins request traffic to only
            the surviving ones); it's ``unhealthy``/``unreachable`` only once
            every replica is down. A module whose crash-loop breaker has
            given up on all its replicas (``failed_instances``) is reported
            as ``"failed (given up)"`` instead of the generic
            ``"unreachable"`` a worker mid-restart-backoff also produces.

            Returns 503 when any module is not ``ok`` — a standard readiness
            contract (orchestrators stop routing traffic to a 503 instance)
            that a constant 200 could never express.
            """
            denied = _actuator_auth_response(request)
            if denied is not None:
                return denied

            async def check_one(rule: RoutingRule) -> tuple[str, str]:
                async def check_backend(url: str) -> str:
                    try:
                        resp = await asyncio.wait_for(read_health(url), 2.0)
                    except Exception:
                        rule.mark_down(url)
                        return "unreachable"
                    if not record_identity(rule, url, resp):
                        return "foreign deployment"
                    if resp.status_code == 200:
                        rule.mark_up(url)
                        return "ok"
                    rule.mark_down(url)
                    return "unhealthy"

                statuses = await asyncio.gather(*(check_backend(url) for url in rule.backend_urls))
                if "ok" in statuses:
                    return rule.prefix, "ok"
                if "foreign deployment" in statuses:
                    return rule.prefix, "foreign deployment"
                if "unhealthy" in statuses:
                    return rule.prefix, "unhealthy"
                if failed_instances is not None and _module_has_given_up(
                    rule.prefix, failed_instances()
                ):
                    return rule.prefix, "failed (given up)"
                return rule.prefix, "unreachable"

            # Concurrent fan-out: total latency ~max(per-backend latency), not
            # the sum — a sequential loop scales O(N * per-backend timeout).
            backends = dict(await asyncio.gather(*(check_one(rule) for rule in rules)))
            overall = "ok" if all(state == "ok" for state in backends.values()) else "degraded"
            body = {"status": overall, "backends": backends}
            if overall != "ok":
                return JSONResponse(body, status_code=503)
            return body

    # ``include_in_schema=False``: FastAPI derives one operation id per *route*
    # but emits one OpenAPI operation per *method*, so a single multi-method
    # route yields seven operations sharing one id and FastAPI warns
    # ("Duplicate Operation ID") once per collision while building the schema.
    # A catch-all that forwards opaque bytes has nothing to describe anyway.
    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def proxy(request: Request, path: str) -> Response:
        # scope["raw_path"]/["query_string"] carry the exact bytes the client
        # sent, still percent-encoded. request.url.path/.query are built from
        # scope["path"] — already percent-*decoded* per the ASGI spec — so an
        # encoded separator inside a segment (e.g. "%2F" meaning a literal
        # slash within one segment, not a path boundary) would be silently
        # turned into a real "/" and change how many segments the upstream
        # sees. Forwarding the raw bytes preserves the client's exact request.
        # Rule matching runs on the decoding of those same bytes, so the rule
        # that matched and the path that is forwarded cannot disagree.
        # _RejectUnsafeTargets has already answered 400 for any target that
        # does not start with "/" or holds a dot segment.
        raw_path = _raw_path(request.scope)
        target = unquote(raw_path.decode("latin-1"))
        rule = _match_rule(target, rules)
        if rule is None:
            return JSONResponse({"detail": f"no worker route for {target!r}"}, status_code=404)

        # One backend per request, round-robin across every replica of this
        # module (skipping any marked down by a failed health check or a
        # prior connect failure) — see RoutingRule.next_backend().
        backend = rule.next_backend()
        # Skip past replicas whose port answers for another deployment before
        # any request bytes (cookies, auth headers) reach them. An unreachable
        # probe leaves the backend unverified; the send loop re-probes it.
        for _ in range(len(rule.backend_urls)):
            if backend is None:
                break
            try:
                if await confirm_identity(rule, backend):
                    break
            except TimeoutError:
                return identity_timed_out(backend)
            except httpx.HTTPError:
                break
            backend = rule.next_backend()
        if backend is None:
            return JSONResponse(
                {"detail": f"no worker of this deployment serves {rule.prefix}"},
                status_code=503,
            )
        query_string = request.scope.get("query_string", b"")
        # Log-only rendering; never parsed. The request itself is built from
        # components by _upstream_url.
        upstream = backend + raw_path.decode("latin-1")

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
        # header twice (e.g. two Cookie lines) forwards both — dict(request.
        # headers) keeps only one of any repeated name and silently drops the
        # rest.
        fwd_headers = _filter_headers(_header_pairs(request.headers.raw))
        # Drop the client's Host so httpx sets it to the loopback worker's
        # authority. Forwarding the external Host (e.g. api.example.com) makes
        # workers behave as if internet-facing for URL generation / vhost /
        # Host-allowlist logic — a reverse-proxy correctness/security smell.
        # Also drop any client-supplied forwarding headers: workers spawned by
        # the supervisor bind loopback-only and trust X-Forwarded-* from that
        # peer unconditionally (uvicorn's proxy_headers/forwarded_allow_ips
        # defaults), so an unfiltered client value would let any caller spoof
        # its own IP, scheme, host and port to every module. Overwrite with
        # values the proxy itself observed on this connection instead.
        fwd_headers = [
            (k, v) for k, v in fwd_headers if k.lower() not in _DROPPED_FORWARDING_HEADERS
        ]
        client_host = request.client.host if request.client is not None else ""
        port = request.url.port or (443 if request.url.scheme == "https" else 80)
        fwd_headers.extend(
            [
                ("x-forwarded-for", client_host),
                ("x-forwarded-proto", request.url.scheme),
                ("x-forwarded-host", request.headers.get("host", "")),
                ("x-forwarded-port", str(port)),
            ]
        )
        try:
            upstream_req = http_client.build_request(
                method=request.method,
                url=_upstream_url(backend, raw_path, query_string),
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
                upstream,
                exc,
            )
            return JSONResponse({"detail": "invalid request"}, status_code=400)
        attempts = max(1, connect_retry_attempts)
        for attempt in range(attempts):
            try:
                if not await confirm_identity(rule, backend):
                    return JSONResponse(
                        {"detail": f"no worker of this deployment serves {rule.prefix}"},
                        status_code=503,
                    )
                upstream_resp = await http_client.send(upstream_req, stream=True)
            except TimeoutError:
                return identity_timed_out(backend)
            except httpx.ConnectError as exc:
                # Worker port not bound yet (initial start or crash-respawn
                # window). The TCP connection never completed, so no request
                # bytes were sent — safe to retry any method. Bounded budget,
                # then fall through to 502.
                if attempt + 1 >= attempts:
                    rule.mark_down(backend)
                    logger.warning(
                        "backend %s unreachable after %d connect attempts for %s: %s",
                        backend,
                        attempts,
                        upstream,
                        exc,
                    )
                    return JSONResponse({"detail": "backend unreachable"}, status_code=502)
                await asyncio.sleep(connect_retry_backoff)
                continue
            except httpx.PoolTimeout:
                # Every pooled connection is busy with another in-flight
                # response. The backend is fine; this proxy is at capacity.
                logger.warning(
                    "proxy connection pool exhausted (%s connections in use) for %s",
                    max_connections,
                    upstream,
                )
                return JSONResponse({"detail": "proxy connection pool exhausted"}, status_code=503)
            except httpx.TransportError as exc:
                # TransportError covers the whole connect/read failure tree —
                # ConnectError (refused/DNS), ConnectTimeout (reachable but
                # unresponsive), ReadError/ReadTimeout/RemoteProtocolError (worker
                # died mid-handshake). All mean "backend unavailable" → 502, never
                # an uncaught 500.
                rule.mark_down(backend)
                logger.warning(
                    "backend %s unreachable for %s: %s",
                    backend,
                    upstream,
                    exc,
                )
                return JSONResponse({"detail": "backend unreachable"}, status_code=502)
            except httpx.RequestError as exc:
                # RequestError siblings outside the TransportError subtree —
                # httpx.TooManyRedirects (a redirect-looping backend behind an
                # injected follow_redirects=True client) and
                # httpx.DecodingError. Both mean "no valid response could be
                # obtained from the backend" → 502, honoring the
                # never-an-uncaught-500 contract documented above.
                logger.warning(
                    "backend %s returned no usable response for %s: %s",
                    rule.backend_url,
                    upstream,
                    exc,
                )
                return JSONResponse({"detail": "backend error"}, status_code=502)
            # success: this backend answered — clear any prior down-marking.
            rule.mark_up(backend)
            response = StreamingResponse(
                _safe_stream(upstream_resp, upstream),
                status_code=upstream_resp.status_code,
            )
            # Passing headers= to StreamingResponse builds a plain dict internally
            # (Response.init_headers), which loses duplicates the same way as on
            # the request side (e.g. multiple Set-Cookie). Setting raw_headers
            # directly after construction preserves every occurrence.
            response.raw_headers = [
                (k.lower().encode("latin-1"), v.encode("latin-1"))
                for k, v in _relativize_location(
                    _filter_headers(_header_pairs(upstream_resp.headers.raw)),
                    rule.backend_url,
                )
            ]
            return response
        # Loop always returns above; satisfies the type checker.
        return JSONResponse({"detail": "backend unreachable"}, status_code=502)

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

# Client-supplied forwarding/identity headers, dropped and re-derived from the
# proxy's own view of the connection (see the comment in ``proxy()``) — a
# worker must never see a value an external caller chose for these.
_DROPPED_FORWARDING_HEADERS = frozenset(
    {
        "host",
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-forwarded-host",
        "x-forwarded-port",
        "forwarded",
        "x-real-ip",
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


def _relativize_location(headers: list[tuple[str, str]], backend_url: str) -> list[tuple[str, str]]:
    """Strip the worker's own authority off a ``Location`` redirect.

    Workers see the loopback authority as their Host (the client's is dropped
    on the way in), so any absolute URL they generate — most commonly
    Starlette's default trailing-slash redirect, but also ``url_for`` and
    OpenAPI ``servers`` — points at ``http://127.0.0.1:<worker port>``, which
    the client cannot follow. The proxy forwards the full path including the
    module prefix, so the worker's path is already the public one: dropping
    scheme+authority yields a relative Location the client resolves against
    the authority it actually asked for. A redirect to anywhere else (an
    external site) is left alone.
    """
    prefix = backend_url.rstrip("/")
    return [
        (name, value[len(prefix) :] or "/")
        if name.lower() == "location" and (value == prefix or value.startswith(prefix + "/"))
        else (name, value)
        for name, value in headers
    ]


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


def _raw_path(scope: Scope) -> bytes:
    """The client's still-encoded path; rebuilt from ``path`` if the server omits it."""
    raw: bytes | None = scope.get("raw_path")
    return raw or quote(scope["path"]).encode("ascii")


class _RejectUnsafeTargets:
    """Answer 400 for a request-target the proxy must not route or forward.

    Runs ahead of routing, so it covers the actuator routes as well as the
    catch-all. A target that does not start with ``/`` (which uvicorn's h11
    parser passes through) would otherwise either miss every route or name a
    different authority once appended to a backend URL.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            raw_path = _raw_path(scope)
            if not raw_path.startswith(b"/") or _has_dot_segment(
                unquote(raw_path.decode("latin-1"))
            ):
                response = JSONResponse({"detail": "invalid request target"}, status_code=400)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def _has_dot_segment(path: str) -> bool:
    """Whether the decoded ``path`` has a ``.`` or ``..`` segment.

    URL normalization (httpx's included) removes such segments, so a request
    matched on ``/orders/..`` would reach the worker as something outside
    ``/orders``. Checking the fully decoded path also covers ``%2e`` and
    ``%2F``-joined forms.
    """
    return any(segment in (".", "..") for segment in path.split("/"))


def _upstream_url(backend: str, raw_path: bytes, query: bytes) -> httpx.URL:
    """The backend URL with the client's raw path and query as components.

    Built from components rather than concatenated text, so no request-target
    can change the scheme, host or port; the check below enforces that.
    """
    base = httpx.URL(backend)
    raw = base.raw_path.rstrip(b"/") + raw_path
    if query:
        raw += b"?" + query
    url = base.copy_with(raw_path=raw)
    if (url.scheme, url.host, url.port) != (base.scheme, base.host, base.port):
        raise httpx.InvalidURL(f"upstream URL left backend {backend}")
    return url


def _answers_for(resp: httpx.Response, deployment_token: str) -> bool:
    """Whether a worker ``/health`` response echoes this deployment's token."""
    try:
        echoed = resp.json().get("deployment")
    except (ValueError, AttributeError):
        return False
    return isinstance(echoed, str) and hmac.compare_digest(
        echoed.encode("utf-8"), deployment_token.encode("utf-8")
    )


def _module_has_given_up(prefix: str, failed: frozenset[str]) -> bool:
    """Whether any instance backing ``prefix`` is in the breaker's give-up set.

    Only consulted once every replica has already failed its live ``/health``
    probe (see ``check_one``), so this only ever upgrades an already-total
    ``"unreachable"`` into the more informative ``"failed (given up)"`` — it
    never masks a module that still has a healthy or merely-restarting
    replica.
    """
    module_name = prefix.lstrip("/")
    return any(name == module_name or name.startswith(f"{module_name}-") for name in failed)


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
