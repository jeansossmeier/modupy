"""Regression tests for proxy + supervisor audit findings.

Each test cites the audit finding id it reproduces. The proxy tests drive the
real ASGI app through ``httpx.ASGITransport`` (no sockets, no mocks of the
code under test); supervisor lifecycle tests spawn real subprocesses and are
marked ``integration`` like the rest of the lifecycle suite.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import sys
import textwrap
import time
from collections.abc import AsyncIterator, MutableMapping
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from modulith.proxy import RoutingRule, create_proxy_app
from modulith.supervisor import (
    Supervisor,
    WorkerSpec,
    _build_worker_env,
    _resolve_actuator,
    derive_specs_from_config,
    run_supervised,
)

# ---------------------------------------------------------------------------
# A8-r1-25 — body-size cap must be enforced while streaming, not after
# ---------------------------------------------------------------------------


async def test_proxy_aborts_oversized_chunked_body_before_buffering_it() -> None:
    """A8-r1-25: a chunked upload (no Content-Length) must be rejected with
    413 as soon as the accumulated size exceeds max_request_body_bytes —
    without first materializing the whole body in memory."""
    upstream = FastAPI()

    @upstream.post("/orders/echo")
    async def echo() -> dict[str, bool]:  # pragma: no cover - must not be hit
        return {"reached": True}

    proxy_app = create_proxy_app(
        [RoutingRule(prefix="/orders", backend_url="http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream)),
        max_request_body_bytes=64,
    )

    total_chunks = 1000
    consumed = 0

    async def body_gen() -> AsyncIterator[bytes]:
        nonlocal consumed
        for _ in range(total_chunks):
            consumed += 1
            yield b"x" * 32  # 32 KiB total, 512x over the 64-byte cap

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as client:
        # An async-iterable body is sent chunked, with no Content-Length —
        # bypassing the header fast-path entirely.
        resp = await client.post("/orders/echo", content=body_gen())

    assert resp.status_code == 413
    # The cap (64 bytes) is crossed on the 3rd 32-byte chunk; the proxy must
    # stop reading there instead of buffering all 1000 chunks first.
    assert consumed < total_chunks


# ---------------------------------------------------------------------------
# A8-r2-93 / S3-r1-62 — build_request failures must not escape as raw 500s
# ---------------------------------------------------------------------------


def _echo_upstream() -> FastAPI:
    up = FastAPI()

    @up.get("/orders/ping")
    async def ping() -> dict[str, bool]:
        return {"pong": True}

    return up


@pytest.mark.parametrize("encoded", ["%00", "%7f"])
async def test_proxy_forwards_percent_encoded_control_bytes_upstream(encoded: str) -> None:
    """Task 7 (supersedes A8-r2-93): building the upstream URL from
    ``request.url.path`` (already percent-*decoded*) made httpx.build_request
    raise InvalidURL for a decoded non-printable byte — an uncaught 500.
    Task 7 forwards the client's exact raw, still-encoded bytes
    (``scope["raw_path"]``) instead, so httpx never sees a decoded control
    byte and the request reaches the upstream intact — which is the correct
    reverse-proxy behavior (let the backend decide), not a 400 rejection."""
    proxy_app = create_proxy_app(
        [RoutingRule(prefix="/orders", backend_url="http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_echo_upstream())),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as client:
        # No exception may escape the app either way; the request is
        # genuinely forwarded, so the upstream's own 404 (no such route)
        # comes back — not the proxy's build-time 400.
        resp = await client.get(f"/orders/{encoded}")
        control = await client.get("/orders/ping")

    assert resp.status_code == 404
    assert control.status_code == 200  # the app keeps serving normally


async def test_proxy_maps_non_ascii_header_bytes_to_clean_400() -> None:
    """S3-r1-62: a forwarded header carrying a raw non-ASCII octet (latin-1
    decoded by the ASGI server) makes build_request raise UnicodeEncodeError
    outside the try/except — an unhandled 500. Must be a clean 400 instead."""
    proxy_app = create_proxy_app(
        [RoutingRule(prefix="/orders", backend_url="http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_echo_upstream())),
    )

    # httpx refuses to *send* non-ASCII headers, so drive the raw ASGI app
    # with a hand-built scope — exactly what a real HTTP/1.1 server hands the
    # app for a header line containing the octet 0xE9.
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/orders/ping",
        "raw_path": b"/orders/ping",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"proxy"), (b"x-evil", b"\xe9value")],
        "client": ("127.0.0.1", 12345),
        "server": ("proxy", 80),
    }
    messages: list[MutableMapping[str, Any]] = []

    async def receive() -> MutableMapping[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: MutableMapping[str, Any]) -> None:
        messages.append(message)

    # The exception must not propagate out of the ASGI call...
    await proxy_app(scope, receive, send)
    # ...and the response the client sees must be the proxy's own 400, not
    # ServerErrorMiddleware's 500.
    start = next(m for m in messages if m["type"] == "http.response.start")
    assert start["status"] == 400


# ---------------------------------------------------------------------------
# A8-r4-181 — /_modulith/health must fan out concurrently, not sequentially
# ---------------------------------------------------------------------------


async def test_health_actuator_checks_backends_concurrently() -> None:
    """A8-r4-181: the health fan-out awaited each backend in a plain for
    loop, so total latency scaled O(N * per-backend latency). Rendezvous
    barrier: every /health handler waits until ALL checks have arrived, then
    answers ok. Sequential checks can never rendezvous (the first would block
    forever); only a concurrent fan-out turns the overall status "ok"."""
    n_backends = 3
    arrived: list[int] = []
    all_arrived = asyncio.Event()

    upstream = FastAPI()

    @upstream.get("/health")
    async def health() -> dict[str, str]:
        arrived.append(1)
        if len(arrived) >= n_backends:
            all_arrived.set()
        # Bounded wait: under sequential dispatch this times out (ASGI
        # transport enforces no client timeout) instead of hanging the test.
        await asyncio.wait_for(all_arrived.wait(), timeout=2.0)
        return {"status": "ok"}

    rules = [
        RoutingRule(prefix=f"/mod{i}", backend_url=f"http://backend-{i}") for i in range(n_backends)
    ]
    proxy_app = create_proxy_app(
        rules, client=httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream))
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
    ) as client:
        resp = await client.get("/_modulith/health")

    body = resp.json()
    assert body["status"] == "ok"
    assert all(state == "ok" for state in body["backends"].values())


# ---------------------------------------------------------------------------
# A8-r1-28 — actuator token comparison (behavioral regression)
# ---------------------------------------------------------------------------


def test_actuator_token_rejects_wrong_and_partial_tokens() -> None:
    """A8-r1-28: the comparison moved from `==` to hmac.compare_digest (the
    timing property itself is not testable deterministically); this pins the
    behavioral contract — wrong, prefix-matching, and absent tokens are all
    401, the exact token is 200."""
    proxy_app = create_proxy_app(
        [RoutingRule(prefix="/orders", backend_url="http://orders-worker")],
        client=httpx.AsyncClient(transport=httpx.ASGITransport(app=_echo_upstream())),
        actuator_token="secret-token",
    )
    with TestClient(proxy_app) as client:
        assert client.get("/_modulith/topology").status_code == 401
        assert (
            client.get(
                "/_modulith/topology", headers={"authorization": "Bearer secret-tokex"}
            ).status_code
            == 401
        )
        assert (
            client.get(
                "/_modulith/topology", headers={"authorization": "Bearer secret"}
            ).status_code
            == 401
        )
        assert (
            client.get(
                "/_modulith/topology", headers={"authorization": "Bearer secret-token"}
            ).status_code
            == 200
        )


# ---------------------------------------------------------------------------
# A8-r1-27 — actuator token must be reachable from a real config surface
# ---------------------------------------------------------------------------


class _NoopSupervisor(Supervisor):
    """Real Supervisor type (satisfies run_supervised's annotation) that
    skips subprocess management — these tests only inspect the proxy app."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


async def test_run_supervised_wires_actuator_token_from_env(monkeypatch) -> None:
    """A8-r1-27: create_proxy_app's actuator_token existed but no production
    call path could ever set it — run_supervised built the proxy without it,
    leaving actuator endpoints permanently unauthenticated. The
    MODULITH_ACTUATOR_TOKEN env var must reach the proxy app."""
    monkeypatch.setenv("MODULITH_ACTUATOR_TOKEN", "env-secret")
    captured: dict[str, Any] = {}

    async def fake_serve(app: object, host: str, port: int) -> None:
        captured["app"] = app

    await run_supervised(
        [WorkerSpec("orders", "app", 9001)],
        "127.0.0.1",
        8000,
        supervisor=_NoopSupervisor([]),
        serve=fake_serve,
    )

    with TestClient(captured["app"]) as client:
        denied = client.get("/_modulith/topology")
        allowed = client.get("/_modulith/topology", headers={"authorization": "Bearer env-secret"})
    assert denied.status_code == 401
    assert allowed.status_code == 200


async def test_run_supervised_without_token_leaves_actuator_open(monkeypatch) -> None:
    """A8-r1-27 (companion): no env var, no kwarg → actuator stays open, the
    documented default."""
    monkeypatch.delenv("MODULITH_ACTUATOR_TOKEN", raising=False)
    captured: dict[str, Any] = {}

    async def fake_serve(app: object, host: str, port: int) -> None:
        captured["app"] = app

    await run_supervised(
        [WorkerSpec("orders", "app", 9001)],
        "127.0.0.1",
        8000,
        supervisor=_NoopSupervisor([]),
        serve=fake_serve,
    )

    with TestClient(captured["app"]) as client:
        assert client.get("/_modulith/topology").status_code == 200


# ---------------------------------------------------------------------------
# A8-r3-141 — a 0/negative worker count must fail loudly, not collide ports
# ---------------------------------------------------------------------------


def test_derive_specs_rejects_zero_worker_count(make_fake_app) -> None:
    """A8-r3-141: derive_specs_from_config advanced the port counter by the
    raw (unclamped) count while _instance_plan clamps to max(1, count) — a
    workers.<module>=0 entry silently assigned two modules the same port.
    Loud-config-error contract: reject counts < 1 at derivation time."""
    make_fake_app({"orders": "", "inventory": "", "reports": ""})

    with pytest.raises(ValueError, match="orders"):
        derive_specs_from_config({"package": "fakeapp", "workers": {"default": 1, "orders": 0}})


def test_derive_specs_rejects_negative_worker_count(make_fake_app) -> None:
    """A8-r3-141: a negative count ran the port counter *backward*, assigning
    a later module a port before an earlier one. Must raise instead."""
    make_fake_app({"a": "", "b": "", "c": ""})

    with pytest.raises(ValueError, match="b"):
        derive_specs_from_config({"package": "fakeapp", "workers": {"b": -2}})


def test_derive_specs_rejects_non_positive_default_count(make_fake_app) -> None:
    """A8-r3-141: the shared `default` count gets the same >= 1 validation."""
    make_fake_app({"orders": ""})

    with pytest.raises(ValueError, match="default"):
        derive_specs_from_config({"package": "fakeapp", "workers": {"default": 0}})


# ---------------------------------------------------------------------------
# A8-r1-29 — completed log-forwarder tasks must not accumulate until stop()
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_log_forwarder_tasks_are_pruned_when_worker_exits() -> None:
    """A8-r1-29: every (re)spawn appended two log-forwarder tasks to
    Supervisor._log_tasks and nothing removed finished entries outside
    stop() — a crash-looping worker grew the list without bound. Completed
    forwarders must drop out of the tracking collection on their own."""
    spec = WorkerSpec("orders", "fakeapp", 9001)
    sup = Supervisor([spec], command_builder=lambda s, p: [sys.executable, "-c", "print('bye')"])

    proc = await sup._spawn("orders", spec, 9001)
    await proc.wait()
    # Both forwarders (stdout + stderr) hit EOF once the process is gone;
    # drain them, give done-callbacks one loop tick, then the tracking
    # collection must be empty — no stop() involved.
    await asyncio.gather(*list(sup._log_tasks), return_exceptions=True)
    await asyncio.sleep(0)

    assert len(sup._log_tasks) == 0


# ---------------------------------------------------------------------------
# A8-r5-213 — a worker respawned during stop() must still get SIGTERM
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_worker_respawned_during_stop_still_gets_sigterm() -> None:
    """A8-r5-213: stop()'s SIGTERM cascade is a one-shot snapshot of
    self._processes. A monitor that is *inside* _spawn (respawning a crashed
    worker) when stop() runs registers its process after the snapshot — that
    worker was never SIGTERM'd, the monitor blocked on it for the full
    shutdown_timeout, and the final reap SIGKILLed it. The respawned worker
    must exit via SIGTERM, gracefully."""
    spawn_entered = asyncio.Event()
    spawn_gate = asyncio.Event()
    spawn_calls: list[int] = []

    class _GatedSupervisor(Supervisor):
        """Real Supervisor whose respawn path can be held at a barrier so the
        test can run stop()'s cascade at exactly the racy moment."""

        async def _spawn(
            self, name: str, spec: WorkerSpec, port: int
        ) -> asyncio.subprocess.Process:
            spawn_calls.append(1)
            if len(spawn_calls) >= 2:  # the respawn, not the initial spawn
                spawn_entered.set()
                await spawn_gate.wait()
            return await super()._spawn(name, spec, port)

    def builder(spec: WorkerSpec, port: int) -> list[str]:
        if len(spawn_calls) <= 1:  # initial worker crashes immediately
            return [sys.executable, "-c", "import sys; sys.exit(7)"]
        return [sys.executable, "-c", "import time; time.sleep(30)"]

    sup = _GatedSupervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=builder,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        shutdown_timeout=5.0,
    )
    await sup.start()
    # Deterministic rendezvous: the monitor saw the crash, slept its backoff,
    # and is now held inside the respawn — before process registration.
    await asyncio.wait_for(spawn_entered.wait(), timeout=10.0)

    stop_task = asyncio.create_task(sup.stop())
    await asyncio.sleep(0)  # let stop() run its synchronous SIGTERM pass
    assert sup._stopping
    spawn_gate.set()  # respawn proceeds and registers AFTER the cascade
    await stop_task

    proc = sup._processes["orders"]
    # Graceful shutdown: SIGTERM (-15), never only the SIGKILL (-9) reap.
    assert proc.returncode == -signal.SIGTERM


# ---------------------------------------------------------------------------
# A8-r1-26 — SIGKILL of the supervisor must not orphan worker processes
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux-only")
async def test_workers_die_when_supervisor_is_sigkilled(tmp_path: Path) -> None:
    """A8-r1-26: workers were spawned with no parent-death signal, so a
    SIGKILL'd (OOM-killed) supervisor orphaned them; the orphan kept its
    statically-assigned port bound and permanently blocked the module's
    restart under a fresh supervisor. With PR_SET_PDEATHSIG the worker must
    exit shortly after its supervisor dies."""
    runner = tmp_path / "runner.py"
    runner.write_text(
        textwrap.dedent(
            """
            import asyncio, sys
            from modulith.supervisor import Supervisor, WorkerSpec

            async def main():
                sup = Supervisor(
                    [WorkerSpec("w", "pkg", 9001)],
                    command_builder=lambda s, p: [
                        sys.executable, "-c", "import time; time.sleep(60)",
                    ],
                )
                await sup.start()
                proc = next(iter(sup._processes.values()))
                print(proc.pid, flush=True)
                await asyncio.sleep(60)

            asyncio.run(main())
            """
        )
    )
    worker_pid: int | None = None
    runner_proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(runner),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert runner_proc.stdout is not None
        line = await asyncio.wait_for(runner_proc.stdout.readline(), timeout=15.0)
        worker_pid = int(line)
        os.kill(worker_pid, 0)  # worker is alive under the live supervisor

        runner_proc.kill()  # SIGKILL: Supervisor.stop() never runs
        await runner_proc.wait()

        # PDEATHSIG delivers SIGTERM to the worker; bounded poll (max 5s)
        # for it to vanish rather than sleeping a fixed interval.
        for _ in range(200):
            try:
                os.kill(worker_pid, 0)
            except ProcessLookupError:
                break
            await asyncio.sleep(0.025)
        else:
            pytest.fail(f"worker {worker_pid} survived the supervisor's SIGKILL — orphaned")
    finally:
        if runner_proc.returncode is None:
            runner_proc.kill()
            await runner_proc.wait()
        if worker_pid is not None:
            try:
                os.kill(worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


# ---------------------------------------------------------------------------
# Task 7 — socket-level proxy tests
#
# Every other proxy test drives the ASGI app in-process via ASGITransport,
# which never exercises a real HTTP/1.1 wire parser. These tests run the
# proxy AND its upstream as real uvicorn servers talking over real loopback
# TCP sockets, so header duplication and raw path encoding survive an actual
# wire round-trip (h11's request parsing), not just Starlette's in-process
# Request object.
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


async def _serve_app_over_socket(app: Any, port: int) -> asyncio.Task[None]:
    """Bind ``app`` to a real loopback socket via uvicorn; block until reachable."""
    import uvicorn

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    deadline = time.monotonic() + 15.0
    async with httpx.AsyncClient() as probe:
        while True:
            try:
                await probe.get(f"http://127.0.0.1:{port}/", timeout=1.0)
                break
            except httpx.TransportError:
                assert not task.done(), f"server on port {port} died: {task.exception()!r}"
                assert time.monotonic() < deadline, f"server on port {port} never came up"
                await asyncio.sleep(0.05)
    return task


async def _stop_socket_server(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _raw_echo_upstream() -> Any:
    """A raw ASGI app (no FastAPI routing) that echoes exactly what it received.

    Bypassing FastAPI/Starlette's own request parsing on the upstream side
    means the response reports precisely the bytes h11 handed to the ASGI
    scope — the ground truth for what actually crossed the wire.
    """

    async def app(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        body_chunks: list[bytes] = []
        while True:
            message = await receive()
            body_chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        import json

        payload = json.dumps(
            {
                "raw_path": scope["raw_path"].decode("latin-1"),
                "query_string": scope["query_string"].decode("latin-1"),
                "headers": [
                    [k.decode("latin-1"), v.decode("latin-1")] for k, v in scope["headers"]
                ],
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": payload})

    return app


@pytest.mark.integration
async def test_proxy_preserves_duplicate_headers_and_raw_path_over_real_sockets() -> None:
    """Task 7: real end-to-end proof (not ASGITransport) that the proxy (a)
    preserves every occurrence of a repeated header name and (b) forwards an
    encoded-slash path segment (``%2F``) as the exact bytes the client sent,
    rather than the decoded (and therefore re-segmented) path."""
    upstream_port = _free_port()
    proxy_port = _free_port()

    upstream_task = await _serve_app_over_socket(_raw_echo_upstream(), upstream_port)
    try:
        proxy_app = create_proxy_app(
            [RoutingRule(prefix="/orders", backend_url=f"http://127.0.0.1:{upstream_port}")],
        )
        proxy_task = await _serve_app_over_socket(proxy_app, proxy_port)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"http://127.0.0.1:{proxy_port}/orders/a%2Fb?x=1",
                    headers=[("cookie", "a=1"), ("cookie", "b=2")],
                )
        finally:
            await _stop_socket_server(proxy_task)
    finally:
        await _stop_socket_server(upstream_task)

    assert resp.status_code == 200
    body = resp.json()
    # The encoded slash must survive as one segment ("a%2Fb"), not be
    # decoded into "a/b" (which would look like two path segments upstream).
    assert body["raw_path"] == "/orders/a%2Fb"
    assert body["query_string"] == "x=1"
    cookie_values = [v for k, v in body["headers"] if k.lower() == "cookie"]
    assert cookie_values == ["a=1", "b=2"]  # both occurrences forwarded, not just one


@pytest.mark.integration
async def test_proxy_actuator_reachable_over_real_sockets() -> None:
    """Task 7: sanity check that the actuator endpoints themselves — not just
    the catch-all proxy route — work over a genuine TCP round trip."""
    proxy_port = _free_port()
    proxy_app = create_proxy_app([RoutingRule(prefix="/orders", backend_url="http://127.0.0.1:1")])
    task = await _serve_app_over_socket(proxy_app, proxy_port)
    try:
        async with httpx.AsyncClient() as client:
            live = await client.get(f"http://127.0.0.1:{proxy_port}/_modulith/live")
    finally:
        await _stop_socket_server(task)

    assert live.status_code == 200
    assert live.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Task 7 — liveness vs readiness must be genuinely distinct
# ---------------------------------------------------------------------------


async def test_liveness_stays_ok_while_readiness_reports_degraded() -> None:
    """Task 7: /_modulith/live must answer 200 regardless of backend health —
    it only asserts "this proxy process is up" — while /_modulith/health (the
    readiness contract) reports 503 for the exact same unreachable backend.
    A single combined endpoint could never express both contracts at once."""
    rules = [RoutingRule(prefix="/orders", backend_url="http://127.0.0.1:59999")]
    proxy_app = create_proxy_app(rules, client=httpx.AsyncClient())

    with TestClient(proxy_app) as client:
        live = client.get("/_modulith/live")
        health = client.get("/_modulith/health")

    assert live.status_code == 200
    assert live.json() == {"status": "ok"}
    assert health.status_code == 503
    assert health.json()["status"] == "degraded"


# ---------------------------------------------------------------------------
# Task 7 — _resolve_actuator: every actuator_mode branch
# ---------------------------------------------------------------------------


def test_resolve_actuator_disabled_ignores_token() -> None:
    enabled, token = _resolve_actuator(mode="disabled", production=True, host="0.0.0.0", token="t")
    assert (enabled, token) == (False, None)


def test_resolve_actuator_open_ignores_missing_token() -> None:
    enabled, token = _resolve_actuator(mode="open", production=True, host="0.0.0.0", token=None)
    assert (enabled, token) == (True, None)


def test_resolve_actuator_token_mode_requires_token() -> None:
    from modulith.config import ConfigurationError

    with pytest.raises(ConfigurationError, match="requires a bearer token"):
        _resolve_actuator(mode="token", production=False, host="127.0.0.1", token=None)


def test_resolve_actuator_token_mode_accepts_token_even_on_loopback() -> None:
    enabled, token = _resolve_actuator(
        mode="token", production=False, host="127.0.0.1", token="secret"
    )
    assert (enabled, token) == (True, "secret")


def test_resolve_actuator_auto_stays_open_on_loopback_dev() -> None:
    enabled, token = _resolve_actuator(mode="auto", production=False, host="127.0.0.1", token=None)
    assert (enabled, token) == (True, None)


@pytest.mark.parametrize("host", ["0.0.0.0", "10.0.0.5"])
def test_resolve_actuator_auto_requires_token_on_non_loopback_host(host: str) -> None:
    from modulith.config import ConfigurationError

    with pytest.raises(ConfigurationError, match="non-loopback host"):
        _resolve_actuator(mode="auto", production=False, host=host, token=None)


def test_resolve_actuator_auto_requires_token_in_production_even_on_loopback() -> None:
    from modulith.config import ConfigurationError

    with pytest.raises(ConfigurationError, match="production=True"):
        _resolve_actuator(mode="auto", production=True, host="127.0.0.1", token=None)


def test_resolve_actuator_auto_accepts_token_in_production() -> None:
    enabled, token = _resolve_actuator(mode="auto", production=True, host="0.0.0.0", token="secret")
    assert (enabled, token) == (True, "secret")


# ---------------------------------------------------------------------------
# Task 7 — oversized worker log lines must not silence log forwarding
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_forward_logs_drains_oversized_line_without_dying(caplog: Any) -> None:
    """Task 7: a single log line longer than StreamReader's buffer limit
    raises ValueError from readline() — _forward_logs must log a truncation
    notice and keep forwarding subsequent lines, not silently stop forwarding
    forever after the first oversized line."""
    caplog.set_level("INFO", logger="modulith.supervisor")
    spec = WorkerSpec("orders", "fakeapp", 9001)
    # A line far past the default 64 KiB StreamReader limit, followed by a
    # normal line — proves the loop resumes cleanly at the next line. The
    # sleep+flush between writes forces two separate pipe reads: StreamReader
    # clears its *entire* buffer on overrun (not just up to the oversized
    # line), so the second line must arrive in a later read to be preserved —
    # exactly how real streaming log output behaves, unlike a single burst
    # write that lands both lines in the same buffer fill.
    command = [
        sys.executable,
        "-c",
        "import sys, time; sys.stdout.write('x' * 200_000 + '\\n'); sys.stdout.flush(); "
        "time.sleep(0.2); sys.stdout.write('after-oversized-line\\n'); sys.stdout.flush()",
    ]
    sup = Supervisor([spec], command_builder=lambda s, p: command)

    proc = await sup._spawn("orders", spec, 9001)
    await proc.wait()
    await asyncio.gather(*list(sup._log_tasks), return_exceptions=True)

    assert "buffer limit" in caplog.text
    assert "after-oversized-line" in caplog.text


# ---------------------------------------------------------------------------
# Task 7 — worker environment precedence
# ---------------------------------------------------------------------------


def test_build_worker_env_spec_env_overrides_inherited_but_not_identity_vars(
    monkeypatch,
) -> None:
    """Task 7: precedence must be inherited os.environ < spec.env < the three
    reserved identity vars — spec.env can override an inherited var, but can
    never override MODULITH_MODULE/MODULITH_APP_PACKAGE/MODULITH_TOPOLOGY."""
    monkeypatch.setenv("SHARED_VAR", "from-os-environ")
    spec = WorkerSpec(
        module_name="orders",
        package="myapp",
        port=9001,
        env={
            "SHARED_VAR": "from-spec-env",
            "MODULITH_MODULE": "attempted-override",
        },
    )

    env = _build_worker_env(spec)

    assert env["SHARED_VAR"] == "from-spec-env"  # spec.env beats inherited os.environ
    assert env["MODULITH_MODULE"] == "orders"  # identity vars always win
    assert env["MODULITH_APP_PACKAGE"] == "myapp"
    assert env["MODULITH_TOPOLOGY"] == "processes"


def test_build_worker_env_redis_aliases_do_not_override_explicit_specific_names() -> None:
    """Companion to the alias-forwarding behavior: an explicitly-set specific
    name (e.g. REDIS_URL) is the user's deliberate override and must not be
    clobbered by the generic MODULITH_BROKER_URL alias."""
    spec = WorkerSpec(
        module_name="orders",
        package="myapp",
        port=9001,
        env={
            "MODULITH_BROKER": "redis-streams",
            "MODULITH_BROKER_URL": "redis://generic:6379",
            "REDIS_URL": "redis://explicit-override:6379",
        },
    )

    env = _build_worker_env(spec)

    assert env["REDIS_URL"] == "redis://explicit-override:6379"


def test_build_worker_env_preserves_inherited_broker_url(monkeypatch) -> None:
    """Inherited MODULITH_BROKER_URL beats a pyproject URL forwarded in spec.env
    — matches adapter env > broker_options precedence."""
    monkeypatch.setenv("MODULITH_BROKER_URL", "postgresql+asyncpg://db/prod")
    spec = WorkerSpec(
        module_name="orders",
        package="myapp",
        port=9001,
        env={
            "MODULITH_BROKER": "database",
            "MODULITH_BROKER_URL": "sqlite+aiosqlite:///dev.db",
        },
    )

    env = _build_worker_env(spec)

    assert env["MODULITH_BROKER_URL"] == "postgresql+asyncpg://db/prod"


@pytest.mark.parametrize(
    "key",
    [
        "MODULITH_BROKER_STATE_DIR",
        "MODULITH_BROKER_SQLITE_PATH",
        "MODULITH_BROKER_HINT_PATH",
    ],
)
def test_build_worker_env_preserves_nonempty_inherited_state_paths(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.setenv(key, "/deployment/state")
    spec = WorkerSpec(
        module_name="orders",
        package="myapp",
        port=9001,
        env={key: "/parent/resolved/state"},
    )

    assert _build_worker_env(spec)[key] == "/deployment/state"
