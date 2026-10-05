"""Tests for the process-per-module supervisor.

Two layers:

  * **Unit** — ``derive_specs_from_config`` is pure-ish (it runs discovery over
    a fake app) and ``WorkerSpec`` allocation is deterministic; tested directly.

  * **Lifecycle** (``@pytest.mark.integration``) — the supervisor really spawns
    OS subprocesses, so these drive ``start``/``stop``/restart against trivial
    ``python -c`` commands injected via the ``command_builder`` seam. They need
    no external service (no Docker), run in well under a second, and verify the
    genuine ``asyncio.create_subprocess_exec`` path rather than a mock.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
import os
import re
import signal
import socket
import sys
import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import Request

from modulith.supervisor import (
    Supervisor,
    WorkerSpec,
    _env_flag,
    _exited,
    _port_held,
    _RestartPolicy,
    _rules_from_specs,
    derive_specs_from_config,
    run_supervised,
)

from conftest import _free_port, _health_answer

# Trivial worker commands — stand in for the real uvicorn worker.
_SLEEP = [sys.executable, "-c", "import time; time.sleep(30)"]
_CRASH = [sys.executable, "-c", "import sys; sys.exit(7)"]


def _sleep_builder(spec: WorkerSpec, port: int) -> list[str]:
    return _SLEEP


# ---------------------------------------------------------------------------
# derive_specs_from_config (unit)
# ---------------------------------------------------------------------------


def test_derive_specs_one_per_discovered_module(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config({"package": "fakeapp"})

    by_name = {s.module_name: s for s in specs}
    assert set(by_name) == {"orders", "inventory"}
    assert all(s.package == "fakeapp" for s in specs)
    # ports assigned from 9001, incrementing
    assert {s.port for s in specs} == {9001, 9002}


def test_derive_specs_respects_worker_counts_and_port_gaps(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config({"package": "fakeapp", "workers": {"orders": 2}})
    by_name = {s.module_name: s for s in specs}

    assert by_name["orders"].worker_count == 2
    # Modules are assigned ports in sorted order (inventory, then orders), and
    # each spec's port advances by its worker_count so replicas never collide:
    #   inventory → 9001 (1 replica), orders → 9002 (replicas 9002, 9003)
    assert by_name["inventory"].port == 9001
    assert by_name["orders"].port == 9002
    assert sorted(s.port for s in specs) == [9001, 9002]


def test_derive_specs_skips_the_contracts_package(make_fake_app) -> None:
    """The contracts package holds shared types, not a module: it exposes no
    router and no listeners, so a worker for it hosts nothing while shifting
    every real module's port by one and publishing a dead /contracts prefix
    on the proxy."""
    make_fake_app({"orders": "", "inventory": "", "contracts": ""})

    specs = derive_specs_from_config({"package": "fakeapp"})

    assert {s.module_name for s in specs} == {"orders", "inventory"}
    assert {s.port for s in specs} == {9001, 9002}
    assert "/contracts" not in {r.prefix for r in _rules_from_specs(specs)}


def test_derive_specs_isolate_filters_modules(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config({"package": "fakeapp", "isolate": ["orders"]})

    assert {s.module_name for s in specs} == {"orders"}


@pytest.mark.parametrize(
    ("config", "unknown"),
    [
        ({"workers": {"ordrs": 4}}, "ordrs"),
        ({"isolate": ["ordrs"]}, "ordrs"),
        ({"isolate": ["orders", "inventry"]}, "inventry"),
        ({"isolate": ["contracts"]}, "contracts"),
        ({"workers": {"contracts": 2}}, "contracts"),
    ],
)
def test_derive_specs_rejects_a_name_that_is_not_a_discoverable_module(
    config, unknown, make_fake_app
) -> None:
    """A typo in --workers, --isolate or [tool.modulith.workers] used to be
    ignored: the count silently never applied, and an unknown isolate yielded
    no workers at all. The contracts package is not a module either."""
    from modulith import ConfigurationError

    make_fake_app({"orders": "", "inventory": "", "contracts": ""})

    with pytest.raises(ConfigurationError) as excinfo:
        derive_specs_from_config({"package": "fakeapp", **config})

    message = str(excinfo.value)
    assert repr(unknown) in message
    assert "inventory, orders" in message


def test_derive_specs_accepts_the_default_key_and_known_module_names(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config(
        {"package": "fakeapp", "workers": {"default": 2, "orders": 3}, "isolate": ["orders"]}
    )

    assert [(s.module_name, s.worker_count) for s in specs] == [("orders", 3)]


def test_derive_specs_requires_package() -> None:
    with pytest.raises(ValueError, match="package"):
        derive_specs_from_config({})


def test_derive_specs_assigns_contiguous_ports_from_the_configured_base(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config(
        {"package": "fakeapp", "worker_port_base": 19001, "workers": {"inventory": 2}}
    )

    assert [(s.module_name, s.port, s.worker_count) for s in specs] == [
        ("inventory", 19001, 2),
        ("orders", 19003, 1),
    ]


def test_derive_specs_refuses_replicas_past_the_last_port(make_fake_app) -> None:
    """A base the config accepts can still push a module's last replica past
    65535; no such port exists, so the spec must not be built."""
    from modulith import ConfigurationError

    make_fake_app({"orders": ""})

    with pytest.raises(
        ConfigurationError, match=r"'orders' starts at worker port 65535 with 3 replicas"
    ):
        derive_specs_from_config(
            {"package": "fakeapp", "worker_port_base": 65535, "workers": {"orders": 3}}
        )


def test_derive_specs_accepts_replicas_ending_on_the_last_port(make_fake_app) -> None:
    make_fake_app({"orders": ""})

    specs = derive_specs_from_config(
        {"package": "fakeapp", "worker_port_base": 65534, "workers": {"orders": 2}}
    )

    assert [(s.port, s.worker_count) for s in specs] == [(65534, 2)]


async def test_run_supervised_refuses_replicas_that_share_a_port() -> None:
    """Hand-built specs can overlap (derived ones never do); the second module
    would never bind and its prefix would reach the first one's worker."""
    from modulith import ConfigurationError

    sup = _FakeSupervisor()
    specs = [
        WorkerSpec("alpha", "app", 9001, worker_count=2),
        WorkerSpec("beta", "app", 9002),
    ]

    async def must_not_serve(app: object, h: str, p: int) -> None:
        raise AssertionError("the proxy must not be served")

    with pytest.raises(ConfigurationError, match=r"'alpha' and 'beta' are both assigned port 9002"):
        await run_supervised(specs, "127.0.0.1", 8000, supervisor=sup, serve=must_not_serve)

    assert sup.events == []  # no worker was spawned


@pytest.mark.parametrize("port", ["9001", 9001.5, True, None])
def test_worker_spec_rejects_a_non_int_port(port: object) -> None:
    with pytest.raises(TypeError, match=r"'orders' port must be an int"):
        WorkerSpec("orders", "app", port)  # type: ignore[arg-type]


@pytest.mark.parametrize(("host", "port"), [("0.0.0.0", 9002), ("127.0.0.1", 9003)])
async def test_run_supervised_refuses_a_proxy_port_inside_the_worker_range(
    host: str, port: int
) -> None:
    """The proxy binds before a freshly spawned worker does, so a shared port
    leaves that module permanently unbindable and its prefix proxying to the
    proxy itself. Every replica port counts, not only each module's first."""
    from modulith import ConfigurationError

    sup = _FakeSupervisor()
    specs = [
        WorkerSpec("inventory", "app", 9001, worker_count=2),
        WorkerSpec("orders", "app", 9003),
    ]
    owner = "inventory" if port == 9002 else "orders"

    async def must_not_serve(app: object, h: str, p: int) -> None:
        raise AssertionError("the proxy must not be served")

    with pytest.raises(ConfigurationError, match=rf"port {port}.*{owner}"):
        await run_supervised(specs, host, port, supervisor=sup, serve=must_not_serve)

    assert sup.events == []  # no worker was spawned


@contextlib.contextmanager
def _listening_port() -> Iterator[int]:
    """A loopback port some other process is listening on."""
    with socket.socket() as holder:
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        yield holder.getsockname()[1]


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
async def test_run_supervised_refuses_a_proxy_port_another_process_listens_on(host: str) -> None:
    """Exit 3 from uvicorn, after the workers were already spawned, told the
    operator nothing; the busy port is named before any worker starts."""
    from modulith import ConfigurationError

    sup = _FakeSupervisor()
    specs = [WorkerSpec("orders", "app", _free_port())]

    with _listening_port() as port:
        with pytest.raises(
            ConfigurationError, match=rf"port {port} is already in use; choose another --port"
        ):
            await run_supervised(specs, host, port, supervisor=sup)

    assert sup.events == []  # no worker was spawned


async def _asgi_app_failing_its_startup(scope: Any, receive: Any, send: Any) -> None:
    assert scope["type"] == "lifespan"
    await receive()
    await send({"type": "lifespan.startup.failed", "message": "boom"})


async def _asgi_app_that_never_runs(scope: Any, receive: Any, send: Any) -> None:
    raise AssertionError("no request reaches this app")


async def test_serve_uvicorn_names_a_port_taken_between_the_pre_check_and_the_bind() -> None:
    from modulith import ConfigurationError
    from modulith.supervisor import _serve_uvicorn

    with _listening_port() as port:
        with pytest.raises(
            ConfigurationError, match=rf"port {port} is already in use; choose another --port"
        ):
            await _serve_uvicorn(_asgi_app_that_never_runs, "127.0.0.1", port)


async def test_serve_uvicorn_leaves_other_startup_failures_alone() -> None:
    """uvicorn also exits 3 when the app's lifespan startup fails. With the
    port free that is not a port problem, and must not be reported as one."""
    from modulith.supervisor import _serve_uvicorn

    with pytest.raises(SystemExit) as exc_info:
        await _serve_uvicorn(_asgi_app_failing_its_startup, "127.0.0.1", _free_port())

    assert exc_info.value.code == 3


async def test_supervisor_tells_spawn_listeners_about_every_spawn_and_restart() -> None:
    spawned: list[int] = []

    def crash_builder(spec: WorkerSpec, port: int) -> list[str]:
        return _CRASH

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=crash_builder,
        restart_initial_delay=0.05,
        restart_max_delay=0.05,
    )
    sup.add_spawn_listener(spawned.append)
    try:
        await sup.start()
        await asyncio.sleep(0.4)
    finally:
        await sup.stop()

    assert len(spawned) >= 2
    assert set(spawned) == {9001}


async def test_proxy_reverifies_a_worker_port_after_the_supervisor_respawns_it() -> None:
    import httpx
    from fastapi import FastAPI

    from conftest import _serve

    class _ListeningSupervisor(_FakeSupervisor):
        def __init__(self) -> None:
            super().__init__()
            self.listeners: list[Any] = []

        def add_spawn_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

    port = _free_port()
    sup = _ListeningSupervisor()
    captured: dict[str, Any] = {}

    async def fake_serve(app: object, host: str, p: int) -> None:
        captured["app"] = app

    spec = WorkerSpec("orders", "app", port)
    await run_supervised([spec], "127.0.0.1", 8000, supervisor=sup, serve=fake_serve)
    answering = {"token": (spec.env or {})["MODULITH_DEPLOYMENT_TOKEN"]}
    hits: list[str] = []
    backend = FastAPI()

    @backend.get("/orders/x")
    async def x() -> dict[str, bool]:
        hits.append("/orders/x")
        return {"ok": True}

    @backend.get("/health")
    async def health(request: Request) -> dict[str, str]:
        hits.append("/health")
        return _health_answer(request, answering["token"])

    proxy_app = captured["app"]
    server, task = await _serve(backend, port, "h11")
    try:
        async with proxy_app.router.lifespan_context(proxy_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
            ) as c:
                first = await c.get("/orders/x")
                answering["token"] = "another-deployment"
                for listener in sup.listeners:
                    listener(port)
                second = await c.get("/orders/x")
    finally:
        server.should_exit = True
        await task

    assert (first.status_code, second.status_code) == (200, 503)
    assert hits == ["/health", "/orders/x", "/health"]


@pytest.mark.parametrize(("max_restarts", "exited_log"), [(0, "giving up"), (5, "restarting in")])
async def test_proxy_reverifies_a_worker_port_as_soon_as_its_worker_exits(
    caplog: pytest.LogCaptureFixture, max_restarts: int, exited_log: str
) -> None:
    """Between a worker's exit and its respawn (or forever, once the breaker
    gives up) another process can bind its port; a request must not reach it
    on the dead worker's verification."""
    import httpx
    from fastapi import FastAPI

    from conftest import _serve

    port = _free_port()
    spec = WorkerSpec("orders", "app", port)
    sup = Supervisor(
        [spec],
        command_builder=lambda s, p: [
            sys.executable,
            "-c",
            "import sys, time; time.sleep(1.0); sys.exit(3)",
        ],
        max_restarts=max_restarts,
        restart_initial_delay=30.0,
        restart_max_delay=30.0,
    )
    answering: dict[str, str] = {}
    hits: list[tuple[str, str | None, str | None]] = []
    backend = FastAPI()

    @backend.get("/orders/x")
    async def x(request: Request) -> dict[str, bool]:
        hits.append(
            ("/orders/x", request.headers.get("cookie"), request.headers.get("authorization"))
        )
        return {"ok": True}

    @backend.get("/health")
    async def health(request: Request) -> dict[str, str]:
        hits.append(("/health", None, None))
        return _health_answer(request, answering["token"])

    statuses: list[int] = []

    async def serve(proxy_app: Any, host: str, p: int) -> None:
        answering["token"] = (spec.env or {})["MODULITH_DEPLOYMENT_TOKEN"]
        server, task = await _serve(backend, port, "h11")
        try:
            async with proxy_app.router.lifespan_context(proxy_app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=proxy_app), base_url="http://proxy"
                ) as c:
                    statuses.append((await c.get("/orders/x")).status_code)
                    answering["token"] = "another-deployment"
                    deadline = time.monotonic() + 10.0
                    while not any(exited_log in r.getMessage() for r in caplog.records):
                        assert time.monotonic() < deadline, "worker never exited"
                        await asyncio.sleep(0.02)
                    secret = {"cookie": "sid=SECRET", "authorization": "Bearer T"}
                    statuses.append((await c.get("/orders/x", headers=secret)).status_code)
        finally:
            server.should_exit = True
            await task

    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await run_supervised([spec], "127.0.0.1", 8000, supervisor=sup, serve=serve)

    assert statuses == [200, 503]
    assert hits == [("/health", None, None), ("/orders/x", None, None), ("/health", None, None)]
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert [m for m in errors if "giving up" not in m] == []


# ---------------------------------------------------------------------------
# _rules_from_specs (pure) — derives the reverse-proxy routing table
# ---------------------------------------------------------------------------


def test_rules_from_specs_one_rule_per_module() -> None:
    specs = [
        WorkerSpec("orders", "app", 9001),
        WorkerSpec("inventory", "app", 9002),
    ]

    rules = _rules_from_specs(specs)

    by_prefix = {r.prefix: r.backend_url for r in rules}
    assert by_prefix == {
        "/orders": "http://127.0.0.1:9001",
        "/inventory": "http://127.0.0.1:9002",
    }


def test_rules_from_specs_carries_every_replica_backend() -> None:
    """`_rules_from_specs` used to emit only the first replica's port, so a
    multi-worker module got no HTTP traffic and no health check on any
    replica beyond the first. It must carry every replica's backend for
    round-robin selection and health aggregation."""
    specs = [WorkerSpec("orders", "app", 9001, worker_count=2)]

    rules = _rules_from_specs(specs)

    assert len(rules) == 1
    rule = rules[0]
    assert rule.backend_url == "http://127.0.0.1:9001"  # first replica, unchanged
    assert rule.backend_urls == ("http://127.0.0.1:9001", "http://127.0.0.1:9002")


# ---------------------------------------------------------------------------
# run_supervised — orchestration glue (supervisor + proxy), injected fakes
# ---------------------------------------------------------------------------


class _FakeSupervisor:
    """Records lifecycle calls; stands in for the real Supervisor so
    run_supervised can be tested without spawning subprocesses."""

    def __init__(self) -> None:
        self.events: list[str] = []

    async def start(self) -> None:
        self.events.append("start")

    async def stop(self) -> None:
        self.events.append("stop")

    def failed_instances(self) -> frozenset[str]:
        return frozenset()

    def add_spawn_listener(self, listener: Any) -> None:
        pass


async def test_run_supervised_starts_serves_then_stops() -> None:
    sup = _FakeSupervisor()
    captured: dict[str, object] = {}

    async def fake_serve(app: object, host: str, port: int) -> None:
        sup.events.append("serve")
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    specs = [WorkerSpec("orders", "app", 9001), WorkerSpec("inventory", "app", 9002)]

    await run_supervised(specs, "0.0.0.0", 8000, supervisor=sup, serve=fake_serve)

    assert sup.events == ["start", "serve", "stop"]
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 8000
    # The served app is the reverse proxy, carrying one route per worker.
    served = captured["app"]
    assert served.title == "modulith-proxy"


async def test_run_supervised_stops_even_when_serve_raises() -> None:
    sup = _FakeSupervisor()

    async def boom(app: object, host: str, port: int) -> None:
        sup.events.append("serve")
        raise RuntimeError("server crashed")

    with pytest.raises(RuntimeError, match="server crashed"):
        await run_supervised(
            [WorkerSpec("orders", "app", 9001)],
            "127.0.0.1",
            8000,
            supervisor=sup,
            serve=boom,
        )

    # stop() must run on the way out so workers aren't orphaned on crash.
    assert sup.events == ["start", "serve", "stop"]


async def test_run_supervised_wires_failed_instances_into_the_health_endpoint() -> None:
    """`Supervisor._failed_instances` is documented as "surfaced for health
    reporting" but nothing wired it anywhere outside tests. run_supervised
    must pass the supervisor's failed_instances() lookup through to the
    proxy app, so /_modulith/health can tell a permanently abandoned module
    apart from one still mid restart-backoff."""
    from fastapi.testclient import TestClient

    class _GivenUpSupervisor(_FakeSupervisor):
        def failed_instances(self) -> frozenset[str]:
            return frozenset({"orders"})

    sup = _GivenUpSupervisor()
    captured: dict[str, object] = {}

    async def capture_serve(app: object, host: str, port: int) -> None:
        captured["app"] = app

    await run_supervised(
        [WorkerSpec("orders", "app", 9001)],
        "127.0.0.1",
        8000,
        supervisor=sup,
        serve=capture_serve,
    )

    with TestClient(captured["app"]) as client:
        resp = client.get("/_modulith/health")

    assert resp.json()["backends"]["/orders"] == "failed (given up)"


async def test_run_supervised_stops_workers_when_start_fails_partway() -> None:
    """``await sup.start()`` used to sit
    OUTSIDE the try/finally guarding serve_fn, and Supervisor.start() has no
    mid-loop rollback — a partial-spawn failure (e.g. 3rd of 5 workers fails)
    never triggered sup.stop(), orphaning the already-spawned workers.
    start() must be guarded so stop() reaps the partial spawn."""

    class _PartialStartSupervisor(_FakeSupervisor):
        async def start(self) -> None:
            # Models: some workers already spawned, then one spawn fails.
            self.events.append("start-partial")
            raise RuntimeError("worker 3 of 5 failed to spawn")

    sup = _PartialStartSupervisor()
    served: list[str] = []

    async def never_serve(app: object, host: str, port: int) -> None:
        served.append("serve")

    with pytest.raises(RuntimeError, match="failed to spawn"):
        await run_supervised(
            [WorkerSpec("orders", "app", 9001)],
            "127.0.0.1",
            8000,
            supervisor=sup,
            serve=never_serve,
        )

    assert sup.events == ["start-partial", "stop"]  # cleanup reaps the partial spawn
    assert served == []  # the proxy never served — start() failed first


@pytest.mark.real_process
async def test_run_supervised_reaps_partial_spawn_of_real_workers() -> None:
    """Real-subprocess form of the partial-spawn reap: the 2nd of two
    workers fails to spawn (nonexistent binary) partway through start() —
    the worker spawned before the failure must be reaped, not orphaned."""
    specs = [WorkerSpec("alpha", "fakeapp", 9001), WorkerSpec("bad", "fakeapp", 9002)]

    def builder(spec: WorkerSpec, port: int) -> list[str]:
        if spec.module_name == "bad":
            return ["/nonexistent/modulith-w2-no-such-binary"]
        return _SLEEP

    sup = Supervisor(specs, command_builder=builder)

    async def never_serve(app: object, host: str, port: int) -> None:
        pytest.fail("serve must not run when start() fails")

    with pytest.raises(FileNotFoundError):
        await run_supervised(specs, "127.0.0.1", 8000, supervisor=sup, serve=never_serve)

    # alpha WAS spawned before the failure — and was terminated on the way out.
    assert "alpha" in sup._processes
    assert all(p.returncode is not None for p in sup._processes.values())


@pytest.mark.real_process
async def test_run_supervised_reaps_workers_on_sigterm_during_start_window() -> None:
    """A SIGTERM/SIGINT arriving after workers are spawned but before serve_fn
    installs its own handlers must still reach ``finally: sup.stop()`` —
    without a handler for that window, Python's default SIGTERM disposition
    kills the process immediately, skipping the finally and orphaning the
    just-spawned workers."""
    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=_sleep_builder)
    served: list[str] = []

    async def slow_serve(app: object, host: str, port: int) -> None:
        # Stands in for the vulnerable window: sup.start() already returned
        # (workers spawned) but serve_fn hasn't installed its own signal
        # handlers yet. Send SIGTERM to ourselves right as "serving" begins.
        served.append("serve")
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(10)  # would hang forever without the handler

    await run_supervised(
        [WorkerSpec("orders", "fakeapp", 9001)],
        "127.0.0.1",
        8000,
        supervisor=sup,
        serve=slow_serve,
    )

    assert served == ["serve"]
    # Workers spawned during start() must be reaped, not orphaned.
    assert sup._processes
    assert all(p.returncode is not None for p in sup._processes.values())


@pytest.mark.real_process
async def test_run_supervised_stands_down_once_serve_fn_owns_the_signal() -> None:
    """uvicorn takes SIGTERM/SIGINT over with ``signal.signal()``, which does
    NOT displace run_supervised's ``add_signal_handler`` registration — both
    callbacks fire on one signal. Cancelling the task at that point throws
    into ``Server.main_loop`` before ``Server.shutdown()`` ever runs: listening
    sockets stay open, in-flight requests die mid-response with a 500, and the
    lifespan shutdown is skipped. Once serve_fn owns the signal, our handler
    must let serve_fn finish on its own terms."""
    sup = _FakeSupervisor()
    handled: list[str] = []

    async def serve_like_uvicorn(app: object, host: str, port: int) -> None:
        previous = signal.signal(signal.SIGTERM, lambda *_: handled.append("serve_fn"))
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            # Long enough for the loop to dispatch a still-armed asyncio
            # signal callback — the stale cancel would land right here.
            await asyncio.sleep(0.2)
            sup.events.append("drained")
        finally:
            signal.signal(signal.SIGTERM, previous)

    await run_supervised(
        [WorkerSpec("orders", "app", 9001)],
        "127.0.0.1",
        8000,
        supervisor=sup,
        serve=serve_like_uvicorn,
    )

    assert handled == ["serve_fn"]  # serve_fn's own handler ran
    assert sup.events == ["start", "drained", "stop"]  # serve_fn was not cancelled


async def test_run_supervised_honors_the_proxy_body_cap_env_var(monkeypatch) -> None:
    """create_proxy_app's 10 MiB request-body cap is otherwise unreachable
    from ``modulith run``: no flag, no pyproject key, no env var, so an app
    with larger uploads has to abandon the CLI and hand-roll the supervisor."""
    from fastapi.testclient import TestClient

    monkeypatch.setenv("MODULITH_PROXY_MAX_BODY_BYTES", "4")
    captured: dict[str, object] = {}

    async def capture_serve(app: object, host: str, port: int) -> None:
        captured["app"] = app

    await run_supervised(
        [WorkerSpec("orders", "app", 9001)],
        "127.0.0.1",
        8000,
        supervisor=_FakeSupervisor(),
        serve=capture_serve,
    )

    with TestClient(captured["app"]) as client:
        resp = client.post("/orders/echo", content=b"over-the-cap")

    assert resp.status_code == 413


async def test_run_supervised_rejects_a_non_numeric_proxy_body_cap(monkeypatch) -> None:
    """A garbage cap is user error with an actionable message, not a crash."""
    from modulith.config import ConfigurationError

    monkeypatch.setenv("MODULITH_PROXY_MAX_BODY_BYTES", "10MB")

    async def never_serve(app: object, host: str, port: int) -> None:
        pytest.fail("must not serve with an unusable body cap")

    with pytest.raises(ConfigurationError, match="positive integer"):
        await run_supervised([], "127.0.0.1", 8000, supervisor=_FakeSupervisor(), serve=never_serve)


async def test_run_supervised_rejects_an_unparseable_production_flag(monkeypatch) -> None:
    """MODULITH_PRODUCTION must be parsed as strictly here as it is in
    ``load_configuration()``. The supervisor used its own lenient reader, so
    a typo like "ture" hard-failed application config yet silently resolved
    to False in this process — quietly dropping the production-only actuator
    hardening. A typo must never turn a security posture off in silence."""
    from modulith.config import ConfigurationError

    monkeypatch.setenv("MODULITH_PRODUCTION", "ture")

    async def never_serve(app: object, host: str, port: int) -> None:
        pytest.fail("must not serve with an unparseable MODULITH_PRODUCTION")

    with pytest.raises(ConfigurationError, match="MODULITH_PRODUCTION"):
        await run_supervised([], "127.0.0.1", 8000, supervisor=_FakeSupervisor(), serve=never_serve)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, False), ("", False), ("1", True), ("true", True), ("YES", True), ("no", False)],
)
def test_env_flag_mirrors_the_configuration_boolean_parser(monkeypatch, raw, expected) -> None:
    """Strictness must not come at the cost of the documented spellings:
    _env_flag accepts exactly what load_configuration() accepts (1/true/yes,
    case-insensitive) and treats unset or empty as False."""
    if raw is None:
        monkeypatch.delenv("MODULITH_PRODUCTION", raising=False)
    else:
        monkeypatch.setenv("MODULITH_PRODUCTION", raw)

    assert _env_flag("MODULITH_PRODUCTION") is expected


# ---------------------------------------------------------------------------
# Lifecycle (real subprocesses)
# ---------------------------------------------------------------------------


@pytest.mark.real_process
async def test_start_spawns_all_workers() -> None:
    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=_sleep_builder)
    try:
        await sup.start()
        assert len(sup._processes) == 1
        proc = next(iter(sup._processes.values()))
        assert proc.returncode is None  # alive
    finally:
        await sup.stop()


@pytest.mark.real_process
async def test_worker_count_spawns_replicas() -> None:
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001, worker_count=2)],
        command_builder=_sleep_builder,
    )
    try:
        await sup.start()
        assert len(sup._processes) == 2
    finally:
        await sup.stop()


@pytest.mark.real_process
async def test_replica_logs_are_prefixed_with_the_instance_name(caplog) -> None:
    """Multiplexed output must be attributable to a single replica. Prefixing
    with the module name makes all N replicas of one module indistinguishable
    in the supervisor's stdout, while the crash/restart messages next to them
    are keyed by instance — so an operator cannot match a traceback to the
    instance the restart breaker is counting."""

    def talkative_builder(spec: WorkerSpec, port: int) -> list[str]:
        return [
            sys.executable,
            "-c",
            f"import time; print('listening on {port}', flush=True); time.sleep(30)",
        ]

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001, worker_count=2)],
        command_builder=talkative_builder,
    )
    with caplog.at_level(logging.INFO, logger="modulith.supervisor"):
        try:
            await sup.start()
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if sum("listening on" in r.getMessage() for r in caplog.records) >= 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await sup.stop()

    messages = [r.getMessage() for r in caplog.records]
    assert "[orders-0] listening on 9001" in messages
    assert "[orders-1] listening on 9002" in messages
    assert any(m.startswith("spawned worker 'orders-0'") for m in messages)
    assert any(m.startswith("spawned worker 'orders-1'") for m in messages)


@pytest.mark.real_process
async def test_single_worker_logs_keep_the_bare_module_name(caplog) -> None:
    """The instance name equals the module name when worker_count == 1, so the
    replica-aware prefix must not change single-worker output."""

    def talkative_builder(spec: WorkerSpec, port: int) -> list[str]:
        return [sys.executable, "-c", "import time; print('up', flush=True); time.sleep(30)"]

    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=talkative_builder)
    with caplog.at_level(logging.INFO, logger="modulith.supervisor"):
        try:
            await sup.start()
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if any("[orders] up" == r.getMessage() for r in caplog.records):
                    break
                await asyncio.sleep(0.05)
        finally:
            await sup.stop()

    assert "[orders] up" in [r.getMessage() for r in caplog.records]


@pytest.mark.real_process
async def test_stop_terminates_all_workers() -> None:
    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=_sleep_builder)
    await sup.start()
    proc = next(iter(sup._processes.values()))

    await sup.stop()

    assert proc.returncode is not None  # exited


@pytest.mark.real_process
async def test_crashed_worker_is_restarted_with_backoff() -> None:
    calls: list[tuple[str, int]] = []

    def crash_builder(spec: WorkerSpec, port: int) -> list[str]:
        calls.append((spec.module_name, port))
        return _CRASH

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=crash_builder,
        restart_initial_delay=0.05,
        restart_max_delay=0.05,
    )
    try:
        await sup.start()
        # Each crash respawns after ~0.05s; in 0.4s we should see restarts.
        await asyncio.sleep(0.4)
        assert len(calls) >= 2  # initial spawn + at least one restart
    finally:
        await sup.stop()


# ---------------------------------------------------------------------------
# _RestartPolicy (pure — backoff + circuit breaker)
# ---------------------------------------------------------------------------


def test_restart_policy_backoff_escalates_and_caps() -> None:
    """Delay doubles each crash, capped at max_delay."""
    p = _RestartPolicy(initial_delay=1.0, max_delay=10.0, healthy_uptime=100.0, max_restarts=100)
    delays = [p.on_crash(uptime=0.0, now=float(i)) for i in range(5)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 10.0]  # last is capped at max


def test_restart_policy_resets_backoff_after_healthy_uptime() -> None:
    """A worker that ran past healthy_uptime resets backoff to initial (#50)."""
    p = _RestartPolicy(initial_delay=1.0, max_delay=10.0, healthy_uptime=5.0, max_restarts=100)
    # Three quick crashes escalate the backoff.
    assert p.on_crash(uptime=0.0, now=0.0) == 1.0
    assert p.on_crash(uptime=0.0, now=1.0) == 2.0
    assert p.on_crash(uptime=0.0, now=2.0) == 4.0
    # A crash AFTER a healthy run resets the next delay back to initial.
    assert p.on_crash(uptime=10.0, now=3.0) == 1.0


def test_restart_policy_trips_breaker_after_max_restarts() -> None:
    """More than max_restarts crashes in a row (rapid burst) → give up (None) (#33)."""
    p = _RestartPolicy(initial_delay=0.01, max_delay=0.01, healthy_uptime=100.0, max_restarts=3)
    assert p.on_crash(uptime=0.0, now=0.0) is not None
    assert p.on_crash(uptime=0.0, now=1.0) is not None
    assert p.on_crash(uptime=0.0, now=2.0) is not None
    # 4th crash in a row exceeds the cap → breaker trips.
    assert p.on_crash(uptime=0.0, now=3.0) is None


def test_restart_policy_trips_on_slow_steady_crash_loop() -> None:
    """A module crashing every 15s (never healthy) still trips eventually.

    Root-cause regression test: the breaker previously counted crashes inside
    a rolling time window, so a module crashing slower than roughly
    max_restarts/window kept the in-window count at or below max_restarts
    forever and was respawned indefinitely. Counting the crash streak instead
    of wall-clock spacing means a steady 15s-interval loop trips exactly like
    a rapid burst — cadence no longer matters, only the streak length.
    """
    p = _RestartPolicy(initial_delay=0.01, max_delay=0.01, healthy_uptime=60.0, max_restarts=5)
    # 6 crashes, 15s apart, each run far too short to count as healthy.
    results = [p.on_crash(uptime=1.0, now=float(i * 15)) for i in range(6)]
    assert results[:5] == [0.01] * 5  # first 5 crashes still respawn
    assert results[5] is None  # 6th crash (streak of 6) exceeds max_restarts=5


def test_restart_policy_resets_crash_streak_after_recovery() -> None:
    """A worker that recovers resets the streak; a later isolated crash doesn't trip."""
    p = _RestartPolicy(initial_delay=0.01, max_delay=0.01, healthy_uptime=5.0, max_restarts=3)
    # Three quick crashes (never healthy) bring the streak right to the cap.
    assert p.on_crash(uptime=0.0, now=0.0) is not None
    assert p.on_crash(uptime=0.0, now=1.0) is not None
    assert p.on_crash(uptime=0.0, now=2.0) is not None
    # Recovers: stays up well past healthy_uptime, then crashes once. The
    # streak resets to 1 before this crash, so it must NOT trip.
    assert p.on_crash(uptime=10.0, now=20.0) is not None


class _SpyProc:
    """Bare process double for testing stop()'s platform branching in
    isolation — no real subprocess, no monitor tasks, no OS signal semantics."""

    def __init__(self) -> None:
        self.returncode: int | None = None
        self.kill_called = False

    def terminate(self) -> None:
        pass  # never actually exits on its own — proves stop() doesn't rely on it

    def kill(self) -> None:
        self.kill_called = True
        self.returncode = -9

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = 0  # resolves regardless of kill(), so stop() never hangs
        return self.returncode


async def test_stop_escalates_to_kill_on_posix_when_process_survives_terminate(
    monkeypatch,
) -> None:
    monkeypatch.setattr("modulith.supervisor.sys.platform", "linux")
    proc = _SpyProc()
    sup = Supervisor([])
    sup._processes["orders"] = proc  # type: ignore[assignment]

    await sup.stop()

    assert proc.kill_called is True


async def test_stop_skips_the_redundant_kill_pass_on_windows(monkeypatch) -> None:
    """subprocess.Popen.kill() is a plain alias for terminate() on Windows
    (both call TerminateProcess) — stop()'s second pass must not pretend to
    escalate what is already an identical, already-issued hard kill."""
    monkeypatch.setattr("modulith.supervisor.sys.platform", "win32")
    proc = _SpyProc()
    sup = Supervisor([])
    sup._processes["orders"] = proc  # type: ignore[assignment]

    await sup.stop()

    assert proc.kill_called is False


async def test_start_warns_when_orphan_protection_is_unavailable(monkeypatch, caplog) -> None:
    """`_pdeathsig_preexec` is None on any non-Linux platform — the operator
    must be told once at startup, not left to discover it only when a
    SIGKILL'd supervisor leaves orphaned workers holding their ports."""
    monkeypatch.setattr("modulith.supervisor._pdeathsig_preexec", None)
    sup = Supervisor([])
    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await sup.start()

    assert any("orphan protection" in r.getMessage() for r in caplog.records)


async def test_start_does_not_warn_when_orphan_protection_is_available(monkeypatch, caplog) -> None:
    monkeypatch.setattr("modulith.supervisor._pdeathsig_preexec", lambda: None)
    sup = Supervisor([])
    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await sup.start()

    assert not any("orphan protection" in r.getMessage() for r in caplog.records)


@pytest.mark.real_process
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="asserts a POSIX signal-encoded returncode; on Windows terminate() "
    "is already TerminateProcess and returncode is a positive exit code",
)
async def test_stop_does_not_sigkill_worker_that_exits_within_grace_window() -> None:
    """A worker that dies from SIGTERM (proc.terminate()) well within
    shutdown_timeout must NOT also receive SIGKILL — escalating unconditionally
    could cut a real worker off mid runtime.shutdown() (broker close / outbox
    drain). Distinguish by exit signal: SIGTERM (-15) means only terminate()
    fired; SIGKILL (-9) means stop() escalated on top of an already-dead proc."""
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=_sleep_builder,
        shutdown_timeout=5.0,  # comfortably longer than SIGTERM-default death
    )
    await sup.start()
    proc = next(iter(sup._processes.values()))

    await sup.stop()

    assert proc.returncode == -signal.SIGTERM


@pytest.mark.real_process
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="asserts POSIX signal-encoded returncodes; on Windows terminate() "
    "and kill() are both TerminateProcess and cannot be told apart this way",
)
async def test_stop_sigkills_only_the_worker_still_alive_at_timeout() -> None:
    """With two workers — one that dies on SIGTERM, one that traps and ignores
    it — only the still-alive one is escalated to SIGKILL at shutdown_timeout;
    the one that already exited is left alone."""
    import tempfile

    marker = tempfile.mktemp()
    stubborn_cmd = [
        sys.executable,
        "-c",
        (
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, lambda *a: None)\n"
            f"open({marker!r}, 'w').close()\n"
            "time.sleep(30)\n"
        ),
    ]

    def builder(spec: WorkerSpec, port: int) -> list[str]:
        return _SLEEP if spec.module_name == "fast" else stubborn_cmd

    specs = [WorkerSpec("fast", "fakeapp", 9001), WorkerSpec("stubborn", "fakeapp", 9002)]
    sup = Supervisor(specs, command_builder=builder, shutdown_timeout=0.3)
    await sup.start()
    procs = dict(sup._processes)

    # Wait for the stubborn worker's SIGTERM handler to actually be installed
    # before stopping — otherwise stop()'s terminate() could race ahead of it
    # and kill the process via default disposition, defeating the scenario.
    deadline = time.monotonic() + 5.0
    while not os.path.exists(marker) and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert os.path.exists(marker), "stubborn worker never installed its SIGTERM handler"

    await sup.stop()

    assert procs["fast"].returncode == -signal.SIGTERM  # died on the first signal
    assert procs["stubborn"].returncode == -signal.SIGKILL  # still alive -> escalated


# A worker that starts a helper subprocess inheriting its stdout and stderr,
# records the helper's pid, then either exits at once or keeps running.
# argv: pid file, helper sleep seconds, "exit" | "stay".
_DESCENDANT_WORKER = """
import os, subprocess, sys, time
helper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(%s)" % sys.argv[2]])
with open(sys.argv[1], "a") as f:
    f.write("%d\\n" % helper.pid)
if sys.argv[3] == "exit":
    os._exit(3)
time.sleep(30)
"""


def _descendant_builder(pid_file: Any, helper_sleep: int, then: str) -> Any:
    return lambda spec, port: [
        sys.executable,
        "-c",
        _DESCENDANT_WORKER,
        str(pid_file),
        str(helper_sleep),
        then,
    ]


def _kill_descendants(pid_file: Any) -> None:
    if pid_file.exists():
        for pid in pid_file.read_text().split():
            with contextlib.suppress(ProcessLookupError):
                os.kill(int(pid), signal.SIGKILL)


@pytest.mark.real_process
async def test_a_worker_exit_is_seen_while_a_descendant_still_holds_its_pipes(tmp_path) -> None:
    """The worker's helper outlives it and keeps its stdout and stderr open.
    The exit must still reach the exit listeners, the restart and the
    crash-loop breaker straight away, not when the helper finally exits."""
    pid_file = tmp_path / "descendants"
    notified: list[int] = []
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=_descendant_builder(pid_file, 30, "exit"),
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=1,
        restart_healthy_uptime=30.0,
    )
    sup.add_spawn_listener(notified.append)
    try:
        await sup.start()
        deadline = time.monotonic() + 3.0
        while not sup.failed_instances() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        # spawn, exit, respawn, exit: the breaker then gives up
        assert notified == [9001, 9001, 9001, 9001]
        assert sup.failed_instances() == frozenset({"orders"})
    finally:
        _kill_descendants(pid_file)
        await sup.stop()


@pytest.mark.real_process
async def test_stop_returns_within_its_timeout_while_a_descendant_holds_a_workers_pipes(
    tmp_path,
) -> None:
    pid_file = tmp_path / "descendants"
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=_descendant_builder(pid_file, 10, "stay"),
        shutdown_timeout=1.0,
    )
    await sup.start()
    procs = list(sup._processes.values())
    try:
        deadline = time.monotonic() + 5.0
        while not pid_file.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert pid_file.exists(), "the worker never started its helper"

        started = time.monotonic()
        await sup.stop()
        elapsed = time.monotonic() - started

        assert elapsed < 2.0
        assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []
    finally:
        _kill_descendants(pid_file)
        for proc in procs:
            await asyncio.wait_for(proc.wait(), timeout=5.0)


def _transport_closing(proc: asyncio.subprocess.Process) -> bool:
    return proc._transport.is_closing()  # type: ignore[attr-defined,no-any-return]


@pytest.mark.real_process
async def test_restarts_with_a_descendant_holding_the_pipes_release_each_spawns_resources(
    tmp_path,
) -> None:
    """Every respawn must settle the previous spawn: its forwarders end and
    its transport closes, so nothing accumulates across restarts."""
    pid_file = tmp_path / "descendants"
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=_descendant_builder(pid_file, 20, "exit"),
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=10,
        restart_healthy_uptime=30.0,
    )
    seen: list[asyncio.subprocess.Process] = []
    try:
        await sup.start()
        deadline = time.monotonic() + 20.0
        while len(seen) < 4 and time.monotonic() < deadline:
            proc = sup._processes["orders"]
            if not seen or proc is not seen[-1]:
                seen.append(proc)
            await asyncio.sleep(0.02)
        assert len(seen) == 4, "the worker was not respawned three times"

        # the fourth spawn may still be starting: wait for its predecessor's release
        while not _transport_closing(seen[-2]) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        assert [_transport_closing(p) for p in seen[:-1]] == [True, True, True]
        assert len(sup._log_tasks) <= 2  # at most the live spawn's two forwarders
    finally:
        _kill_descendants(pid_file)
        await sup.stop()


@pytest.mark.real_process
async def test_stop_closes_the_transport_of_a_worker_whose_pipes_a_descendant_holds(
    tmp_path,
) -> None:
    pid_file = tmp_path / "descendants"
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=_descendant_builder(pid_file, 10, "stay"),
        shutdown_timeout=1.0,
    )
    await sup.start()
    proc = sup._processes["orders"]
    try:
        deadline = time.monotonic() + 5.0
        while not pid_file.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert pid_file.exists(), "the worker never started its helper"

        await sup.stop()

        assert _transport_closing(proc)
    finally:
        _kill_descendants(pid_file)


class _ExitedWithPipesHeld:
    """A process that has exited while something still holds its pipes."""

    returncode = 3

    async def wait(self) -> int:
        await asyncio.Event().wait()
        return 3


async def test_exited_passes_on_a_cancellation_aimed_at_its_caller() -> None:
    task = asyncio.create_task(_exited(_ExitedWithPipesHeld()))  # type: ignore[arg-type]
    await asyncio.sleep(0)  # the task now awaits its leftover wait() task

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# A worker that listens on its port the way asyncio's create_server does. The
# first run hands its listening socket to a helper that holds it for
# argv[3] seconds, then dies; later runs serve, or exit 3 when the bind fails.
# argv: port, pid file (also the first-run marker), helper hold seconds.
_PORT_SHARING_WORKER = """
import os, socket, subprocess, sys, time
port, pid_file, hold = int(sys.argv[1]), sys.argv[2], sys.argv[3]
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    sock.bind(("127.0.0.1", port))
except OSError as exc:
    print("bind failed: %s" % exc, file=sys.stderr, flush=True)
    sys.exit(3)
sock.listen()
if not os.path.exists(pid_file):
    helper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(%s)" % hold], pass_fds=[sock.fileno()]
    )
    with open(pid_file, "w") as f:
        f.write("%d\\n" % helper.pid)
    os._exit(3)
print("serving", flush=True)
time.sleep(30)
"""


def _port_sharing_builder(pid_file: Any, hold: float) -> Any:
    return lambda spec, port: [
        sys.executable,
        "-c",
        _PORT_SHARING_WORKER,
        str(port),
        str(pid_file),
        str(hold),
    ]


def _messages(caplog: pytest.LogCaptureFixture, text: str, level: int) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level and text in r.getMessage()]


_POSIX_ONLY = pytest.mark.skipif(
    sys.platform == "win32", reason="the worker hands its socket on through pass_fds"
)


@pytest.mark.real_process
@_POSIX_ONLY
async def test_respawn_waits_for_a_descendant_to_release_the_workers_port(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A descendant of the dead worker still listens on its port for a few
    seconds. The respawn must wait for the port instead of failing to bind
    until the crash-loop breaker gives up on the module."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    pid_file = tmp_path / "descendants"
    port = _free_port()
    notified: list[int] = []
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", port)],
        command_builder=_port_sharing_builder(pid_file, 2.0),
        restart_initial_delay=0.05,
        restart_max_delay=10.0,
        max_restarts=1,
        restart_healthy_uptime=30.0,
    )
    sup.add_spawn_listener(notified.append)
    try:
        await sup.start()
        deadline = time.monotonic() + 10.0
        while not _messages(caplog, "[orders] serving", logging.INFO):
            if sup.failed_instances() or time.monotonic() > deadline:
                break
            await asyncio.sleep(0.05)

        assert sup.failed_instances() == frozenset()
        assert _messages(caplog, "[orders] serving", logging.INFO) == ["[orders] serving"]
        assert _messages(caplog, "bind failed", logging.WARNING) == []
        assert notified == [port, port, port]  # spawn, exit, one respawn
        waits = _messages(caplog, f"port {port}", logging.WARNING)
        assert len(waits) == 1
        assert "orders" in waits[0]
        assert "10s" in waits[0]
    finally:
        _kill_descendants(pid_file)
        await sup.stop()


@pytest.mark.real_process
@_POSIX_ONLY
async def test_a_port_held_past_the_wait_still_ends_in_the_crash_loop_give_up(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    pid_file = tmp_path / "descendants"
    port = _free_port()
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", port)],
        command_builder=_port_sharing_builder(pid_file, 30.0),
        restart_initial_delay=0.05,
        restart_max_delay=0.3,
        max_restarts=1,
        restart_healthy_uptime=30.0,
    )
    try:
        await sup.start()
        deadline = time.monotonic() + 10.0
        while not sup.failed_instances() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

        assert sup.failed_instances() == frozenset({"orders"})
        assert len(_messages(caplog, f"port {port}", logging.WARNING)) == 1
        assert len(_messages(caplog, "bind failed", logging.WARNING)) == 1
        assert len(_messages(caplog, "giving up", logging.ERROR)) == 1
        (give_up,) = _messages(caplog, "giving up", logging.ERROR)
        assert f"Port {port} was still held by another process" in give_up
        assert "Fix the module" not in give_up
    finally:
        _kill_descendants(pid_file)
        await sup.stop()


@pytest.mark.real_process
async def test_a_crash_loop_with_a_free_port_still_tells_the_operator_to_fix_the_module(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", _free_port())],
        command_builder=lambda spec, port: _CRASH,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=1,
        restart_healthy_uptime=30.0,
    )
    try:
        await sup.start()
        deadline = time.monotonic() + 10.0
        while not sup.failed_instances() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        (give_up,) = _messages(caplog, "giving up", logging.ERROR)
        assert "Fix the module" in give_up
        assert "still held" not in give_up
    finally:
        await sup.stop()


@pytest.mark.real_process
@_POSIX_ONLY
async def test_stop_interrupts_the_wait_for_a_held_port(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    pid_file = tmp_path / "descendants"
    port = _free_port()
    notified: list[int] = []
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", port)],
        command_builder=_port_sharing_builder(pid_file, 30.0),
        restart_initial_delay=0.05,
        restart_max_delay=30.0,
    )
    sup.add_spawn_listener(notified.append)
    try:
        await sup.start()
        deadline = time.monotonic() + 10.0
        while not _messages(caplog, f"port {port}", logging.WARNING):
            assert time.monotonic() < deadline, "the supervisor never waited for the port"
            await asyncio.sleep(0.05)
        assert notified == [port, port]  # the exit was announced before the wait

        started = time.monotonic()
        await sup.stop()

        assert time.monotonic() - started < 1.5
        assert notified == [port, port]  # nothing was respawned
    finally:
        _kill_descendants(pid_file)
        await sup.stop()


class _SocketModuleOutOfDescriptors:
    """The supervisor's ``socket`` module in a process at its descriptor limit."""

    def __getattr__(self, name: str) -> Any:
        return getattr(socket, name)

    @staticmethod
    def socket(*args: Any, **kwargs: Any) -> Any:
        raise OSError(errno.EMFILE, "Too many open files")


@pytest.mark.real_process
async def test_a_port_probe_that_cannot_open_a_socket_keeps_the_worker_supervised(
    monkeypatch, caplog
) -> None:
    caplog.set_level("ERROR", logger="modulith.supervisor")
    monkeypatch.setattr("modulith.supervisor.socket", _SocketModuleOutOfDescriptors())
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", _free_port())],
        command_builder=lambda spec, port: _CRASH,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=3,
        restart_healthy_uptime=5.0,
    )
    try:
        await sup.start()
        for _ in range(300):  # bounded poll — no fixed-sleep synchronization
            await asyncio.sleep(0.01)
            if "orders" in sup._failed_instances:
                break

        assert "orders" in sup._failed_instances
        assert any(r.levelname == "ERROR" for r in caplog.records)
    finally:
        await sup.stop()


def test_a_port_the_probe_cannot_bind_at_all_reads_as_free() -> None:
    assert _port_held(65536) is False


def test_a_port_probe_that_cannot_run_logs_one_warning(monkeypatch, caplog) -> None:
    caplog.set_level(logging.WARNING, logger="modulith.supervisor")
    monkeypatch.setattr("modulith.supervisor.socket", _SocketModuleOutOfDescriptors())

    assert _port_held(9001) is False

    (warning,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    message = warning.getMessage()
    assert "cannot probe port 9001" in message
    assert "Too many open files" in message


def test_a_port_probe_that_finds_the_port_free_or_held_logs_nothing(caplog) -> None:
    caplog.set_level(logging.WARNING, logger="modulith.supervisor")
    with _listening_port() as held:
        assert _port_held(held) is True
    assert _port_held(_free_port()) is False

    assert caplog.records == []


@pytest.mark.real_process
async def test_stop_still_forwards_a_workers_final_lines(caplog) -> None:
    """A worker writing its last lines as it shuts down, with no descendant
    holding its pipes: stop() must forward every one of them."""
    worker = (
        "import signal, sys, time\n"
        "def bye(*_):\n"
        "    for i in range(500):\n"
        "        print('final-line-%d' % i, file=sys.stderr)\n"
        "    sys.exit(0)\n"
        "signal.signal(signal.SIGTERM, bye)\n"
        "print('ready', flush=True)\n"
        "time.sleep(30)\n"
    )
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=lambda s, p: [sys.executable, "-c", worker],
        shutdown_timeout=5.0,
    )
    await sup.start()
    deadline = time.monotonic() + 5.0
    while not any("[orders] ready" in r.getMessage() for r in caplog.records):
        assert time.monotonic() < deadline, "worker never became ready"
        await asyncio.sleep(0.02)

    await sup.stop()

    forwarded = [r.getMessage() for r in caplog.records if "final-line-" in r.getMessage()]
    assert forwarded == [f"[orders] final-line-{i}" for i in range(500)]


# A worker that hands its pipes to a helper which outlives it, then stays up.
# argv: pid file, helper source.
_PIPE_HOLDING_WORKER = """
import subprocess, sys, time
helper = subprocess.Popen([sys.executable, "-c", sys.argv[2]])
with open(sys.argv[1], "a") as f:
    f.write("%d\\n" % helper.pid)
time.sleep(30)
"""
_CHATTY_HELPER = (
    "import time\nfor _ in range(600):\n    print('chatter', flush=True)\n    time.sleep(0.05)"
)
_QUIET_HELPER = "import time; time.sleep(30)"


@pytest.mark.real_process
async def test_a_chatty_orphan_does_not_keep_a_quiet_workers_forwarders_alive(
    tmp_path, caplog
) -> None:
    """Each forwarder is cancelled once its own pipe has been quiet; output on
    another worker's pipe must not hold it open until the drain timeout."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    pid_file = tmp_path / "descendants"
    helpers = {"chatty": _CHATTY_HELPER, "quiet": _QUIET_HELPER}
    sup = Supervisor(
        [WorkerSpec("chatty", "fakeapp", 9001), WorkerSpec("quiet", "fakeapp", 9002)],
        command_builder=lambda spec, port: [
            sys.executable,
            "-c",
            _PIPE_HOLDING_WORKER,
            str(pid_file),
            helpers[spec.module_name],
        ],
        shutdown_timeout=1.0,
    )
    await sup.start()
    quiet = set(sup._instance_log_tasks["quiet"])
    chatty = set(sup._instance_log_tasks["chatty"])
    try:
        deadline = time.monotonic() + 10.0
        while not any("[chatty] chatter" in r.getMessage() for r in caplog.records):
            assert time.monotonic() < deadline, "the orphan never started writing"
            await asyncio.sleep(0.02)
        while not (pid_file.exists() and len(pid_file.read_text().split()) == 2):
            assert time.monotonic() < deadline, "the quiet worker never started its helper"
            await asyncio.sleep(0.02)
    except BaseException:
        _kill_descendants(pid_file)
        await sup.stop()
        raise
    stopping = asyncio.create_task(sup.stop())
    try:
        deadline = time.monotonic() + 3.0
        while not all(t.done() for t in quiet) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        assert all(t.done() for t in quiet), "the quiet worker's forwarders were kept alive"
        assert not stopping.done()
        assert any(not t.done() for t in chatty), "the chatty orphan's pipe was cut off early"
    finally:
        _kill_descendants(pid_file)
        await asyncio.wait_for(stopping, timeout=10.0)


# A worker whose last output has no trailing newline; a helper holds the pipe.
# argv: pid file.
_PARTIAL_LINE_WORKER = """
import subprocess, sys, time
sys.stdout.write("last words")
sys.stdout.flush()
helper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
with open(sys.argv[1], "a") as f:
    f.write("%d\\n" % helper.pid)
time.sleep(30)
"""


@pytest.mark.real_process
async def test_stop_forwards_a_last_line_without_a_newline_from_a_held_pipe(
    tmp_path, caplog
) -> None:
    """stop() cancels a forwarder whose pipe a descendant holds; the partial
    line it has read so far is still logged, once."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    pid_file = tmp_path / "descendants"
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=lambda s, p: [sys.executable, "-c", _PARTIAL_LINE_WORKER, str(pid_file)],
        shutdown_timeout=1.0,
    )
    await sup.start()
    try:
        deadline = time.monotonic() + 5.0
        while not pid_file.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert pid_file.exists(), "the worker never started its helper"

        await sup.stop()
    finally:
        _kill_descendants(pid_file)

    forwarded = [r.getMessage() for r in caplog.records if "last words" in r.getMessage()]
    assert forwarded == ["[orders] last words"]


@pytest.mark.real_process
async def test_a_last_line_without_a_newline_is_forwarded_at_eof(caplog) -> None:
    """Lines split across reads stay whole, and the unterminated tail is
    logged at the severity of the stream it came from."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    spec = WorkerSpec("orders", "fakeapp", 9001)
    command = [
        sys.executable,
        "-c",
        "import sys, time\n"
        "sys.stderr.write('par'); sys.stderr.flush(); time.sleep(0.1)\n"
        "sys.stderr.write('tial\\nnext\\ntail with no newline'); sys.stderr.flush()",
    ]
    sup = Supervisor([spec], command_builder=lambda s, p: command)

    proc = await sup._spawn("orders", spec, 9001)
    await proc.wait()
    await asyncio.gather(*list(sup._log_tasks), return_exceptions=True)

    forwarded = [
        (r.levelno, r.getMessage()) for r in caplog.records if r.name.endswith("supervisor")
    ]
    assert [m for _, m in forwarded if m.startswith("[orders]")] == [
        "[orders] partial",
        "[orders] next",
        "[orders] tail with no newline",
    ]
    assert {lvl for lvl, m in forwarded if m.startswith("[orders]")} == {logging.WARNING}


@pytest.mark.real_process
async def test_a_raising_listener_is_logged_and_the_worker_is_still_restarted(caplog) -> None:
    calls: list[int] = []

    def flaky_listener(port: int) -> None:
        calls.append(port)
        if len(calls) > 1:  # every call after the first spawn, the exit included
            raise RuntimeError("metrics backend down")

    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=lambda s, p: _CRASH,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=5,
        restart_healthy_uptime=30.0,
    )
    sup.add_spawn_listener(flaky_listener)

    def spawns() -> int:
        return sum("spawned worker" in r.getMessage() for r in caplog.records)

    try:
        await sup.start()
        deadline = time.monotonic() + 3.0
        while spawns() < 3 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        assert spawns() >= 3
        listener_errors = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR and "listener" in r.getMessage() and r.exc_info
        ]
        assert listener_errors
    finally:
        await sup.stop()


@pytest.mark.real_process
async def test_failed_respawn_retries_under_backoff_and_trips_the_breaker(caplog) -> None:
    """A respawn that raises (fork EAGAIN under pid/thread pressure, ENOMEM,
    EMFILE on the pipes) used to kill the monitor task outright. Nothing
    retrieved the exception — the strong ref in _monitor_tasks suppresses
    asyncio's "never retrieved" warning and stop() gathers it away — so the
    module silently stopped being supervised with "restarting in Ns" as the
    last thing an operator saw: no ERROR log, no _failed_instances entry, no
    further respawn ever. The failure must run through the same backoff and
    crash-loop breaker as a crash does."""
    caplog.set_level("ERROR", logger="modulith.supervisor")
    calls: list[int] = []

    def flaky_builder(spec: WorkerSpec, port: int) -> list[str]:
        calls.append(port)
        if len(calls) > 1:  # every respawn after the initial spawn fails
            raise BlockingIOError(11, "Resource temporarily unavailable")
        return _CRASH

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=flaky_builder,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=3,
        restart_healthy_uptime=5.0,
    )
    try:
        await sup.start()
        for _ in range(300):  # bounded poll — no fixed-sleep synchronization
            await asyncio.sleep(0.01)
            if "orders" in sup._failed_instances:
                break

        assert "orders" in sup._failed_instances
        assert len(calls) > 1  # the failing respawn was retried, not swallowed
        assert any(r.levelname == "ERROR" for r in caplog.records)
    finally:
        await sup.stop()


@pytest.mark.real_process
async def test_crash_loop_gives_up_after_max_restarts() -> None:
    """An always-crashing worker is abandoned once the breaker trips (#33).

    Without a cap the supervisor respawned forever; now it stops respawning,
    marks the instance failed, and the spawn count stays bounded.
    """
    calls: list[tuple[str, int]] = []

    def crash_builder(spec: WorkerSpec, port: int) -> list[str]:
        calls.append((spec.module_name, port))
        return _CRASH

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=crash_builder,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=3,
        # Comfortably longer than real subprocess spawn+exit latency so a
        # rapid real crash loop is never mistaken for a healthy recovery.
        restart_healthy_uptime=5.0,
    )
    try:
        await sup.start()
        # Wait for the breaker to trip (bounded poll, no fixed sleep).
        for _ in range(300):
            await asyncio.sleep(0.01)
            if "orders" in sup._failed_instances:
                break
        assert "orders" in sup._failed_instances
        settled = len(calls)
        # No further respawns after giving up.
        await asyncio.sleep(0.1)
        assert len(calls) == settled
        # Bounded: initial spawn + at most max_restarts respawns.
        assert settled <= sup._max_restarts + 1
    finally:
        await sup.stop()


@pytest.mark.real_process
async def test_failed_instances_is_surfaced_after_the_breaker_gives_up() -> None:
    """Supervisor.failed_instances() is the read API the health wiring
    consumes — it must reflect what _monitor_worker records internally in
    ``_failed_instances``, not just the private attribute itself."""

    def crash_builder(spec: WorkerSpec, port: int) -> list[str]:
        return _CRASH

    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=crash_builder,
        restart_initial_delay=0.01,
        restart_max_delay=0.01,
        max_restarts=1,
        restart_healthy_uptime=5.0,
    )
    try:
        await sup.start()
        for _ in range(300):
            await asyncio.sleep(0.01)
            if sup.failed_instances():
                break
        assert sup.failed_instances() == frozenset({"orders"})
    finally:
        await sup.stop()


# ---------------------------------------------------------------------------
# _serve_uvicorn — the real production proxy server
# ---------------------------------------------------------------------------


@pytest.mark.real_process
async def test_run_supervised_default_serve_binds_real_uvicorn() -> None:
    """With no ``serve=`` override, run_supervised must serve the
    proxy via the real ``_serve_uvicorn`` — the production default behind
    ``modulith run``/``dev`` (cli.py calls run_supervised with no override) —
    and actually be reachable over HTTP on the requested port.

    Every other test bypasses this: CLI tests monkeypatch run_supervised, the
    supervisor tests inject ``serve=``, and the proxy e2e tests use an
    ASGITransport. This is the one place the shipped server binding runs.
    """
    import httpx

    port = _free_port()
    # No worker specs: the proxy serves only its actuator endpoints, so no
    # subprocesses spawn and the test exercises exactly the uvicorn binding.
    task = asyncio.create_task(run_supervised([], "127.0.0.1", port))

    try:
        async with httpx.AsyncClient() as client:
            deadline = time.monotonic() + 15.0
            while True:  # bounded readiness poll — no fixed-sleep synchronization
                try:
                    resp = await client.get(f"http://127.0.0.1:{port}/_modulith/health")
                    break
                except httpx.TransportError:
                    assert not task.done(), f"server died during startup: {task.exception()!r}"
                    assert time.monotonic() < deadline, "uvicorn never became reachable"
                    await asyncio.sleep(0.05)

        assert resp.status_code == 200
        assert resp.json() == {"status": "ok", "backends": {}}
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class _LogLines(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


@pytest.fixture
def no_access_filters() -> Iterator[None]:
    """Start from a ``uvicorn.access`` logger without filters, and leave it as found.

    A worker app built by an earlier test installs the filter for the rest of the
    process; without the reset this test would pass on that filter alone.
    """
    logger = logging.getLogger("uvicorn.access")
    filters = logger.filters[:]
    logger.filters.clear()
    yield
    logger.filters[:] = filters


@pytest.mark.real_process
async def test_proxy_access_log_hides_the_query_string(caplog, no_access_filters: None) -> None:
    """uvicorn's own access record, from the proxy ``run_supervised`` really serves,
    reaches its handlers without the query string.

    The request goes through the real h11/httptools protocol, so the record has
    whatever shape the installed uvicorn gives it, not one this test assumes.
    """
    import httpx

    # _serve_uvicorn takes uvicorn's log level from the root logger, and the
    # access line is INFO.
    caplog.set_level(logging.INFO)
    access = logging.getLogger("uvicorn.access")
    seen = _LogLines()
    port = _free_port()
    task = asyncio.create_task(run_supervised([], "127.0.0.1", port))

    try:
        async with httpx.AsyncClient() as client:
            deadline = time.monotonic() + 15.0
            while True:
                try:
                    await client.get(f"http://127.0.0.1:{port}/_modulith/health")
                    break
                except httpx.TransportError:
                    assert not task.done(), f"server died during startup: {task.exception()!r}"
                    assert time.monotonic() < deadline, "uvicorn never became reachable"
                    await asyncio.sleep(0.05)

            # Configuring the server replaced the logger's handlers, so ours goes on now.
            access.addHandler(seen)
            resp = await client.get(f"http://127.0.0.1:{port}/_modulith/health?token=hunter2")

        assert resp.status_code == 200
        assert len(seen.lines) == 1
        assert re.fullmatch(
            r'127\.0\.0\.1:\d+ - "GET /_modulith/health HTTP/1\.1" 200', seen.lines[0]
        ), seen.lines[0]
    finally:
        access.removeHandler(seen)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.fixture
def no_httpx_filters() -> Iterator[None]:
    """Start from an ``httpx`` logger without filters, and leave it as found."""
    logger = logging.getLogger("httpx")
    filters = logger.filters[:]
    logger.filters.clear()
    yield
    logger.filters[:] = filters


@pytest.mark.real_process
async def test_proxy_process_httpx_lines_hide_the_query_string(
    caplog, no_httpx_filters: None
) -> None:
    """httpx logs every request it sends at INFO with the full URL, and the proxy
    forwards through it, so the process ``run_supervised`` serves the proxy from
    must keep the query string out of those lines.

    The requests come from a real httpx client in that process, so the record
    has whatever shape the installed httpx gives it, not one this test assumes.
    """
    import httpx

    caplog.set_level(logging.INFO)
    httpx_logger = logging.getLogger("httpx")
    seen = _LogLines()
    httpx_logger.addHandler(seen)
    port = _free_port()
    task = asyncio.create_task(run_supervised([], "127.0.0.1", port))

    try:
        async with httpx.AsyncClient() as client:
            deadline = time.monotonic() + 15.0
            while True:
                try:
                    await client.get(f"http://127.0.0.1:{port}/_modulith/health")
                    break
                except httpx.TransportError:
                    assert not task.done(), f"server died during startup: {task.exception()!r}"
                    assert time.monotonic() < deadline, "uvicorn never became reachable"
                    await asyncio.sleep(0.05)

            resp = await client.get(f"http://127.0.0.1:{port}/_modulith/health?token=hunter2")

        assert resp.status_code == 200
        line = f'HTTP Request: GET http://127.0.0.1:{port}/_modulith/health "HTTP/1.1 200 OK"'
        assert seen.lines == [line, line]
    finally:
        httpx_logger.removeHandler(seen)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
