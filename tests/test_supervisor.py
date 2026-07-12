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
import socket
import sys
import time

import pytest

from modulith.supervisor import (
    Supervisor,
    WorkerSpec,
    _RestartPolicy,
    _rules_from_specs,
    derive_specs_from_config,
    run_supervised,
)

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


def test_derive_specs_isolate_filters_modules(make_fake_app) -> None:
    make_fake_app({"orders": "", "inventory": ""})

    specs = derive_specs_from_config({"package": "fakeapp", "isolate": ["orders"]})

    assert {s.module_name for s in specs} == {"orders"}


def test_derive_specs_requires_package() -> None:
    with pytest.raises(ValueError, match="package"):
        derive_specs_from_config({})


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


# ---------------------------------------------------------------------------
# Lifecycle (real subprocesses)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_start_spawns_all_workers() -> None:
    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=_sleep_builder)
    try:
        await sup.start()
        assert len(sup._processes) == 1
        proc = next(iter(sup._processes.values()))
        assert proc.returncode is None  # alive
    finally:
        await sup.stop()


@pytest.mark.integration
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


@pytest.mark.integration
async def test_stop_terminates_all_workers() -> None:
    sup = Supervisor([WorkerSpec("orders", "fakeapp", 9001)], command_builder=_sleep_builder)
    await sup.start()
    proc = next(iter(sup._processes.values()))

    await sup.stop()

    assert proc.returncode is not None  # exited


@pytest.mark.integration
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
    p = _RestartPolicy(
        initial_delay=1.0, max_delay=10.0, healthy_uptime=100.0, max_restarts=100, window=1000.0
    )
    delays = [p.on_crash(uptime=0.0, now=float(i)) for i in range(5)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 10.0]  # last is capped at max


def test_restart_policy_resets_backoff_after_healthy_uptime() -> None:
    """A worker that ran past healthy_uptime resets backoff to initial (#50)."""
    p = _RestartPolicy(
        initial_delay=1.0, max_delay=10.0, healthy_uptime=5.0, max_restarts=100, window=1000.0
    )
    # Three quick crashes escalate the backoff.
    assert p.on_crash(uptime=0.0, now=0.0) == 1.0
    assert p.on_crash(uptime=0.0, now=1.0) == 2.0
    assert p.on_crash(uptime=0.0, now=2.0) == 4.0
    # A crash AFTER a healthy run resets the next delay back to initial.
    assert p.on_crash(uptime=10.0, now=3.0) == 1.0


def test_restart_policy_trips_breaker_after_max_restarts() -> None:
    """More than max_restarts crashes inside the window → give up (None) (#33)."""
    p = _RestartPolicy(
        initial_delay=0.01, max_delay=0.01, healthy_uptime=100.0, max_restarts=3, window=1000.0
    )
    assert p.on_crash(uptime=0.0, now=0.0) is not None
    assert p.on_crash(uptime=0.0, now=1.0) is not None
    assert p.on_crash(uptime=0.0, now=2.0) is not None
    # 4th crash within window exceeds the cap → breaker trips.
    assert p.on_crash(uptime=0.0, now=3.0) is None


def test_restart_policy_window_prunes_old_crashes() -> None:
    """Crashes older than the window don't count toward the cap."""
    p = _RestartPolicy(
        initial_delay=0.01, max_delay=0.01, healthy_uptime=100.0, max_restarts=3, window=10.0
    )
    p.on_crash(uptime=0.0, now=0.0)
    p.on_crash(uptime=0.0, now=1.0)
    p.on_crash(uptime=0.0, now=2.0)
    # Far in the future: the three earlier crashes age out of the window, so
    # this is the only crash in-window and the breaker does NOT trip.
    assert p.on_crash(uptime=0.0, now=100.0) is not None


@pytest.mark.integration
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
        restart_window=60.0,
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


# ---------------------------------------------------------------------------
# _serve_uvicorn — the real production proxy server (S3-r2-121)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


@pytest.mark.integration
async def test_run_supervised_default_serve_binds_real_uvicorn() -> None:
    """S3-r2-121: with no ``serve=`` override, run_supervised must serve the
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
