"""Process supervisor for the process-per-module topology.

Spawns one subprocess per module, monitors them, restarts crashed
workers, multiplexes their logs back to the supervisor's stdout.

Mental model:

    ┌─────────────────────────────────────────────────────┐
    │  modulith supervisor (port 8000)                    │
    │  - Reverse proxy (modulith.proxy)                   │
    │  - Process supervisor (this module)                 │
    │  - Actuator endpoints                               │
    └────────┬────────────────────┬───────────────────────┘
             │                    │
    ┌────────┴────┐      ┌────────┴────┐
    │ Worker:     │      │ Worker:     │  ...
    │ orders      │      │ inventory   │
    │ (uvicorn    │      │ (uvicorn    │
    │  :9001)     │      │  :9002)     │
    └─────────────┘      └─────────────┘
             │                    │
             └────────┬───────────┘
                      │
            ┌─────────┴─────────┐
            │ Broker (Redis,    │
            │ Kafka, etc.) for  │
            │ event IPC         │
            └───────────────────┘

The supervisor itself is just a Python process running asyncio. It does
not replace systemd or supervisord for production — that's the user's
existing infrastructure. It's the dev experience and an optional
production runtime for users who don't want a separate orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .proxy import RoutingRule

logger = logging.getLogger("modulith.supervisor")

# Bound on how long stop() waits for log forwarders to drain naturally (pipe
# EOF) after their process is confirmed dead, before cancelling stragglers.
# Draining is expected to be near-instant — this is a safety backstop, not a
# tunable, so it isn't threaded through Supervisor.__init__.
_LOG_DRAIN_TIMEOUT = 5.0

# Parent-death signal support (Linux only). libc is resolved in the *parent*
# at import time so the child-side preexec_fn — which runs between fork() and
# exec(), where imports/allocations can deadlock — only makes one
# already-bound C call.
_PR_SET_PDEATHSIG = 1  # linux/prctl.h
_pdeathsig_preexec: Callable[[], None] | None
if sys.platform == "linux":
    import ctypes

    _libc = ctypes.CDLL(None, use_errno=True)

    def _linux_pdeathsig_preexec() -> None:
        """Child-side hook: SIGTERM this worker when its parent dies.

        Runs in the worker between fork() and exec(). Without it a
        SIGKILL'd/crashed supervisor (e.g. OOM-killed) orphans its workers;
        the orphans keep their statically-assigned ports bound, so a fresh
        supervisor's respawns fail with EADDRINUSE until an operator hunts
        them down.
        """
        _libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)

    _pdeathsig_preexec = _linux_pdeathsig_preexec
else:
    # Non-Linux has no parent-death signal: workers CAN be orphaned when the
    # supervisor dies without running stop(). Documented limitation.
    _pdeathsig_preexec = None


# ---------------------------------------------------------------------------
# Worker spec — one per module
# ---------------------------------------------------------------------------


@dataclass
class WorkerSpec:
    """Configuration for one worker subprocess."""

    module_name: str  # e.g. "orders"
    package: str  # e.g. "myapp"
    port: int  # uvicorn binds here (first replica; +1 per extra replica)
    worker_count: int = 1  # multiple processes per module if needed
    env: dict[str, str] | None = None  # additional env vars


# A command builder maps (spec, port) -> argv. Injectable so tests can spawn
# trivial processes instead of a full uvicorn worker.
CommandBuilder = Callable[[WorkerSpec, int], list[str]]

# redis_broker.py predates the generic MODULITH_BROKER_<KEY> convention that
# cli.py uses to forward a resolved broker_options table (matching
# db_broker.py's _broker_opt) — it only reads these specific historical
# names. Mirror the generic form onto them so a broker_options.url etc.
# resolved by the parent and forwarded as MODULITH_BROKER_URL actually
# reaches a re-bootstrapping redis-streams worker, instead of silently never
# arriving because the adapter looks for a different env var name.
_REDIS_BROKER_ENV_ALIASES = {
    "MODULITH_BROKER_URL": "REDIS_URL",
    "MODULITH_BROKER_STREAM_PREFIX": "MODULITH_STREAM_PREFIX",
    "MODULITH_BROKER_CONSUMER_GROUP": "MODULITH_CONSUMER_GROUP",
    "MODULITH_BROKER_MAX_STREAM_LEN": "MODULITH_STREAM_MAXLEN",
}


def _build_worker_env(spec: WorkerSpec) -> dict[str, str]:
    """Build one worker's subprocess environment.

    Precedence (lowest to highest): the supervisor's own inherited
    ``os.environ``, then the module's configured ``spec.env`` (e.g. broker
    settings forwarded by the CLI), then the three reserved identity vars —
    so nothing in ``spec.env`` can ever misroute a worker to the wrong
    module/package/topology.
    """
    env = dict(os.environ)
    if spec.env:
        env.update(spec.env)
    env["MODULITH_MODULE"] = spec.module_name
    env["MODULITH_APP_PACKAGE"] = spec.package
    env["MODULITH_TOPOLOGY"] = "processes"

    if env.get("MODULITH_BROKER") == "redis-streams":
        for generic, specific in _REDIS_BROKER_ENV_ALIASES.items():
            if generic in env and specific not in env:
                env[specific] = env[generic]
    return env


def _default_command(spec: WorkerSpec, port: int) -> list[str]:
    """The production worker command: uvicorn hosting one module's app."""
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "modulith._worker:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]


# ---------------------------------------------------------------------------
# Restart policy: backoff + circuit breaker (pure, unit-testable)
# ---------------------------------------------------------------------------


class _RestartPolicy:
    """Per-instance restart state: exponential backoff + a crash-loop breaker.

    Kept as a pure object (no I/O, time injected) so the two non-trivial
    decisions are deterministically testable without spawning subprocesses or
    sleeping:

      * Backoff reset (recovery): a worker that stayed up beyond
        ``healthy_uptime`` has effectively recovered, so its backoff resets to
        ``initial_delay`` — otherwise a flapping-then-stable worker stays pinned
        at ``max_delay`` for every later transient crash.
      * Circuit breaker: more than ``max_restarts`` crashes inside a rolling
        ``window`` means the module is deterministically broken; stop respawning
        rather than fork-churn forever.
    """

    def __init__(
        self,
        *,
        initial_delay: float,
        max_delay: float,
        healthy_uptime: float,
        max_restarts: int,
        window: float,
    ) -> None:
        self._initial = initial_delay
        self._max = max_delay
        self._healthy_uptime = healthy_uptime
        self._max_restarts = max_restarts
        self._window = window
        self._delay = initial_delay
        self._crashes: deque[float] = deque()

    def on_crash(self, *, uptime: float, now: float) -> float | None:
        """Record a crash; return the delay to wait before respawn.

        Returns ``None`` when the breaker has tripped (caller must give up and
        not respawn). ``now`` is a monotonic timestamp; ``uptime`` is how long
        the just-exited worker had been running.
        """
        # Recovery → reset backoff before this crash's delay is read.
        if uptime >= self._healthy_uptime:
            self._delay = self._initial

        # Circuit breaker: prune crashes outside the window, then bound the rate.
        self._crashes.append(now)
        while self._crashes and now - self._crashes[0] > self._window:
            self._crashes.popleft()
        if len(self._crashes) > self._max_restarts:
            return None

        delay = self._delay
        self._delay = min(self._delay * 2, self._max)
        return delay


# ---------------------------------------------------------------------------
# The Supervisor class
# ---------------------------------------------------------------------------


class Supervisor:
    """Manages a fleet of per-module uvicorn subprocesses.

    Lifecycle:
      1. start() — spawn all configured workers
      2. monitor — watch for crashes, restart with exponential backoff
      3. stop() — graceful SIGTERM cascade, SIGKILL stragglers on timeout
    """

    def __init__(
        self,
        specs: list[WorkerSpec],
        *,
        restart_initial_delay: float = 1.0,
        restart_max_delay: float = 60.0,
        shutdown_timeout: float = 30.0,
        command_builder: CommandBuilder | None = None,
        max_restarts: int = 5,
        restart_window: float = 60.0,
        restart_healthy_uptime: float | None = None,
    ) -> None:
        self._specs = specs
        self._restart_initial_delay = restart_initial_delay
        self._restart_max_delay = restart_max_delay
        self._shutdown_timeout = shutdown_timeout
        self._command_builder: CommandBuilder = command_builder or _default_command
        # Circuit-breaker bounds: more than `max_restarts` crashes within
        # `restart_window` seconds → give up on that instance (a deterministic
        # crash must not be respawned forever). A worker that stays up at least
        # `restart_healthy_uptime` seconds is treated as recovered and its
        # backoff resets; defaults to restart_max_delay.
        self._max_restarts = max_restarts
        self._restart_window = restart_window
        self._restart_healthy_uptime = (
            restart_healthy_uptime if restart_healthy_uptime is not None else restart_max_delay
        )
        # Keyed by instance name (module_name, or module_name-N for replicas).
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._monitor_tasks: list[asyncio.Task[None]] = []
        # A set with drop-on-done callbacks: every (re)spawn adds two log
        # forwarders, so pruning only in stop() would grow this without bound
        # under a crash-looping worker (each restart leaks two done tasks).
        self._log_tasks: set[asyncio.Task[None]] = set()
        # Instances the breaker has given up on — surfaced for health reporting.
        self._failed_instances: set[str] = set()
        self._stopping = False
        # Set by stop() so a monitor mid-backoff (asyncio.sleep(delay), which
        # can be up to restart_max_delay) wakes immediately instead of
        # blocking stop() until the shutdown_timeout cancellation backstop.
        self._stop_event = asyncio.Event()

    def _instance_plan(self) -> list[tuple[str, WorkerSpec, int]]:
        """Expand specs into one (instance_name, spec, port) per replica."""
        plan: list[tuple[str, WorkerSpec, int]] = []
        for spec in self._specs:
            for i in range(max(1, spec.worker_count)):
                name = spec.module_name if spec.worker_count == 1 else f"{spec.module_name}-{i}"
                plan.append((name, spec, spec.port + i))
        return plan

    async def start(self) -> None:
        """Spawn all workers and start monitoring them."""
        self._stopping = False
        self._stop_event.clear()
        for name, spec, port in self._instance_plan():
            proc = await self._spawn(name, spec, port)
            self._monitor_tasks.append(
                asyncio.create_task(self._monitor_worker(name, spec, port, proc))
            )
        logger.info("supervisor started %d worker(s)", len(self._processes))

    async def _spawn(self, name: str, spec: WorkerSpec, port: int) -> asyncio.subprocess.Process:
        """Build env + command, spawn one worker, register it, forward its logs.

        Registration into ``self._processes`` happens with no ``await`` between
        process creation and the dict assignment, so ``stop()`` can never miss a
        live process (which would orphan its subprocess transport).

        On Linux, workers are spawned with ``PR_SET_PDEATHSIG`` (SIGTERM) so
        they self-terminate if the supervisor dies without running ``stop()``
        (SIGKILL, OOM, hard crash) instead of lingering as orphans that hold
        their statically-assigned ports. On other platforms no equivalent
        exists — there, a hard-killed supervisor can still orphan workers.
        """
        cmd = self._command_builder(spec, port)
        env = _build_worker_env(spec)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=_pdeathsig_preexec,  # None outside Linux
        )
        self._processes[name] = proc  # atomic: no await before this point
        logger.info("spawned worker %r on port %d (pid %s)", spec.module_name, port, proc.pid)
        if proc.stdout is not None:
            self._track_log_task(
                asyncio.create_task(self._forward_logs(spec.module_name, proc.stdout))
            )
        if proc.stderr is not None:
            self._track_log_task(
                asyncio.create_task(self._forward_logs(spec.module_name, proc.stderr))
            )
        return proc

    def _track_log_task(self, task: asyncio.Task[None]) -> None:
        """Hold a strong reference to a log forwarder only while it runs.

        The done-callback prunes the task the moment it finishes (worker
        exited → pipe EOF), so restarts don't accumulate completed tasks
        until stop() — the set stays at ~2 live entries per live worker.
        """
        self._log_tasks.add(task)
        task.add_done_callback(self._log_tasks.discard)

    async def _monitor_worker(
        self, name: str, spec: WorkerSpec, port: int, proc: asyncio.subprocess.Process
    ) -> None:
        """Watch one worker; restart on crash with backoff + a crash-loop cap.

        Loops as ``while True`` (rather than ``while not self._stopping``)
        because ``self._stopping`` is flipped by ``stop()`` *across* the
        ``await proc.wait()`` below — the post-await re-checks are the real
        termination guards, and ``stop()`` also cancels this task.

        Backoff and the give-up decision are delegated to ``_RestartPolicy``:
        a worker that exceeds ``max_restarts`` crashes within ``restart_window``
        is abandoned (logged + marked failed) instead of respawned forever, and
        one that ran healthily long enough has its backoff reset.
        """
        policy = _RestartPolicy(
            initial_delay=self._restart_initial_delay,
            max_delay=self._restart_max_delay,
            healthy_uptime=self._restart_healthy_uptime,
            max_restarts=self._max_restarts,
            window=self._restart_window,
        )
        while True:
            started = time.monotonic()
            return_code = await proc.wait()
            if self._stopping:
                return
            uptime = time.monotonic() - started
            delay = policy.on_crash(uptime=uptime, now=time.monotonic())
            if delay is None:
                logger.error(
                    "worker %s exceeded %d restarts within %.0fs (last exit code %s) — "
                    "giving up; not respawning. Fix the module and restart the supervisor.",
                    name,
                    self._max_restarts,
                    self._restart_window,
                    return_code,
                )
                self._failed_instances.add(name)
                return
            logger.warning(
                "worker %s exited with code %s; restarting in %.1fs",
                name,
                return_code,
                delay,
            )
            # Interruptible backoff: waiting on _stop_event (vs. plain
            # asyncio.sleep(delay)) means stop() wakes this immediately —
            # without it, a crash-looping worker backed off near
            # restart_max_delay (up to 60s) would block stop() until the
            # shutdown_timeout cancellation backstop instead of returning
            # right away.
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except TimeoutError:
                pass
            else:
                return  # stop() interrupted the backoff; do not respawn
            if self._stopping:
                # Reachable: stop() may flip _stopping during the sleep above.
                # mypy narrows it to False from the earlier check and can't
                # model concurrent mutation across the await (cf. runtime.py).
                return  # type: ignore[unreachable]
            proc = await self._spawn(name, spec, port)
            # Reachable for the same reason as the check above: stop() may
            # flip _stopping during _spawn's subprocess-creation await, and
            # its one-shot SIGTERM cascade snapshotted self._processes before
            # this process was registered. Cascade the SIGTERM here —
            # otherwise the worker blocks stop() for the full
            # shutdown_timeout and only ever gets the final SIGKILL reap.
            # The loop's proc.wait() + _stopping check handle the rest.
            if self._stopping and proc.returncode is None:  # type: ignore[unreachable]
                proc.terminate()  # type: ignore[unreachable]

    async def _forward_logs(self, prefix: str, stream: asyncio.StreamReader) -> None:
        """Read a worker's output line-by-line and re-log it with its name.

        ``readline()`` raises ``ValueError`` when a single line exceeds the
        stream's buffer limit (e.g. an unbounded stack trace or a bulk debug
        dump) — it already discards the offending bytes from its internal
        buffer before raising, so the next ``readline()`` call cleanly picks
        up at the following line. Using ``async for line in stream`` instead
        would let that ValueError escape the loop and permanently silence
        this worker's log forwarding after just one oversized line.
        """
        try:
            while True:
                try:
                    line = await stream.readline()
                except ValueError:
                    logger.warning("[%s] <log line exceeded the buffer limit; truncated>", prefix)
                    continue
                if not line:
                    return  # EOF
                logger.info("[%s] %s", prefix, line.decode(errors="replace").rstrip())
        except asyncio.CancelledError:
            raise
        except Exception:  # a dead pipe must not crash the supervisor
            logger.debug("log forwarder for %s stopped", prefix, exc_info=True)

    async def stop(self) -> None:
        """Graceful shutdown: SIGTERM all workers, wait, SIGKILL stragglers.

        Monitors are allowed to observe the termination and return on their own
        (so a monitor mid-respawn finishes registering its process); they're
        cancelled only as a timeout backstop. A final reap sweep then waits on
        every tracked process — including any spawned during shutdown — so no
        subprocess transport is left to be garbage-collected after the loop.
        """
        self._stopping = True
        self._stop_event.set()  # wakes any monitor mid-restart-backoff

        for proc in self._processes.values():
            if proc.returncode is None:
                proc.terminate()

        # Let monitors exit naturally; cancel only if they overrun the timeout.
        if self._monitor_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._monitor_tasks, return_exceptions=True),
                    timeout=self._shutdown_timeout,
                )
            except TimeoutError:
                logger.warning(
                    "monitors did not settle in %.0fs; cancelling", self._shutdown_timeout
                )
                for task in self._monitor_tasks:
                    task.cancel()
                await asyncio.gather(*self._monitor_tasks, return_exceptions=True)

        # Final reap: kill and wait on anything still alive (covers late respawns).
        for proc in self._processes.values():
            if proc.returncode is None:
                proc.kill()
        await asyncio.gather(
            *(proc.wait() for proc in self._processes.values()), return_exceptions=True
        )

        # Snapshot: done-callbacks discard from the set as tasks finish, so
        # iterate and await over a stable copy. Every process is dead by now
        # (killed + waited above), so its pipes are at EOF and the
        # forwarders should drain and finish on their own almost
        # immediately — give them a bounded grace period to do that first.
        # Cancelling unconditionally (the old behavior) could cut off log
        # lines the worker wrote just before dying but that were still
        # sitting unread in the OS pipe buffer, silently dropping a
        # crashing worker's last, most diagnostically useful output.
        log_tasks = list(self._log_tasks)
        if log_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*log_tasks, return_exceptions=True),
                    timeout=_LOG_DRAIN_TIMEOUT,
                )
            except TimeoutError:
                for task in log_tasks:
                    task.cancel()
                await asyncio.gather(*log_tasks, return_exceptions=True)
        self._monitor_tasks.clear()
        self._log_tasks.clear()
        logger.info("supervisor stopped")


# ---------------------------------------------------------------------------
# Top-level orchestration entry point
# ---------------------------------------------------------------------------


def _rules_from_specs(specs: list[WorkerSpec]) -> list[RoutingRule]:
    """Build the reverse-proxy routing table: one rule per module.

    Each module's public prefix (``/orders``) maps to its worker's loopback
    backend (``http://127.0.0.1:9001``). When a module runs multiple replicas
    the proxy targets the first replica's port; cross-replica load balancing
    is a v2 enhancement (the broker already load-shares event consumption
    across replicas via the shared consumer group).
    """
    from .proxy import RoutingRule

    return [
        RoutingRule(
            prefix=f"/{spec.module_name}",
            backend_url=f"http://127.0.0.1:{spec.port}",
        )
        for spec in specs
    ]


async def _serve_uvicorn(app: Any, host: str, port: int) -> None:
    """Default proxy server: run uvicorn until a shutdown signal arrives.

    uvicorn installs its own SIGINT/SIGTERM handlers and ``serve()`` returns
    when one fires, which lets ``run_supervised`` fall through to its
    ``finally`` and stop the workers cleanly.
    """
    import uvicorn

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()


def _is_loopback_host(host: str) -> bool:
    """Whether ``host`` only accepts connections from this machine."""
    return host in ("127.0.0.1", "localhost", "::1")


def _env_flag(name: str) -> bool:
    """Loose boolean env-var read: 1/true/yes (case-insensitive) is True."""
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _resolve_actuator(
    *, mode: str, production: bool, host: str, token: str | None
) -> tuple[bool, str | None]:
    """Resolve ``actuator_mode`` into ``(enabled, token)`` for create_proxy_app.

    - ``"disabled"``: actuator unmounted; token is moot.
    - ``"open"``: explicit opt-out — always open, even if a token happens to
      be configured (e.g. for other, unrelated purposes).
    - ``"token"``: token-guarded is mandatory; refuse to start without one.
    - ``"auto"`` (default): a loopback-only, non-production proxy stays open
      for dev convenience (today's behavior). Otherwise — production, or
      bound to a host reachable from outside this machine — a token becomes
      mandatory too: leaving actuator metadata (topology, health) open on a
      network-reachable production proxy is a real information disclosure,
      not a convenience worth defaulting to.
    """
    from .config import ConfigurationError

    if mode == "disabled":
        return False, None
    if mode == "open":
        return True, None
    if mode == "token":
        if not token:
            raise ConfigurationError(
                "actuator_mode='token' requires a bearer token: set "
                "MODULITH_ACTUATOR_TOKEN or pass actuator_token=... to "
                "run_supervised()."
            )
        return True, token
    # "auto"
    if (production or not _is_loopback_host(host)) and not token:
        raise ConfigurationError(
            "actuator_mode='auto' requires MODULITH_ACTUATOR_TOKEN (or "
            "actuator_token=...) when production=True or the proxy binds a "
            f"non-loopback host ({host!r}) — refusing to start with an "
            "unauthenticated actuator reachable from outside this machine. "
            "Set actuator_mode='open' to explicitly opt out of this guard."
        )
    return True, token


async def run_supervised(
    specs: list[WorkerSpec],
    proxy_host: str,
    proxy_port: int,
    *,
    supervisor: Supervisor | None = None,
    serve: Callable[[Any, str, int], Awaitable[None]] | None = None,
    actuator_token: str | None = None,
    actuator_mode: str | None = None,
    production: bool | None = None,
) -> None:
    """Run the supervisor + reverse proxy together.

    Spawns one worker subprocess per module (via the Supervisor), builds a
    reverse proxy whose routing table maps each module's prefix to its
    worker's port, and serves that proxy on ``(proxy_host, proxy_port)`` until
    a shutdown signal arrives. On the way out — normal exit *or* exception —
    the workers are always stopped so none are orphaned.

    ``actuator_token`` guards the proxy's ``/_modulith/*`` actuator endpoints
    with a bearer token. ``actuator_mode`` (``"auto"`` | ``"token"`` |
    ``"open"`` | ``"disabled"``) governs when that token is required — see
    ``_resolve_actuator``. Neither passed explicitly falls back to the
    ``MODULITH_ACTUATOR_TOKEN`` / ``MODULITH_ACTUATOR_MODE`` /
    ``MODULITH_PRODUCTION`` environment variables (mirroring
    ``Configuration``'s own env resolution), so production deployments can
    enable the guard without code changes.

    ``supervisor`` and ``serve`` are injection seams for testing; production
    callers pass neither and get a real Supervisor plus a uvicorn server.
    """
    from .proxy import create_proxy_app

    if actuator_token is None:
        actuator_token = os.environ.get("MODULITH_ACTUATOR_TOKEN") or None
    if actuator_mode is None:
        actuator_mode = os.environ.get("MODULITH_ACTUATOR_MODE") or "auto"
    if production is None:
        production = _env_flag("MODULITH_PRODUCTION")

    actuator_enabled, actuator_token = _resolve_actuator(
        mode=actuator_mode, production=production, host=proxy_host, token=actuator_token
    )

    rules = _rules_from_specs(specs)
    proxy_app = create_proxy_app(
        rules, actuator_token=actuator_token, actuator_enabled=actuator_enabled
    )
    sup = supervisor if supervisor is not None else Supervisor(specs)
    serve_fn = serve if serve is not None else _serve_uvicorn

    # start() sits INSIDE the try: Supervisor.start() has no mid-loop
    # rollback, so a partial-spawn failure (e.g. the 3rd of 5 workers fails
    # to exec) would otherwise never reach stop() and the already-spawned
    # workers would be orphaned (S3-r3-161). stop() is safe on a partial
    # start — it only reaps what _spawn registered.
    try:
        await sup.start()
        await serve_fn(proxy_app, proxy_host, proxy_port)
    finally:
        await sup.stop()


def derive_specs_from_config(config: dict[str, Any]) -> list[WorkerSpec]:
    """Read application config, discover modules, build one WorkerSpec each.

    ``config`` is a ``[tool.modulith]``-shaped dict:
      - ``package``  — application root package (required)
      - ``workers``  — ``{module_name: count}`` plus optional ``default``
      - ``isolate``  — restrict to this subset of modules (optional)

    Ports are assigned from 9001, incrementing by each module's worker_count
    so replicas never collide.
    """
    package = config.get("package")
    if not package:
        raise ValueError(
            "derive_specs_from_config requires a 'package' key "
            "(set [tool.modulith].package or pass it explicitly)"
        )

    from .manager import create_plugin_manager

    pm = create_plugin_manager()
    module_infos = pm.hook.modulith_discover_modules(app_package=package) or []
    names = sorted(m.name for m in module_infos)

    isolate = config.get("isolate")
    if isolate:
        wanted = set(isolate)
        names = [n for n in names if n in wanted]

    workers = config.get("workers") or {}
    default_count = int(workers.get("default", 1))
    if default_count < 1:
        raise ValueError(f"[tool.modulith.workers] default must be >= 1, got {default_count}")
    worker_env = dict(config.get("env") or {})

    specs: list[WorkerSpec] = []
    port = 9001
    for name in names:
        count = int(workers.get(name, default_count))
        if count < 1:
            # Loud-config-error contract: the port counter advances by each
            # module's count, so a 0/negative entry would silently assign a
            # later module the same (or an earlier) port as this one.
            raise ValueError(
                f"[tool.modulith.workers] {name} must be >= 1, got {count} — "
                "a module cannot run zero worker processes"
            )
        specs.append(
            WorkerSpec(
                module_name=name,
                package=package,
                port=port,
                worker_count=count,
                env=worker_env.copy() or None,
            )
        )
        port += count
    return specs


__all__ = [
    "Supervisor",
    "WorkerSpec",
    "_rules_from_specs",
    "derive_specs_from_config",
    "run_supervised",
]
