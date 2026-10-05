"""Process supervisor for the process-per-module topology.

Spawns one subprocess per module, monitors them, restarts crashed
workers, and re-emits their output through the supervisor's own logger
(``modulith.supervisor``) so one terminal carries the whole deployment.
Where those lines actually land — stderr, a file, a collector — is
whatever the supervisor process configured logging to do; nothing here
writes to stdout directly.

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
import errno
import logging
import os
import secrets
import signal
import socket
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._worker import LogTargetQueryFilter, install_access_log_filter, install_filter_once

if TYPE_CHECKING:
    from .proxy import RoutingRule

logger = logging.getLogger("modulith.supervisor")

# Level names a worker line can carry, mapped to the level the supervisor
# re-emits it at. uvicorn's default formatter prefixes every record with one
# of these plus a colon ("INFO:     Started server process [123]"), so a
# worker's real severity is readable straight off the line.
_LEVEL_TOKENS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
_MAX_LEVEL_TOKEN_LEN = max(len(name) for name in _LEVEL_TOKENS)

# Bound on how long stop() waits for log forwarders to drain naturally (pipe
# EOF) after their process is confirmed dead, before cancelling stragglers.
# Draining is expected to be near-instant — this is a safety backstop, not a
# tunable, so it isn't threaded through Supervisor.__init__.
_LOG_DRAIN_TIMEOUT = 5.0
# A forwarder that has received nothing for this long after its process died
# is waiting on a pipe a surviving descendant holds open, not on unread output:
# it is cancelled then rather than waiting out _LOG_DRAIN_TIMEOUT.
_LOG_QUIET_PERIOD = 0.5
# Longest worker line forwarded whole, and the size of each pipe read. It
# equals asyncio's default StreamReader limit, so the reader's own buffer
# never overruns first.
_LOG_LINE_LIMIT = 2**16
# How often a waiter re-checks a worker's returncode (see _exited).
_EXIT_POLL_INTERVAL = 0.05


async def _exited(proc: asyncio.subprocess.Process) -> int:
    """Return ``proc``'s exit code as soon as the process has exited.

    ``Process.wait()`` resolves only after the process has exited AND every
    pipe to it has closed (``asyncio.base_subprocess``'s ``_try_finish``). A
    descendant that inherited the worker's stdout or stderr would hold it off
    until that descendant exits, possibly never. ``returncode`` is set by the
    child watcher at exit, whatever holds the pipes, so it is polled alongside
    ``wait()``; ``wait()`` still answers first when nothing else holds them.
    """
    wait = asyncio.ensure_future(proc.wait())
    try:
        while proc.returncode is None and not wait.done():
            await asyncio.wait({wait}, timeout=_EXIT_POLL_INTERVAL)
    finally:
        if not wait.done():
            wait.cancel()
            try:
                await wait
            except asyncio.CancelledError:
                # A cancel aimed at this task while it awaits `wait` reaches
                # it only through `wait`; swallowing that would lose it.
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise
    code = proc.returncode
    return code if code is not None else wait.result()


# asyncio's create_server, which uvicorn binds the worker with, sets
# SO_REUSEADDR by default on exactly these platforms
# (asyncio.base_events.BaseEventLoop.create_server). The port probe mirrors it,
# so it fails exactly when the worker's own bind would: on POSIX a TIME_WAIT
# leftover does not count as held, and on Windows, where SO_REUSEADDR lets a
# bind succeed on a port in use, it is not set.
_REUSE_ADDRESS = os.name == "posix" and sys.platform != "cygwin"
# The address every worker binds (see _default_command) and the proxy routes to.
_WORKER_HOST = "127.0.0.1"
# How often a respawn waiting for its port re-probes it.
_PORT_POLL_INTERVAL = 0.1


def _port_held(port: int) -> bool:
    """Whether a worker's bind to ``port`` would fail because it is in use.

    Any other failure to probe (no descriptor left, a port out of range) reads
    as free: the respawn then goes ahead, and its own error handling and the
    crash-loop breaker report the problem instead of the monitor task dying.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            if _REUSE_ADDRESS:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((_WORKER_HOST, port))
    except OSError as exc:
        return exc.errno == errno.EADDRINUSE
    except OverflowError:
        return False
    return False


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

    def __post_init__(self) -> None:
        # bool is an int subclass, so it needs its own check.
        if not isinstance(self.port, int) or isinstance(self.port, bool):
            raise TypeError(
                f"worker {self.module_name!r} port must be an int, "
                f"got {type(self.port).__name__} {self.port!r}"
            )


def _replica_ports(spec: WorkerSpec) -> list[int]:
    """Every replica's port for one module spec, in instance order."""
    return [spec.port + i for i in range(max(1, spec.worker_count))]


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
    "MODULITH_BROKER_MAX_STREAM_LEN": "MODULITH_STREAM_MAXLEN",
}


def _build_worker_env(spec: WorkerSpec) -> dict[str, str]:
    """Build one worker's subprocess environment.

    Precedence (lowest to highest): the supervisor's own inherited
    ``os.environ``, then the module's configured ``spec.env`` (e.g. broker
    settings forwarded by the CLI), then the three reserved identity vars —
    so nothing in ``spec.env`` can ever misroute a worker to the wrong
    module/package/topology.

    Exception: non-empty inherited ``MODULITH_BROKER*`` values beat
    ``spec.env`` for the same key. That matches the adapter's documented
    env > broker_options order and prevents a pyproject URL forwarded in
    ``spec.env`` from clobbering a deployment ``MODULITH_BROKER_URL``.

    ``FORWARDED_ALLOW_IPS`` is dropped from the inherited environment so
    workers trust forwarded headers only from the local proxy at 127.0.0.1.
    """
    env = dict(os.environ)
    env.pop("FORWARDED_ALLOW_IPS", None)
    if spec.env:
        for key, value in spec.env.items():
            if key.startswith("MODULITH_BROKER") and env.get(key):
                continue
            env[key] = value
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
        _WORKER_HOST,
        "--port",
        str(port),
    ]


def _line_level(text: str, default: int) -> int:
    """Read a leading ``LEVEL:`` token off one worker line, else ``default``.

    Only an exact standard level name is honoured, so ordinary prose that
    happens to contain a colon ("Traceback (most recent call last):") falls
    through to ``default`` instead of being mistaken for a severity.
    """
    head, separator, _ = text.partition(":")
    if separator and len(head) <= _MAX_LEVEL_TOKEN_LEN:
        return _LEVEL_TOKENS.get(head.upper(), default)
    return default


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
      * Circuit breaker: more than ``max_restarts`` crashes IN A ROW since the
        last healthy recovery means the module is deterministically broken;
        stop respawning rather than fork-churn forever. Counting consecutive
        crashes (rather than crashes inside a rolling time window) is
        deliberate: a module crashing at a steady interval slower than
        roughly max_restarts/window would keep a window's in-window count at
        or below max_restarts forever, so it was respawned indefinitely
        instead of ever tripping. Consecutive counting catches any steady
        crash loop regardless of how far apart the crashes are.
    """

    def __init__(
        self,
        *,
        initial_delay: float,
        max_delay: float,
        healthy_uptime: float,
        max_restarts: int,
    ) -> None:
        self._initial = initial_delay
        self._max = max_delay
        self._healthy_uptime = healthy_uptime
        self._max_restarts = max_restarts
        self._delay = initial_delay
        self._consecutive_crashes = 0

    def on_crash(self, *, uptime: float, now: float) -> float | None:
        """Record a crash; return the delay to wait before respawn.

        Returns ``None`` when the breaker has tripped (caller must give up and
        not respawn). ``uptime`` is how long the just-exited worker had been
        running. ``now`` (a monotonic timestamp) is accepted for call-site
        symmetry but not used: the breaker's decision depends only on the
        crash streak, never on wall-clock spacing between crashes.
        """
        # Recovery: the previous run stayed up long enough to count as
        # healthy, so both backoff and the consecutive-crash streak reset
        # before this crash is counted.
        if uptime >= self._healthy_uptime:
            self._delay = self._initial
            self._consecutive_crashes = 0

        self._consecutive_crashes += 1
        if self._consecutive_crashes > self._max_restarts:
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
        restart_healthy_uptime: float | None = None,
    ) -> None:
        self._specs = specs
        self._restart_initial_delay = restart_initial_delay
        self._restart_max_delay = restart_max_delay
        self._shutdown_timeout = shutdown_timeout
        self._command_builder: CommandBuilder = command_builder or _default_command
        self._spawn_listeners: list[Callable[[int], None]] = []
        # Circuit-breaker bound: more than `max_restarts` crashes IN A ROW,
        # with no healthy run in between, means the module is deterministically
        # broken → give up on that instance rather than respawn forever. A
        # worker that stays up at least `restart_healthy_uptime` seconds counts
        # as a healthy recovery: it resets both the backoff AND the crash
        # streak. Defaults to restart_max_delay — comfortably longer than any
        # steady crash-loop interval that should trip the breaker, while still
        # being reachable by a module that's actually stabilized.
        self._max_restarts = max_restarts
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
        # The forwarders of each instance's current spawn, so a respawn can
        # settle the previous spawn's (see _release_spawn).
        self._instance_log_tasks: dict[str, set[asyncio.Task[None]]] = {}
        # Monotonic time each live forwarder last read output; read by
        # _drain_log_tasks to cancel a forwarder whose own pipe has gone quiet.
        self._last_log_read: dict[asyncio.Task[Any] | None, float] = {}
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
            for i, port in enumerate(_replica_ports(spec)):
                name = spec.module_name if spec.worker_count == 1 else f"{spec.module_name}-{i}"
                plan.append((name, spec, port))
        return plan

    def add_spawn_listener(self, listener: Callable[[int], None]) -> None:
        """Call ``listener(port)`` each time a worker is about to be spawned
        on ``port``, the first start and every restart alike, and again as
        soon as a worker on ``port`` exits: before any restart backoff, and
        also when the crash-loop breaker gives up and nothing is respawned."""
        self._spawn_listeners.append(listener)

    def failed_instances(self) -> frozenset[str]:
        """Instances the crash-loop breaker has permanently given up on.

        Read by ``run_supervised``'s health wiring so ``/_modulith/health``
        can report a definitively abandoned instance distinctly from one
        still mid restart-backoff — see ``_failed_instances``.
        """
        return frozenset(self._failed_instances)

    async def start(self) -> None:
        """Spawn all workers and start monitoring them."""
        self._stopping = False
        self._stop_event.clear()
        if _pdeathsig_preexec is None:
            logger.warning(
                "orphan protection unavailable on this platform (%s): a "
                "SIGKILL'd/OOM-killed supervisor can leave workers running "
                "with their ports still bound",
                sys.platform,
            )
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
        self._notify_listeners(port)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            preexec_fn=_pdeathsig_preexec,  # None outside Linux
        )
        self._processes[name] = proc  # atomic: no await before this point
        # Log under the INSTANCE name, not spec.module_name: with
        # worker_count > 1 every replica shares one module name, so a
        # module-name prefix would make all replicas' interleaved output
        # indistinguishable and unattributable to the crash/restart messages,
        # which are keyed by instance. Single-replica instances are named
        # after their module, so their output is unchanged.
        logger.info("spawned worker %r on port %d (pid %s)", name, port, proc.pid)
        if proc.stdout is not None:
            self._track_log_task(
                name, asyncio.create_task(self._forward_logs(name, proc.stdout, logging.INFO))
            )
        if proc.stderr is not None:
            self._track_log_task(
                name, asyncio.create_task(self._forward_logs(name, proc.stderr, logging.WARNING))
            )
        return proc

    def _notify_listeners(self, port: int) -> None:
        """Call every spawn listener; one that raises is logged, never fatal.

        A raising listener must not stop a spawn or kill a monitor task: the
        module would silently stop being supervised.
        """
        for listener in self._spawn_listeners:
            try:
                listener(port)
            except Exception:
                logger.exception("spawn listener %r failed for port %d", listener, port)

    def _track_log_task(self, name: str, task: asyncio.Task[None]) -> None:
        """Hold a strong reference to a log forwarder only while it runs.

        The done-callback prunes the task the moment it finishes (worker
        exited → pipe EOF), so restarts don't accumulate completed tasks
        until stop() — the set stays at ~2 live entries per live worker.
        """
        instance_tasks = self._instance_log_tasks.setdefault(name, set())
        for tasks in (self._log_tasks, instance_tasks):
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        task.add_done_callback(lambda done: self._last_log_read.pop(done, None))

    async def _drain_log_tasks(self, log_tasks: set[asyncio.Task[None]]) -> None:
        """Give the forwarders of dead processes a bounded grace period, then cancel.

        Every process is dead by now, so its last output already sits in the
        OS pipe buffer and the forwarders should reach EOF almost immediately.
        Cancelling them unconditionally could drop a crashing worker's last,
        most diagnostically useful lines. A forwarder whose own pipe stays quiet
        for _LOG_QUIET_PERIOD is waiting on a pipe a surviving descendant holds
        open, and is cancelled then, however much the others are still reading.
        """
        pending = set(log_tasks)  # done-callbacks discard from the original
        started = time.monotonic()
        while pending:
            _, pending = await asyncio.wait(pending, timeout=_LOG_QUIET_PERIOD)
            now = time.monotonic()
            if now - started >= _LOG_DRAIN_TIMEOUT:
                stale = pending
            else:
                stale = {
                    task
                    for task in pending
                    if now - max(started, self._last_log_read.get(task, 0.0)) >= _LOG_QUIET_PERIOD
                }
            for task in stale:
                task.cancel()
            await asyncio.gather(*stale, return_exceptions=True)
            pending = pending - stale

    @staticmethod
    def _close_transport(proc: asyncio.subprocess.Process) -> None:
        """Close a dead process's transport, and with it any pipe a descendant holds.

        asyncio has no public handle for this, and the transport (with its pipe
        descriptors) stays open for as long as a descendant keeps a pipe open.
        ``Process._transport`` exists on CPython 3.11 to 3.14.
        """
        transport = getattr(proc, "_transport", None)
        if transport is not None:
            transport.close()

    async def _release_spawn(self, name: str, proc: asyncio.subprocess.Process) -> None:
        """Settle a dead spawn: drain its forwarders, then close its transport."""
        await self._drain_log_tasks(self._instance_log_tasks.pop(name, set()))
        self._close_transport(proc)

    async def _monitor_worker(
        self, name: str, spec: WorkerSpec, port: int, proc: asyncio.subprocess.Process
    ) -> None:
        """Watch one worker; restart on crash with backoff + a crash-loop cap.

        Loops as ``while True`` (rather than ``while not self._stopping``)
        because ``self._stopping`` is flipped by ``stop()`` *across* the
        ``await _exited(proc)`` below — the post-await re-checks are the real
        termination guards, and ``stop()`` also cancels this task.

        Backoff and the give-up decision are delegated to ``_RestartPolicy``:
        a worker that crashes more than ``max_restarts`` times IN A ROW, with
        no healthy run in between, is abandoned (logged + marked failed)
        instead of respawned forever, and one that ran healthily long enough
        has its backoff AND crash streak reset.

        Before each respawn the worker's port must be free (see
        ``_await_port_release``): time spent waiting for it is not a crash.
        """
        policy = _RestartPolicy(
            initial_delay=self._restart_initial_delay,
            max_delay=self._restart_max_delay,
            healthy_uptime=self._restart_healthy_uptime,
            max_restarts=self._max_restarts,
        )
        while True:
            started = time.monotonic()
            return_code = await _exited(proc)
            self._notify_listeners(port)
            if self._stopping:
                return
            await self._release_spawn(name, proc)
            uptime = time.monotonic() - started
            delay = policy.on_crash(uptime=uptime, now=time.monotonic())
            if delay is None:
                logger.error(
                    "worker %s crashed %d times in a row with no healthy run in "
                    "between (last exit code %s) — giving up; not respawning. "
                    "Fix the module and restart the supervisor.",
                    name,
                    self._max_restarts + 1,
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
            await self._await_port_release(name, port)
            if self._stopping:
                # Reachable: stop() may flip _stopping during the sleep above.
                # mypy narrows it to False from the earlier check and can't
                # model concurrent mutation across the await (cf. runtime.py).
                return  # type: ignore[unreachable]
            try:
                proc = await self._spawn(name, spec, port)
            except Exception:
                # create_subprocess_exec fails under host resource pressure
                # (fork EAGAIN on a pid/thread cap, ENOMEM, EMFILE on the
                # pipes). Letting that escape kills this monitor task, and
                # nothing ever retrieves its exception — the module silently
                # stops being supervised, with "restarting in Ns" as the last
                # word an operator sees. Loop instead: the dead proc's wait()
                # returns immediately, so the retry runs under the same
                # backoff and the same crash-loop breaker as a crash does.
                logger.exception("failed to respawn worker %s; retrying under backoff", name)
                continue
            # Reachable for the same reason as the check above: stop() may
            # flip _stopping during _spawn's subprocess-creation await, and
            # its one-shot SIGTERM cascade snapshotted self._processes before
            # this process was registered. Cascade the SIGTERM here —
            # otherwise the worker blocks stop() for the full
            # shutdown_timeout and only ever gets the final SIGKILL reap.
            # The loop's _exited(proc) + _stopping check handle the rest.
            if self._stopping and proc.returncode is None:  # type: ignore[unreachable]
                proc.terminate()  # type: ignore[unreachable]

    async def _await_port_release(self, name: str, port: int) -> None:
        """Wait, up to ``restart_max_delay`` seconds, for ``port`` to be free.

        A process the dead worker started (a fork-started pool child, say)
        inherits its listening socket and can outlive it. Every respawn
        would then fail to bind, and each failure would count as a crash,
        so a hold of a few tens of seconds made the breaker abandon the
        module for good. Waiting here costs no crash. Past the bound the
        respawn goes ahead, and the breaker handles a port that stays held.
        ``stop()`` ends the wait at once. The processes holding the port are
        left alone.
        """
        if not _port_held(port):
            return
        bound = self._restart_max_delay
        logger.warning(
            "worker %s: port %d is still in use, probably by a process the dead "
            "worker started; waiting up to %gs for it to be released before respawning",
            name,
            port,
            bound,
        )
        deadline = time.monotonic() + bound
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=min(_PORT_POLL_INTERVAL, remaining)
                )
            except TimeoutError:
                if not _port_held(port):
                    return
            else:
                return

    async def _forward_logs(
        self, prefix: str, stream: asyncio.StreamReader, default_level: int
    ) -> None:
        """Read a worker's output line-by-line and re-log it with its name.

        Each line is re-emitted at the severity the WORKER gave it, never at a
        fixed level: re-logging everything at INFO put the supervisor's own
        level filter in charge of what an operator sees, and since nothing
        configures the root logger in a plain deployment, that silently
        swallowed every warning a worker emitted — including the one saying a
        module exposes no ``router`` and therefore 404s every request.

        ``default_level`` is the severity for a line that carries no level
        token, and differs by stream because the two streams mean different
        things. On stderr an untagged line came either through
        ``logging.lastResort`` (nothing configures logging in a worker
        process, and lastResort only emits WARNING and above) or from an
        interpreter-level traceback, so WARNING is its floor. stdout is not a
        logging stream at all — uvicorn logs to stderr — so a line there is
        application ``print()`` output making no severity claim, and stays at
        INFO. Untagged lines are never promoted past their floor, which is
        what keeps a worker's routine INFO chatter out of an operator's
        warning-level view.

        The stream is read in chunks into a local buffer rather than with
        ``readline()``, because ``readline()`` keeps a partial line inside the
        reader: when stop() cancels a forwarder whose pipe a descendant holds
        open, that unterminated last line would be lost. Whatever the buffer
        holds is logged as one final line at EOF, on cancellation and on a
        pipe error. A line longer than ``_LOG_LINE_LIMIT`` (an unbounded stack
        trace or a bulk debug dump) is replaced by one truncation notice and
        skipped up to its newline, so forwarding carries on after it.
        """
        buffer = b""
        skipping = False  # inside a line already reported as oversized
        try:
            while chunk := await stream.read(_LOG_LINE_LIMIT):
                self._last_log_read[asyncio.current_task()] = time.monotonic()
                *lines, buffer = (buffer + chunk).split(b"\n")
                for raw in lines:
                    if skipping:
                        skipping = False
                    elif len(raw) > _LOG_LINE_LIMIT:
                        self._warn_line_truncated(prefix)
                    else:
                        self._log_worker_line(prefix, raw, default_level)
                if skipping:
                    buffer = b""
                elif len(buffer) > _LOG_LINE_LIMIT:
                    self._warn_line_truncated(prefix)
                    skipping = True
                    buffer = b""
        except asyncio.CancelledError:
            raise
        except Exception:  # a dead pipe must not crash the supervisor
            logger.debug("log forwarder for %s stopped", prefix, exc_info=True)
        finally:
            if buffer:
                self._log_worker_line(prefix, buffer, default_level)

    @staticmethod
    def _warn_line_truncated(prefix: str) -> None:
        logger.warning("[%s] <log line exceeded the buffer limit; truncated>", prefix)

    @staticmethod
    def _log_worker_line(prefix: str, raw: bytes, default_level: int) -> None:
        text = raw.decode(errors="replace").rstrip()
        logger.log(_line_level(text, default_level), "[%s] %s", prefix, text)

    async def stop(self) -> None:
        """Shut down every worker; graceful only where the platform allows it.

        POSIX: SIGTERM every worker (``terminate()``), wait up to
        ``shutdown_timeout`` for monitors to observe the exit, then SIGKILL
        any still alive — the wait gives a worker's ASGI lifespan a real
        chance to run before the hard kill.

        Windows has no signal delivery on ``subprocess.Popen``: both
        ``terminate()`` and ``kill()`` call ``TerminateProcess`` — an
        immediate, unmaskable hard kill with no softer first step and no
        harder second one. The final SIGKILL pass is skipped there: it would
        be the exact same call already made, and running it anyway would
        misrepresent a no-op as a stronger escalation.

        Monitors are allowed to observe the termination and return on their own
        (so a monitor mid-respawn finishes registering its process); they're
        cancelled only as a timeout backstop. A final reap sweep then waits
        for every tracked process to exit, including any spawned during
        shutdown. A process whose pipes a surviving descendant still holds
        counts as exited: stop() neither waits for nor kills that descendant.
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

        # Final reap: escalate anything still alive (POSIX only — see the
        # docstring above), then wait for every tracked process (covers late
        # respawns) to exit.
        if sys.platform != "win32":
            for proc in self._processes.values():
                if proc.returncode is None:
                    proc.kill()
        await asyncio.gather(
            *(_exited(proc) for proc in self._processes.values()), return_exceptions=True
        )

        await self._drain_log_tasks(self._log_tasks)
        for proc in self._processes.values():
            self._close_transport(proc)
        self._monitor_tasks.clear()
        self._log_tasks.clear()
        self._instance_log_tasks.clear()
        logger.info("supervisor stopped")


# ---------------------------------------------------------------------------
# Top-level orchestration entry point
# ---------------------------------------------------------------------------


def _rules_from_specs(specs: list[WorkerSpec]) -> list[RoutingRule]:
    """Build the reverse-proxy routing table: one rule per module, carrying
    every replica's backend.

    Each module's public prefix (``/orders``) maps to ALL of its worker
    replicas' loopback backends (``http://127.0.0.1:9001``, ``:9002``, ...).
    ``RoutingRule.next_backend()`` round-robins HTTP traffic across them per
    request, and ``/_modulith/health`` reports the module healthy if any one
    replica is — the broker already load-shares event consumption across
    replicas via the shared consumer group; this is what makes HTTP traffic
    and health checks reach every replica too, not just the first.
    """
    from .proxy import RoutingRule

    return [
        RoutingRule(
            prefix=f"/{spec.module_name}",
            backend_url=f"http://127.0.0.1:{spec.port}",
            backend_urls=tuple(f"http://127.0.0.1:{port}" for port in _replica_ports(spec)),
        )
        for spec in specs
    ]


# What httpx logs for each request it sends: method, URL, HTTP version, status
# and reason phrase.
_HTTPX_REQUEST_LINE = 'HTTP Request: %s %s "%s %d %s"'


async def _serve_uvicorn(app: Any, host: str, port: int) -> None:
    """Default proxy server: run uvicorn until a shutdown signal arrives.

    uvicorn installs its own SIGINT/SIGTERM handlers and ``serve()`` returns
    when one fires, which lets ``run_supervised`` fall through to its
    ``finally`` and stop the workers cleanly.

    The proxy logs at whatever level this process's root logger is set to
    (uvicorn's ``Config`` takes a numeric level as readily as a name), rather
    than a hardcoded one: the CLI's ``--log-level`` sets that root level, and
    a proxy that ignored it would keep narrating every forwarded request into
    an operator's deliberately quiet terminal.

    The access lines it writes carry no query string: the proxy sees every
    client's request, and tokens passed as query parameters must not reach the
    logs. Each worker does the same in ``create_app``. The proxy forwards
    through httpx, which logs every request it sends at INFO with the URL it
    requested, query string included; those lines lose the query too. A filter
    rather than a higher level for the ``httpx`` logger, because that line is
    this process's only record of which worker answered a forwarded request or
    a readiness probe.
    """
    import uvicorn

    config = uvicorn.Config(
        app, host=host, port=port, log_level=logging.getLogger().getEffectiveLevel()
    )
    install_access_log_filter()
    install_filter_once("httpx", LogTargetQueryFilter(_HTTPX_REQUEST_LINE, 1))
    await uvicorn.Server(config).serve()


def _is_loopback_host(host: str) -> bool:
    """Whether ``host`` only accepts connections from this machine."""
    return host in ("127.0.0.1", "localhost", "::1")


def _env_flag(name: str) -> bool:
    """Boolean env-var read, unset (or empty) meaning False.

    Delegates to ``config._env_bool`` so the supervisor and the application
    configuration parse the same spellings and reject the same garbage. A
    lenient parser here was a safety downgrade: ``MODULITH_PRODUCTION=ture``
    raised in ``load_configuration()`` but silently resolved to False in this
    process, quietly turning production hardening (the actuator's token
    requirement) back off.
    """
    from .config import _env_bool

    return _env_bool(name) is True


def _env_positive_int(name: str, default: int) -> int:
    """Read a positive-integer env var; anything else is a configuration error."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    from .config import ConfigurationError

    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


def _resolve_actuator(
    *, mode: str, production: bool, host: str, token: str | None
) -> tuple[bool, str | None]:
    """Resolve ``actuator_mode`` into ``(enabled, token)`` for create_proxy_app.

    - ``"disabled"``: actuator unmounted; token is moot.
    - ``"open"``: explicit opt-out — always open, even if a token happens to
      be configured (e.g. for other, unrelated purposes).
    - ``"token"``: token-guarded is mandatory; refuse to start without one.
    - ``"auto"`` (default): a loopback-only, non-production proxy stays open
      for dev convenience. Otherwise — production, or bound to a host
      reachable from outside this machine — a token is required, because
      leaving actuator metadata (topology, health) open on a network-reachable
      production proxy is a real information disclosure. With no token
      configured the actuator is left unmounted rather than the whole
      application refused a start: ``modulith run`` binds ``0.0.0.0`` by
      default, so refusing would make the documented production command
      unstartable out of the box, and nothing is exposed either way. Set
      ``actuator_mode="token"`` to make the missing token fatal instead.
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
        logger.warning(
            "actuator disabled: actuator_mode='auto' will not serve an "
            "unauthenticated /_modulith/* when production=True or the proxy "
            "binds a non-loopback host (%r), and no token is configured. Set "
            "MODULITH_ACTUATOR_TOKEN (or actuator_token=...) to enable it, or "
            "actuator_mode='open' to serve it unauthenticated.",
            host,
        )
        return False, None
    return True, token


def _check_proxy_port(specs: list[WorkerSpec], proxy_port: int) -> None:
    """Refuse a proxy port that any worker replica is also assigned.

    Compared by port alone, whatever the proxy host: workers bind 127.0.0.1,
    which a wildcard or loopback proxy bind overlaps. A proxy bound to a
    specific non-loopback address would not actually collide, but is refused
    too; moving either port costs less than debugging one module that never
    binds while its prefix proxies back into the proxy.
    """
    from .config import ConfigurationError

    for spec in specs:
        if proxy_port in _replica_ports(spec):
            raise ConfigurationError(
                f"proxy port {proxy_port} is also assigned to worker {spec.module_name!r} "
                f"(ports {spec.port}-{spec.port + max(1, spec.worker_count) - 1}); "
                "choose another --port or move the workers with --worker-port-base, "
                "[tool.modulith] worker_port_base or MODULITH_WORKER_PORT_BASE"
            )


def _check_replica_overlap(specs: list[WorkerSpec]) -> None:
    """Refuse two replicas assigned the same port.

    Derived specs are consecutive and never overlap; hand-built ones can. The
    later replica would never bind and its prefix would reach the earlier
    one's worker.
    """
    from .config import ConfigurationError

    owners: dict[int, str] = {}
    for spec in specs:
        for port in _replica_ports(spec):
            if port in owners:
                raise ConfigurationError(
                    f"workers {owners[port]!r} and {spec.module_name!r} are both assigned "
                    f"port {port}; give each replica its own port"
                )
            owners[port] = spec.module_name


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

    ``MODULITH_PROXY_MAX_BODY_BYTES`` raises the proxy's request-body cap
    (10 MiB by default) — the proxy buffers each request body in memory, so
    the cap exists, but an app with large uploads has no other way past it
    from ``modulith run``.

    ``MODULITH_PROXY_MAX_CONNECTIONS`` bounds the proxy's concurrent upstream
    connections (1000 by default). Each in-flight proxied request, including
    a long-poll or streaming response, holds one until it finishes.

    Each call generates a random deployment token and adds it to every spec's
    ``env`` as ``MODULITH_DEPLOYMENT_TOKEN`` (mutating the specs, so an injected
    supervisor built from them passes it on too). Workers echo it on
    ``/health``, and the proxy reports any backend that does not as a
    ``"foreign deployment"`` rather than healthy.

    Raises ``ConfigurationError`` before spawning anything when ``proxy_port``
    equals any worker replica's port (see ``_check_proxy_port``), or when two
    replicas share a port.

    ``supervisor`` and ``serve`` are injection seams for testing; production
    callers pass neither and get a real Supervisor plus a uvicorn server.
    """
    from .proxy import DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_REQUEST_BODY_BYTES, create_proxy_app

    if actuator_token is None:
        actuator_token = os.environ.get("MODULITH_ACTUATOR_TOKEN") or None
    if actuator_mode is None:
        actuator_mode = os.environ.get("MODULITH_ACTUATOR_MODE") or "auto"
    if production is None:
        production = _env_flag("MODULITH_PRODUCTION")

    actuator_enabled, actuator_token = _resolve_actuator(
        mode=actuator_mode, production=production, host=proxy_host, token=actuator_token
    )

    _check_proxy_port(specs, proxy_port)
    _check_replica_overlap(specs)

    deployment_token = secrets.token_hex(16)
    for spec in specs:
        spec.env = {**(spec.env or {}), "MODULITH_DEPLOYMENT_TOKEN": deployment_token}

    sup = supervisor if supervisor is not None else Supervisor(specs)
    rules = _rules_from_specs(specs)

    # Once a worker exits, its port may be bound by another process, so the
    # proxy re-probes /health before forwarding to it again.
    def forget_identity(port: int) -> None:
        url = f"http://127.0.0.1:{port}"
        for rule in rules:
            if url in rule.backend_urls:
                rule.forget_identity(url)

    sup.add_spawn_listener(forget_identity)
    proxy_app = create_proxy_app(
        rules,
        actuator_token=actuator_token,
        actuator_enabled=actuator_enabled,
        max_request_body_bytes=_env_positive_int(
            "MODULITH_PROXY_MAX_BODY_BYTES", DEFAULT_MAX_REQUEST_BODY_BYTES
        ),
        failed_instances=sup.failed_instances,
        deployment_token=deployment_token,
        max_connections=_env_positive_int(
            "MODULITH_PROXY_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS
        ),
    )
    serve_fn = serve if serve is not None else _serve_uvicorn

    # A SIGTERM/SIGINT arriving after workers are spawned but before serve_fn
    # (uvicorn) installs its own handlers would hit Python's default signal
    # disposition (immediate process death), skipping the finally below and
    # orphaning the just-spawned workers. Install a handler for that window
    # that cancels this coroutine's own task so `finally: sup.stop()` still
    # runs.
    #
    # uvicorn takes the signal over with ``signal.signal()``, which replaces
    # the Python-level disposition but leaves asyncio's add_signal_handler
    # registration — and the wakeup fd that drives it — in place, so BOTH
    # callbacks fire on one signal. Cancelling the task then throws straight
    # into ``Server.main_loop``, and ``Server.shutdown()`` never runs:
    # listening sockets stay open, in-flight requests die mid-response with a
    # 500, and the app's lifespan shutdown is skipped. So compare the current
    # disposition against the one asyncio installed for us and stand down when
    # they differ — whoever replaced it owns the shutdown from that point.
    # Nobody replacing it (a serve_fn that installs no handlers) still gets the
    # cancel, which is what keeps the spawn window covered.
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    shutdown_signalled = False
    installed_signals: list[signal.Signals] = []
    own_dispositions: dict[signal.Signals, Any] = {}

    def _handle_shutdown_signal(sig: signal.Signals) -> None:
        nonlocal shutdown_signalled
        if signal.getsignal(sig) is not own_dispositions[sig]:
            return
        shutdown_signalled = True
        if main_task is not None:
            main_task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_shutdown_signal, sig)
        except (NotImplementedError, RuntimeError):
            # No signal support (Windows) or not the main thread — the
            # pre-existing orphan risk on those platforms is unchanged.
            continue
        installed_signals.append(sig)
        own_dispositions[sig] = signal.getsignal(sig)

    # start() sits INSIDE the try: Supervisor.start() has no mid-loop
    # rollback, so a partial-spawn failure (e.g. the 3rd of 5 workers fails
    # to exec) would otherwise never reach stop() and the already-spawned
    # workers would be orphaned. stop() is safe on a partial
    # start — it only reaps what _spawn registered.
    try:
        await sup.start()
        await serve_fn(proxy_app, proxy_host, proxy_port)
    except asyncio.CancelledError:
        if not shutdown_signalled:
            raise
    finally:
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        await sup.stop()


def _log_http_surface(package: str, names: list[str]) -> None:
    """Report the deployment's HTTP surface before any worker is spawned.

    A worker mounts exactly one thing — its module package's ``router``
    attribute — under ``/<module>``. A module whose ``APIRouter`` lives in a
    submodule (``orders/api.py``) and is never re-exported from
    ``orders/__init__.py`` therefore serves nothing, while its worker still
    starts and still reports healthy: every request the proxy forwards to it
    404s.

    Discovery has already imported every module package into this process, so
    presence is read from ``sys.modules`` and nothing is imported here. A
    module missing from it (a custom discovery hook that doesn't import) is
    left out rather than reported as router-less.

    Severity follows what this process — the only one that sees every module —
    can actually conclude. A deployment where NO module exposes a ``router``
    serves no HTTP at all behind the proxy and is unambiguously wrong, so it
    warns. One router-less module among others is ordinary (a listener-only
    module is a first-class shape), so it stays at INFO instead of training
    operators to ignore the warning.
    """
    inspected = [
        (name, sys.modules[f"{package}.{name}"])
        for name in names
        if f"{package}.{name}" in sys.modules
    ]
    with_router = [name for name, mod in inspected if getattr(mod, "router", None) is not None]
    without_router = [name for name, mod in inspected if getattr(mod, "router", None) is None]
    if not without_router:
        return
    if with_router:
        logger.info(
            "module(s) with no 'router' attribute serve no HTTP routes: %s (serving: %s)",
            ", ".join(without_router),
            ", ".join(with_router),
        )
        return
    logger.warning(
        "no module exposes a 'router' attribute (%s): each worker mounts "
        "<module>.router under /<module>, so the proxy will answer 404 to "
        "every request. Re-export each module's APIRouter from its package, "
        "e.g. 'from .api import router' in %s/<module>/__init__.py.",
        ", ".join(without_router),
        package.replace(".", "/"),
    )


def discover_module_names(package: str, contracts_module: str = "contracts") -> list[str]:
    """Sorted names of the package's modules that run as workers."""
    from .manager import create_plugin_manager

    pm = create_plugin_manager()
    module_infos = pm.hook.modulith_discover_modules(app_package=package) or []
    return sorted(m.name for m in module_infos if m.name != contracts_module)


def derive_specs_from_config(config: dict[str, Any]) -> list[WorkerSpec]:
    """Read application config, discover modules, build one WorkerSpec each.

    ``config`` is a ``[tool.modulith]``-shaped dict:
      - ``package``  — application root package (required)
      - ``workers``  — ``{module_name: count}`` plus optional ``default``
      - ``isolate``  — restrict to this subset of modules (optional)
      - ``contracts_module`` — the shared-types package (default
        ``"contracts"``), which discovery lists as a module but which gets no
        worker of its own: it exposes no router and no listeners, so a process
        for it would host nothing while shifting every real module's port by
        one and publishing a dead ``/contracts`` prefix on the proxy.

    Ports are assigned from ``worker_port_base`` (default 9001), incrementing
    by each module's worker_count so replicas never collide; a module whose
    last replica port would pass 65535 raises ``ConfigurationError``.

    Also reports the resulting HTTP surface (see ``_log_http_surface``): this
    is the only process that sees every module, so it is where "nothing in
    this deployment exposes a router" can be said at all.
    """
    package = config.get("package")
    if not package:
        raise ValueError(
            "derive_specs_from_config requires a 'package' key "
            "(set [tool.modulith].package or pass it explicitly)"
        )

    names = discover_module_names(package, config.get("contracts_module") or "contracts")

    isolate = config.get("isolate")
    if isolate:
        wanted = set(isolate)
        names = [n for n in names if n in wanted]

    _log_http_surface(package, names)

    workers = config.get("workers") or {}
    default_count = int(workers.get("default", 1))
    if default_count < 1:
        raise ValueError(f"[tool.modulith.workers] default must be >= 1, got {default_count}")
    worker_env = dict(config.get("env") or {})

    specs: list[WorkerSpec] = []
    port = int(config.get("worker_port_base") or 9001)
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
        if port + count - 1 > 65535:
            from .config import ConfigurationError

            raise ConfigurationError(
                f"module {name!r} starts at worker port {port} with {count} replicas, "
                "which runs past port 65535; lower worker_port_base or the replica counts"
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
