"""Process supervisor for the process-per-module topology.

Spawns one subprocess per module, monitors them, restarts crashed
workers, multiplexes their logs back to the supervisor's stdout.

Implementation status: SKELETON. ~200 lines when complete.

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
from dataclasses import dataclass

logger = logging.getLogger("modulith.supervisor")


# ---------------------------------------------------------------------------
# Worker spec — one per module
# ---------------------------------------------------------------------------


@dataclass
class WorkerSpec:
    """Configuration for one worker subprocess."""

    module_name: str  # e.g. "orders"
    package: str  # e.g. "myapp"
    port: int  # uvicorn binds here
    worker_count: int = 1  # multiple processes per module if needed
    env: dict[str, str] = None  # additional env vars


# ---------------------------------------------------------------------------
# The Supervisor class
# ---------------------------------------------------------------------------


class Supervisor:
    """Manages a fleet of per-module uvicorn subprocesses.

    Lifecycle:
      1. start() — spawn all configured workers
      2. monitor — watch for crashes, restart with backoff
      3. stop() — graceful SIGTERM cascade, kill on timeout
    """

    def __init__(
        self,
        specs: list[WorkerSpec],
        *,
        restart_initial_delay: float = 1.0,
        restart_max_delay: float = 60.0,
        shutdown_timeout: float = 30.0,
    ) -> None:
        self._specs = specs
        self._restart_initial_delay = restart_initial_delay
        self._restart_max_delay = restart_max_delay
        self._shutdown_timeout = shutdown_timeout
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._monitor_tasks: list[asyncio.Task] = []
        self._stopping = False

    async def start(self) -> None:
        """Spawn all workers and start monitoring them.

        IMPLEMENTATION TODO:
        For each spec:
          1. Build the uvicorn command:
             [sys.executable, "-m", "uvicorn",
              "modulith._worker:create_app", "--factory",
              "--host", "127.0.0.1", "--port", str(spec.port)]
          2. Build env: os.environ + MODULITH_MODULE + spec.env
          3. proc = await asyncio.create_subprocess_exec(
                 *cmd, env=env,
                 stdout=PIPE, stderr=PIPE,
             )
          4. Store in self._processes[spec.module_name].
          5. Start a monitor task: asyncio.create_task(
                 self._monitor_worker(spec, proc))
          6. Start log-forwarder tasks for stdout/stderr.

        The monitor task watches proc.wait() — when it returns, the
        process exited. Either intentional shutdown (self._stopping)
        or a crash. Crashes get restarted with exponential backoff.
        """
        raise NotImplementedError("Phase 3 — see TODO above")

    async def _monitor_worker(self, spec: WorkerSpec, proc: asyncio.subprocess.Process) -> None:
        """Watch one worker; restart on crash with backoff.

        IMPLEMENTATION TODO:
        delay = self._restart_initial_delay
        while not self._stopping:
            return_code = await proc.wait()
            if self._stopping:
                return
            logger.warning(
                "worker %s exited with code %d; restarting in %.1fs",
                spec.module_name, return_code, delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._restart_max_delay)
            # Respawn
            proc = await self._spawn(spec)
            self._processes[spec.module_name] = proc
        """
        raise NotImplementedError("Phase 3")

    async def _forward_logs(self, prefix: str, stream: asyncio.StreamReader) -> None:
        """Read worker output line-by-line, prefix with worker name, log it.

        IMPLEMENTATION TODO:
        async for line in stream:
            text = line.decode().rstrip()
            print(f"[{prefix}] {text}")

        Use a configurable formatter so users can route this to their
        own logging systems. For dev mode, raw stdout is fine.
        """
        raise NotImplementedError("Phase 3")

    async def stop(self) -> None:
        """Graceful shutdown: SIGTERM all workers, wait, SIGKILL stragglers.

        IMPLEMENTATION TODO:
        1. Set self._stopping = True so monitors don't restart.
        2. For each process: process.terminate() (SIGTERM).
        3. Wait up to self._shutdown_timeout for all to exit.
        4. Any still alive: process.kill() (SIGKILL).
        5. Cancel monitor tasks.
        """
        raise NotImplementedError("Phase 3")

    async def _spawn(self, spec: WorkerSpec) -> asyncio.subprocess.Process:
        """Helper: build env + command and spawn one worker."""
        raise NotImplementedError("Phase 3")


# ---------------------------------------------------------------------------
# Top-level orchestration entry point
# ---------------------------------------------------------------------------


async def run_supervised(
    specs: list[WorkerSpec],
    proxy_host: str,
    proxy_port: int,
) -> None:
    """Run the supervisor + reverse proxy together.

    IMPLEMENTATION TODO:
    1. Build Supervisor with specs.
    2. Build reverse proxy (modulith.proxy.create_proxy_app()) with a
       routing table derived from specs (module_name -> port).
    3. await supervisor.start()
    4. Run uvicorn programmatically against the proxy app on
       (proxy_host, proxy_port).
    5. Install SIGINT/SIGTERM handlers that call supervisor.stop()
       and shut down the proxy.
    6. Wait for shutdown signal.
    """
    raise NotImplementedError("Phase 3")


def derive_specs_from_config(config: dict) -> list[WorkerSpec]:
    """Read pyproject.toml config, build worker specs.

    Reads:
      - [tool.modulith].package
      - [tool.modulith.workers] for per-module worker counts
      - Discovered modules (default: all)
      - [tool.modulith.isolate] for selective isolation

    IMPLEMENTATION TODO: load the application package, run discovery,
    build one WorkerSpec per discovered module. Assign ports starting
    at 9001, incrementing.
    """
    raise NotImplementedError("Phase 3")


__all__ = [
    "Supervisor",
    "WorkerSpec",
    "derive_specs_from_config",
    "run_supervised",
]
