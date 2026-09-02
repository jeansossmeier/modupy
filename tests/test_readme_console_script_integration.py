"""The README's process-per-module command, driven exactly as a reader runs it.

README's "Run the same code process-per-module" section tells a reader to run
``modulith run <app> --topology=processes`` from their project directory and
then ``curl`` the proxy. Nothing else in the suite walks that path: the other
process-topology tests build ``WorkerSpec``s in-process, drive ``Supervisor``
and ``create_proxy_app`` directly, and hand each worker subprocess an explicit
``PYTHONPATH``. That bypasses the two things the console script has to get
right on its own:

* Making an **uninstalled** application package importable. A console script's
  ``sys.path[0]`` is the directory holding the script (``.venv/bin`` after
  ``pip install``), never the working directory, so
  ``modulith.cli._add_project_root_to_syspath`` has to put the directory
  holding ``pyproject.toml`` on the path before bootstrap — otherwise the very
  package ``[tool.modulith].package`` names is invisible to ``import``.
* Letting the worker **subprocesses** the supervisor spawns resolve that same
  package for themselves.

Either failure is silent in the worst way: the workers boot, ``/health``
reports them ready, and the proxy answers 404 on every application route with
nothing in the logs to explain it. So this drives the real console script
against the real ``examples/demo_app`` and asserts the two responses a README
reader actually sees — a module's own GET route, and the ``POST /orders`` the
README curls.

Spawns a real CLI process plus three real uvicorn workers (slow) ->
``@pytest.mark.integration``.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, NoReturn

import httpx
import pytest

from conftest import _free_port

pytestmark = [pytest.mark.integration]

# The real demo app the README quickstart uses. It is deliberately NOT
# installed into the environment — resolving it from its own directory is the
# thing under test.
DEMO_ROOT = Path(__file__).resolve().parent.parent / "examples" / "demo_app"

# The ``modulith`` entry point an install of modupy writes next to the
# interpreter. The interpreter's own scripts directory is searched first so a
# stray ``modulith`` on PATH from an unrelated environment can never stand in
# for the one under test.
_SCRIPT_DIR = str(Path(sys.executable).parent)
CONSOLE_SCRIPT = shutil.which("modulith", path=_SCRIPT_DIR) or shutil.which("modulith")

# The prefixes the proxy must publish: one per demo module. ``contracts`` holds
# event definitions rather than behaviour and gets no worker of its own.
EXPECTED_BACKENDS = {"/orders", "/inventory", "/notifications"}


async def _drain(stream: asyncio.StreamReader, sink: list[str]) -> None:
    """Accumulate the CLI's merged stdout/stderr so failures are diagnosable.

    Draining continuously (rather than reading at teardown) also keeps the
    pipe from filling and blocking the supervisor mid-run.
    """
    async for raw in stream:
        sink.append(raw.decode(errors="replace").rstrip())


def _fail(message: str, output: list[str]) -> NoReturn:
    """Fail with the CLI's own output attached.

    Every failure mode here — a worker that could not import the app package, a
    proxy answering 404, a supervisor that exited during startup — explains
    itself in the process output, which is otherwise discarded and leaves a
    bare timeout behind.
    """
    raise AssertionError(message + "\n--- modulith run output ---\n" + "\n".join(output))


async def _wait_ready(
    client: httpx.AsyncClient,
    base_url: str,
    proc: asyncio.subprocess.Process,
    output: list[str],
    *,
    timeout: float = 90.0,
) -> dict[str, Any]:
    """Poll the proxy's readiness actuator until every worker reports healthy.

    ``/_modulith/health`` aggregates each worker's own ``/health`` and answers
    503 until all of them are up, so it is the readiness signal the deployment
    docs point at — no fixed sleep can substitute for it. A CLI process that
    exits during startup fails immediately rather than burning the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            _fail(f"modulith run exited with code {proc.returncode} before serving", output)
        try:
            resp = await client.get(f"{base_url}/_modulith/health")
        except httpx.HTTPError:
            await asyncio.sleep(0.2)
            continue
        if resp.status_code == 200:
            body: dict[str, Any] = resp.json()
            return body
        await asyncio.sleep(0.2)
    _fail(f"workers never became ready within {timeout}s", output)


async def test_console_script_serves_the_demo_app_process_per_module(tmp_path: Path) -> None:
    """``modulith run --topology processes`` serves the uninstalled demo app.

    Runs the installed console script from the demo's own directory, exactly as
    the README does, and asserts the proxy routes real HTTP to the right worker
    for both a module GET route and the documented ``POST /orders``.
    """
    if CONSOLE_SCRIPT is None:
        pytest.skip(f"modulith console script not installed (searched {_SCRIPT_DIR} and PATH)")

    proxy_port = _free_port()
    # The demo's pyproject leaves the cross-process profile commented out, so
    # the broker is selected through the documented MODULITH_BROKER* env vars.
    # Pointing them at a tmp_path SQLite file keeps the run self-contained: no
    # Redis, no server, and no broker state written next to the demo sources.
    env = {
        **os.environ,
        "MODULITH_BROKER": "database",
        "MODULITH_BROKER_URL": f"sqlite+aiosqlite:///{tmp_path / 'broker.db'}",
        "MODULITH_BROKER_POLL_INTERVAL_MS": "100",
        # Unbuffered so the CLI's own output is captured even if it is killed.
        "PYTHONUNBUFFERED": "1",
    }

    proc = await asyncio.create_subprocess_exec(
        CONSOLE_SCRIPT,
        "run",
        "shop.main:app",
        "--topology",
        "processes",
        "--host",
        "127.0.0.1",
        "--port",
        str(proxy_port),
        cwd=str(DEMO_ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    output: list[str] = []
    drain = asyncio.create_task(_drain(proc.stdout, output))
    base_url = f"http://127.0.0.1:{proxy_port}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            health = await _wait_ready(client, base_url, proc, output)

            # Discovery ran against the uninstalled package: one worker per
            # demo module, each reachable behind its own proxy prefix.
            assert set(health["backends"]) == EXPECTED_BACKENDS, output

            # A module's own GET route through the proxy. A 404 here is the
            # exact silent defect: a worker whose module package does not
            # re-export ``router`` still boots and still reports healthy, so
            # nothing but this response reveals that it serves nothing.
            reserved = await client.get(f"{base_url}/inventory/reserved")
            if reserved.status_code != 200:
                _fail(
                    f"GET /inventory/reserved returned {reserved.status_code}, expected 200",
                    output,
                )
            assert reserved.json() == {"reserved": []}

            # The request the README curls, routed to the orders worker.
            placed = await client.post(
                f"{base_url}/orders", json={"customer_id": "alice", "total": 19.99}
            )
            if placed.status_code != 200:
                _fail(
                    f"POST /orders returned {placed.status_code}, expected 200: {placed.text}",
                    output,
                )
            order_id = placed.json()["order_id"]
            assert isinstance(order_id, str) and order_id
    finally:
        # SIGTERM is the documented shutdown: run_supervised stops every worker
        # on the way out, so the fixed worker ports (9001+) are released for the
        # next run. SIGKILL only as a backstop if it does not.
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=30.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        await drain
