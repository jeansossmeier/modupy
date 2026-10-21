"""Measure the tickets API while the agent hangs, blocks or crashes.

Each run serves a fresh copy of this directory with one topology and one
``DESK_AGENT_FAULT``, opens a seed ticket so the agent starts on it, then sends
requests at a fixed rate, whether or not earlier ones have returned: one in ten
opens a ticket, the rest read the seed ticket. A request that fails or takes
longer than the client timeout counts as taking the whole timeout, so a dead
server cannot report fast percentiles. The agent runs with a short
``DESK_AGENT_TIMEOUT``, recorded in each row, so a hung call is cancelled and
retried within the row.
"""

import argparse
import asyncio
import json
import os
import random
import shutil
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any

import httpx

HERE = Path(__file__).resolve().parent
FAULTS = ("none", "hang", "block", "crash")
TOPOLOGIES = ("single", "processes")
CLIENT_TIMEOUT = 2.0
# The default 30 s would outlast a row, so a hang would never reach its timeout.
# At 2 s, --topology processes dead-letters the seed ticket after its fifth
# attempt, about 25 s into a 30 s row, its retries queued for a listener slot
# behind newer tickets' hung calls; one process retries it through the outbox.
AGENT_TIMEOUT = 2.0
STOP_WAIT = 10.0
COPY_IGNORE = shutil.ignore_patterns(
    "*.db", "*.db-*", ".modulith", "__pycache__", ".*_cache", "bench-results.json"
)


def ports_free(base: int, size: int) -> bool:
    """Whether nothing listens on ``base`` through ``base + size - 1``."""
    for port in range(base, base + size):
        with socket.socket() as probe:
            # Like uvicorn, so a port the previous run left in TIME_WAIT counts as free.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                return False
    return True


def free_port_block(size: int) -> int:
    """A free run of ``size`` ports, drawn below the ephemeral range."""
    for _ in range(100):
        base = random.randrange(20000, 30000)
        if ports_free(base, size):
            return base
    raise RuntimeError("found no free block of ports")


def status_of(port: int, path: str) -> int | None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=1) as response:
            return int(response.status)
    except urllib.error.HTTPError as error:
        return error.code
    except OSError:
        return None


def open_seed_ticket(port: int, topology: str) -> None:
    # The proxy answers before its workers do; its health turns 200 once they all do.
    path, ready = ("/_modulith/health", 200) if topology == "processes" else ("/tickets/seed", 404)
    deadline = time.monotonic() + 60
    while status_of(port, path) != ready:
        if time.monotonic() > deadline:
            raise RuntimeError(f"the {topology} server on port {port} never became ready")
        time.sleep(0.2)
    seed = json.dumps({"ticket_id": "seed", "subject": "Refund", "body": "Charged twice."})
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/tickets",
        data=seed.encode(),
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=CLIENT_TIMEOUT):
        pass
    time.sleep(1)  # the agent picks the seed ticket up and fails as told


async def drive(port: int, rate: float, seconds: float) -> list[tuple[float, bool, bool]]:
    """(latency, ok, opened a ticket) for each request sent."""
    limits = httpx.Limits(max_connections=1000)
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=CLIENT_TIMEOUT, limits=limits
    ) as client:

        async def send(number: int) -> tuple[float, bool, bool]:
            opens = number % 10 == 0
            started = time.perf_counter()
            try:
                if opens:
                    body = {"ticket_id": f"t-{number}", "subject": "Site down", "body": "500s"}
                    response = await client.post("/tickets", json=body)
                else:
                    response = await client.get("/tickets/seed")
                ok = response.status_code == 200
            except httpx.HTTPError:
                ok = False
            latency = time.perf_counter() - started
            return (min(latency, CLIENT_TIMEOUT) if ok else CLIENT_TIMEOUT, ok, opens)

        start = time.perf_counter()
        sent = []
        for number in range(int(rate * seconds)):
            await asyncio.sleep(max(0.0, start + number / rate - time.perf_counter()))
            sent.append(asyncio.create_task(send(number)))
        return list(await asyncio.gather(*sent))


def stop(server: subprocess.Popen[bytes]) -> None:
    if server.poll() is not None:
        return
    os.killpg(server.pid, signal.SIGINT)
    try:
        server.wait(STOP_WAIT)
    except subprocess.TimeoutExpired:
        # A blocked event loop never runs its SIGINT handler.
        os.killpg(server.pid, signal.SIGKILL)
        server.wait()


def run(
    topology: str, fault: str, rate: float, seconds: float, port_base: int | None
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as scratch:
        workdir = Path(scratch) / "desk"
        shutil.copytree(HERE, workdir, ignore=COPY_IGNORE)
        env = {key: value for key, value in os.environ.items() if not key.startswith("MODULITH_")}
        env["DESK_AGENT_FAULT"] = "" if fault == "none" else fault
        env["DESK_AGENT_TIMEOUT"] = str(AGENT_TIMEOUT)
        python = sys.executable
        subprocess.run([python, "-m", "modulith", "migrate"], cwd=workdir, env=env, check=True)
        subprocess.run([python, "-m", "desk.schema"], cwd=workdir, env=env, check=True)
        port = free_port_block(3) if port_base is None else port_base
        if not ports_free(port, 3):
            raise RuntimeError(f"ports {port}-{port + 2} are in use")
        command = [python, "-m", "uvicorn", "desk.main:app", "--port", str(port)]
        if topology == "processes":
            command = [
                python,
                "-m",
                "modulith",
                "run",
                "desk.main:app",
                "--topology",
                "processes",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--worker-port-base",
                str(port + 1),
            ]
        with open(workdir / "server.log", "wb") as log:
            server = subprocess.Popen(
                command,
                cwd=workdir,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        try:
            open_seed_ticket(port, topology)
            results = asyncio.run(drive(port, rate, seconds))
            alive = server.poll() is None
        except BaseException:
            sys.stderr.write((workdir / "server.log").read_text(errors="replace")[-5000:])
            raise
        finally:
            stop(server)
        with closing(sqlite3.connect(workdir / "desk.db")) as database:
            (triaged,) = database.execute("SELECT count(*) FROM agent_triage").fetchone()
        server_log = (workdir / "server.log").read_text(errors="replace")
    latencies = [latency * 1000 for latency, _, _ in results]
    percentiles = statistics.quantiles(latencies, n=100, method="inclusive")
    return {
        "topology": topology,
        "fault": fault,
        "agent_timeout_s": AGENT_TIMEOUT,
        "requests": len(results),
        "errors": sum(not ok for _, ok, _ in results),
        "p50_ms": round(percentiles[49], 1),
        "p95_ms": round(percentiles[94], 1),
        "p99_ms": round(percentiles[98], 1),
        "tickets_opened": 1 + sum(ok and opens for _, ok, opens in results),
        "tickets_triaged": triaged,
        "server_alive": alive,
        "agent_restarts": server_log.count("worker agent exited with code"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, default=15.0)
    parser.add_argument("--rate", type=float, default=50.0, help="requests per second")
    parser.add_argument("--faults", default=",".join(FAULTS))
    parser.add_argument("--topologies", default=",".join(TOPOLOGIES))
    parser.add_argument("--output", type=Path, default=HERE / "bench-results.json")
    parser.add_argument(
        "--port-base",
        type=int,
        help="first of the three ports a run serves on (default: a free block at random)",
    )
    args = parser.parse_args()
    faults = [name.strip() for name in args.faults.split(",")]
    topologies = [name.strip() for name in args.topologies.split(",")]
    for names, known in ((faults, FAULTS), (topologies, TOPOLOGIES)):
        unknown = [name for name in names if name not in known]
        if unknown:
            parser.error(f"unknown {', '.join(map(repr, unknown))}; choose from {', '.join(known)}")
    rows = []
    print(
        f"{'topology':<10} {'fault':<6} {'errors':>7} {'p50 ms':>8} {'p95 ms':>8} "
        f"{'p99 ms':>8} {'triaged':>8} {'restarts':>9}"
    )
    for topology in topologies:
        for fault in faults:
            row = run(topology, fault, args.rate, args.seconds, args.port_base)
            rows.append(row)
            print(
                f"{topology:<10} {fault:<6} {row['errors'] / row['requests']:>7.0%} "
                f"{row['p50_ms']:>8} {row['p95_ms']:>8} {row['p99_ms']:>8} "
                f"{row['tickets_triaged']:>4}/{row['tickets_opened']:<3} "
                f"{row['agent_restarts']:>9}",
                flush=True,
            )
    args.output.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
