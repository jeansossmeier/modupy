"""Measure the tickets API while the agent hangs, blocks, crashes or slows down.

Each run serves a fresh copy of this directory with one topology and one
``DESK_AGENT_FAULT``, opens a seed ticket so the agent starts on it, then sends
requests at a fixed rate, whether or not earlier ones have returned: one in ten
opens a ticket, the rest read the seed ticket. A request that fails or takes
longer than the client timeout counts as taking the whole timeout, so a dead
server cannot report fast percentiles. The agent runs with a short
``DESK_AGENT_TIMEOUT``, recorded in each row, so a hung call is cancelled and
retried within the row.

``--postgres`` serves each run from a fresh database on a Postgres server
instead of SQLite, where the desk's engine has SQLAlchemy's default connection
pool, and records the most connections the database had open at once. With
``--hold-session`` the agent keeps its session open across the model calls, so
the ``slow`` fault measures a pile-up of waiting calls.
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
import threading
import time
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from sqlalchemy.engine import make_url

HERE = Path(__file__).resolve().parent
FAULTS = ("none", "hang", "block", "crash", "slow")
# slow is opt-in: under the default 2 s agent timeout it fails like a hang.
DEFAULT_FAULTS = ("none", "hang", "block", "crash")
TOPOLOGIES = ("single", "processes")
POSTGRES_DATABASE = "desk_benchmark"
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


def recreate_database(server: str) -> str:
    """Drop and create the benchmark database on ``server``; the desk's URL for it."""
    with psycopg.connect(server, autocommit=True) as admin:
        name = sql.Identifier(POSTGRES_DATABASE)
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(name))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(name))
    url = make_url(server).set(drivername="postgresql+psycopg", database=POSTGRES_DATABASE)
    return url.render_as_string(hide_password=False)


class ConnectionPeak(threading.Thread):
    """Samples the benchmark database's open connections, keeping the most seen at once."""

    def __init__(self, server: str) -> None:
        super().__init__(daemon=True)
        self.server = server
        self.peak = 0
        self.stopping = threading.Event()

    def run(self) -> None:
        with psycopg.connect(self.server, autocommit=True) as monitor:
            while not self.stopping.wait(0.5):
                row = monitor.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = %s",
                    (POSTGRES_DATABASE,),
                ).fetchone()
                self.peak = max(self.peak, row[0] if row else 0)


def count_triaged(workdir: Path, postgres: str | None) -> int:
    query = "SELECT count(*) FROM agent_triage"
    if postgres is None:
        with closing(sqlite3.connect(workdir / "desk.db")) as database:
            (count,) = database.execute(query).fetchone()
    else:
        with psycopg.connect(make_conninfo(postgres, dbname=POSTGRES_DATABASE)) as database:
            (count,) = database.execute(query).fetchone() or (0,)
    return int(count)


def open_seed_ticket(port: int, topology: str, client_timeout: float) -> None:
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
    with urllib.request.urlopen(request, timeout=client_timeout):
        pass
    time.sleep(1)  # the agent picks the seed ticket up and fails as told


async def drive(
    port: int, rate: float, seconds: float, client_timeout: float
) -> list[tuple[float, bool, bool]]:
    """(latency, ok, opened a ticket) for each request sent."""
    limits = httpx.Limits(max_connections=1000)
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=client_timeout, limits=limits
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
            return (min(latency, client_timeout) if ok else client_timeout, ok, opens)

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
    topology: str,
    fault: str,
    rate: float,
    seconds: float,
    port_base: int | None,
    *,
    client_timeout: float = CLIENT_TIMEOUT,
    agent_timeout: float = AGENT_TIMEOUT,
    postgres: str | None = None,
    hold_session: bool = False,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as scratch:
        workdir = Path(scratch) / "desk"
        shutil.copytree(HERE, workdir, ignore=COPY_IGNORE)
        env = {key: value for key, value in os.environ.items() if not key.startswith("MODULITH_")}
        env["DESK_AGENT_FAULT"] = "" if fault == "none" else fault
        env["DESK_AGENT_TIMEOUT"] = str(agent_timeout)
        env["DESK_AGENT_HOLD_SESSION"] = "1" if hold_session else ""
        connections = None
        if postgres is not None:
            env["MODULITH_OUTBOX_URL"] = recreate_database(postgres)
            connections = ConnectionPeak(postgres)
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
        if connections is not None:
            connections.start()
        try:
            open_seed_ticket(port, topology, client_timeout)
            results = asyncio.run(drive(port, rate, seconds, client_timeout))
            alive = server.poll() is None
        except BaseException:
            sys.stderr.write((workdir / "server.log").read_text(errors="replace")[-5000:])
            raise
        finally:
            stop(server)
            if connections is not None:
                connections.stopping.set()
                connections.join()
        triaged = count_triaged(workdir, postgres)
        server_log = (workdir / "server.log").read_text(errors="replace")
    latencies = [latency * 1000 for latency, _, _ in results]
    percentiles = statistics.quantiles(latencies, n=100, method="inclusive")
    return {
        "topology": topology,
        "fault": fault,
        "agent_timeout_s": agent_timeout,
        "client_timeout_s": client_timeout,
        "database": "sqlite" if postgres is None else "postgres",
        "hold_session": hold_session,
        "requests": len(results),
        "errors": sum(not ok for _, ok, _ in results),
        "p50_ms": round(percentiles[49], 1),
        "p95_ms": round(percentiles[94], 1),
        "p99_ms": round(percentiles[98], 1),
        "tickets_opened": 1 + sum(ok and opens for _, ok, opens in results),
        "tickets_triaged": triaged,
        "server_alive": alive,
        "agent_restarts": server_log.count("worker agent exited with code"),
        "peak_connections": None if connections is None else connections.peak,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, default=15.0)
    parser.add_argument("--rate", type=float, default=50.0, help="requests per second")
    parser.add_argument(
        "--faults", default=",".join(DEFAULT_FAULTS), help=f"any of {', '.join(FAULTS)}"
    )
    parser.add_argument("--topologies", default=",".join(TOPOLOGIES))
    parser.add_argument("--output", type=Path, default=HERE / "bench-results.json")
    parser.add_argument(
        "--port-base",
        type=int,
        help="first of the three ports a run serves on (default: a free block at random)",
    )
    parser.add_argument(
        "--client-timeout",
        type=float,
        default=CLIENT_TIMEOUT,
        help="seconds a request may take before it counts as failed",
    )
    parser.add_argument(
        "--agent-timeout", type=float, default=AGENT_TIMEOUT, help="the agent's DESK_AGENT_TIMEOUT"
    )
    parser.add_argument(
        "--postgres",
        metavar="URL",
        help=f"a Postgres server; each run recreates the database {POSTGRES_DATABASE} on it "
        "and serves the desk from it instead of SQLite",
    )
    parser.add_argument(
        "--hold-session",
        action="store_true",
        help="the agent keeps its session open across the model calls",
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
        f"{'p99 ms':>8} {'triaged':>8} {'restarts':>9} {'conns':>6}"
    )
    for topology in topologies:
        for fault in faults:
            row = run(
                topology,
                fault,
                args.rate,
                args.seconds,
                args.port_base,
                client_timeout=args.client_timeout,
                agent_timeout=args.agent_timeout,
                postgres=args.postgres,
                hold_session=args.hold_session,
            )
            rows.append(row)
            print(
                f"{topology:<10} {fault:<6} {row['errors'] / row['requests']:>7.0%} "
                f"{row['p50_ms']:>8} {row['p95_ms']:>8} {row['p99_ms']:>8} "
                f"{row['tickets_triaged']:>4}/{row['tickets_opened']:<3} "
                f"{row['agent_restarts']:>9} {row['peak_connections'] or '-':>6}",
                flush=True,
            )
    args.output.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
