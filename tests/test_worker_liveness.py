"""Workers that stop answering: the supervisor's liveness watchdog and the worker's
parent watchdog.

A worker whose event loop is blocked never exits, still holds its port, and the
kernel still accepts connections on it, so a supervisor that only restarts
workers that exit never notices it. ``worker_liveness_timeout_seconds`` has the
supervisor kill such a worker; the worker itself exits once its supervisor is
gone, where the parent-death SIGTERM cannot reach a blocked event loop.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import textwrap
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from modulith import ConfigurationError
from modulith.config import load_configuration
from modulith.supervisor import Supervisor, WorkerSpec, run_supervised

from conftest import _free_port

# ---------------------------------------------------------------------------
# The configuration key
# ---------------------------------------------------------------------------


@pytest.fixture
def _clean_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith("MODULITH_"):
            monkeypatch.delenv(key)


@pytest.mark.usefixtures("_clean_config")
def test_the_liveness_timeout_is_off_by_default() -> None:
    assert load_configuration().worker_liveness_timeout_seconds is None


@pytest.mark.usefixtures("_clean_config")
def test_the_liveness_timeout_is_read_from_pyproject_and_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\nworker_liveness_timeout_seconds = 15\n"
    )
    from_pyproject = load_configuration().worker_liveness_timeout_seconds
    monkeypatch.setenv("MODULITH_WORKER_LIVENESS_TIMEOUT_SECONDS", "2.5")

    assert (from_pyproject, load_configuration().worker_liveness_timeout_seconds) == (15, 2.5)


@pytest.mark.usefixtures("_clean_config")
@pytest.mark.parametrize("value", ["0", "-1", "true", '"10"', "inf", "nan"])
def test_the_liveness_timeout_must_be_a_positive_finite_number(tmp_path: Path, value: str) -> None:
    (tmp_path / "pyproject.toml").write_text(
        f"[tool.modulith]\nworker_liveness_timeout_seconds = {value}\n"
    )
    with pytest.raises(ConfigurationError, match="worker_liveness_timeout_seconds must be"):
        load_configuration()


@pytest.mark.usefixtures("_clean_config")
@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("soon", "MODULITH_WORKER_LIVENESS_TIMEOUT_SECONDS must be a number of seconds"),
        ("0", "worker_liveness_timeout_seconds must be a finite number"),
    ],
)
def test_a_bad_liveness_timeout_in_the_environment_is_refused(
    monkeypatch: pytest.MonkeyPatch, value: str, reason: str
) -> None:
    monkeypatch.setenv("MODULITH_WORKER_LIVENESS_TIMEOUT_SECONDS", value)
    with pytest.raises(ConfigurationError, match=reason):
        load_configuration()


# ---------------------------------------------------------------------------
# The supervisor's liveness watchdog
# ---------------------------------------------------------------------------

# A stand-in worker: it binds its port after BOOT seconds, answers ANSWERS
# requests with 503, logging its pid for each, then stops answering while its
# port keeps accepting connections, as a worker whose event loop is blocked.
# ANSWERS -1 answers for ever; on SIGTERM that one stops answering, then exits
# 0 after 1.5 s, as a worker running a slow graceful shutdown.
_FAKE_WORKER = """
import os, signal, socket, sys, time

port, boot, answers, log = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
time.sleep(boot)
server = socket.create_server(("127.0.0.1", port))


def graceful_stop(*_):
    server.close()
    time.sleep(1.5)
    os._exit(0)


signal.signal(signal.SIGTERM, graceful_stop)
served = 0
while answers < 0 or served < answers:
    conn, _ = server.accept()
    conn.recv(65536)
    conn.sendall(b"HTTP/1.1 503 Service Unavailable\\r\\ncontent-length: 0\\r\\nconnection: close\\r\\n\\r\\n")
    conn.close()
    served += 1
    with open(log, "a") as f:
        f.write(f"{os.getpid()}\\n")
time.sleep(600)
"""


def _fake_worker(
    tmp_path: Path, *, boot: float, answers: int
) -> tuple[Path, Callable[[WorkerSpec, int], list[str]]]:
    script, log = tmp_path / "worker.py", tmp_path / "answers.log"
    script.write_text(_FAKE_WORKER)

    def command(spec: WorkerSpec, port: int) -> list[str]:
        return [sys.executable, str(script), str(port), str(boot), str(answers), str(log)]

    return log, command


def _answers_by_pid(log: Path) -> dict[int, int]:
    counts: dict[int, int] = {}
    for line in log.read_text().split() if log.exists() else ():
        counts[int(line)] = counts.get(int(line), 0) + 1
    return counts


async def _until(predicate: Callable[[], bool], seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.05)


@pytest.mark.real_process
async def test_a_worker_that_stops_answering_health_is_killed_and_restarted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """It boots slower than the timeout and answers only 503, for longer than
    the timeout, and is still left alone until it stops answering; then the
    restart policy respawns it."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    log, command = _fake_worker(tmp_path, boot=1.0, answers=10)
    sup = Supervisor(
        [WorkerSpec("w", "pkg", _free_port())],
        command_builder=command,
        liveness_timeout=0.5,
        restart_initial_delay=0.05,
    )
    await sup.start()
    try:
        await _until(lambda: len(_answers_by_pid(log)) >= 2, 20.0)
    finally:
        await sup.stop()

    first = next(iter(_answers_by_pid(log).values()))
    messages = [r.getMessage() for r in caplog.records]
    assert first == 10
    assert any(
        r.levelno == logging.ERROR
        and "worker w has not answered /health for 0.5s" in r.getMessage()
        for r in caplog.records
    )
    assert any(m.startswith("worker w exited with code -9; restarting") for m in messages)


@pytest.mark.real_process
async def test_a_worker_that_blocks_before_it_ever_answers_is_killed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A worker can block on its first message right after it binds, before
    any poll reaches it; its open port is what arms the watchdog."""
    caplog.set_level(logging.INFO, logger="modulith.supervisor")
    _, command = _fake_worker(tmp_path, boot=1.0, answers=0)
    sup = Supervisor(
        [WorkerSpec("w", "pkg", _free_port())],
        command_builder=command,
        liveness_timeout=0.5,
        restart_initial_delay=0.05,
    )
    await sup.start()
    try:
        await _until(lambda: "worker w exited with code -9; restarting" in caplog.text, 20.0)
    finally:
        await sup.stop()

    assert "worker w has not answered /health for 0.5s" in caplog.text


@pytest.mark.real_process
async def test_the_liveness_watchdog_leaves_a_stopping_worker_alone(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """stop() gives workers their own time to exit; a graceful shutdown that stops
    answering for longer than the liveness timeout is not cut short."""
    log, command = _fake_worker(tmp_path, boot=0.0, answers=-1)
    sup = Supervisor(
        [WorkerSpec("w", "pkg", _free_port())],
        command_builder=command,
        liveness_timeout=0.3,
        shutdown_timeout=10.0,
    )
    await sup.start()
    proc = next(iter(sup._processes.values()))
    try:
        await _until(lambda: bool(_answers_by_pid(log)), 10.0)
    finally:
        await sup.stop()

    assert proc.returncode == 0
    assert "has not answered" not in caplog.text


@pytest.mark.real_process
async def test_run_supervised_liveness_timeout_reaches_the_supervisor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    log, command = _fake_worker(tmp_path, boot=0.0, answers=2)
    monkeypatch.setattr("modulith.supervisor._default_command", command)

    async def serve(app: object, host: str, port: int) -> None:
        await _until(lambda: "has not answered" in caplog.text, 20.0)

    await run_supervised(
        [WorkerSpec("w", "pkg", _free_port())],
        "127.0.0.1",
        _free_port(),
        serve=serve,
        liveness_timeout=0.5,
    )

    assert next(iter(_answers_by_pid(log).values())) == 2


# ---------------------------------------------------------------------------
# The worker's parent watchdog
# ---------------------------------------------------------------------------

# A worker whose event loop is blocked: it cannot act on the parent-death
# SIGTERM, so it ignores it, builds its app as uvicorn's factory call does,
# and blocks.
_BLOCKED_WORKER = """
import signal, sys, time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
import modulith._worker as worker

worker._ORPHAN_GRACE_SECONDS = 2.0
worker._PARENT_POLL_SECONDS = 0.05
worker.create_app()
Path(sys.argv[1]).write_text("watching")
time.sleep(600)
"""

_SUPERVISOR = """
import asyncio, os, sys
from modulith.supervisor import Supervisor, WorkerSpec


async def main():
    sup = Supervisor(
        [WorkerSpec("orders", "deskapp", 9001, env={"PYTHONPATH": os.pathsep.join(
            filter(None, (sys.argv[3], os.environ.get("PYTHONPATH")))
        )})],
        command_builder=lambda spec, port: [sys.executable, sys.argv[1], sys.argv[2]],
    )
    await sup.start()
    print(next(iter(sup._processes.values())).pid, flush=True)
    await asyncio.sleep(600)


asyncio.run(main())
"""


def _alive(pid: int) -> bool:
    """Whether ``pid`` runs and is not a zombie its new parent has yet to reap."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] not in ("Z", "X")
    except FileNotFoundError:
        return False


@pytest.mark.real_process
@pytest.mark.skipif(sys.platform != "linux", reason="reads /proc")
async def test_a_blocked_worker_never_outlives_a_sigkilled_supervisor(tmp_path: Path) -> None:
    worker_script, supervisor_script = tmp_path / "worker.py", tmp_path / "supervisor.py"
    worker_script.write_text(textwrap.dedent(_BLOCKED_WORKER))
    supervisor_script.write_text(textwrap.dedent(_SUPERVISOR))
    marker = tmp_path / "watching"
    (tmp_path / "deskapp" / "orders").mkdir(parents=True)
    (tmp_path / "deskapp" / "__init__.py").write_text("")
    (tmp_path / "deskapp" / "orders" / "__init__.py").write_text("")
    runner = await asyncio.create_subprocess_exec(
        sys.executable,
        str(supervisor_script),
        str(worker_script),
        str(marker),
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
    )
    worker_pid: int | None = None
    try:
        assert runner.stdout is not None
        worker_pid = int(await asyncio.wait_for(runner.stdout.readline(), 15.0))
        await _until(marker.exists, 15.0)

        runner.kill()
        await runner.wait()
        await asyncio.sleep(0.5)
        survived_the_sigterm = _alive(worker_pid)
        await _until(lambda: not _alive(worker_pid), 15.0)
    finally:
        if runner.returncode is None:
            runner.kill()
            await runner.wait()
        if worker_pid is not None and _alive(worker_pid):
            os.kill(worker_pid, signal.SIGKILL)

    assert survived_the_sigterm
