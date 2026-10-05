"""Tests for the modulith CLI (typer-based).

The CLI is a thin orchestration layer over already-tested subsystems:
bootstrap (runtime), the boundary verifier, the docs generator, and the
outbox maintenance API. These tests verify the CLI *wiring and contracts* —
exit codes, argument parsing, output shape, and that each command drives the
right subsystem — using typer's ``CliRunner``. The subsystems' own behavior
is covered exhaustively in their dedicated test modules.

``modulith dev``/``run`` are verified by mocking ``os.execvp`` (they replace
the process in production). ``verify`` is exercised against fake apps with and
without real boundary violations, including ratchet/baseline flows. The outbox
commands run against a stub store, with one full end-to-end dispatch to prove
``retry`` actually delivers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import select
import socket
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import click
import pytest
from typer.testing import CliRunner

from modulith import EventPublication, Violation, hookimpl
from modulith.builtin import outbox
from modulith.cli import _parse_duration, app
from modulith.serializers import JsonEventSerializer

from conftest import replace_current_event_loop

runner = CliRunner()

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _restore_root_log_level():
    """Undo the root-logger level ``dev``/``run`` install.

    Owning the root logger is deliberate in those two commands (they are
    process entry points), but in-process ``CliRunner`` invocations would
    otherwise leak that level into every later test.
    """
    root = logging.getLogger()
    level = root.level
    try:
        yield
    finally:
        root.setLevel(level)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class StubStore:
    """In-memory PublicationStore for CLI outbox tests.

    Mirrors the five-method protocol plus the two duck-typed maintenance
    capabilities (``count_completed``, ``purge_completed``) the outbox plugin
    probes for. State is a dict keyed by publication id so completion and
    deletion are observable by assertions.
    """

    def __init__(self) -> None:
        self.pubs: dict[UUID, EventPublication] = {}
        self.purged = 0

    async def save(self, publication: EventPublication) -> None:
        self.pubs[publication.id] = publication

    async def mark_complete(self, publication_id: UUID) -> None:
        pub = self.pubs.get(publication_id)
        if pub is not None and pub.completed_at is None:
            pub.completed_at = datetime.now(UTC)

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        cutoff = datetime.now(UTC) - older_than
        return [
            p
            for p in self.pubs.values()
            if p.completed_at is None and (p.published_at is None or p.published_at <= cutoff)
        ]

    async def archive(self, publication_id: UUID) -> None:
        self.pubs.pop(publication_id, None)

    async def delete(self, publication_id: UUID) -> None:
        self.pubs.pop(publication_id, None)

    async def count_completed(self) -> int:
        return sum(1 for p in self.pubs.values() if p.completed_at is not None)

    async def purge_completed(self, older_than: timedelta) -> int:
        return self.purged


@pytest.fixture(autouse=True)
def _reset_outbox_state():
    """Keep outbox + runtime state isolated per test.

    ``make_fake_app`` resets the runtime singleton, but not the outbox
    plugin's module globals — and tests that do NOT use ``make_fake_app``
    (e.g. the ``dev`` tests, whose startup verify pass bootstraps the
    runtime) would otherwise leak a bootstrapped singleton into later
    tests, breaking their ``configure()`` calls.

    The outbox CLI commands call ``asyncio.run()``, which leaves the thread
    with no current event loop. In production each command is its own
    process, so that is harmless — but under pytest it pollutes the shared
    process, breaking later (sync) tests that rely on the implicit current
    loop via ``asyncio.get_event_loop()``. Re-establish a fresh loop on
    teardown so this module's tests stay self-contained.
    """
    from modulith.runtime import _runtime

    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()
    _runtime._reset_for_testing()
    replace_current_event_loop()
    # dev() sets this directly on os.environ (not via monkeypatch — it must
    # survive an os.execvp that replaces the process), so it needs its own
    # explicit cleanup or it leaks into every later test in this process.
    os.environ.pop("MODULITH_DEV_WARN_ONLY", None)


# ---------------------------------------------------------------------------
# modulith info
# ---------------------------------------------------------------------------


def test_info_reports_package_modules_config_and_plugins(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""})

    result = runner.invoke(app, ["info"])

    assert result.exit_code == 0, result.output
    out = result.output
    assert "fakeapp" in out
    assert "orders" in out
    assert "inventory" in out
    # Config surface.
    assert "memory" in out  # default outbox + broker
    assert "single" in out  # default topology
    # At least one built-in plugin is listed.
    assert "modulith.builtin.verifier" in out


# ---------------------------------------------------------------------------
# modulith dev / run
# ---------------------------------------------------------------------------


def test_dev_invokes_uvicorn_with_reload(monkeypatch):
    captured: dict[str, object] = {}

    def fake_execvp(file: str, args: list[str]) -> None:
        captured["file"] = file
        captured["args"] = args

    monkeypatch.setattr(os, "execvp", fake_execvp)

    result = runner.invoke(app, ["dev", "myapp:app", "--port", "9001"])

    assert result.exit_code == 0, result.output
    assert captured["file"] == "uvicorn"
    args = captured["args"]
    assert "myapp:app" in args
    assert "--reload" in args
    assert "9001" in args


def test_run_invokes_uvicorn_without_reload(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setenv("MODULITH_DEV_WARN_ONLY", "1")

    def fake_execvp(file: str, args: list[str]) -> None:
        captured["file"] = file
        captured["args"] = args

    monkeypatch.setattr(os, "execvp", fake_execvp)

    result = runner.invoke(app, ["run", "myapp:app"])

    assert result.exit_code == 0, result.output
    assert captured["file"] == "uvicorn"
    assert "--reload" not in captured["args"]
    assert "MODULITH_DEV_WARN_ONLY" not in os.environ


# ---------------------------------------------------------------------------
# --log-level
# ---------------------------------------------------------------------------


def test_run_configures_the_root_logger_and_tells_uvicorn(monkeypatch):
    """README's two remedies both say "enable INFO logging" — this is the
    mechanism that makes them possible. Without it the root logger has no
    handler, ``logging.lastResort`` drops everything under WARNING, and every
    INFO diagnostic modulith emits is unreachable from any CLI flag."""
    argv: list[str] = []
    monkeypatch.setattr(os, "execvp", lambda file, args: argv.extend(args))
    logging.getLogger().setLevel(logging.CRITICAL)

    result = runner.invoke(app, ["run", "myapp:app", "--log-level", "debug"])

    assert result.exit_code == 0, result.output
    assert logging.getLogger().level == logging.DEBUG
    assert argv[argv.index("--log-level") + 1] == "debug"


def test_dev_accepts_a_log_level_in_any_case(monkeypatch):
    argv: list[str] = []
    monkeypatch.setattr(os, "execvp", lambda file, args: argv.extend(args))

    result = runner.invoke(app, ["dev", "myapp:app", "--log-level", "WARNING"])

    assert result.exit_code == 0, result.output
    assert logging.getLogger().level == logging.WARNING
    assert argv[argv.index("--log-level") + 1] == "warning"


def test_unknown_log_level_exits_one_instead_of_defaulting(monkeypatch):
    """Silently keeping the previous level on a typo is the exact failure the
    flag exists to end, so an unrecognized name is a user error."""
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["run", "myapp:app", "--log-level", "verbose"])

    assert result.exit_code == 1
    assert "verbose" in result.stderr
    assert "critical" in result.stderr


def test_processes_topology_forwards_the_log_level_to_workers(make_fake_app, monkeypatch):
    """A worker is its own process with its own logging config, so the flag has
    to cross the process boundary or it only quiets the supervisor."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(
        app, ["run", "fakeapp:app", "--topology", "processes", "--log-level", "warning"]
    )

    assert result.exit_code == 0, result.output
    specs = captured["specs"]
    assert [s.env["UVICORN_LOG_LEVEL"] for s in specs] == ["warning"]


def test_processes_topology_forwards_the_project_root_to_workers(
    make_fake_app, tmp_path, monkeypatch
):
    """A worker is `python -m uvicorn`, so its sys.path starts at the inherited
    working directory. Run from a subdirectory of the project, the parent still
    finds the application package (it walks up to pyproject.toml) while every
    worker dies importing it — a restart loop behind a proxy that stays up."""
    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "fakeapp"\n')
    monkeypatch.chdir(tmp_path / "fakeapp" / "orders")
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["run", "fakeapp:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    specs = captured["specs"]
    assert [s.env["PYTHONPATH"] for s in specs] == [str(tmp_path)]


@pytest.mark.parametrize(
    ("pyproject_base", "argv", "expected"),
    [
        (None, [], [9001, 9002]),
        (19001, [], [19001, 19002]),
        (19001, ["--worker-port-base", "29001"], [29001, 29002]),
    ],
)
def test_processes_topology_assigns_worker_ports_from_the_configured_base(
    make_fake_app, tmp_path, monkeypatch, pyproject_base, argv, expected
):
    make_fake_app({"orders": "", "inventory": ""})
    if pyproject_base is not None:
        (tmp_path / "pyproject.toml").write_text(
            f'[tool.modulith]\npackage = "fakeapp"\nworker_port_base = {pyproject_base}\n'
        )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["run", "fakeapp:app", "--topology", "processes", *argv])

    assert result.exit_code == 0, result.output
    assert [s.port for s in captured["specs"]] == expected


def test_run_refuses_a_proxy_port_inside_the_worker_range(make_fake_app, monkeypatch):
    """Exit 1 with the collision named, before any worker is spawned."""
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    monkeypatch.setattr(
        "modulith.supervisor.Supervisor.start",
        lambda self: pytest.fail("no worker may be spawned"),
    )

    result = runner.invoke(
        app,
        ["run", "fakeapp:app", "--topology", "processes", "--port", "9002"],
    )

    assert result.exit_code == 1, result.output
    assert "port 9002" in result.output
    assert "orders" in result.output


def test_run_refuses_a_proxy_port_another_process_listens_on(make_fake_app, monkeypatch):
    """Exit 1 naming the port, not uvicorn's bare exit 3 after workers spawned."""
    import socket

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    monkeypatch.setattr(
        "modulith.supervisor.Supervisor.start",
        lambda self: pytest.fail("no worker may be spawned"),
    )

    with socket.socket() as holder:
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        port = holder.getsockname()[1]

        argv = ["run", "fakeapp:app", "--topology", "processes", "--host", "127.0.0.1"]
        result = runner.invoke(app, [*argv, "--port", str(port)])

    assert result.exit_code == 1, result.output
    assert f"port {port} is already in use; choose another --port" in result.output


def test_dev_port_collision_names_only_remedies_dev_accepts(make_fake_app, monkeypatch):
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    monkeypatch.setattr(
        "modulith.supervisor.Supervisor.start",
        lambda self: pytest.fail("no worker may be spawned"),
    )

    result = runner.invoke(
        app,
        ["dev", "fakeapp:app", "--topology", "processes", "--port", "9002"],
    )

    assert result.exit_code == 1, result.output
    assert "--worker-port-base" in result.output
    assert "MODULITH_WORKER_PORT_BASE" in result.output


@pytest.mark.parametrize(
    "pyproject_base,argv,expected",
    [
        (None, [], [9001, 9002]),
        (19001, [], [19001, 19002]),
        (19001, ["--worker-port-base", "29001"], [29001, 29002]),
    ],
)
def test_dev_processes_topology_accepts_worker_port_base(
    make_fake_app, tmp_path, monkeypatch, pyproject_base, argv, expected
):
    make_fake_app({"orders": "", "inventory": ""})
    if pyproject_base is not None:
        (tmp_path / "pyproject.toml").write_text(
            f'[tool.modulith]\npackage = "fakeapp"\nworker_port_base = {pyproject_base}\n'
        )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp:app", "--topology", "processes", *argv])

    assert result.exit_code == 0, result.output
    assert [s.port for s in captured["specs"]] == expected


def test_single_process_run_banner_has_a_space_after_the_arrow(monkeypatch):
    monkeypatch.setattr(os, "execvp", lambda file, args: None)

    result = runner.invoke(app, ["run", "myapp:app", "--port", "8123"])

    assert result.exit_code == 0, result.output
    assert "modulith run → myapp:app on http://0.0.0.0:8123" in result.output


# ---------------------------------------------------------------------------
# python -m modulith
# ---------------------------------------------------------------------------


def test_cli_is_reachable_as_a_module():
    """``python -m modulith`` must work, not just the console script: it is the
    only entry point available when the script directory is off PATH — an
    unactivated virtualenv, a shadowed name, a sandbox that permits only an
    interpreter invoked by path."""
    completed = subprocess.run(
        [sys.executable, "-m", "modulith", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "run" in completed.stdout


def test_dev_processes_topology_runs_supervisor(make_fake_app, monkeypatch):
    # The process-per-module path derives one worker per discovered module and
    # hands them to the supervisor — it must NOT exec a single uvicorn.
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "myapp:app", "--topology", "processes", "--port", "8080"])

    assert result.exit_code == 0, result.output
    assert {s.module_name for s in captured["specs"]} == {"orders", "inventory"}
    assert captured["port"] == 8080


def test_dev_isolate_warns_that_every_other_module_is_dropped(make_fake_app, monkeypatch):
    """SPEC.md's own usage line ('--isolate=reports  # only reports gets its
    own process') reads as if the rest of the app keeps running. It does
    not: derive_specs_from_config hard-filters to just the isolated module,
    so every other module gets no worker and 404s through the proxy. The CLI
    must say so instead of leaving that silent."""
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "myapp:app", "--isolate", "orders"])

    assert result.exit_code == 0, result.output
    assert {s.module_name for s in captured["specs"]} == {"orders"}
    assert "orders" in result.stderr
    assert "not start" in result.stderr


def test_dev_isolate_help_states_it_excludes_other_modules():
    """The option help previously read 'Module to isolate in its own
    process', which implies the rest keeps running — the opposite of the
    actual hard-filter behavior in derive_specs_from_config."""
    result = runner.invoke(app, ["dev", "--help"])

    assert result.exit_code == 0, result.output
    assert "only" in result.stdout.lower()


def test_extract_force_help_states_the_import_check_is_never_overridden():
    result = runner.invoke(app, ["extract", "--help"])

    assert result.exit_code == 0, result.output
    help_text = " ".join(click.unstyle(result.stdout).replace("│", " ").split())
    assert "never overrides the import check" in help_text


def test_dev_processes_topology_uses_app_module_package_and_worker_env(make_fake_app, monkeypatch):
    """CLI-only process runs must hand package/broker config to workers.

    A user should not need ``MODULITH_PACKAGE`` just because they chose the
    process topology: the required ``app_module`` argument already names the
    application package. The spawned worker specs also need the same broker
    name, otherwise child processes validate ``topology=processes`` against the
    default in-memory broker and fail at startup.
    """
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (spec,) = captured["specs"]
    assert spec.package == "fakeapp"
    assert spec.env["MODULITH_BROKER"] == "testbroker"


def test_dev_processes_topology_forwards_broker_options_to_workers(
    make_fake_app, monkeypatch, tmp_path
):
    """MEDIUM-6: broker_options the parent resolved (here from pyproject) must
    reach each worker as MODULITH_BROKER_<KEY> env vars — a re-bootstrapping
    worker can't otherwise get e.g. the database broker's URL, so it would fail
    to build an engine."""
    make_fake_app(
        {"orders": ""},
        extra_files={
            "main.py": "from fastapi import FastAPI\napp = FastAPI()\n",
        },
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'broker = "database"\n'
        "[tool.modulith.broker_options]\n"
        'url = "sqlite+aiosqlite:///wf.db"\n'
        "poll_interval_ms = 250\n"
        'expected_consumer_groups = { "fakeapp.orders.WidgetCreated" = ["inventory"] }\n'
    )
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (spec,) = captured["specs"]
    assert spec.env["MODULITH_BROKER"] == "database"
    assert spec.env["MODULITH_BROKER_URL"] == "sqlite+aiosqlite:///wf.db"
    assert spec.env["MODULITH_BROKER_POLL_INTERVAL_MS"] == "250"
    assert json.loads(spec.env["MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS"]) == {
        "fakeapp.orders.WidgetCreated": ["inventory"]
    }


def test_process_topology_resolves_shm_paths_once_before_worker_cwd_changes(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    state_home = tmp_path / "state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (spec,) = captured["specs"]
    expected_dir = Path(spec.env["MODULITH_BROKER_STATE_DIR"])
    assert expected_dir.parent == state_home / "modulith"
    assert expected_dir.name.startswith("fakeapp-")
    expected_sqlite = expected_dir / ".modulith-shm-broker.db"
    expected_hint = expected_dir / ".modulith-shm-broker.hints"
    assert Path(spec.env["MODULITH_BROKER_SQLITE_PATH"]) == expected_sqlite
    assert Path(spec.env["MODULITH_BROKER_HINT_PATH"]) == expected_hint

    different_cwd = tmp_path / "worker-cwd"
    different_cwd.mkdir()
    monkeypatch.chdir(different_cwd)
    from modulith.supervisor import _build_worker_env

    worker_env = _build_worker_env(spec)
    assert Path(worker_env["MODULITH_BROKER_SQLITE_PATH"]) == expected_sqlite
    assert Path(worker_env["MODULITH_BROKER_HINT_PATH"]) == expected_hint


def _state_dir_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and "state_dir" in record.getMessage()
    ]


def test_run_processes_warns_once_when_shm_state_dir_is_defaulted(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    monkeypatch.delenv("MODULITH_BROKER_STATE_DIR", raising=False)
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    with caplog.at_level(logging.WARNING):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (record,) = _state_dir_warnings(caplog)
    message = record.getMessage()
    assert "install path" in message
    assert "[tool.modulith.broker_options].state_dir" in message
    assert "MODULITH_BROKER_STATE_DIR" in message
    assert "Production deploys must set" in message


def test_run_processes_does_not_warn_when_shm_state_dir_is_explicit(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    monkeypatch.setenv("MODULITH_BROKER_STATE_DIR", str(tmp_path / "explicit-state"))
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    with caplog.at_level(logging.WARNING):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    assert _state_dir_warnings(caplog) == []


@pytest.mark.parametrize(
    ("env_key", "absolute", "warns"),
    [
        ("MODULITH_BROKER_SQLITE_PATH", True, False),
        ("MODULITH_BROKER_URL", True, False),
        ("MODULITH_BROKER_SQLITE_PATH", False, True),
    ],
)
def test_run_processes_warns_only_when_the_shm_store_itself_is_defaulted(
    make_fake_app, monkeypatch, tmp_path, caplog, env_key, absolute, warns
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    monkeypatch.delenv("MODULITH_BROKER_STATE_DIR", raising=False)
    store = str(tmp_path / "pinned" / "broker.db") if absolute else "broker.db"
    monkeypatch.setenv(env_key, store)
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    with caplog.at_level(logging.WARNING):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    assert len(_state_dir_warnings(caplog)) == (1 if warns else 0)


def test_dev_processes_topology_env_url_beats_pyproject_url(make_fake_app, monkeypatch, tmp_path):
    """Parent MODULITH_BROKER_URL must reach workers, not the pyproject URL."""
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'broker = "database"\n'
        "[tool.modulith.broker_options]\n"
        'url = "sqlite+aiosqlite:///dev.db"\n'
    )
    monkeypatch.setenv("MODULITH_BROKER_URL", "sqlite+aiosqlite:///prod.db")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (spec,) = captured["specs"]
    # CLI must not forward the lower-priority pyproject URL into spec.env.
    assert "MODULITH_BROKER_URL" not in (spec.env or {})
    # Supervisor still inherits the parent's env for the worker process.
    from modulith.supervisor import _build_worker_env

    assert _build_worker_env(spec)["MODULITH_BROKER_URL"] == "sqlite+aiosqlite:///prod.db"


def _dev_processes_worker_env(make_fake_app, monkeypatch, tmp_path) -> dict[str, str]:
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'broker = "database"\n'
        'outbox = "postgres"\n'
        'outbox_url = "sqlite+aiosqlite:///outbox.db"\n'
        "[tool.modulith.broker_options]\n"
        'url = "sqlite+aiosqlite:///wf.db"\n'
    )
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    (spec,) = captured["specs"]
    return dict(spec.env or {})


def test_dev_processes_topology_forwards_outbox_settings_to_workers(
    make_fake_app, monkeypatch, tmp_path
):
    env = _dev_processes_worker_env(make_fake_app, monkeypatch, tmp_path)

    assert (env.get("MODULITH_OUTBOX"), env.get("MODULITH_OUTBOX_URL")) == (
        "postgres",
        "sqlite+aiosqlite:///outbox.db",
    )


def test_dev_processes_topology_leaves_env_outbox_settings_to_the_workers_env(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", "sqlite+aiosqlite:///prod.db")

    env = _dev_processes_worker_env(make_fake_app, monkeypatch, tmp_path)

    assert ("MODULITH_OUTBOX" in env, "MODULITH_OUTBOX_URL" in env) == (False, False)


def test_run_processes_topology_runs_supervisor(make_fake_app, monkeypatch):
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(
        app,
        ["run", "myapp:app", "--topology", "processes", "--workers", '{"orders": 2}'],
    )

    assert result.exit_code == 0, result.output
    by_name = {s.module_name: s for s in captured["specs"]}
    assert set(by_name) == {"orders", "inventory"}
    assert by_name["orders"].worker_count == 2


def test_run_processes_topology_uses_pyproject_worker_counts(make_fake_app, monkeypatch, tmp_path):
    """[tool.modulith.workers] must feed process-topology worker specs."""
    make_fake_app(
        {"orders": "", "inventory": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'package = "fakeapp"\n'
        'broker = "testbroker"\n'
        "[tool.modulith.workers]\n"
        "orders = 3\n"
    )
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    by_name = {s.module_name: s for s in captured["specs"]}
    assert by_name["orders"].worker_count == 3
    assert by_name["inventory"].worker_count == 1


# ---------------------------------------------------------------------------
# modulith verify
# ---------------------------------------------------------------------------


def test_verify_clean_exits_zero(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""})

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 0, result.output


def test_verify_dirty_exits_one_and_reports_violation(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    # orders reaches into inventory's private package — a hard boundary break.
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "no-internal-imports" in result.output
    assert "orders" in result.output


def test_verify_under_strict_boundaries_reports_violations_and_exits_one(
    make_fake_app, monkeypatch
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "no-internal-imports" in result.output
    assert "boundary violations detected with strict_boundaries" not in result.output


class _TeamRulePlugin:
    @hookimpl
    def modulith_verify_module(self, module, all_modules):
        return [Violation(rule="team-rule", message="team convention broken", module=module.name)]


def test_verify_fails_on_a_plugin_rule_violation(make_fake_app, monkeypatch):
    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    _runtime._extra_plugins.append(_TeamRulePlugin())

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "team-rule" in result.output


def test_verify_disabled_rules_turns_off_a_plugin_rule(make_fake_app, monkeypatch, tmp_path):
    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.verify]\ndisabled_rules = ["team-rule"]\n'
    )
    _runtime._extra_plugins.append(_TeamRulePlugin())

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 0, result.output
    assert "team-rule" not in result.output


def test_verify_unknown_disabled_rule_still_reports_builtin_violations(
    make_fake_app, monkeypatch, tmp_path
):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.verify]\ndisabled_rules = ["team-rule"]\n'
    )

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "no-internal-imports" in result.output


def test_verify_unimportable_package_exits_one(make_fake_app, monkeypatch):
    """An application package whose own __init__ raises must fail `modulith
    verify` (exit 1 with the cause) — not report '✓ no boundary violations'
    and exit 0, going CI-green on an unimportable app."""
    monkeypatch.setenv("MODULITH_PACKAGE", "brokenrootapp")
    make_fake_app(
        {},
        package_name="brokenrootapp",
        extra_files={"__init__.py": "raise NameError('simulated bug in application __init__')\n"},
    )

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "brokenrootapp" in result.output
    assert "no boundary violations" not in result.output


def test_verify_missing_package_exits_one(monkeypatch):
    """A package that doesn't exist at all must also fail verify."""
    monkeypatch.setenv("MODULITH_PACKAGE", "w3_ghost_pkg_does_not_exist")

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "w3_ghost_pkg_does_not_exist" in result.output


def test_verify_ratchet_update_writes_baseline(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    baseline = tmp_path / "baseline.json"

    result = runner.invoke(
        app,
        ["verify", "--mode", "ratchet", "--baseline", str(baseline), "--update-baseline"],
    )

    assert result.exit_code == 0, result.output
    assert baseline.exists()
    assert "no-internal-imports" in baseline.read_text()


def test_verify_ratchet_grandfathers_baselined_violation(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    baseline = tmp_path / "baseline.json"

    # First, bless the current violations.
    seed = runner.invoke(
        app,
        ["verify", "--mode", "ratchet", "--baseline", str(baseline), "--update-baseline"],
    )
    assert seed.exit_code == 0, seed.output

    # Now a plain ratchet run sees no *new* violations → clean.
    result = runner.invoke(app, ["verify", "--mode", "ratchet", "--baseline", str(baseline)])

    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# modulith docs
# ---------------------------------------------------------------------------


def test_docs_command_produces_files(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": "", "inventory": ""})
    out_dir = tmp_path / "generated-docs"

    result = runner.invoke(app, ["docs", "--output-dir", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "architecture.mmd").exists()


def test_docs_under_strict_boundaries_still_generates_files(make_fake_app, monkeypatch, tmp_path):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    make_fake_app(
        {
            "orders": "from fakeapp.inventory._internal import secret\n",
            "inventory": "",
        },
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    out_dir = tmp_path / "generated-docs"

    result = runner.invoke(app, ["docs", "--output-dir", str(out_dir)])

    assert result.exit_code == 0, result.output
    assert (out_dir / "architecture.mmd").exists()
    assert (out_dir / "events.mmd").exists()
    assert "architecture.mmd" in result.output


# ---------------------------------------------------------------------------
# modulith outbox
# ---------------------------------------------------------------------------


def test_outbox_status_reports_counts(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = StubStore()
    now = datetime.now(UTC)
    # one incomplete, one dead-lettered (>= default threshold of 10), one done
    store.pubs[uuid4()] = EventPublication(
        id=uuid4(), payload=b"{}", event_type="x", listener="a", published_at=now
    )
    store.pubs[uuid4()] = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="x",
        listener="b",
        published_at=now,
        attempt_count=10,
    )
    done = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="x",
        listener="c",
        published_at=now,
        completed_at=now,
    )
    store.pubs[done.id] = done
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 0, result.output
    out = result.output.lower()
    assert "incomplete" in out
    assert "dead" in out
    assert "completed" in out


def test_outbox_status_without_store_errors(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    # No outbox.configure() — memory mode, no durable store.

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 1
    assert "outbox" in result.output.lower()


def test_outbox_purge_reports_deleted_count(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = StubStore()
    store.purged = 7
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "purge", "--older-than", "30d"])

    assert result.exit_code == 0, result.output
    assert "7" in result.output


def test_outbox_retry_invalid_id_errors(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=StubStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "retry", "not-a-uuid"])

    assert result.exit_code == 1
    assert "id" in result.output.lower()


def test_outbox_retry_dispatches_to_listener(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class Ping:
                    value: str

                seen = []

                @listener
                async def on_ping(evt: Ping) -> None:
                    seen.append(evt)
            """
        }
    )
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()
    import fakeapp.orders as orders

    store = StubStore()
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)
    pub = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(orders.Ping(value="x")),
        event_type="fakeapp.orders.Ping",
        # Module-qualified identity, mirroring persist() — so _resolve_listener
        # matches the registered @listener on the dispatch round trip.
        listener=outbox._listener_id(orders.on_ping),
        published_at=datetime.now(UTC),
    )
    store.pubs[pub.id] = pub

    result = runner.invoke(app, ["outbox", "retry", str(pub.id)])

    assert result.exit_code == 0, result.output
    assert len(orders.seen) == 1
    assert store.pubs[pub.id].completed_at is not None


class CappedScanStore(StubStore):
    """A store with ``find_by_id`` whose ``find_incomplete`` always misses.

    Simulates a real durable store's capped scan window (LIMIT 100): the
    targeted publication sits further back in a large backlog, so
    ``find_incomplete``/``list_dead_lettered`` return nothing for it, but the
    dedicated ``find_by_id`` point lookup still finds it directly.
    """

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        return []

    async def find_by_id(self, publication_id: UUID) -> EventPublication | None:
        return self.pubs.get(publication_id)


def test_outbox_retry_uses_direct_lookup_outside_capped_scan_window(make_fake_app, monkeypatch):
    """`outbox retry` must use the store's ``find_by_id`` direct point
    lookup — mirroring ``force_retry`` — rather than only the capped
    ``find_incomplete``/``list_dead_lettered`` scan, which can miss a
    targeted row further back in a large backlog."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class Ping:
                    value: str

                seen = []

                @listener
                async def on_ping(evt: Ping) -> None:
                    seen.append(evt)
            """
        }
    )
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()
    import fakeapp.orders as orders

    store = CappedScanStore()
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)
    pub = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(orders.Ping(value="x")),
        event_type="fakeapp.orders.Ping",
        listener=outbox._listener_id(orders.on_ping),
        published_at=datetime.now(UTC),
    )
    store.pubs[pub.id] = pub

    result = runner.invoke(app, ["outbox", "retry", str(pub.id)])

    assert result.exit_code == 0, result.output
    assert len(orders.seen) == 1
    assert store.pubs[pub.id].completed_at is not None


def test_outbox_dead_letter_lists_dead_publications(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = StubStore()
    now = datetime.now(UTC)
    dead = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="fakeapp.orders.Boom",
        listener="handler",
        published_at=now,
        attempt_count=10,
        last_error="kaboom",
    )
    store.pubs[dead.id] = dead
    # a healthy incomplete one should NOT appear in the dead-letter listing
    store.pubs[uuid4()] = EventPublication(
        id=uuid4(), payload=b"{}", event_type="x", listener="ok", published_at=now
    )
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "dead-letter"])

    assert result.exit_code == 0, result.output
    assert str(dead.id) in result.output
    assert "kaboom" in result.output


class FailingStore(StubStore):
    """StubStore plus the optional keyset-paged ``find_failing`` capability."""

    async def find_failing(
        self, *, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> list[EventPublication]:
        rows = sorted(
            (p for p in self.pubs.values() if p.completed_at is None and 0 < p.attempt_count < 10),
            key=lambda p: (p.published_at, p.id),
        )
        if after is not None:
            rows = [p for p in rows if (p.published_at, p.id) > after]
        return rows[:limit]


def _failing_pub(**overrides) -> EventPublication:
    fields = dict(
        id=uuid4(),
        payload=b"{}",
        event_type="fakeapp.orders.Boom",
        listener="handler",
        published_at=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
        attempt_count=3,
        last_error="kaboom",
        last_attempt_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )
    fields.update(overrides)
    return EventPublication(**fields)


def test_outbox_failing_lists_failing_publications_with_next_retry(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = FailingStore()
    failing = _failing_pub()  # attempt 3 → 2 ** 2 = 4s after the last attempt
    never_attempted = _failing_pub(attempt_count=0, last_error=None, last_attempt_at=None)
    dead = _failing_pub(attempt_count=10)
    done = _failing_pub(completed_at=datetime(2026, 1, 1, 12, 5, tzinfo=UTC))
    for pub in (failing, never_attempted, dead, done):
        store.pubs[pub.id] = pub
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if str(failing.id) in line]
    assert len(lines) == 1
    for expected in (
        "fakeapp.orders.Boom",
        "listener=handler",
        "attempts=3",
        "last_error=kaboom",
        "next_retry_at=2026-01-01T12:00:04+00:00",
    ):
        assert expected in lines[0]
    for other in (never_attempted, dead, done):
        assert str(other.id) not in result.output


def test_outbox_failing_shows_now_for_a_row_without_timestamps(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = FailingStore()
    undated = _failing_pub(published_at=None, last_attempt_at=None)
    store.pubs[undated.id] = undated
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if str(undated.id) in line]
    assert len(lines) == 1
    assert lines[0].endswith("next_retry_at=now")


def test_outbox_failing_pages_past_one_page(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = FailingStore()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for i in range(101):
        pub = _failing_pub(published_at=base + timedelta(seconds=i))
        store.pubs[pub.id] = pub
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 0, result.output
    assert "101 failing publication(s):" in result.output
    assert all(str(pub_id) in result.output for pub_id in store.pubs)


def test_outbox_failing_with_nothing_failing_says_so(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=FailingStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 0, result.output
    assert "no failing publications" in result.output


def test_outbox_failing_exits_1_for_a_store_without_find_failing(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=StubStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 1
    assert "find_failing" in result.output


def test_outbox_failing_without_store_errors(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 1
    assert "no outbox store" in result.output


def test_outbox_dead_letter_retry_all_redispatches(make_fake_app, monkeypatch):
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class Ping:
                    value: str

                seen = []

                @listener
                async def on_ping(evt: Ping) -> None:
                    seen.append(evt)
            """
        }
    )
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()
    import fakeapp.orders as orders

    store = StubStore()
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)
    dead = EventPublication(
        id=uuid4(),
        payload=JsonEventSerializer().serialize(orders.Ping(value="z")),
        event_type="fakeapp.orders.Ping",
        listener=outbox._listener_id(orders.on_ping),
        published_at=datetime.now(UTC),
        attempt_count=10,
    )
    store.pubs[dead.id] = dead

    result = runner.invoke(app, ["outbox", "dead-letter", "--retry-all"])

    assert result.exit_code == 0, result.output
    assert len(orders.seen) == 1
    assert store.pubs[dead.id].completed_at is not None


def test_outbox_dead_letter_retry_all_delivers_the_events_its_listeners_publish(
    make_fake_app, monkeypatch, tmp_path
):
    """A listener that publishes inside a bound session schedules after-commit
    dispatches on the command's event loop. The command waits for them, so the
    cascaded event is delivered and no row is left for a lease to expire on."""
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine

    from modulith.adapters import postgres_outbox
    from modulith.adapters.postgres_outbox import (
        Base,
        EventPublicationRow,
        PostgresPublicationStore,
    )

    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", url)
    make_fake_app(
        {
            "orders": """
                import asyncio
                import os
                from dataclasses import dataclass

                from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

                from modulith import event, listener, publish
                from modulith.adapters.postgres_outbox import bind_session, unbind_session

                @event
                @dataclass(frozen=True)
                class Ping:
                    value: str

                @event
                @dataclass(frozen=True)
                class Pong:
                    value: str

                seen_pong = []

                @listener
                async def on_ping(evt: Ping) -> None:
                    engine = create_async_engine(os.environ["MODULITH_OUTBOX_URL"])
                    async with async_sessionmaker(engine)() as session:
                        token = bind_session(session)
                        try:
                            await publish(Pong(value=evt.value))
                            await session.commit()
                        finally:
                            unbind_session(token)
                    await engine.dispose()

                @listener
                async def on_pong(evt: Pong) -> None:
                    await asyncio.sleep(0.2)
                    seen_pong.append(evt.value)
            """
        }
    )

    async def seed_and_read(seed: bool) -> list[bool]:
        engine = create_async_engine(url)
        try:
            if seed:
                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                import fakeapp.orders as orders

                seeder = PostgresPublicationStore(engine=engine, dead_letter_after_attempts=1)
                await seeder.save(
                    EventPublication(
                        id=uuid4(),
                        payload=JsonEventSerializer().serialize(orders.Ping(value="z")),
                        event_type="fakeapp.orders.Ping",
                        listener=outbox._listener_id(orders.on_ping),
                        published_at=datetime.now(UTC),
                        attempt_count=1,
                        last_error="boom",
                    )
                )
                await seeder.dispose()
                return []
            async with engine.connect() as conn:
                rows = (await conn.execute(select(EventPublicationRow))).all()
            return [row.completed_at is not None for row in rows]
        finally:
            await engine.dispose()

    from modulith.runtime import _runtime

    try:
        _runtime.ensure_bootstrapped()
        import fakeapp.orders as orders

        asyncio.run(seed_and_read(seed=True))

        result = runner.invoke(app, ["outbox", "dead-letter", "--retry-all"])

        rows = asyncio.run(seed_and_read(seed=False))
    finally:
        postgres_outbox._reset_for_testing()

    assert result.exit_code == 0, result.output
    assert orders.seen_pong == ["z"]
    assert rows == [True, True]


# ---------------------------------------------------------------------------
# Duration parsing helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("30d", timedelta(days=30)),
        ("7d", timedelta(days=7)),
        ("24h", timedelta(hours=24)),
        ("30m", timedelta(minutes=30)),
        ("90s", timedelta(seconds=90)),
    ],
)
def test_parse_duration_valid(text: str, expected: timedelta) -> None:
    assert _parse_duration(text) == expected


@pytest.mark.parametrize(
    "text",
    ["", "30", "abc", "10y", "-5d", "d30", "999999999999999999999d"],
)
def test_parse_duration_invalid(text: str) -> None:
    # The last case is syntactically valid but overflows timedelta's C int —
    # it must surface as ValueError (clean CLI error), not OverflowError.
    with pytest.raises(ValueError):
        _parse_duration(text)


def test_parse_duration_overflow_message_is_clean() -> None:
    with pytest.raises(ValueError, match="too large"):
        _parse_duration("999999999999999999999d")


# ---------------------------------------------------------------------------
# regression: --version, stderr streams, dead-letter flag exclusivity
# ---------------------------------------------------------------------------


def test_version_flag_prints_version() -> None:
    from modulith import __version__

    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "modulith" in result.output
    assert __version__ in result.output


def test_audit_missing_path_error_goes_to_stderr() -> None:
    result = runner.invoke(app, ["audit", "/no/such/path/xyzzy"])
    assert result.exit_code == 1
    assert "does not exist" in result.stderr
    assert "does not exist" not in result.stdout


def test_outbox_purge_overflow_duration_is_clean_cli_error(make_fake_app, monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=StubStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "purge", "--older-than", "999999999999999999999d"])

    assert result.exit_code == 1
    assert "too large" in result.stderr
    # The fix: a clean ValueError-driven exit, NOT a raw OverflowError traceback.
    assert not isinstance(result.exception, OverflowError)


def test_dead_letter_list_and_retry_all_are_mutually_exclusive(make_fake_app, monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=StubStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "dead-letter", "--list", "--retry-all"])

    assert result.exit_code == 1
    assert "mutually exclusive" in result.stderr


# ---------------------------------------------------------------------------
# regression: W2 audit fixes (G06_cli)
# ---------------------------------------------------------------------------


def test_dev_rejects_unknown_topology(monkeypatch) -> None:
    """A typo'd --topology must error loudly (echoing the actual value),
    not silently launch the process-per-module supervisor."""
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["dev", "myapp:app", "--topology", "sngle"])

    assert result.exit_code == 1
    assert "sngle" in result.stderr
    assert "topology" in result.stderr


def test_run_rejects_unknown_topology(monkeypatch) -> None:
    """`run` validates --topology the same way `dev` does."""
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["run", "myapp:app", "--topology", "processess"])

    assert result.exit_code == 1
    assert "processess" in result.stderr


def test_verify_rejects_unknown_mode(make_fake_app, monkeypatch) -> None:
    """`verify --mode ratchset` must error loudly instead of silently
    taking the strict branch and ignoring the baseline."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["verify", "--mode", "ratchset"])

    assert result.exit_code == 1
    assert "ratchset" in result.stderr


def test_run_help_names_the_table_workers_replaces() -> None:
    """`run --help` must name the pyproject table ``--workers`` overrides.

    typer renders help through rich, which treats square-bracketed text as a
    style tag and deletes it: unescaped, both the option help and the command
    description read "replaces the pyproject  table entirely" — dropping the
    one fact a user needs before overwriting their worker counts.
    """
    result = runner.invoke(app, ["run", "--help"], env={"COLUMNS": "200"})

    assert result.exit_code == 0, result.output
    # Once in the --workers option help, once in the command description.
    assert result.output.count("[tool.modulith.workers]") == 2


def test_run_workers_json_non_object_is_clean_error(make_fake_app, monkeypatch) -> None:
    """A syntactically-valid but non-object --workers JSON value must be a
    clean CLI error, not an AttributeError traceback."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(
        app, ["run", "myapp:app", "--topology", "processes", "--workers", "[1, 2, 3]"]
    )

    assert result.exit_code == 1
    assert "invalid --workers JSON" in result.stderr
    assert not isinstance(result.exception, AttributeError)


def test_run_workers_json_unknown_module_is_clean_error(make_fake_app, monkeypatch) -> None:
    """A mistyped module name in --workers used to be ignored, leaving the
    intended module on one worker. It must exit 1 naming the bad key and the
    modules that exist."""
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        pytest.fail("must not spawn workers")

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(
        app, ["run", "myapp:app", "--topology", "processes", "--workers", '{"ordrs": 4}']
    )

    assert result.exit_code == 1, result.output
    assert "ordrs" in result.stderr
    assert "inventory, orders" in result.stderr


def test_dev_isolate_unknown_module_is_clean_error(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        pytest.fail("must not spawn workers")

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "myapp:app", "--isolate", "ordrs"])

    assert result.exit_code == 1, result.output
    assert "ordrs" in result.stderr
    assert "inventory, orders" in result.stderr


@pytest.mark.parametrize(("command", "option"), [("run", "--workers"), ("dev", "--isolate")])
def test_help_states_that_the_names_must_be_discovered_modules(command, option) -> None:
    result = runner.invoke(app, [command, "--help"], env={"COLUMNS": "200"})

    assert result.exit_code == 0, result.output
    help_text = " ".join(click.unstyle(result.stdout).replace("│", " ").split())
    assert option in help_text
    assert "must name a discovered module" in help_text


@pytest.mark.parametrize("count", ["0", "-3", '"many"', '"2"', "true"])
def test_run_workers_json_bad_count_is_clean_error(count, make_fake_app, monkeypatch) -> None:
    """A bad worker count reached derive_specs_from_config and surfaced as a
    ValueError traceback with exit 2 — while the identical value written to
    [tool.modulith.workers] is a clean exit 1. Same input, same contract:
    positive int, never a bool or a numeric string."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(
        app,
        ["run", "myapp:app", "--topology", "processes", "--workers", f'{{"orders": {count}}}'],
    )

    assert result.exit_code == 1, result.output
    assert "positive integer counts" in result.stderr


def test_run_process_topology_config_error_exits_one(make_fake_app, monkeypatch) -> None:
    """A ConfigurationError raised inside run_supervised used to escape to
    main()'s catch-all: a raw traceback and exit 2, which CI reads as a
    modulith crash rather than the user's own bad configuration."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setenv("MODULITH_PROXY_MAX_BODY_BYTES", "10MB")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["run", "myapp:app", "--topology", "processes"])

    assert result.exit_code == 1, result.output
    assert "MODULITH_PROXY_MAX_BODY_BYTES" in result.stderr
    assert "Traceback" not in result.stderr


def test_outbox_retry_unknown_id_errors(make_fake_app, monkeypatch) -> None:
    """Retrying a nonexistent publication must exit 1 with a 'not found'
    message, not print unconditional success."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    outbox.configure(store=StubStore(), serializer=JsonEventSerializer(), start_loop=False)

    result = runner.invoke(app, ["outbox", "retry", str(uuid4())])

    assert result.exit_code == 1
    assert "not found" in result.stderr
    assert "requested retry" not in result.stdout


class PeerHoldsEveryClaimStore(CappedScanStore):
    """``claim_publication`` finds every row claimed by a live peer."""

    async def claim_publication(
        self, publication_id: UUID, *, owner: str, lease_seconds: float
    ) -> EventPublication | None:
        return None


def test_outbox_retry_of_a_row_a_peer_holds_exits_1_and_says_it_was_left_to_its_holder(
    make_fake_app, monkeypatch
) -> None:
    """A row a live peer's claim holds is not delivered, so the command exits 1
    like the not-found path: a script must not read it as a retry."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    store = PeerHoldsEveryClaimStore()
    outbox.configure(store=store, serializer=JsonEventSerializer(), start_loop=False)
    pub = EventPublication(
        id=uuid4(),
        payload=b"{}",
        event_type="fakeapp.orders.Ping",
        listener="handler",
        published_at=datetime.now(UTC),
    )
    store.pubs[pub.id] = pub

    result = runner.invoke(app, ["outbox", "retry", str(pub.id)])

    assert result.exit_code == 1
    assert "left to its holder" in result.stderr
    assert str(pub.id) in result.stderr
    assert "requested retry" not in result.stdout
    assert store.pubs[pub.id].completed_at is None


def test_dev_empty_app_module_is_clean_error(monkeypatch) -> None:
    """An empty app_module must be a clean CLI error, not an unhandled
    `ValueError: Empty module name` traceback."""
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["dev", "", "--topology", "processes"])

    assert result.exit_code == 1
    assert "app module" in result.stderr.lower()
    assert not isinstance(result.exception, ValueError)


def test_dead_letter_flag_conflict_reported_before_store_precondition(
    make_fake_app, monkeypatch
) -> None:
    """The --list/--retry-all conflict is an argument error and must be
    reported even when no outbox store is configured."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    # Deliberately no outbox.configure(): the store precondition would fail.

    result = runner.invoke(app, ["outbox", "dead-letter", "--list", "--retry-all"])

    assert result.exit_code == 1
    assert "mutually exclusive" in result.stderr
    assert "no outbox store" not in result.stderr


def test_outbox_store_error_does_not_prescribe_already_set_config(
    make_fake_app, monkeypatch
) -> None:
    """The missing-store error must not send a configured user in a circle.

    A durable adapter is already named, so "set [tool.modulith].outbox =
    'postgres'" is a dead end: the value selects an adapter, it never binds a
    store. The message has to name the real cause — nothing called
    ``outbox.configure()`` in *this* process — and the real remedy.
    """
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    make_fake_app({"orders": ""})
    # Deliberately no outbox.configure(): this is the state the message describes.

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 1
    assert "outbox = 'postgres'" not in result.stderr
    assert "no store is bound in this process" in result.stderr
    assert "outbox.configure(" in result.stderr
    assert "MODULITH_OUTBOX_URL" in result.stderr


def test_outbox_store_error_with_discovery_off_points_at_auto_discover(
    make_fake_app, monkeypatch
) -> None:
    """With auto_discover off, bootstrap binds no store and imports no module.

    outbox_url is already set, and an import-time ``outbox.configure()`` never
    runs in a process that imports nothing, so neither is the remedy.
    """
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("MODULITH_AUTO_DISCOVER", "false")
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 1
    assert "auto_discover is off" in result.stderr
    assert "MODULITH_AUTO_DISCOVER=true" in result.stderr
    assert "Set [tool.modulith].outbox_url" not in result.stderr
    assert "module import time" not in result.stderr


def test_outbox_store_error_with_discovery_off_names_the_plugin_route(
    make_fake_app, monkeypatch
) -> None:
    """Bootstrap loads entry-point plugins even with auto_discover off, so the
    message must say it imports none of *your* modules itself and offer the
    plugin import as the second way to bind a store."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_AUTO_DISCOVER", "false")
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 1
    stderr = " ".join(result.stderr.split())
    assert "bootstrap itself imports none of your modules" in stderr
    assert "MODULITH_AUTO_DISCOVER=true" in stderr
    assert "bind the store from an entry-point plugin's import" in stderr


def test_outbox_status_uses_store_built_from_outbox_url(make_fake_app, monkeypatch, tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine

    from modulith.adapters.postgres_outbox import Base

    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"

    async def create_schema() -> None:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

    asyncio.run(create_schema())
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", url)
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["outbox", "status"])

    assert (result.exit_code, result.output.splitlines()[:1]) == (0, ["incomplete:    0"])


@pytest.fixture
def outbox_url_app(make_fake_app, monkeypatch, tmp_path) -> None:
    """A fake app whose outbox store bootstrap builds from a SQLite ``outbox_url``."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from modulith.adapters.postgres_outbox import Base

    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"

    async def create_schema() -> None:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

    asyncio.run(create_schema())
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", url)
    make_fake_app({"orders": ""})


def _outbox_url_engine_state() -> tuple[object, int]:
    """The ownership record bootstrap made for its ``outbox_url`` engine (None
    once shutdown disposed it) and that engine's pool's idle connections."""
    from modulith.runtime import _runtime

    assert _runtime._owned_outbox is not None
    _, engine = _runtime._owned_outbox
    return outbox._owned_resources, engine.pool.checkedin()


@pytest.mark.parametrize(
    ("argv", "exit_code"),
    [
        pytest.param(["status"], 0, id="status"),
        # An id naming no publication still runs the coroutine, finds nothing, exits 1.
        pytest.param(["retry", "00000000-0000-4000-8000-000000000000"], 1, id="retry"),
        pytest.param(["purge", "--older-than", "7d"], 0, id="purge"),
        pytest.param(["dead-letter"], 0, id="dead-letter"),
        pytest.param(["dead-letter", "--retry-all"], 0, id="dead-letter-retry-all"),
        pytest.param(["failing"], 0, id="failing"),
    ],
)
def test_outbox_command_closes_the_engine_built_from_outbox_url(outbox_url_app, argv, exit_code):
    result = runner.invoke(app, ["outbox", *argv])

    assert result.exit_code == exit_code, result.output
    assert _outbox_url_engine_state() == (None, 0)


def test_outbox_command_closes_the_engine_when_its_coroutine_raises(outbox_url_app, monkeypatch):
    from modulith.adapters.postgres_outbox import PostgresPublicationStore

    find_failing = PostgresPublicationStore.find_failing

    async def find_failing_then_raise(self, **kwargs):
        await find_failing(self, **kwargs)  # leaves a pooled connection for shutdown to close
        raise NotImplementedError("PostgresPublicationStore does not implement find_failing")

    monkeypatch.setattr(PostgresPublicationStore, "find_failing", find_failing_then_raise)

    result = runner.invoke(app, ["outbox", "failing"])

    assert result.exit_code == 1, result.output
    assert "cannot list failing publications" in result.stderr
    assert _outbox_url_engine_state() == (None, 0)


def test_outbox_store_error_on_memory_outbox_names_the_config_key(
    make_fake_app, monkeypatch
) -> None:
    """With the default memory outbox, pointing at the config key IS the fix."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})

    result = runner.invoke(app, ["outbox", "status"])

    assert result.exit_code == 1
    assert "[tool.modulith].outbox" in result.stderr
    assert "'memory' outbox persists nothing" in result.stderr


def test_processes_topology_warns_on_app_module_package_mismatch(
    make_fake_app, monkeypatch
) -> None:
    """When configuration already names a package, a conflicting app_module
    argument must produce a loud warning naming both packages, not silently
    launch the configured package's workers."""
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        pass

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "totallydifferent.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    assert "totallydifferent" in result.stderr
    assert "fakeapp" in result.stderr


def test_verify_uses_pyproject_project_name_not_cli_frame(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    """With no explicit package config, CLI bootstrap must resolve the
    package from pyproject [project].name — the caller-stack heuristic
    'detects' the CLI framework itself ('typer'), so `verify` exited 0
    without ever scanning the real app."""
    monkeypatch.delenv("MODULITH_PACKAGE", raising=False)
    make_fake_app(
        {"orders": "from realapp2.inventory._internal import secret\n", "inventory": ""},
        package_name="realapp2",
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "realapp2"\n')

    result = runner.invoke(app, ["verify"])

    assert result.exit_code == 1, result.output
    assert "no-internal-imports" in result.output


def test_info_without_any_package_config_errors_cleanly(make_fake_app, monkeypatch) -> None:
    """No package config anywhere → clean actionable error, never a silent
    bootstrap against the CLI framework's own package."""
    monkeypatch.delenv("MODULITH_PACKAGE", raising=False)
    make_fake_app({})  # importable dir exists, but nothing names the package

    result = runner.invoke(app, ["info"])

    assert result.exit_code == 1
    assert "package" in result.stderr
    assert "typer" not in result.stdout


def test_dev_echoes_boundary_warnings_at_startup(make_fake_app, monkeypatch) -> None:
    """`modulith dev` runs the verifier at startup and echoes violations
    as non-fatal warnings — the dev server still starts."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(os, "execvp", lambda file, args: captured.update(file=file, args=args))

    result = runner.invoke(app, ["dev", "fakeapp.main:app"])

    assert result.exit_code == 0, result.output
    assert captured["file"] == "uvicorn"  # violations never block dev
    assert "no-internal-imports" in result.stderr
    assert "orders" in result.stdout  # discovered module list printed


def test_dev_single_process_strict_boundaries_warns_not_raises(
    make_fake_app, monkeypatch, caplog
) -> None:
    """Per README's "CLI" section (the `modulith dev` warn-only note),
    single-process `modulith dev`'s interactive development
    contract is inviolable: a boundary violation under
    strict_boundaries=True must warn and let the dev server start, never
    crash bootstrap. Before the fix, ensure_bootstrapped() raised
    ConfigurationError here (caught only by the broad except in
    _echo_dev_verify_warnings, which left the runtime un-bootstrapped and
    rolled back) — the crash then hit unprotected on the next lazy
    bootstrap (e.g. the first publish()) once uvicorn's app actually ran."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(os, "execvp", lambda file, args: captured.update(file=file, args=args))

    with caplog.at_level(logging.WARNING, logger="modulith"):
        result = runner.invoke(app, ["dev", "fakeapp.main:app"])

    assert result.exit_code == 0, result.output
    assert captured["file"] == "uvicorn"  # dev server still launches
    from modulith.runtime import _runtime

    assert _runtime._bootstrapped  # bootstrap completed, was not rolled back
    assert any(
        "boundary violations detected" in r.message and "no-internal-imports" in r.message
        for r in caplog.records
    )


def test_dev_processes_topology_strict_boundaries_still_raises(make_fake_app, monkeypatch) -> None:
    """Scope check on the same README "CLI" section note: the warn-only
    downgrade is exclusive to
    single-process dev. `modulith dev --topology=processes` must still fail
    fast on a boundary violation under strict_boundaries=True."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    monkeypatch.setenv("MODULITH_DEV_WARN_ONLY", "1")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, ["dev", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 1
    assert "boundary violations detected" in result.stderr
    assert "MODULITH_DEV_WARN_ONLY" not in os.environ


def test_dev_resolves_topology_from_pyproject_toml(make_fake_app, tmp_path, monkeypatch) -> None:
    """Without --topology flag, `modulith dev` uses topology from [tool.modulith]."""
    make_fake_app({"orders": "", "inventory": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\npackage = "fakeapp"\ntopology = "processes"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp:app"])

    assert result.exit_code == 0, result.output
    assert "specs" in captured  # processes topology was used


def test_dev_resolves_topology_from_env_var(make_fake_app, tmp_path, monkeypatch) -> None:
    """Without --topology flag, `modulith dev` uses MODULITH_TOPOLOGY env var."""
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_TOPOLOGY", "processes")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["dev", "fakeapp:app"])

    assert result.exit_code == 0, result.output
    assert "specs" in captured  # processes topology was used


def test_dev_flag_overrides_configured_topology(make_fake_app, tmp_path, monkeypatch) -> None:
    """The --topology flag overrides [tool.modulith] topology setting."""
    make_fake_app({"orders": "", "inventory": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\npackage = "fakeapp"\ntopology = "processes"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setattr(os, "execvp", lambda file, args: None)

    result = runner.invoke(app, ["dev", "fakeapp:app", "--topology", "single"])

    assert result.exit_code == 0, result.output
    # single-process dev shows the banner with reload status
    assert "modulith dev →" in result.output and "reload=" in result.output


def test_run_resolves_topology_from_pyproject_toml(make_fake_app, tmp_path, monkeypatch) -> None:
    """Without --topology flag, `modulith run` uses topology from [tool.modulith]."""
    make_fake_app({"orders": "", "inventory": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\npackage = "fakeapp"\ntopology = "processes"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["run", "fakeapp:app"])

    assert result.exit_code == 0, result.output
    assert "specs" in captured  # processes topology was used


def test_run_resolves_topology_from_env_var(make_fake_app, tmp_path, monkeypatch) -> None:
    """Without --topology flag, `modulith run` uses MODULITH_TOPOLOGY env var."""
    make_fake_app({"orders": "", "inventory": ""})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_TOPOLOGY", "processes")
    monkeypatch.setenv("MODULITH_BROKER", "testbroker")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))
    captured: dict[str, object] = {}

    async def fake_run_supervised(specs, host, port, **kwargs):
        captured["specs"] = specs

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    result = runner.invoke(app, ["run", "fakeapp:app"])

    assert result.exit_code == 0, result.output
    assert "specs" in captured  # processes topology was used


@pytest.mark.parametrize("command", ["run", "dev"])
def test_unflagged_topology_reports_an_invalid_configuration(
    command: str, make_fake_app, tmp_path, monkeypatch
) -> None:
    """Without --topology the command reads the configuration to pick one, so
    an invalid configuration is the documented one-line error and exit 1, not
    a silent single-process launch."""
    make_fake_app({"orders": ""})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_TOPOLOGY", "procesess")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    result = runner.invoke(app, [command, "fakeapp:app"])

    assert result.exit_code == 1, result.output
    assert "procesess" in result.stderr
    assert "Traceback" not in result.stderr


def test_run_flag_overrides_configured_topology(make_fake_app, tmp_path, monkeypatch) -> None:
    """The --topology flag overrides [tool.modulith] topology setting for run."""
    make_fake_app({"orders": "", "inventory": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\npackage = "fakeapp"\ntopology = "processes"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setattr(os, "execvp", lambda file, args: None)

    result = runner.invoke(app, ["run", "fakeapp:app", "--topology", "single"])

    assert result.exit_code == 0, result.output
    # single-process run shows the banner
    assert "modulith run →" in result.output


def test_app_bootstrap_still_raises_under_strict_boundaries_after_a_tool_command(
    make_fake_app, monkeypatch
) -> None:
    """The tolerance is private to the inspection commands: once `verify`
    has run in this process, bootstrapping the application itself must
    still refuse to start on a boundary violation."""
    import modulith
    from modulith.config import ConfigurationError
    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "1")
    make_fake_app(
        {"orders": "from fakeapp.inventory._internal import secret\n", "inventory": ""},
        extra_files={"inventory/_internal.py": "secret = 1\n"},
    )

    assert runner.invoke(app, ["verify"]).exit_code == 1
    _runtime._reset_for_testing()

    with pytest.raises(ConfigurationError, match="boundary violations detected"):
        modulith.bootstrap()


def test_verify_warning_only_violations_pass_unless_fail_on_warnings(
    make_fake_app, monkeypatch
) -> None:
    """WARNING-severity violations are reported but exit 0 by default;
    --fail-on-warnings opts in to exit 1."""
    from modulith import manifest as manifest_module

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app(
        {
            "orders": "",
            "reporting": """
                from sqlalchemy import MetaData, Table
                metadata = MetaData()
                orders_table = Table("orders", metadata)
            """,
        },
        extra_files={
            "orders/_manifest.py": """
                from modulith.manifest import declare_module
                declare_module(owns_tables=["orders"])
            """
        },
    )

    try:
        default = runner.invoke(app, ["verify"])
        assert default.exit_code == 0, default.output
        assert "WARNING" in default.output

        strict = runner.invoke(app, ["verify", "--fail-on-warnings"])
        assert strict.exit_code == 1, strict.output
    finally:
        manifest_module._reset_for_testing()


def test_main_unexpected_internal_error_exits_2(monkeypatch) -> None:
    """Design decision: exit 2 = unexpected internal error (user errors and
    violations exit 1, success exits 0)."""
    import sys as _sys

    import modulith.cli as cli

    def _boom() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "_bootstrap_or_exit", _boom)
    monkeypatch.setattr(_sys, "argv", ["modulith", "info"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 2


# ---------------------------------------------------------------------------
# uvicorn console script missing from PATH
# ---------------------------------------------------------------------------


def test_run_missing_uvicorn_binary_is_a_clean_user_error(monkeypatch, capsys) -> None:
    """When the uvicorn console script is not on PATH, the CLI should print
    an actionable error naming uvicorn and exit 1 (user/environment error)
    — never a raw traceback with the internal-error code."""
    import sys as _sys

    import modulith.cli as cli

    def missing_execvp(file: str, args: list[str]) -> None:
        # What the real os.execvp raises for an unresolvable executable.
        raise FileNotFoundError(2, "No such file or directory", file)

    monkeypatch.setattr(os, "execvp", missing_execvp)
    monkeypatch.setattr(_sys, "argv", ["modulith", "run", "myapp:app"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user/environment error, not internal (2)
    assert "uvicorn" in captured.err.lower()
    assert "Traceback" not in captured.err


def test_dev_missing_uvicorn_binary_is_a_clean_user_error(monkeypatch, capsys) -> None:
    """The `dev` single-process path execs uvicorn too — a missing binary
    must be the same clean exit-1 user error as `run`, never a raw
    FileNotFoundError traceback with exit code 2."""
    import sys as _sys

    import modulith.cli as cli

    def missing_execvp(file: str, args: list[str]) -> None:
        raise FileNotFoundError(2, "No such file or directory", file)

    monkeypatch.setattr(os, "execvp", missing_execvp)
    monkeypatch.setattr(_sys, "argv", ["modulith", "dev", "myapp:app"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user/environment error, not internal (2)
    assert "uvicorn" in captured.err.lower()
    assert "Traceback" not in captured.err


# ---------------------------------------------------------------------------
# corrupt ratchet baseline is a user error
# ---------------------------------------------------------------------------


def test_verify_ratchet_corrupt_baseline_is_clean_user_error(
    make_fake_app, monkeypatch, capsys, tmp_path
) -> None:
    """`verify --mode=ratchet` on a corrupt baseline must surface
    load_baseline's actionable ConfigurationError and exit 1 (user error)
    — never the raw traceback + exit 2 reserved for internal bugs."""
    import sys as _sys

    import modulith.cli as cli

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{not valid json", encoding="utf-8")
    monkeypatch.setattr(
        _sys,
        "argv",
        ["modulith", "verify", "--mode", "ratchet", "--baseline", str(baseline)],
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user error, not internal (2)
    assert "baseline" in captured.err
    assert "Traceback" not in captured.err


def test_verify_update_baseline_unwritable_path_is_clean_user_error(
    make_fake_app, monkeypatch, capsys, tmp_path
) -> None:
    """`verify --update-baseline` with a --baseline path in a nonexistent
    directory must exit 1 with a clean actionable message — never the raw
    FileNotFoundError traceback + exit 2 reserved for internal bugs."""
    import sys as _sys

    import modulith.cli as cli

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    baseline = tmp_path / "no_such_dir" / "baseline.json"
    monkeypatch.setattr(
        _sys,
        "argv",
        [
            "modulith",
            "verify",
            "--mode",
            "ratchet",
            "--baseline",
            str(baseline),
            "--update-baseline",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user error, not internal (2)
    assert "baseline" in captured.err
    assert "Traceback" not in captured.err


def test_docs_command_config_error_from_render_hook_is_clean_user_error(
    make_fake_app, monkeypatch, capsys, tmp_path
) -> None:
    """The docs generator raises ConfigurationError for duplicate/unsafe
    module names — the `docs` command must map it to the documented exit 1
    (user error), never the exit-2 traceback reserved for internal bugs."""
    import sys as _sys

    import modulith.cli as cli
    from modulith import hookimpl as _hookimpl
    from modulith.config import ConfigurationError
    from modulith.runtime import _runtime

    class _BadDocsPlugin:
        @_hookimpl
        def modulith_render_documentation(self, modules: list, output_dir: str) -> list[str]:
            raise ConfigurationError("duplicate module name(s) in documentation render")

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    _runtime._extra_plugins.append(_BadDocsPlugin())
    monkeypatch.setattr(
        _sys, "argv", ["modulith", "docs", "--output-dir", str(tmp_path / "gen-docs")]
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user error, not internal (2)
    assert "duplicate module name" in captured.err


def test_docs_output_dir_collides_with_file_is_clean_user_error(
    make_fake_app, monkeypatch, capsys, tmp_path
) -> None:
    """`--output-dir` colliding with an existing file raises FileExistsError
    from the generator's own ``mkdir`` — a filesystem/user error per the
    documented exit codes, never the exit-2 traceback reserved for internal
    bugs."""
    import sys as _sys

    import modulith.cli as cli

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    make_fake_app({"orders": ""})
    blocking_file = tmp_path / "gen-docs"
    blocking_file.write_text("not a directory", encoding="utf-8")
    monkeypatch.setattr(_sys, "argv", ["modulith", "docs", "--output-dir", str(blocking_file)])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user/filesystem error, not internal (2)
    assert "Traceback" not in captured.err


def test_audit_unwritable_output_path_is_clean_user_error(
    make_fake_app, monkeypatch, capsys, tmp_path
) -> None:
    """`audit --output` in a nonexistent directory raises FileNotFoundError
    from ``Path.write_text`` — a filesystem/user error per the documented
    exit codes, never the exit-2 traceback reserved for internal bugs."""
    import sys as _sys

    import modulith.cli as cli

    app_dir = tmp_path / "myapp"
    (app_dir / "orders").mkdir(parents=True)
    (app_dir / "__init__.py").write_text("", encoding="utf-8")
    (app_dir / "orders" / "__init__.py").write_text("", encoding="utf-8")
    bad_output = tmp_path / "no_such_dir" / "report.md"
    monkeypatch.setattr(
        _sys, "argv", ["modulith", "audit", str(app_dir), "--output", str(bad_output)]
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 1  # user/filesystem error, not internal (2)
    assert "Traceback" not in captured.err


# ---------------------------------------------------------------------------
# the entry point makes the discovered project root importable
# ---------------------------------------------------------------------------


def test_entry_point_imports_app_package_at_project_root(tmp_path) -> None:
    """An app package at the project root must import from the console script.

    A console script's ``sys.path[0]`` is the directory holding the script
    (``.venv/bin``), never the working directory — so the application package
    sitting next to ``pyproject.toml``, which is what every quickstart layout
    produces, is invisible to ``import`` unless the CLI puts that root on the
    path itself. Reproduced with a launcher outside the project and no
    PYTHONPATH: ``modulith info`` must still discover and import ``shop``.
    """
    import subprocess
    import sys as _sys

    project = tmp_path / "project"
    (project / "shop" / "orders").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "shop"\nversion = "0.1.0"\n\n[tool.modulith]\npackage = "shop"\n',
        encoding="utf-8",
    )
    (project / "shop" / "__init__.py").write_text("", encoding="utf-8")
    (project / "shop" / "orders" / "__init__.py").write_text("", encoding="utf-8")

    # Stands in for the installed console script: a launcher that lives
    # outside the project, so sys.path[0] is its own directory and the
    # project root is reachable only if the CLI adds it.
    launcher_dir = tmp_path / "bin"
    launcher_dir.mkdir()
    launcher = launcher_dir / "modulith_launcher.py"
    launcher.write_text(
        "import sys\nfrom modulith.cli import main\nsys.exit(main())\n", encoding="utf-8"
    )

    env = {k: v for k, v in os.environ.items() if not k.startswith("MODULITH_")}
    env.pop("PYTHONPATH", None)

    proc = subprocess.run(
        [_sys.executable, str(launcher), "info"],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "package: shop" in proc.stdout
    assert "orders" in proc.stdout


def _seed_shm_groups(tmp_path: Path, monkeypatch, groups: dict[str, int]) -> Path:
    """Subscribe each group in the app's default SHM store and publish its backlog."""
    from modulith.adapters.shm_broker import ShmBroker, _resolve_shm_paths

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    _, db_path, hint_path = _resolve_shm_paths("fakeapp", {})

    async def seed() -> None:
        broker = ShmBroker(shm_name=str(hint_path), db_path=str(db_path))
        try:
            for group, count in groups.items():
                target = f"fakeapp.contracts.{group}"
                await broker.subscribe([target], group)
                for _ in range(count):
                    await broker.publish(target, b"x", {"event_type": target})
        finally:
            await broker.close()

    asyncio.run(seed())
    return db_path


def _shm_group_backlog(db_path: Path) -> dict[str, int]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        subscribed = {
            row[0]: 0 for row in conn.execute("SELECT consumer_group FROM shm_subscription")
        }
        for group, count in conn.execute(
            "SELECT consumer_group, COUNT(*) FROM shm_delivery GROUP BY consumer_group"
        ):
            subscribed[group] = count
        return subscribed
    finally:
        conn.close()


def test_process_run_warns_about_subscribed_groups_no_module_derives(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-retired": 3})
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    with caplog.at_level(logging.WARNING, logger="modulith"):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    retired = [m for m in warnings if "modulith-retired" in m]
    assert len(retired) == 1, warnings
    assert "3 pending" in retired[0]
    assert "modulith broker drop-group modulith-retired" in retired[0]
    assert "Every later publication to its targets is queued for it too" in retired[0]
    assert not [m for m in warnings if "modulith-orders" in m]


def _run_processes_warnings(monkeypatch, caplog) -> list[str]:
    """Start ``modulith run --topology processes`` with a no-op supervisor; return its WARNINGs."""
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)
    with caplog.at_level(logging.WARNING, logger="modulith"):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])
    assert result.exit_code == 0, result.output
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


_FAKE_MAIN = {"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"}


def _assert_leftover_backlog_wording(message: str) -> None:
    assert "3 pending" in message
    assert "no subscription" in message
    assert "no new publication reaches it" in message
    assert "only its leftover backlog remains" in message
    assert "queued for it" not in message
    assert "modulith broker drop-group modulith-retired" in message


def test_process_run_says_an_unsubscribed_shm_group_receives_no_new_publications(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    make_fake_app({"orders": ""}, extra_files=_FAKE_MAIN)
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-retired": 3})
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM shm_subscription WHERE consumer_group='modulith-retired'")
    conn.commit()
    conn.close()

    warnings = _run_processes_warnings(monkeypatch, caplog)

    retired = [m for m in warnings if "modulith-retired" in m]
    assert len(retired) == 1, warnings
    _assert_leftover_backlog_wording(retired[0])


def _seed_database_groups(url: str, groups: dict[str, int]) -> None:
    from modulith.adapters.db_broker import DatabaseBroker

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            for group, count in groups.items():
                target = f"fakeapp.contracts.{group}"
                await broker.subscribe([target], group)
                for _ in range(count):
                    await broker.publish(target, b"x", {"event_type": target})
        finally:
            await broker.close()

    asyncio.run(seed())


@pytest.mark.parametrize("subscribed", [True, False], ids=["subscribed", "unsubscribed"])
def test_process_run_words_the_database_retired_group_warning_by_its_subscription(
    make_fake_app, monkeypatch, tmp_path, caplog, subscribed
):
    make_fake_app({"orders": ""}, extra_files=_FAKE_MAIN)
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _seed_database_groups(url, {"modulith-orders": 2, "modulith-retired": 3})
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE broker_subscription SET updated_at='2000-01-01 00:00:00.000000'")
    if not subscribed:
        conn.execute("DELETE FROM broker_subscription WHERE consumer_group='modulith-retired'")
    conn.commit()
    conn.close()

    warnings = _run_processes_warnings(monkeypatch, caplog)

    retired = [m for m in warnings if "modulith-retired" in m]
    assert len(retired) == 1, warnings
    if subscribed:
        assert "Every later publication to its targets is queued for it too" in retired[0]
        assert "leftover backlog" not in retired[0]
    else:
        _assert_leftover_backlog_wording(retired[0])


@pytest.mark.timeout(30)
def test_process_run_continues_when_the_broker_never_answers_the_retired_group_check(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    from modulith.adapters.shm_broker import ShmBroker

    make_fake_app({"orders": ""}, extra_files=_FAKE_MAIN)
    _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-retired": 3})

    async def never_returns(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(ShmBroker, "group_backlog", never_returns)
    monkeypatch.setattr("modulith.cli._RETIRED_CHECK_TIMEOUT_S", 0.05)
    started: list[bool] = []

    async def fake_run_supervised(specs, host, port, **kwargs):
        started.append(True)

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)
    with caplog.at_level(logging.WARNING, logger="modulith"):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    assert started == [True]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    skipped = [m for m in warnings if "shm" in m and "retired" in m]
    assert len(skipped) == 1, warnings
    assert "within" in skipped[0]


def test_broker_drop_group_removes_a_retired_groups_backlog(make_fake_app, monkeypatch, tmp_path):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-retired": 3})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 0, result.output
    assert "1 subscription(s)" in result.output
    assert "3 pending or claimed delivery(ies)" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 2}


def test_broker_drop_group_target_removes_only_that_targets_subscription_and_backlog(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.shm_broker import ShmBroker, _resolve_shm_paths

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    _, db_path, hint_path = _resolve_shm_paths("fakeapp", {})

    async def seed() -> None:
        broker = ShmBroker(shm_name=str(hint_path), db_path=str(db_path))
        try:
            await broker.subscribe(["t.Stale", "t.Live"], "modulith-orders")
            for target, count in (("t.Stale", 2), ("t.Live", 3)):
                for _ in range(count):
                    await broker.publish(target, b"x", {"event_type": target})
        finally:
            await broker.close()

    asyncio.run(seed())

    declined = runner.invoke(
        app, ["broker", "drop-group", "modulith-orders", "--target", "t.Stale"], input="n\n"
    )
    assert declined.exit_code == 1, declined.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 5}

    from modulith.runtime import _runtime

    _runtime._reset_for_testing()
    result = runner.invoke(
        app, ["broker", "drop-group", "modulith-orders", "--target", "t.Stale", "--yes"]
    )

    assert result.exit_code == 0, result.output
    assert "1 subscription(s)" in result.output
    assert "2 pending or claimed delivery(ies)" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 3}


def test_broker_drop_group_asks_before_removing(make_fake_app, monkeypatch, tmp_path):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def _seed_shm_target_backlogs(tmp_path: Path, monkeypatch, backlogs: dict[str, int]) -> Path:
    """Subscribe 'modulith-retired' to each target and queue that many deliveries on it."""
    from modulith.adapters.shm_broker import ShmBroker, _resolve_shm_paths

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    _, db_path, hint_path = _resolve_shm_paths("fakeapp", {})

    async def seed() -> None:
        broker = ShmBroker(shm_name=str(hint_path), db_path=str(db_path))
        try:
            await broker.subscribe(list(backlogs), "modulith-retired")
            for target, count in backlogs.items():
                for _ in range(count):
                    await broker.publish(target, b"x", {"event_type": target})
        finally:
            await broker.close()

    asyncio.run(seed())
    return db_path


def test_broker_drop_group_prompt_states_how_many_deliveries_the_drop_deletes(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-retired": 3})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="y\n")

    assert result.exit_code == 0, result.output
    assert "Drop group 'modulith-retired' and delete its 3 pending and claimed messages?" in (
        result.output
    )
    assert "3 pending or claimed delivery(ies)" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 2}


def test_broker_drop_group_target_prompt_counts_only_that_target(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_target_backlogs(tmp_path, monkeypatch, {"t.Stale": 2, "t.Live": 3})

    result = runner.invoke(
        app, ["broker", "drop-group", "modulith-retired", "--target", "t.Stale"], input="y\n"
    )

    assert result.exit_code == 0, result.output
    assert (
        "Drop targets ['t.Stale'] of group 'modulith-retired' and delete its pending and "
        "claimed messages (t.Stale: 2)?"
    ) in result.output
    assert "2 pending or claimed delivery(ies)" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def test_broker_drop_group_prompt_lists_each_targets_count_and_the_drop_deletes_their_sum(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_target_backlogs(
        tmp_path, monkeypatch, {"t.Stale": 2, "t.Other": 4, "t.Live": 3}
    )

    result = runner.invoke(
        app,
        [
            "broker",
            "drop-group",
            "modulith-retired",
            "--target",
            "t.Stale",
            "--target",
            "t.Other",
        ],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    assert "(t.Stale: 2, t.Other: 4)?" in result.output
    assert "6 pending or claimed delivery(ies)" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def test_broker_drop_group_prompt_counts_the_database_brokers_deliveries(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.db_broker import DatabaseBroker

    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            await broker.subscribe(["t.A", "t.B"], "modulith-retired")
            for target, count in (("t.A", 2), ("t.B", 1)):
                for _ in range(count):
                    await broker.publish(target, b"x", {"event_type": target})
        finally:
            await broker.close()

    asyncio.run(seed())
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE broker_subscription SET updated_at='2000-01-01 00:00:00.000000'")
    conn.commit()
    conn.close()

    result = runner.invoke(
        app, ["broker", "drop-group", "modulith-retired", "--target", "t.A"], input="y\n"
    )

    assert result.exit_code == 0, result.output
    assert "claimed messages (t.A: 2)?" in result.output
    assert "2 pending or claimed delivery(ies)" in result.output


def test_broker_drop_group_with_yes_neither_prompts_nor_reads_the_backlog(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.shm_broker import ShmBroker

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})

    async def unreachable(self, *, targets=None):
        raise AssertionError("--yes must not read the backlog")

    monkeypatch.setattr(ShmBroker, "group_backlog", unreachable)

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 0, result.output
    assert "pending and claimed messages" not in result.output
    assert "3 pending or claimed delivery(ies)" in result.output


def test_broker_drop_group_still_prompts_without_a_count_on_a_broker_without_group_backlog(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.shm_broker import ShmBroker

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})
    monkeypatch.delattr(ShmBroker, "group_backlog")

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="y\n")

    assert result.exit_code == 0, result.output
    assert "Drop group 'modulith-retired' and delete its pending and claimed messages?" in (
        result.output
    )
    assert _shm_group_backlog(db_path) == {}


def test_broker_drop_group_refuses_a_current_modules_group_without_force(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2})

    refused = runner.invoke(app, ["broker", "drop-group", "modulith-orders", "--yes"])

    assert refused.exit_code == 1, refused.output
    assert "--force" in refused.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 2}

    from modulith.runtime import _runtime

    _runtime._reset_for_testing()  # each CLI invocation is a fresh process
    forced = runner.invoke(app, ["broker", "drop-group", "modulith-orders", "--yes", "--force"])

    assert forced.exit_code == 0, forced.output
    assert _shm_group_backlog(db_path) == {}


def _claim_one(db_path: Path, group: str) -> None:
    from modulith.adapters.shm_broker import ShmBroker

    async def claim() -> None:
        broker = ShmBroker(shm_name=str(db_path.with_suffix(".hints")), db_path=str(db_path))
        try:
            assert await broker.claim_batch(group, batch_size=1, consumer_name=f"{group}:w")
        finally:
            await broker.close()

    asyncio.run(claim())


def test_process_run_does_not_warn_about_a_group_another_service_consumes(
    make_fake_app, monkeypatch, tmp_path, caplog
):
    make_fake_app(
        {"orders": ""},
        extra_files={"main.py": "from fastapi import FastAPI\napp = FastAPI()\n"},
    )
    db_path = _seed_shm_groups(
        tmp_path, monkeypatch, {"modulith-orders": 2, "modulith-extracted": 3}
    )
    _claim_one(db_path, "modulith-extracted")
    monkeypatch.setattr(os, "execvp", lambda *a: pytest.fail("must not exec uvicorn"))

    async def fake_run_supervised(specs, host, port, **kwargs):
        return None

    monkeypatch.setattr("modulith.supervisor.run_supervised", fake_run_supervised)

    with caplog.at_level(logging.WARNING, logger="modulith"):
        result = runner.invoke(app, ["run", "fakeapp.main:app", "--topology", "processes"])

    assert result.exit_code == 0, result.output
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert not [m for m in warnings if "modulith-extracted" in m], warnings


def test_broker_drop_group_refuses_a_recently_active_group_without_force(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-extracted": 3})
    _claim_one(db_path, "modulith-extracted")

    refused = runner.invoke(app, ["broker", "drop-group", "modulith-extracted", "--yes"])

    assert refused.exit_code == 1, refused.output
    assert "--force" in refused.output
    assert _shm_group_backlog(db_path) == {"modulith-extracted": 3}


def test_broker_drop_group_rechecks_liveness_after_the_confirmation_prompt(
    make_fake_app, monkeypatch, tmp_path
):
    import threading

    import typer

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})

    def consumer_starts_while_the_prompt_waits(prompt: str) -> bool:
        worker = threading.Thread(target=_claim_one, args=(db_path, "modulith-retired"))
        worker.start()
        worker.join()
        return True

    monkeypatch.setattr(typer, "confirm", consumer_starts_while_the_prompt_waits)

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"])

    assert result.exit_code == 1, result.output
    assert "'modulith-retired'" in result.output
    assert "--force" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def test_broker_drop_group_confirmation_prompt_is_interruptible_by_ctrl_c(
    make_fake_app, monkeypatch, tmp_path
):
    """asyncio.run installs its own SIGINT handler, which defers the interrupt until a
    blocking input() returns; the prompt must run under the default handler so the
    first Ctrl-C aborts it and a later "y" cannot drop the group."""
    import signal

    import typer

    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})
    handlers: list[object] = []

    def interrupted(prompt: str) -> bool:
        handlers.append(signal.getsignal(signal.SIGINT))
        raise KeyboardInterrupt

    monkeypatch.setattr(typer, "confirm", interrupted)
    before = signal.getsignal(signal.SIGINT)

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"])

    assert handlers == [signal.default_int_handler]
    assert result.exit_code != 0, result.output
    assert signal.getsignal(signal.SIGINT) is before
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def test_a_consumer_missing_two_subscription_refreshes_still_counts_as_live() -> None:
    """A failed refresh is retried one interval later, so two failures in a row age the
    subscription to three intervals; the group must still read as live then."""
    from modulith.adapters._polling_consumer import SUBSCRIPTION_REFRESH_S
    from modulith.cli import _LIVE_GROUP_WINDOW_S

    assert SUBSCRIPTION_REFRESH_S * 3 <= _LIVE_GROUP_WINDOW_S


def test_broker_drop_group_of_an_unknown_group_fails(make_fake_app, monkeypatch, tmp_path):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retierd", "--yes"])

    assert result.exit_code == 1, result.output
    assert "no subscription or undelivered work" in result.output
    assert _shm_group_backlog(db_path) == {"modulith-retired": 3}


def test_broker_drop_group_names_the_shm_store_before_confirming(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 3})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert f"shm broker store: {db_path}" in result.output
    assert result.output.index(str(db_path)) < result.output.index("Drop group")


def test_broker_drop_group_does_not_create_a_missing_shm_store(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    state_home = tmp_path / "empty-state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 1, result.output
    assert "no shm broker store" in result.output
    assert not state_home.exists()


def test_broker_drop_group_refuses_a_broker_without_a_group_ledger_before_connecting(
    make_fake_app, monkeypatch
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "redis-streams")
    with socket.socket() as redis_stand_in:
        redis_stand_in.bind(("127.0.0.1", 0))
        redis_stand_in.listen()
        monkeypatch.setenv("REDIS_URL", f"redis://127.0.0.1:{redis_stand_in.getsockname()[1]}")

        result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

        assert result.exit_code == 1, result.output
        assert (
            "error: broker 'redis-streams' keeps no per-group subscription ledger; "
            "drop-group applies to shm and database"
        ) in result.stderr
        pending, _, _ = select.select([redis_stand_in], [], [], 0)
        assert not pending, "drop-group connected to the broker before refusing it"


def _database_project(tmp_path: Path, monkeypatch, db_file: Path) -> str:
    url = f"sqlite+aiosqlite:///{db_file}"
    (tmp_path / "pyproject.toml").write_text(
        f'[tool.modulith]\nbroker = "database"\n[tool.modulith.broker_options]\nurl = "{url}"\n'
    )
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    return url


def test_broker_drop_group_on_the_database_broker_names_its_store_and_sole_targets(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.db_broker import DatabaseBroker

    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            await broker.subscribe(["t.Only", "t.Shared"], "modulith-retired")
            await broker.subscribe(["t.Shared"], "modulith-orders")
        finally:
            await broker.close()

    asyncio.run(seed())
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE broker_subscription SET updated_at='2000-01-01 00:00:00.000000'")
    conn.commit()
    conn.close()

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert f"database broker store: {url}" in result.output
    assert "only subscriber of 't.Only'" in result.output
    assert "t.Shared" not in result.output
    assert "NoSubscribersError" in result.output


def _seed_database_subscriptions(
    url: str, db_file: Path, subscriptions: dict[str, list[str]], *, served_recently: bool
) -> None:
    from modulith.adapters.db_broker import DatabaseBroker

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            for group, targets in subscriptions.items():
                await broker.subscribe(targets, group)
        finally:
            await broker.close()

    asyncio.run(seed())
    if not served_recently:
        conn = sqlite3.connect(db_file)
        conn.execute("UPDATE broker_subscription SET updated_at='2000-01-01 00:00:00.000000'")
        conn.commit()
        conn.close()


@pytest.mark.parametrize(
    ("options", "expected", "absent"),
    [
        (
            {"NO_SUBSCRIBER_POLICY": "store", "ORPHAN_RETENTION_SECONDS": "3600"},
            [
                "kept for 3600 s (orphan_retention_seconds)",
                "replayed to every group that subscribes before then",
                "pruned undelivered",
            ],
            ["retained until a group subscribes", "NoSubscribersError"],
        ),
        (
            {"NO_SUBSCRIBER_POLICY": "store", "ORPHAN_REPLAY_POLICY": "first_groups"},
            [
                "kept for 86400 s (orphan_retention_seconds)",
                "replayed to the first group that subscribes before then",
            ],
            ["retained until a group subscribes"],
        ),
        (
            {
                "NO_SUBSCRIBER_POLICY": "store",
                "ORPHAN_REPLAY_POLICY": "expected_groups",
                "EXPECTED_CONSUMER_GROUPS": '{"t.Only": ["modulith-orders"]}',
            },
            [
                "go at once to the groups expected_consumer_groups lists for each target",
                "a group that subscribes later receives none of them",
                "a publish to a target with no expected_consumer_groups entry "
                "raises ConfigurationError",
            ],
            ["retained until a group subscribes", "kept for", "fanned out"],
        ),
        (
            {"NO_SUBSCRIBER_POLICY": "wait", "NO_SUBSCRIBER_WAIT_TIMEOUT_SECONDS": "5"},
            [
                "each later publish to them waits up to 5 s (no_subscriber_wait_timeout_seconds)",
                "succeeds if a group subscribes meanwhile",
                "raises NoSubscribersError otherwise",
            ],
            [],
        ),
        (
            {"NO_SUBSCRIBER_POLICY": "store", "ORPHAN_RETENTION_SECONDS": "2592000"},
            ["kept for 2592000 s (orphan_retention_seconds)"],
            ["e+06"],
        ),
        (
            {"NO_SUBSCRIBER_POLICY": "wait", "NO_SUBSCRIBER_WAIT_TIMEOUT_SECONDS": "1234567"},
            ["waits up to 1234567 s (no_subscriber_wait_timeout_seconds)"],
            ["e+06"],
        ),
    ],
    ids=[
        "store-ttl_all_groups",
        "store-first_groups",
        "store-expected_groups",
        "wait",
        "store-retention-in-plain-seconds",
        "wait-timeout-in-plain-seconds",
    ],
)
def test_broker_drop_group_on_the_database_broker_states_what_happens_to_later_publishes(
    make_fake_app, monkeypatch, tmp_path, options, expected, absent
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    for key, value in options.items():
        monkeypatch.setenv(f"MODULITH_BROKER_{key}", value)
    _seed_database_subscriptions(
        url, db_file, {"modulith-retired": ["t.Only"]}, served_recently=False
    )

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert "only subscriber of 't.Only'" in result.output
    for fragment in expected:
        assert fragment in result.output
    for fragment in absent:
        assert fragment not in result.output


def test_broker_drop_group_on_the_shm_broker_states_the_orphan_retention(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_RETENTION_SECONDS", "7200")
    _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 1})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert "only subscriber of 'fakeapp.contracts.modulith-retired'" in result.output
    assert (
        "kept for 7200 s (orphan_retention_seconds) and replayed to a group that "
        "subscribes within that time" in result.output
    )
    assert (
        "as far as the replay's page limit allows: 8 pages below the store's publish "
        "budget (max_store_bytes), or halfway from the used pages to there under "
        'completion_mode="mark"' in result.output
    )
    assert "reach no consumer" not in result.output


def test_broker_drop_group_on_the_shm_broker_prints_retention_in_plain_seconds(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_RETENTION_SECONDS", "2592000")
    _seed_shm_groups(tmp_path, monkeypatch, {"modulith-retired": 1})

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired"], input="n\n")

    assert result.exit_code == 1, result.output
    assert "kept for 2592000 s (orphan_retention_seconds)" in result.output
    assert "e+06" not in result.output


def test_broker_drop_group_refusal_on_the_shm_broker_says_a_resubscribing_group_is_replayed(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    db_path = _seed_shm_groups(tmp_path, monkeypatch, {"modulith-orders": 2})

    refused = runner.invoke(app, ["broker", "drop-group", "modulith-orders", "--yes"])

    assert refused.exit_code == 1, refused.output
    assert "deletes its queued messages undelivered" not in refused.output
    assert (
        "a group that subscribes again within orphan_retention_seconds is replayed "
        "the retained ones" in refused.output
    )
    assert "--force" in refused.output
    assert _shm_group_backlog(db_path) == {"modulith-orders": 2}


def test_broker_drop_group_refusal_on_the_database_broker_still_says_queued_messages_go(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _seed_database_subscriptions(
        url, db_file, {"modulith-orders": ["t.Only"]}, served_recently=True
    )

    refused = runner.invoke(app, ["broker", "drop-group", "modulith-orders", "--yes"])

    assert refused.exit_code == 1, refused.output
    assert "deletes its queued messages undelivered" in refused.output
    assert "is replayed the retained ones" not in refused.output


def test_broker_drop_group_help_qualifies_the_deleted_messages_on_shm() -> None:
    result = runner.invoke(app, ["broker", "drop-group", "--help"], env={"COLUMNS": "400"})

    assert result.exit_code == 0, result.output
    text = " ".join(result.output.split())
    assert "a group that subscribes again within the retention window is replayed" in text


def _expected_groups_options(monkeypatch, groups: dict[str, list[str]]) -> None:
    monkeypatch.setenv("MODULITH_BROKER_NO_SUBSCRIBER_POLICY", "store")
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_REPLAY_POLICY", "expected_groups")
    monkeypatch.setenv("MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS", json.dumps(groups))


def test_broker_drop_group_says_expected_consumer_groups_still_routes_to_the_group(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _expected_groups_options(
        monkeypatch, {"t.Only": ["modulith-retired"], "t.Other": ["modulith-orders"]}
    )
    _seed_database_subscriptions(
        url, db_file, {"modulith-retired": ["t.Only"]}, served_recently=False
    )

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 0, result.output
    assert "expected_consumer_groups still lists 'modulith-retired' for 't.Only'" in result.output
    assert "every later publish to them queues a pending message for it again" in result.output
    assert "delete the 't.Only' key from expected_consumer_groups" in result.output
    assert "from the group list of 't.Only'" not in result.output
    assert "reports it again" not in result.output
    assert "modulith run --topology processes" in result.output
    assert "no module derives it" in result.output
    assert "t.Other" not in result.output
    assert "dropped group 'modulith-retired'" in result.output


def test_broker_drop_group_says_which_expected_consumer_groups_entries_to_delete_or_trim(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _expected_groups_options(
        monkeypatch,
        {
            "t.Sole": ["modulith-retired"],
            "t.Shared": ["modulith-orders", "modulith-retired"],
        },
    )
    _seed_database_subscriptions(
        url, db_file, {"modulith-retired": ["t.Sole", "t.Shared"]}, served_recently=False
    )

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 0, result.output
    assert "delete the 't.Sole' key from expected_consumer_groups" in result.output
    assert (
        "remove 'modulith-retired' from the group list of 't.Shared' in expected_consumer_groups"
        in result.output
    )
    assert "delete the 't.Shared' key" not in result.output
    assert "from the group list of 't.Sole'" not in result.output


def test_broker_drop_group_no_entry_warning_matches_what_a_publish_does(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters.db_broker import DatabaseBroker
    from modulith.config import ConfigurationError

    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _expected_groups_options(monkeypatch, {"t.Other": ["modulith-orders"]})
    _seed_database_subscriptions(
        url, db_file, {"modulith-retired": ["t.Only"]}, served_recently=False
    )

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 0, result.output
    assert (
        "a publish to a target with no expected_consumer_groups entry raises ConfigurationError"
        in result.output
    )

    async def publish() -> None:
        broker = DatabaseBroker(
            url=url,
            no_subscriber_policy="store",
            orphan_replay_policy="expected_groups",
            expected_consumer_groups={"t.Other": ["modulith-orders"]},
        )
        try:
            await broker.publish("t.Only", b"x", {"event_type": "t.Only"})
        finally:
            await broker.close()

    with pytest.raises(ConfigurationError, match="expected_consumer_groups"):
        asyncio.run(publish())


def test_broker_drop_group_refusal_names_expected_consumer_groups_routing(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "broker.db"
    url = _database_project(tmp_path, monkeypatch, db_file)
    _expected_groups_options(monkeypatch, {"t.Only": ["modulith-retired"]})
    _seed_database_subscriptions(
        url, db_file, {"modulith-retired": ["t.Only"]}, served_recently=True
    )

    refused = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert refused.exit_code == 1, refused.output
    assert "receive no new publications until" not in refused.output
    assert "expected_consumer_groups keeps queueing publishes to 't.Only' for it" in (
        refused.output
    )
    assert "--force" in refused.output


def test_broker_drop_group_force_help_describes_the_live_group_guard() -> None:
    result = runner.invoke(app, ["broker", "drop-group", "--help"], env={"COLUMNS": "400"})

    assert result.exit_code == 0, result.output
    assert (
        "Drop the group even though a current module derives it or a consumer served "
        "it in the last 24 h." in result.output
    )


def test_broker_drop_group_does_not_create_a_missing_database_store(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "absent.db"
    _database_project(tmp_path, monkeypatch, db_file)

    result = runner.invoke(app, ["broker", "drop-group", "modulith-retired", "--yes"])

    assert result.exit_code == 1, result.output
    assert "no database broker tables" in result.output
    assert not db_file.exists()


def _embedded_database_project(monkeypatch, tmp_path: Path) -> Path:
    """A database broker with no URL; returns the empty state home its SQLite file would go in."""
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "database")
    state_home = tmp_path / "empty-state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setenv("LOCALAPPDATA", str(state_home))
    return state_home


@pytest.mark.parametrize(
    ("args", "outcome"),
    [
        (["drop-group", "modulith-retired", "--yes"], "nothing was removed"),
        (["dead-letter"], "nothing was listed or resubmitted"),
    ],
    ids=["drop-group", "dead-letter"],
)
def test_broker_commands_do_not_create_the_embedded_database_file(
    make_fake_app, monkeypatch, tmp_path, args, outcome
):
    from modulith.adapters._state_path import default_state_directory
    from modulith.config import DEFAULT_BROKER_DB_FILENAME

    make_fake_app({"orders": ""})
    state_home = _embedded_database_project(monkeypatch, tmp_path)
    db_file = default_state_directory("fakeapp") / DEFAULT_BROKER_DB_FILENAME

    result = runner.invoke(app, ["broker", *args])

    assert result.exit_code == 1, result.output
    assert not state_home.exists()
    assert f"error: no database broker store at {db_file}; {outcome}." in result.stderr


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (
            ["drop-group", "modulith-retired", "--yes"],
            "dropped group 'modulith-retired': 1 subscription(s)",
        ),
        (["dead-letter"], "no dead-lettered messages"),
    ],
    ids=["drop-group", "dead-letter"],
)
@pytest.mark.parametrize("state_dir", [None, "custom-state"], ids=["default-dir", "state_dir"])
def test_broker_commands_use_the_embedded_database_file_the_service_creates(
    make_fake_app, monkeypatch, tmp_path, args, expected, state_dir
):
    from modulith.adapters._state_path import default_state_directory
    from modulith.config import DEFAULT_BROKER_DB_FILENAME

    make_fake_app({"orders": ""})
    _embedded_database_project(monkeypatch, tmp_path)
    if state_dir is None:
        directory = default_state_directory("fakeapp")
    else:
        directory = tmp_path / state_dir
        monkeypatch.setenv("MODULITH_BROKER_STATE_DIR", str(directory))
    directory.mkdir(parents=True, mode=0o700)
    db_file = directory / DEFAULT_BROKER_DB_FILENAME
    _seed_database_subscriptions(
        f"sqlite+aiosqlite:///{db_file}",
        db_file,
        {"modulith-retired": ["t.Order"]},
        served_recently=False,
    )

    result = runner.invoke(app, ["broker", *args])

    assert result.exit_code == 0, result.output
    assert expected in result.output


@pytest.mark.filterwarnings("ignore:Query string argument:sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize(
    "args",
    [["drop-group", "modulith-retired", "--yes"], ["dead-letter"]],
    ids=["drop-group", "dead-letter"],
)
def test_broker_commands_mask_query_string_secrets_in_the_store_they_name(
    make_fake_app, monkeypatch, tmp_path, args
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("MODULITH_BROKER", "database")
    db_file = tmp_path / "absent.db"
    monkeypatch.setenv("MODULITH_BROKER_URL", f"sqlite+aiosqlite:///{db_file}?password=hunter2")

    result = runner.invoke(app, ["broker", *args])

    assert result.exit_code == 1, result.output
    assert "no database broker tables" in result.output
    assert "hunter2" not in result.output
    assert f"{db_file}?password=***" in result.output


def _seed_dead_delivery(url: str) -> str:
    """One message fanned out to two groups: orders dead-letters it, billing completes it."""
    from modulith.adapters.db_broker import DatabaseBroker

    async def seed() -> str:
        broker = DatabaseBroker(url=url)
        try:
            await broker.subscribe(["t.Order"], "modulith-orders")
            await broker.subscribe(["t.Order"], "modulith-billing")
            await broker.publish("t.Order", b"{}", {"event_type": "t.Order"})
            (orders,) = await broker.claim_batch(
                "modulith-orders", batch_size=5, consumer_name="o1"
            )
            (billing,) = await broker.claim_batch(
                "modulith-billing", batch_size=5, consumer_name="b1"
            )
            await broker.fail(
                orders["id"], "boom: listener raised", consumer_name="o1", max_attempts=1
            )
            await broker.ack(billing["id"], consumer_name="b1")
            return str(orders["id"])
        finally:
            await broker.close()

    return asyncio.run(seed())


def _claim_for(url: str, group: str) -> list[dict]:
    from modulith.adapters.db_broker import DatabaseBroker

    async def claim() -> list[dict]:
        broker = DatabaseBroker(url=url)
        try:
            return await broker.claim_batch(group, batch_size=5, consumer_name="again")
        finally:
            await broker.close()

    return asyncio.run(claim())


@pytest.mark.parametrize("flags", [[], ["--list"]])
def test_broker_dead_letter_lists_an_exhausted_delivery_with_its_error(
    make_fake_app, monkeypatch, tmp_path, flags
):
    make_fake_app({"orders": ""})
    url = _database_project(tmp_path, monkeypatch, tmp_path / "broker.db")
    row_id = _seed_dead_delivery(url)

    result = runner.invoke(app, ["broker", "dead-letter", *flags])

    assert result.exit_code == 0, result.output
    assert "1 dead-lettered message(s):" in result.output
    assert (
        f"  {row_id}  t.Order  target=t.Order  group=modulith-orders  attempts=1  "
        "last_error=boom: listener raised"
    ) in result.output


def test_broker_dead_letter_retry_all_reaches_only_the_group_whose_delivery_died(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    url = _database_project(tmp_path, monkeypatch, tmp_path / "broker.db")
    row_id = _seed_dead_delivery(url)

    result = runner.invoke(app, ["broker", "dead-letter", "--retry-all"])

    assert result.exit_code == 0, result.output
    assert "resubmitted 1 dead-lettered message(s)" in result.output
    assert _claim_for(url, "modulith-billing") == []
    assert [row["id"] for row in _claim_for(url, "modulith-orders")] == [row_id]


def _seed_shm_dead_delivery(tmp_path: Path, monkeypatch) -> Path:
    """One message fanned out to two groups: orders dead-letters it, billing completes it."""
    from modulith.adapters.shm_broker import ShmBroker, _resolve_shm_paths

    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state-home"))
    _, db_path, hint_path = _resolve_shm_paths("fakeapp", {})

    async def seed() -> None:
        broker = ShmBroker(shm_name=str(hint_path), db_path=str(db_path), completion_mode="mark")
        try:
            await broker.subscribe(["t.Order"], "modulith-orders")
            await broker.subscribe(["t.Order"], "modulith-billing")
            await broker.publish("t.Order", b"{}", {"event_type": "t.Order"})
            (orders,) = await broker.claim_batch(
                "modulith-orders", batch_size=5, consumer_name="o1"
            )
            (billing,) = await broker.claim_batch(
                "modulith-billing", batch_size=5, consumer_name="b1"
            )
            await broker.fail(
                orders["id"], "boom: listener raised", consumer_name="o1", max_attempts=1
            )
            await broker.ack(billing["id"], consumer_name="b1")
        finally:
            await broker.close()

    asyncio.run(seed())
    return db_path


def _claim_from_shm(db_path: Path, group: str) -> list[dict]:
    from modulith.adapters.shm_broker import ShmBroker, _resolve_shm_paths

    _, _, hint_path = _resolve_shm_paths("fakeapp", {})

    async def claim() -> list[dict]:
        broker = ShmBroker(shm_name=str(hint_path), db_path=str(db_path))
        try:
            return await broker.claim_batch(group, batch_size=5, consumer_name="again")
        finally:
            await broker.close()

    return asyncio.run(claim())


def test_shm_broker_dead_letter_lists_an_exhausted_delivery_with_its_error(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    import sqlite3

    db_path = _seed_shm_dead_delivery(tmp_path, monkeypatch)
    conn = sqlite3.connect(db_path)
    try:
        (row_id,) = conn.execute("SELECT id FROM shm_delivery WHERE status='dead'").fetchone()
    finally:
        conn.close()

    result = runner.invoke(app, ["broker", "dead-letter"])

    assert result.exit_code == 0, result.output
    assert "1 dead-lettered message(s):" in result.output
    assert (
        f"  {row_id}  t.Order  target=t.Order  group=modulith-orders  attempts=1  "
        "last_error=boom: listener raised"
    ) in result.output


def test_shm_broker_dead_letter_retry_all_reaches_only_the_group_whose_delivery_died(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_path = _seed_shm_dead_delivery(tmp_path, monkeypatch)

    result = runner.invoke(app, ["broker", "dead-letter", "--retry-all"])

    assert result.exit_code == 0, result.output
    assert "resubmitted 1 dead-lettered message(s)" in result.output
    assert _claim_from_shm(db_path, "modulith-billing") == []
    assert [row["consumer_group"] for row in _claim_from_shm(db_path, "modulith-orders")] == [
        "modulith-orders"
    ]


def test_shm_broker_dead_letter_does_not_create_a_missing_store(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    state_home = tmp_path / "empty-state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))

    result = runner.invoke(app, ["broker", "dead-letter", "--retry-all"])

    assert result.exit_code == 1, result.output
    assert "no shm broker store" in result.output
    assert not state_home.exists()


def test_broker_dead_letter_with_nothing_dead_says_so(make_fake_app, monkeypatch, tmp_path):
    from modulith.adapters.db_broker import DatabaseBroker

    make_fake_app({"orders": ""})
    url = _database_project(tmp_path, monkeypatch, tmp_path / "broker.db")

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            await broker.subscribe(["t.Order"], "modulith-orders")
        finally:
            await broker.close()

    asyncio.run(seed())

    result = runner.invoke(app, ["broker", "dead-letter", "--list"])

    assert result.exit_code == 0, result.output
    assert result.output.strip().endswith("no dead-lettered messages")


def test_broker_dead_letter_retry_all_reports_a_refusal_and_exits_1(
    make_fake_app, monkeypatch, tmp_path
):
    from modulith.adapters._dead_letter import DeadLetterRetryRefused
    from modulith.adapters.db_broker import DatabaseBroker

    make_fake_app({"orders": ""})
    url = _database_project(tmp_path, monkeypatch, tmp_path / "broker.db")

    async def seed() -> None:
        broker = DatabaseBroker(url=url)
        try:
            await broker.subscribe(["t.Order"], "modulith-orders")
        finally:
            await broker.close()

    asyncio.run(seed())

    async def refuse(self: DatabaseBroker) -> int:
        raise DeadLetterRetryRefused("t.Order has consumer groups modulith-a, modulith-b")

    monkeypatch.setattr(DatabaseBroker, "retry_dead_letters", refuse)

    result = runner.invoke(app, ["broker", "dead-letter", "--retry-all"])

    assert result.exit_code == 1, result.output
    assert "error: t.Order has consumer groups modulith-a, modulith-b" in result.stderr


def test_broker_dead_letter_flag_conflict_is_reported_before_any_environment_check(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    state_home = tmp_path / "empty-state-home"
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")

    result = runner.invoke(app, ["broker", "dead-letter", "--list", "--retry-all"])

    assert result.exit_code == 1
    assert "--list and --retry-all are mutually exclusive" in result.stderr
    assert "no shm broker store" not in result.output
    assert not state_home.exists()


@pytest.mark.parametrize("flags", [[], ["--retry-all"]])
def test_broker_dead_letter_names_a_broker_without_the_methods(
    make_fake_app, monkeypatch, tmp_path, flags
):
    from modulith.adapters.db_broker import DatabaseBroker

    make_fake_app({"orders": ""})
    _database_project(tmp_path, monkeypatch, tmp_path / "broker.db")
    monkeypatch.delattr(DatabaseBroker, "list_dead_letters")
    monkeypatch.delattr(DatabaseBroker, "retry_dead_letters")

    result = runner.invoke(app, ["broker", "dead-letter", *flags])

    assert result.exit_code == 1, result.output
    assert "broker 'database' does not support dead-letter inspection" in result.stderr


def test_broker_dead_letter_does_not_create_a_missing_database_store(
    make_fake_app, monkeypatch, tmp_path
):
    make_fake_app({"orders": ""})
    db_file = tmp_path / "absent.db"
    _database_project(tmp_path, monkeypatch, db_file)

    result = runner.invoke(app, ["broker", "dead-letter"])

    assert result.exit_code == 1, result.output
    assert "no database broker tables" in result.stderr
    assert not db_file.exists()


@pytest.fixture
def redis_dead_letter_project(make_fake_app, monkeypatch, tmp_path, redis_url, redis_key_prefix):
    """A Redis-configured app holding one dead letter for group ``modulith-orders``."""
    from modulith.adapters.redis_broker import RedisStreamsBroker

    make_fake_app({"orders": ""})
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\nbroker = "redis-streams"\n[tool.modulith.broker_options]\n'
        f'url = "{redis_url}"\nstream_prefix = "{redis_key_prefix}"\n'
    )
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("MODULITH_STREAM_PREFIX", raising=False)

    async def seed() -> None:
        broker = RedisStreamsBroker(url=redis_url, stream_prefix=redis_key_prefix)
        try:
            await broker.ensure_group("t.Order", "modulith-orders")
            await broker.publish("t.Order", b"{}", {"event_type": "t.Order"})
            [(_key, [(message_id, fields)])] = await broker.read(
                "t.Order", consumer="o1", group="modulith-orders", block_ms=50
            )
            await broker.dead_letter("t.Order", message_id.decode(), fields, "modulith-orders")
        finally:
            await broker.close()

    asyncio.run(seed())
    yield redis_key_prefix

    import redis

    client = redis.Redis.from_url(redis_url)
    try:
        for key in client.scan_iter(match=f"{redis_key_prefix}*"):
            client.delete(key)
    finally:
        client.close()


@pytest.mark.integration
def test_broker_dead_letter_lists_a_redis_dead_letter(redis_dead_letter_project):
    result = runner.invoke(app, ["broker", "dead-letter"])

    assert result.exit_code == 0, result.output
    assert "1 dead-lettered message(s):" in result.output
    assert "t.Order  target=t.Order  group=modulith-orders  attempts=1  last_error=None" in (
        result.output
    )


@pytest.mark.integration
def test_broker_dead_letter_retry_all_resubmits_a_redis_dead_letter(
    redis_dead_letter_project, redis_url
):
    from modulith.adapters.redis_broker import RedisStreamsBroker

    result = runner.invoke(app, ["broker", "dead-letter", "--retry-all"])

    assert result.exit_code == 0, result.output
    assert "resubmitted 1 dead-lettered message(s)" in result.output

    async def remaining() -> int:
        broker = RedisStreamsBroker(url=redis_url, stream_prefix=redis_dead_letter_project)
        try:
            return len(await broker.list_dead_letters())
        finally:
            await broker.close()

    assert asyncio.run(remaining()) == 0


# ---------------------------------------------------------------------------
# modulith migrate
# ---------------------------------------------------------------------------


def _packaged_head() -> str:
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    import modulith

    ini = Path(modulith.__file__).parent / "adapters" / "alembic.ini"
    head = ScriptDirectory.from_config(Config(str(ini))).get_current_head()
    assert head is not None
    return head


def _sqlite_tables(db_file: Path) -> set[str]:
    with sqlite3.connect(db_file) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {name for (name,) in rows}


def _sqlite_revision(db_file: Path) -> str:
    with sqlite3.connect(db_file) as conn:
        (revision,) = conn.execute("SELECT version_num FROM modulith_alembic_version").fetchone()
    return str(revision)


def _migrate_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, tool_modulith: str = ""
) -> Path:
    """A project whose pyproject holds ``tool_modulith``, with no MODULITH_* env."""
    for name in list(os.environ):
        if name.startswith("MODULITH_"):
            monkeypatch.delenv(name)
    project = tmp_path / "project"
    project.mkdir()
    (project / "pyproject.toml").write_text(
        f'[project]\nname = "migrateapp"\n\n[tool.modulith]\n{tool_modulith}', encoding="utf-8"
    )
    monkeypatch.chdir(project)
    return project


def test_migrate_applies_the_packaged_chain_to_the_configured_outbox_url(
    tmp_path, monkeypatch
) -> None:
    db_file = tmp_path / "durable.db"
    _migrate_project(
        tmp_path, monkeypatch, tool_modulith=f'outbox_url = "sqlite+aiosqlite:///{db_file}"\n'
    )

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 0, result.output
    assert f"migrated sqlite:///{db_file} to head" in result.output
    assert {
        "event_publications",
        "event_publications_archive",
        "broker_subscription",
        "broker_message",
    } <= _sqlite_tables(db_file)
    assert _sqlite_revision(db_file) == _packaged_head()


def test_migrate_moves_a_revision_tracked_in_alembic_version_to_its_own_table(
    tmp_path, monkeypatch
) -> None:
    db_file = tmp_path / "legacy.db"
    _migrate_project(tmp_path, monkeypatch)
    assert runner.invoke(app, ["migrate", "--url", f"sqlite:///{db_file}"]).exit_code == 0
    with sqlite3.connect(db_file) as conn:
        conn.execute("DROP TABLE IF EXISTS modulith_alembic_version")
        conn.execute("DROP TABLE IF EXISTS alembic_version")
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
        conn.execute("INSERT INTO alembic_version VALUES (?)", (_packaged_head(),))

    result = runner.invoke(app, ["migrate", "--url", f"sqlite:///{db_file}"])

    assert result.exit_code == 0, result.output
    assert "alembic_version" not in _sqlite_tables(db_file)
    assert _sqlite_revision(db_file) == _packaged_head()


def test_migrate_never_bootstraps_the_application_under_strict_boundaries(
    tmp_path, monkeypatch
) -> None:
    db_file = tmp_path / "strict.db"
    project = _migrate_project(
        tmp_path,
        monkeypatch,
        tool_modulith=(
            'package = "migrateapp"\n'
            "strict_boundaries = true\n"
            f'outbox_url = "sqlite+aiosqlite:///{db_file}"\n'
        ),
    )
    package = project / "migrateapp"
    (package / "orders").mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "orders" / "__init__.py").write_text(
        'raise RuntimeError("bootstrap imported the application")\n', encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(project))

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 0, result.output
    assert "migrateapp.orders" not in sys.modules
    assert _sqlite_revision(db_file) == _packaged_head()


def test_migrate_url_option_wins_over_the_configured_outbox_url(tmp_path, monkeypatch) -> None:
    unreachable = tmp_path / "missing-dir" / "configured.db"
    target = tmp_path / "target.db"
    _migrate_project(
        tmp_path, monkeypatch, tool_modulith=f'outbox_url = "sqlite+aiosqlite:///{unreachable}"\n'
    )

    result = runner.invoke(app, ["migrate", "--url", f"sqlite:///{target}"])

    assert result.exit_code == 0, result.output
    assert f"migrated sqlite:///{target} to head" in result.output
    assert _sqlite_revision(target) == _packaged_head()
    assert not unreachable.parent.exists()


def test_migrate_url_option_accepts_an_async_driver_url(tmp_path, monkeypatch) -> None:
    target = tmp_path / "async.db"
    _migrate_project(tmp_path, monkeypatch)

    result = runner.invoke(app, ["migrate", "--url", f"sqlite+aiosqlite:///{target}"])

    assert result.exit_code == 0, result.output
    assert f"migrated sqlite:///{target} to head" in result.output


def test_migrate_reports_the_missing_extra_when_alembic_is_not_installed(
    tmp_path, monkeypatch
) -> None:
    _migrate_project(tmp_path, monkeypatch)
    monkeypatch.setitem(sys.modules, "alembic", None)

    result = runner.invoke(app, ["migrate", "--url", f"sqlite:///{tmp_path / 'x.db'}"])

    assert result.exit_code == 1, result.output
    assert "modupy[postgres]" in result.output
    assert "modupy[database]" in result.output


def test_migrate_url_option_needs_no_readable_configuration(tmp_path, monkeypatch) -> None:
    target = tmp_path / "target.db"
    _migrate_project(tmp_path, monkeypatch, tool_modulith='topology = "nonsense"\n')

    result = runner.invoke(app, ["migrate", "--url", f"sqlite:///{target}"])

    assert result.exit_code == 0, result.output
    assert _sqlite_revision(target) == _packaged_head()


def test_migrate_revision_argument_stops_at_that_revision(tmp_path, monkeypatch) -> None:
    db_file = tmp_path / "stepwise.db"
    _migrate_project(tmp_path, monkeypatch)

    result = runner.invoke(app, ["migrate", "0001_initial", "--url", f"sqlite:///{db_file}"])

    assert result.exit_code == 0, result.output
    assert f"migrated sqlite:///{db_file} to 0001_initial" in result.output
    assert _sqlite_revision(db_file) == "0001_initial"
    assert "broker_message" not in _sqlite_tables(db_file)


def test_migrate_without_a_url_names_both_accepted_forms(tmp_path, monkeypatch) -> None:
    _migrate_project(tmp_path, monkeypatch)

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 1, result.output
    assert "--url <sqlalchemy url>" in result.output
    assert "[tool.modulith] outbox_url" in result.output
    assert "MODULITH_OUTBOX_URL" in result.output


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ("postgresql+asyncpg://u:p@h:5432/db", "postgresql+psycopg://u:p@h:5432/db"),
        ("sqlite+aiosqlite:///rel.db", "sqlite:///rel.db"),
        ("sqlite+aiosqlite:////abs/x.db", "sqlite:////abs/x.db"),
        ("mysql+aiomysql://u:p@h/db?charset=utf8mb4", "mysql+pymysql://u:p@h/db?charset=utf8mb4"),
        ("postgresql+psycopg://u:p@h/db", "postgresql+psycopg://u:p@h/db"),
        ("sqlite:///plain.db", "sqlite:///plain.db"),
    ],
)
def test_migration_url_swaps_the_async_driver_for_the_sync_one(configured, expected) -> None:
    from modulith.cli import _migration_url

    assert _migration_url(configured) == expected


def test_migrate_reads_the_outbox_url_environment_variable(tmp_path, monkeypatch) -> None:
    db_file = tmp_path / "env.db"
    _migrate_project(tmp_path, monkeypatch)
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{db_file}")

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 0, result.output
    assert _sqlite_revision(db_file) == _packaged_head()


def test_masked_url_hides_only_the_password() -> None:
    from modulith.cli import _masked_url

    masked = _masked_url("postgresql+psycopg://user:s3cret@db.example/app?sslmode=require")

    assert masked == "postgresql+psycopg://user:***@db.example/app?sslmode=require"


def test_masked_url_hides_query_parameter_password() -> None:
    from modulith.cli import _masked_url

    masked = _masked_url("postgresql+psycopg://user@db.example/app?password=secret&sslmode=require")

    assert masked == "postgresql+psycopg://user@db.example/app?password=***&sslmode=require"
    assert "secret" not in masked
    assert "sslmode=require" in masked


def test_masked_url_hides_secret_query_parameters() -> None:
    from modulith.cli import _masked_url

    test_cases = [
        ("postgresql://host/db?token=abc123", "token=***"),
        ("postgresql://host/db?secret=xyz", "secret=***"),
        ("postgresql://host/db?apikey=key123", "apikey=***"),
        ("postgresql://host/db?api_key=key456", "api_key=***"),
        ("postgresql://host/db?passwd=pass123", "passwd=***"),
        ("postgresql://host/db?pwd=pass456", "pwd=***"),
        ("postgresql://host/db?PASSWORD=upper", "PASSWORD=***"),
        ("postgresql://host/db?Token=mixed", "Token=***"),
        ("postgresql://host/db?sslpassword=keypass", "sslpassword=***"),
    ]

    for url, expected_hidden in test_cases:
        masked = _masked_url(url)
        assert expected_hidden in masked, f"Failed for {url}"


def test_masked_url_keeps_non_secret_query_parameters_visible() -> None:
    from modulith.cli import _masked_url

    masked = _masked_url("postgresql://user@host/db?password=secret&sslmode=require&pool_size=10")

    assert "password=***" in masked
    assert "sslmode=require" in masked
    assert "pool_size=10" in masked
    assert "secret" not in masked
    assert "require" in masked
    assert "10" in masked


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "sqlite+aiosqlite:///relative.db?password=secret",
            "sqlite+aiosqlite:///relative.db?password=***",
        ),
        ("sqlite+aiosqlite:///relative.db", "sqlite+aiosqlite:///relative.db"),
        (
            "sqlite+aiosqlite:////abs/path.db?password=secret",
            "sqlite+aiosqlite:////abs/path.db?password=***",
        ),
        ("sqlite+aiosqlite://", "sqlite+aiosqlite://"),
        ("sqlite+aiosqlite://?password=secret", "sqlite+aiosqlite://?password=***"),
        (
            "postgresql+psycopg://user:s3cret@db.example:5432/app?token=abc&sslmode=require",
            "postgresql+psycopg://user:***@db.example:5432/app?sslmode=require&token=***",
        ),
        (
            "redis://user:s3cret@cache.example:6379/0?password=secret",
            "redis://user:***@cache.example:6379/0?password=***",
        ),
    ],
)
def test_masked_url_keeps_the_url_form_and_hides_secrets(url: str, expected: str) -> None:
    from modulith.cli import _masked_url

    assert _masked_url(url) == expected


def test_migrate_failure_reports_an_error_without_leaking_the_password(
    tmp_path, monkeypatch
) -> None:
    # The error text under test is psycopg's refused connection; no sqlite URL produces it.
    pytest.importorskip("psycopg", reason="needs the modupy[postgres] extra (psycopg)")
    _migrate_project(
        tmp_path,
        monkeypatch,
        tool_modulith='outbox_url = "postgresql+asyncpg://user:s3cret@127.0.0.1:1/app"\n',
    )

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 1, result.output
    assert result.output.startswith("error: migration failed")
    assert "s3cret" not in result.output


def test_migrate_schema_option_reaches_the_migration_environment(tmp_path, monkeypatch) -> None:
    db_file = tmp_path / "schema.db"
    _migrate_project(tmp_path, monkeypatch)

    result = runner.invoke(
        app, ["migrate", "--url", f"sqlite:///{db_file}", "--schema", "not a schema"]
    )

    assert result.exit_code == 1, result.output
    assert "schema must be a valid unquoted SQL identifier" in result.output
    assert not db_file.exists()


def test_migrate_schema_option_is_ignored_on_sqlite_as_the_migrations_do(
    tmp_path, monkeypatch, caplog
) -> None:
    db_file = tmp_path / "ignored-schema.db"
    _migrate_project(tmp_path, monkeypatch)

    with caplog.at_level(logging.WARNING, logger="modulith.adapters.migrations.env"):
        result = runner.invoke(
            app, ["migrate", "--url", f"sqlite:///{db_file}", "--schema", "orders_outbox"]
        )

    assert result.exit_code == 0, result.output
    assert "only supported on PostgreSQL" in caplog.text
    assert _sqlite_revision(db_file) == _packaged_head()


def _collect_all_commands(
    cmd_obj: Any, prefix: list[str] | None = None
) -> list[tuple[list[str], Any]]:
    """Recursively collect all commands from the typer app and subcommands."""
    if prefix is None:
        prefix = []
    commands = []

    # Get all commands from the current object
    if hasattr(cmd_obj, "registered_commands"):
        for cmd in cmd_obj.registered_commands:
            commands.append(([*prefix, cmd.name], cmd))

    # Get subcommand groups
    if hasattr(cmd_obj, "registered_groups"):
        for group in cmd_obj.registered_groups:
            # Recursively collect from the group
            sub_commands = _collect_all_commands(group, [*prefix, group.name])
            commands.extend(sub_commands)

    return commands


@pytest.mark.parametrize(
    "command_path",
    [
        ["run"],
        ["dev"],
        ["verify"],
        ["docs"],
        ["extract"],
        ["audit"],
        ["k8s-manifest"],
        ["doctor"],
        ["openapi"],
        ["migrate"],
        ["info"],
        ["outbox", "status"],
        ["outbox", "retry"],
        ["outbox", "purge"],
        ["outbox", "dead-letter"],
        ["outbox", "failing"],
        ["broker", "drop-group"],
        ["broker", "dead-letter"],
    ],
)
def test_cli_help_output_has_no_raw_double_backticks(command_path: list[str]) -> None:
    """Verify that --help output contains no raw double backticks.

    Rich markup interprets double backticks as special formatting; raw backticks
    break the help text display. All backticks must be escaped or removed.
    """
    result = runner.invoke(app, [*command_path, "--help"])
    assert result.exit_code == 0, (
        f"Failed to get help for {' '.join(command_path)}: {result.output}"
    )
    assert "``" not in result.output, (
        f"Raw double backticks found in {' '.join(command_path)} help output. "
        f"Backticks must be escaped or removed for rich markup compatibility."
    )


def test_migrate_help_contains_literal_tool_modulith() -> None:
    """Verify that migrate --help contains the literal text [tool.modulith]."""
    result = runner.invoke(app, ["migrate", "--help"])
    assert result.exit_code == 0, result.output
    assert "[tool.modulith]" in result.output, (
        "migrate --help must contain literal '[tool.modulith]' in the help text"
    )


@pytest.mark.parametrize(
    ("command", "default_note"),
    [
        ("run", "(default: [tool.modulith] topology, else single)"),
        ("dev", "(default: [tool.modulith] topology, else single)"),
        ("run", "(default: [tool.modulith] worker_port_base, else 9001)"),
        ("dev", "(default: [tool.modulith] worker_port_base, else 9001)"),
        ("migrate", "(default: [tool.modulith] outbox_url)"),
    ],
)
def test_option_help_names_the_pyproject_key_it_defaults_to(
    command: str, default_note: str
) -> None:
    """rich deletes a bare ``[tool.modulith]`` as a style tag, and a raw
    ``\\[`` escapes the backslash rather than the bracket, so either spelling
    drops the table name from the option's help."""
    result = runner.invoke(app, [command, "--help"], env={"COLUMNS": "400"})

    assert result.exit_code == 0, result.output
    assert default_note in result.output
