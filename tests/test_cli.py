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
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner

from modulith import EventPublication
from modulith.builtin import outbox
from modulith.cli import _parse_duration, app
from modulith.serializers import JsonEventSerializer

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
    asyncio.set_event_loop(asyncio.new_event_loop())
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
    monkeypatch.setenv("MODULITH_BROKER_URL", "postgresql+asyncpg://db/prod")
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

    assert _build_worker_env(spec)["MODULITH_BROKER_URL"] == "postgresql+asyncpg://db/prod"


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
    monkeypatch.setenv("MODULITH_OUTBOX_URL", "postgresql+asyncpg://db/prod")

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
    assert not [m for m in warnings if "modulith-orders" in m]


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
