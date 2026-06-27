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
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner

from modulith import EventPublication
from modulith.builtin import outbox
from modulith.cli import _parse_duration, app
from modulith.serializers import JsonEventSerializer

runner = CliRunner()


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
    """Keep outbox module state isolated per test (make_fake_app only resets
    the runtime singleton, not the outbox plugin's module globals).

    The outbox CLI commands call ``asyncio.run()``, which leaves the thread
    with no current event loop. In production each command is its own
    process, so that is harmless — but under pytest it pollutes the shared
    process, breaking later (sync) tests that rely on the implicit current
    loop via ``asyncio.get_event_loop()``. Re-establish a fresh loop on
    teardown so this module's tests stay self-contained.
    """
    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()
    asyncio.set_event_loop(asyncio.new_event_loop())


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

    def fake_execvp(file: str, args: list[str]) -> None:
        captured["file"] = file
        captured["args"] = args

    monkeypatch.setattr(os, "execvp", fake_execvp)

    result = runner.invoke(app, ["run", "myapp:app"])

    assert result.exit_code == 0, result.output
    assert captured["file"] == "uvicorn"
    assert "--reload" not in captured["args"]


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
# regression: --version, stderr streams, dead-letter flag exclusivity (audit)
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
