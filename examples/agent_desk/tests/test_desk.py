import importlib
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, TypeVar

# These imports stay at module scope on purpose: modulith_app drops every module
# first imported during a test, and SQLAlchemy cannot be re-imported once its
# compiled extensions are dropped. LangGraph and LangChain load compiled
# extensions too.
import aiosqlite  # noqa: F401
import alembic.command  # noqa: F401
import langchain_core.language_models.chat_models  # noqa: F401
import langgraph.graph  # noqa: F401
import pytest
import sqlalchemy.ext.asyncio
import sqlalchemy.orm  # noqa: F401
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp
from pytest import MonkeyPatch
from typer.testing import CliRunner

T = TypeVar("T")
TICKET = {"ticket_id": "t-1", "subject": "Site is down", "body": "Every page returns a 500."}
TRIAGE_ERRORS = (
    "SELECT last_error FROM event_publications "
    "WHERE listener = 'desk.agent.triage' AND last_error IS NOT NULL"
)
# The outbox marks the row as it hands the event to the listener; a first
# attempt that ended has an error.
TRIAGE_RUNNING = (
    "SELECT id FROM event_publications "
    "WHERE listener = 'desk.agent.triage' AND dispatch_started AND last_error IS NULL"
)


def prepare_database(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    database = tmp_path / "desk.db"
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{database}")
    from modulith.cli import app as cli

    from desk.schema import create_tables

    assert CliRunner().invoke(cli, ["migrate"]).exit_code == 0
    create_tables()
    return database


def query(database: Path, sql: str) -> list[Any]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(sql).fetchall()


def eventually(read: Callable[[], T], done: Callable[[T], bool]) -> T:
    """The first value ``read`` returns that is ``done``, or the last after 10s."""
    deadline = time.monotonic() + 10
    value = read()
    while not done(value) and time.monotonic() < deadline:
        time.sleep(0.05)
        value = read()
    return value


def test_the_agent_triages_a_new_ticket_and_reports_back(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    prepare_database(monkeypatch, tmp_path)
    from desk.main import app

    with TestClient(app) as client:
        opened = client.post("/tickets", json=TICKET)
        ticket = eventually(
            lambda: client.get("/tickets/t-1").json(), lambda body: body["reply"] is not None
        )
        activity = eventually(
            lambda: client.get("/activity/t-1").json(), lambda body: len(body["activity"]) == 2
        )

    assert opened.json() == {"ticket_id": "t-1"}
    assert ticket == {
        "ticket_id": "t-1",
        "category": "outage",
        "priority": "high",
        "reply": "Thanks for writing in about 'Site is down'; we are on it.",
    }
    assert activity["activity"] == ["opened", "triaged as outage/high"]


def test_a_slow_model_still_triages_while_the_agent_holds_its_session(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DESK_AGENT_FAULT", "slow")
    monkeypatch.setenv("DESK_AGENT_SLOW_SECONDS", "0.5")
    monkeypatch.setenv("DESK_AGENT_HOLD_SESSION", "1")
    database = prepare_database(monkeypatch, tmp_path)
    from desk.main import app

    with TestClient(app) as client:
        started = time.monotonic()
        client.post("/tickets", json=TICKET)
        ticket = eventually(
            lambda: client.get("/tickets/t-1").json(), lambda body: body["reply"] is not None
        )
        elapsed = time.monotonic() - started

    # Two model calls, each half a second late.
    assert elapsed >= 1.0
    assert ticket["category"] == "outage"
    assert query(database, TRIAGE_ERRORS) == []


def test_a_hung_model_call_is_cancelled_while_the_api_keeps_answering(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DESK_AGENT_FAULT", "hang")
    monkeypatch.setenv("DESK_AGENT_TIMEOUT", "2")
    database = prepare_database(monkeypatch, tmp_path)
    from desk.main import app

    with TestClient(app) as client:
        client.post("/tickets", json=TICKET)
        running = eventually(lambda: query(database, TRIAGE_RUNNING), bool)
        ticket = client.get("/tickets/t-1")
        still_running = query(database, TRIAGE_RUNNING)
        errors = eventually(lambda: query(database, TRIAGE_ERRORS), bool)

    assert running, "the agent never started on the ticket"
    assert still_running == running, "the model call ended before the API answered"
    assert ticket.status_code == 200
    assert ticket.json()["reply"] is None
    assert len(errors) == 1
    assert "did not finish within 2.0s" in errors[0][0]


def test_a_redelivered_ticket_is_not_triaged_again(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    database = prepare_database(monkeypatch, tmp_path)
    from desk.agent import triage
    from desk.contracts import TicketOpened
    from desk.main import app

    with TestClient(app) as client:
        client.post("/tickets", json=TICKET)
        eventually(
            lambda: client.get("/tickets/t-1").json(), lambda body: body["reply"] is not None
        )
        assert client.portal is not None
        # Delivery is at least once: the outbox may hand the listener the event again.
        client.portal.call(triage, TicketOpened(**TICKET))

    assert query(database, "SELECT ticket_id, category FROM agent_triage") == [("t-1", "outage")]


@pytest.mark.parametrize("fault", ["Hang", "hang ", "crsh"])
def test_an_unknown_fault_stops_the_agent_from_importing(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, fault: str
) -> None:
    monkeypatch.setenv("DESK_AGENT_FAULT", fault)

    with pytest.raises(ValueError, match="DESK_AGENT_FAULT must be empty or one of"):
        importlib.import_module("desk.agent.model")
