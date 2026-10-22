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
HAPPY = {"review_id": "r-1", "product": "mug", "rating": 5, "text": "Lovely glaze."}
UNHAPPY = {"review_id": "r-2", "product": "mug", "rating": 2, "text": "The handle broke."}
ABUSIVE = {"review_id": "r-3", "product": "mug", "rating": 1, "text": "Total scam."}
REPLY_ERRORS = (
    "SELECT last_error FROM event_publications "
    "WHERE listener = 'studio.replies.draft_reply' AND last_error IS NOT NULL"
)


def prepare_database(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    database = tmp_path / "studio.db"
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{database}")
    from modulith.cli import app as cli

    from studio.schema import create_tables

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


def settled(client: TestClient, review_id: str) -> dict[str, Any]:
    """The review once moderation and the reply agent have both answered."""
    return eventually(
        lambda: client.get(f"/reviews/{review_id}").json(),
        lambda body: body["status"] != "pending" and body["reply"] is not None,
    )


def test_every_ai_module_answers_for_a_new_review(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        posted = client.post("/reviews", json=HAPPY)
        review = settled(client, "r-1")
        summary = client.get("/summaries/mug")

    assert posted.json() == {"review_id": "r-1"}
    assert review == {
        "review_id": "r-1",
        "product": "mug",
        "rating": 5,
        "status": "published",
        "reply": "Thank you for reviewing the mug!",
    }
    assert summary.json() == {
        "product": "mug",
        "summary": "1 of 1 published reviews of the mug are positive.",
    }


def test_a_review_id_is_taken_once(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        client.post("/reviews", json=HAPPY)
        again = client.post("/reviews", json=UNHAPPY | {"review_id": "r-1"})
        missing = client.get("/reviews/r-9")

    assert again.status_code == 409
    assert missing.status_code == 404


def test_moderation_hides_an_abusive_review_from_the_summary(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        client.post("/reviews", json=ABUSIVE)
        hidden = settled(client, "r-3")
        nothing = client.get("/summaries/mug")
        client.post("/reviews", json=UNHAPPY)
        settled(client, "r-2")
        summary = client.get("/summaries/mug")

    assert hidden["status"] == "hidden"
    assert nothing.status_code == 404
    assert summary.json()["summary"] == "0 of 1 published reviews of the mug are positive."


def test_the_agent_apologizes_to_an_unhappy_reviewer(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        client.post("/reviews", json=UNHAPPY)
        review = settled(client, "r-2")

    assert review["reply"] == "We are sorry the mug let you down; our team will be in touch."


def test_a_hung_reply_is_cancelled_while_the_other_modules_answer(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("STUDIO_REPLIES_FAULT", "hang")
    monkeypatch.setenv("STUDIO_REPLIES_TIMEOUT", "2")
    database = prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        client.post("/reviews", json=HAPPY)
        review = eventually(
            lambda: client.get("/reviews/r-1").json(), lambda body: body["status"] != "pending"
        )
        summary = client.get("/summaries/mug")
        errors = eventually(lambda: query(database, REPLY_ERRORS), bool)

    assert review["status"] == "published"
    assert review["reply"] is None
    assert summary.status_code == 200
    assert len(errors) == 1
    assert "did not finish within 2.0s" in errors[0][0]


def test_a_hung_summary_answers_504_within_its_timeout(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("STUDIO_SUMMARIES_FAULT", "hang")
    monkeypatch.setenv("STUDIO_SUMMARIES_TIMEOUT", "0.5")
    prepare_database(monkeypatch, tmp_path)
    from studio.main import app

    with TestClient(app) as client:
        client.post("/reviews", json=HAPPY)
        settled(client, "r-1")
        started = time.monotonic()
        summary = client.get("/summaries/mug")
        elapsed = time.monotonic() - started
        review = client.get("/reviews/r-1")

    assert summary.status_code == 504
    assert summary.json() == {"detail": "the model did not answer within 0.5s"}
    assert 0.5 <= elapsed < 5
    assert review.status_code == 200


@pytest.mark.parametrize("module", ["moderation", "replies", "summaries"])
@pytest.mark.parametrize("fault", ["Hang", "hang ", "crsh"])
def test_an_unknown_fault_stops_the_model_from_importing(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, module: str, fault: str
) -> None:
    monkeypatch.setenv(f"STUDIO_{module.upper()}_FAULT", fault)

    with pytest.raises(ValueError, match=f"STUDIO_{module.upper()}_FAULT must be empty or one of"):
        importlib.import_module(f"studio.{module}.model")
