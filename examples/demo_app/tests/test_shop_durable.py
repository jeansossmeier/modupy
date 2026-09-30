import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any

# aiosqlite, alembic.command, sqlalchemy.ext.asyncio and sqlalchemy.orm stay at
# module scope on purpose: modulith_app drops every module first imported
# during a test. SQLAlchemy cannot be re-imported once its compiled extensions
# are dropped, and the outbox's after-commit hook binds to the Session class of
# its first import.
import aiosqlite  # noqa: F401
import alembic.command  # noqa: F401
import sqlalchemy.ext.asyncio
import sqlalchemy.orm  # noqa: F401
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp
from pytest import MonkeyPatch
from typer.testing import CliRunner

ORDER = {"order_id": "o-1", "customer_id": "alice", "total": 19.99}
FAIL_ORDER_INSERTS = """
    CREATE TRIGGER fail_order_inserts BEFORE INSERT ON orders_order
    BEGIN SELECT RAISE(ABORT, 'disk full'); END
"""


def prepare_database(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    database = tmp_path / "shop.db"
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{database}")
    from modulith.cli import app as cli

    from shop.schema import create_tables

    assert CliRunner().invoke(cli, ["migrate"]).exit_code == 0
    create_tables()
    return database


def query(database: Path, sql: str) -> list[Any]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(sql).fetchall()


def execute(database: Path, sql: str) -> None:
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(sql)
        connection.commit()


def wait_until_delivered(database: Path) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        publications = query(database, "SELECT completed_at FROM event_publications")
        if len(publications) == 2 and all(completed for (completed,) in publications):
            return
        time.sleep(0.05)
    raise AssertionError(f"publications not delivered: {publications}")


def test_an_order_and_its_events_commit_together(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    database = prepare_database(monkeypatch, tmp_path)
    from shop.main import app

    with TestClient(app) as client:
        response = client.post("/orders", json=ORDER)
        wait_until_delivered(database)

    assert response.status_code == 200
    assert query(database, "SELECT order_id FROM orders_order") == [("o-1",)]
    assert query(database, "SELECT order_id FROM inventory_reservation") == [("o-1",)]
    assert query(database, "SELECT order_id FROM notifications_notification") == [("o-1",)]
    assert sorted(query(database, "SELECT event_type FROM event_publications")) == [
        ("shop.contracts.events.OrderPlaced",),
        ("shop.contracts.events.StockReserved",),
    ]


def test_a_failed_commit_leaves_nothing_behind_and_the_retry_succeeds(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    database = prepare_database(monkeypatch, tmp_path)
    from shop.main import app

    execute(database, FAIL_ORDER_INSERTS)
    with TestClient(app, raise_server_exceptions=False) as client:
        failed = client.post("/orders", json=ORDER)

        assert failed.status_code == 500
        assert query(database, "SELECT order_id FROM orders_order") == []
        assert query(database, "SELECT id FROM event_publications") == []

        execute(database, "DROP TRIGGER fail_order_inserts")
        retried = client.post("/orders", json=ORDER)
        wait_until_delivered(database)

    assert retried.status_code == 200
    assert query(database, "SELECT order_id FROM orders_order") == [("o-1",)]
    assert query(database, "SELECT order_id FROM notifications_notification") == [("o-1",)]
