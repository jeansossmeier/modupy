"""In-repo regression test for the REAL ``shop.main`` durable lifespan.

Unlike ``tests/test_demo_app_durable.py`` (which hand-wires a corrected
engine/store/outbox stack to prove the durable *path* works), this drives
``shop.main.app``'s own ``lifespan`` — the exact code path a real deployment
runs — through ``TestClient`` acting as the ASGI server, via the documented
env vars (``MODULITH_OUTBOX``, ``MODULITH_DB_URL``, ``MODULITH_DEMO_OTEL``,
``MODULITH_DEMO_SERIALIZER``). It is the regression test for the #62 lifespan
fix: entering/exiting the lifespan twice on the same app must leave the
adapter's module globals (``_store_stack``, ``_hook_installed``) exactly as
clean the second time as the first.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from modulith.adapters import postgres_outbox
from modulith.adapters.postgres_outbox import EventPublicationRow
from modulith.builtin import outbox
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer

DEMO_ROOT = Path(__file__).resolve().parent.parent / "examples" / "demo_app"


@pytest.fixture
def demo_shop(monkeypatch: pytest.MonkeyPatch):
    """Bootstrap the on-disk ``shop`` package with clean runtime/outbox state.

    Mirrors ``tests/test_demo_app_durable.py``'s ``demo_shop`` fixture: reset
    runtime/manifest/outbox/adapter globals, make ``shop`` importable, and
    purge ``shop.*`` from ``sys.modules`` on teardown so the next test gets a
    fresh import (env-var-driven module state — e.g. ``shop.telemetry``'s
    ``_initialized`` flag — must not leak between tests).
    """
    from modulith import manifest as _manifest_mod

    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()

    monkeypatch.syspath_prepend(str(DEMO_ROOT))

    yield

    for name in list(sys.modules):
        if name == "shop" or name.startswith("shop."):
            del sys.modules[name]
    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()
    outbox._reset_for_testing()
    postgres_outbox._reset_for_testing()


def _poll(coro_factory, attempts: int = 50, delay: float = 0.05):
    """Run ``coro_factory()`` in a fresh loop up to ``attempts`` times.

    Dispatch after commit is scheduled as a task, not awaited by the request
    handler, so ``completed_at``/payload visibility briefly lags the HTTP
    response. Each attempt opens its own session (NullPool — a fresh
    connection per checkout), so polling from a throwaway ``asyncio.run()``
    loop here is safe even though the app's own engine lives on the
    TestClient portal thread's loop.
    """

    async def _run():
        for _ in range(attempts):
            result = await coro_factory()
            if result is not None:
                return result
            await asyncio.sleep(delay)
        return None

    return asyncio.run(_run())


def test_durable_lifespan_persists_order_and_publication_row(
    demo_shop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Drives the real lifespan through TestClient; asserts direct DB
    evidence (Order row + a completed EventPublicationRow), then a second
    enter/exit cycle proves the #62 teardown fix leaves clean module state."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'demo.db'}"
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_DB_URL", db_url)

    import shop.main

    with TestClient(shop.main.app) as client:
        resp = client.post("/orders", json={"customer_id": "c-lifespan", "total": 12.5})
        assert resp.status_code == 200
        order_id = resp.json()["order_id"]

        sessionmaker = shop.main.app.state.sessionmaker
        assert sessionmaker is not None

        async def _check():
            from shop.orders.models import Order

            async with sessionmaker() as s:
                order_row = (
                    await s.execute(select(Order).where(Order.id == order_id))
                ).scalar_one_or_none()
                if order_row is None:
                    return None
                pub_rows = (await s.execute(select(EventPublicationRow))).scalars().all()
                matching = [
                    r
                    for r in pub_rows
                    if JsonEventSerializer().deserialize(r.payload, r.event_type).order_id
                    == order_id
                ]
                if matching and all(r.completed_at is not None for r in matching):
                    return order_row, matching
            return None

        result = _poll(_check)
        assert result is not None, "Order row + completed EventPublicationRow never appeared"
        order_row, matching_rows = result
        assert order_row.customer_id == "c-lifespan"
        assert len(matching_rows) == 1

    # #62 regression: after `with` exits, the store must be fully torn down —
    # no leaked stack entry, no leaked after-commit hook.
    assert postgres_outbox._store_stack == []
    assert postgres_outbox._hook_installed is False

    # Second enter/exit cycle on the SAME app: clean re-entry must not hit
    # the "another store is already active" warning path, and must leave the
    # same clean state behind.
    with TestClient(shop.main.app) as client:
        resp = client.post("/orders", json={"customer_id": "c-lifespan-2", "total": 3.0})
        assert resp.status_code == 200

    assert postgres_outbox._store_stack == []
    assert postgres_outbox._hook_installed is False


def test_durable_lifespan_custom_serializer_envelope(
    demo_shop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``MODULITH_DEMO_SERIALIZER=custom`` stores the versioned JSON envelope."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'demo.db'}"
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_DB_URL", db_url)
    monkeypatch.setenv("MODULITH_DEMO_SERIALIZER", "custom")

    import shop.main

    with TestClient(shop.main.app) as client:
        resp = client.post("/orders", json={"customer_id": "c-envelope", "total": 1.0})
        assert resp.status_code == 200
        order_id = resp.json()["order_id"]

        sessionmaker = shop.main.app.state.sessionmaker

        async def _find_payload():
            async with sessionmaker() as s:
                pub_rows = (await s.execute(select(EventPublicationRow))).scalars().all()
                for row in pub_rows:
                    envelope = json.loads(row.payload)
                    if envelope.get("body", {}).get("order_id") == order_id:
                        return row.payload
            return None

        payload = _poll(_find_payload)

    assert payload is not None, "no outbox row matched the versioned envelope shape"
    envelope = json.loads(payload)
    assert envelope == {
        "v": 1,
        "body": {"customer_id": "c-envelope", "order_id": order_id, "total": 1.0},
    }


def test_durable_lifespan_with_otel_enabled(
    demo_shop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``MODULITH_DEMO_OTEL=1`` initializes the console tracer during lifespan."""
    pytest.importorskip("opentelemetry")

    db_url = f"sqlite+aiosqlite:///{tmp_path / 'demo.db'}"
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_DB_URL", db_url)
    monkeypatch.setenv("MODULITH_DEMO_OTEL", "1")

    import shop.main
    import shop.telemetry

    assert shop.telemetry._initialized is False

    with TestClient(shop.main.app) as client:
        resp = client.post("/orders", json={"customer_id": "c-otel", "total": 2.0})
        assert resp.status_code == 200

    assert shop.telemetry._initialized is True


def test_durable_post_order_reports_failed_commit_as_error(
    demo_shop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A commit that fails must not be acknowledged with 200.

    Another connection holds an EXCLUSIVE lock on the SQLite file across the
    request, so the order + outbox INSERTs fail with ``database is locked``
    once the 0.2 s busy timeout runs out. The client must see an error status
    and no order row may exist; once the lock is gone the next request must
    succeed, proving the session binding was released on the failure path.
    """
    db_path = tmp_path / "demo.db"
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_DB_URL", f"sqlite+aiosqlite:///{db_path}?timeout=0.2")

    import shop.main

    with TestClient(shop.main.app, raise_server_exceptions=False) as client:
        lock = sqlite3.connect(db_path, timeout=0, isolation_level=None)
        try:
            lock.execute("BEGIN EXCLUSIVE")
            resp = client.post("/orders", json={"customer_id": "c-locked", "total": 1.0})
            lock.execute("ROLLBACK")
        finally:
            lock.close()

        assert resp.status_code == 500, resp.text
        with sqlite3.connect(db_path) as con:
            rows = con.execute(
                "SELECT count(*) FROM shop_orders WHERE customer_id = 'c-locked'"
            ).fetchone()
        assert rows == (0,)

        resp = client.post("/orders", json={"customer_id": "c-after", "total": 2.0})
        assert resp.status_code == 200
