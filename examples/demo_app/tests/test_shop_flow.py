from pathlib import Path

# aiosqlite, sqlalchemy.ext.asyncio and sqlalchemy.orm stay at module scope on
# purpose: modulith_app drops every module first imported during a test, and
# SQLAlchemy cannot be re-imported once its compiled extensions are dropped.
import aiosqlite  # noqa: F401
import sqlalchemy.ext.asyncio
import sqlalchemy.orm  # noqa: F401
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp, Scenario
from pytest import MonkeyPatch, mark

ORDER = {"order_id": "o-1", "customer_id": "alice", "total": 19.99}


def create_schema(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{tmp_path / 'shop.db'}")
    from shop.schema import create_tables

    create_tables()


def test_an_order_flows_through_all_three_modules(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    create_schema(monkeypatch, tmp_path)
    from shop.main import app

    with TestClient(app) as client:
        created = client.post("/orders", json=ORDER)
        order = client.get("/orders/o-1")
        reservation = client.get("/inventory/reservations/o-1")
        notification = client.get("/notifications/o-1")

    assert created.status_code == 200
    assert created.json() == {"order_id": "o-1"}
    assert order.json() == ORDER
    assert reservation.json() == {"order_id": "o-1", "reserved": True}
    assert notification.json() == {"order_id": "o-1", "notified": True}


def test_each_event_is_published_once(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    create_schema(monkeypatch, tmp_path)
    from shop.contracts.events import OrderPlaced, StockReserved
    from shop.main import app

    with TestClient(app) as client:
        client.post("/orders", json=ORDER)

    assert modulith_app.published_events_of_type(OrderPlaced) == [OrderPlaced(**ORDER)]
    assert modulith_app.published_events_of_type(StockReserved) == [StockReserved(order_id="o-1")]


def test_a_duplicate_order_id_is_rejected_without_a_second_event(
    modulith_app: ModulithTestApp, monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    create_schema(monkeypatch, tmp_path)
    from shop.contracts.events import OrderPlaced
    from shop.main import app

    with TestClient(app) as client:
        first = client.post("/orders", json=ORDER)
        second = client.post("/orders", json=ORDER)

    assert first.status_code == 200
    assert second.status_code == 409
    assert len(modulith_app.published_events_of_type(OrderPlaced)) == 1


def test_a_redelivered_order_reserves_stock_once(
    scenario: Scenario,
    modulith_app: ModulithTestApp,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    create_schema(monkeypatch, tmp_path)
    from shop.contracts.events import OrderPlaced, StockReserved

    placed = OrderPlaced(**ORDER)

    scenario.publish(placed).expect_event(StockReserved).within(seconds=2)
    scenario.publish(placed).expect_event(OrderPlaced).within(seconds=2)

    assert modulith_app.published_events_of_type(StockReserved) == [StockReserved(order_id="o-1")]


@mark.parametrize(
    ("module", "route"),
    [
        ("orders", "/orders/{order_id}"),
        ("inventory", "/inventory/reservations/{order_id}"),
        ("notifications", "/notifications/{order_id}"),
    ],
)
def test_a_worker_mounts_only_its_own_router(
    modulith_app: ModulithTestApp,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    module: str,
    route: str,
) -> None:
    from modulith._worker import create_app

    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{tmp_path / 'shop.db'}")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.setenv("MODULITH_MODULE", module)
    monkeypatch.setenv("MODULITH_APP_PACKAGE", "shop")

    paths = create_app().openapi()["paths"]

    assert route in paths
    mounted = {path.split("/")[1] for path in paths}
    assert mounted & {"orders", "inventory", "notifications"} == {module}
