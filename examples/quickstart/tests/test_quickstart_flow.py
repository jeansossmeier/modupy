from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp, Scenario


def test_creating_an_order_fans_out_to_payments(
    scenario: Scenario, modulith_app: ModulithTestApp
) -> None:
    from myapp.contracts.events import OrderCreated, PaymentReceived
    from myapp.orders import create_order

    payment = scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert payment.order_id == "ord-1"
    assert OrderCreated(order_id="ord-1") in modulith_app.published_events


def test_payment_marks_the_order_fulfilled(scenario: Scenario) -> None:
    from myapp.contracts.events import PaymentReceived
    from myapp.orders import create_order, is_fulfilled

    assert not is_fulfilled("ord-1")

    scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert is_fulfilled("ord-1")


def test_http_flow_reaches_orders_and_inventory(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)

    created = client.post("/orders", json={"customer_id": "alice"})
    fulfilment = client.get("/orders/ord-1/fulfilment")
    reservation = client.get("/inventory/ord-1")

    assert created.status_code == 200
    assert created.json() == {"order_id": "ord-1"}
    assert fulfilment.status_code == 200
    assert fulfilment.json() == {"order_id": "ord-1", "fulfilled": True}
    assert reservation.status_code == 200
    assert reservation.json() == {"order_id": "ord-1", "reserved": True}


def test_unknown_order_is_404_until_its_events_arrive(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)
    fulfilment, reservation = "/orders/ord-1/fulfilment", "/inventory/ord-1"

    assert client.get(fulfilment).status_code == 404
    assert client.get(reservation).status_code == 404

    client.post("/orders", json={"customer_id": "alice"})

    assert client.get(fulfilment).status_code == 200
    assert client.get(reservation).status_code == 200


def test_an_orders_worker_serves_orders_and_not_inventory(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modulith._worker import create_app

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.setenv("MODULITH_MODULE", "orders")
    monkeypatch.setenv("MODULITH_APP_PACKAGE", "myapp")

    client = TestClient(create_app())

    paths = client.get("/orders/openapi.json").json()["paths"]

    assert "/orders" in paths
    assert "/orders/{order_id}/fulfilment" in paths
    assert not any(path.startswith("/inventory") for path in paths)
    assert client.get("/inventory/ord-1").status_code == 404
