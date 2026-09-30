import time
from collections.abc import Callable
from typing import Any

from fastapi.testclient import TestClient
from modulith.builtin import outbox
from modulith.testing import ModulithTestApp, Scenario

from .conftest import run_python


def eventually(read: Callable[[], Any], done: Callable[[Any], bool], seconds: float = 20) -> Any:
    deadline = time.monotonic() + seconds
    while True:
        value = read()
        if done(value) or time.monotonic() > deadline:
            return value
        time.sleep(0.1)


def order_body(order_id: str, quantity: int, card_token: str) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "customer_id": "alice",
        "sku": "SKU-MUG",
        "quantity": quantity,
        "card_token": card_token,
        "country": "DE",
    }


def list_mug(stock: int) -> None:
    listed = run_python("-m", "marketplace.catalog", "SKU-MUG", "Stoneware mug", "1200", str(stock))
    assert listed.returncode == 0, listed.stderr


def test_a_paid_order_is_confirmed_and_its_stock_stays_reserved(
    database: str, modulith_app: ModulithTestApp
) -> None:
    list_mug(10)

    from marketplace.main import app

    with TestClient(app) as client:
        assert client.post("/orders", json=order_body("o-100", 2, "tok_visa")).status_code == 200
        confirmed = eventually(
            lambda: client.get("/orders/o-100").json(),
            lambda body: body["status"] in ("confirmed", "cancelled"),
        )
        on_hand = client.get("/inventory/SKU-MUG").json()

    assert confirmed == {"order_id": "o-100", "status": "confirmed", "reason": None}
    assert on_hand == {"sku": "SKU-MUG", "on_hand": 8}


def test_a_declined_order_is_cancelled_only_after_its_stock_is_back_on_hand(
    database: str, modulith_app: ModulithTestApp
) -> None:
    list_mug(10)

    from marketplace.main import app

    with TestClient(app) as client:
        assert (
            client.post("/orders", json=order_body("o-200", 2, "tok_declined")).status_code == 200
        )
        cancelled = eventually(
            lambda: client.get("/orders/o-200").json(),
            lambda body: body["status"] in ("confirmed", "cancelled"),
        )
        on_hand = client.get("/inventory/SKU-MUG").json()

    assert cancelled == {"order_id": "o-200", "status": "cancelled", "reason": "payment declined"}
    assert on_hand == {"sku": "SKU-MUG", "on_hand": 10}


def test_a_redelivered_payment_request_captures_once(
    database: str, modulith_app: ModulithTestApp, scenario: Scenario
) -> None:
    list_mug(10)

    from marketplace.contracts import PaymentCaptured, PaymentRequested
    from marketplace.main import app

    with TestClient(app) as client:
        client.post("/orders", json=order_body("o-100", 2, "tok_visa"))
        eventually(
            lambda: client.get("/orders/o-100").json(),
            lambda body: body["status"] in ("confirmed", "cancelled"),
        )
        redelivered = PaymentRequested(order_id="o-100", amount_cents=2400, card_token="tok_visa")

        assert client.portal is not None
        portal = client.portal
        scenario.publish(redelivered).expect_event(PaymentRequested).within(seconds=5)
        eventually(
            lambda: portal.call(outbox.status),
            lambda counts: counts["incomplete"] == 0,
        )

    assert len(modulith_app.published_events_of_type(PaymentCaptured)) == 1


def test_an_order_beyond_stock_is_cancelled_as_out_of_stock(
    database: str, modulith_app: ModulithTestApp
) -> None:
    listed = run_python("-m", "marketplace.catalog", "SKU-MUG", "Stoneware mug", "1200", "1")
    assert listed.returncode == 0, listed.stderr

    from marketplace.main import app

    with TestClient(app) as client:
        placed = client.post(
            "/orders",
            json={
                "order_id": "o-200",
                "customer_id": "alice",
                "sku": "SKU-MUG",
                "quantity": 2,
                "card_token": "tok_visa",
                "country": "DE",
            },
        )
        cancelled = eventually(
            lambda: client.get("/orders/o-200").json(),
            lambda body: body["status"] != "placed",
        )

    assert placed.status_code == 200
    assert placed.json() == {"order_id": "o-200", "status": "placed"}
    assert cancelled == {"order_id": "o-200", "status": "cancelled", "reason": "out of stock"}
