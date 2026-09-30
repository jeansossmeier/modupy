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


def order_body(
    order_id: str, quantity: int, card_token: str, country: str = "DE"
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "customer_id": "alice",
        "sku": "SKU-MUG",
        "quantity": quantity,
        "card_token": card_token,
        "country": country,
    }


def has_status(*statuses: str) -> Callable[[dict[str, Any]], bool]:
    return lambda body: body.get("status") in statuses


def list_mug(stock: int) -> None:
    listed = run_python("-m", "marketplace.catalog", "SKU-MUG", "Stoneware mug", "1200", str(stock))
    assert listed.returncode == 0, listed.stderr


def test_a_paid_order_is_confirmed_shipped_and_its_stock_stays_reserved(
    database: str, modulith_app: ModulithTestApp
) -> None:
    list_mug(10)

    from marketplace.main import app

    with TestClient(app) as client:
        assert client.post("/orders", json=order_body("o-100", 2, "tok_visa")).status_code == 200
        confirmed = eventually(
            lambda: client.get("/orders/o-100").json(), has_status("confirmed", "cancelled")
        )
        shipment = eventually(lambda: client.get("/shipping/o-100").json(), has_status("booked"))
        on_hand = client.get("/inventory/SKU-MUG").json()
        notifications = eventually(
            lambda: client.get("/notifications/o-100").json(), lambda found: len(found) == 3
        )
        totals = eventually(
            lambda: client.get("/reporting/summary").json(), lambda found: found["shipped"] == 1
        )

    assert confirmed == {"order_id": "o-100", "status": "confirmed", "reason": None}
    assert shipment == {
        "order_id": "o-100",
        "status": "booked",
        "carrier": "DHL",
        "tracking_number": "TRK-o-100",
    }
    assert on_hand == {"sku": "SKU-MUG", "on_hand": 8}
    assert notifications == [
        {"kind": "received", "message": "order received"},
        {"kind": "confirmed", "message": "order confirmed"},
        {"kind": "shipped", "message": "shipped with DHL, tracking TRK-o-100"},
    ]
    assert totals == {"confirmed": 1, "cancelled": 0, "shipped": 1, "revenue_cents": 2400}


def test_a_missing_shipping_zone_dead_letters_the_booking_until_the_operator_retries(
    database: str, modulith_app: ModulithTestApp
) -> None:
    list_mug(10)

    from marketplace.main import app

    with TestClient(app) as client:
        assert client.portal is not None
        portal = client.portal
        placed = client.post("/orders", json=order_body("o-300", 1, "tok_visa", "NZ"))
        stuck = eventually(
            lambda: portal.call(outbox.status),
            lambda counts: counts["dead_lettered"] == 1,
            seconds=60,
        )
        before_retry = client.get("/shipping/o-300").json()

        zone = client.put("/shipping/zones/NZ", json={"carrier": "PostNZ"})
        retried = run_python("-m", "modulith", "outbox", "dead-letter", "--retry-all")
        shipment = eventually(
            lambda: client.get("/shipping/o-300").json(), has_status("booked"), seconds=60
        )
        drained = eventually(
            lambda: portal.call(outbox.status),
            lambda counts: counts["dead_lettered"] == 0 and counts["incomplete"] == 0,
        )
        notifications = eventually(
            lambda: client.get("/notifications/o-300").json(),
            lambda found: len(found) == 3,
            seconds=60,
        )
        totals = eventually(
            lambda: client.get("/reporting/summary").json(), lambda found: found["shipped"] == 1
        )

    assert notifications[-1] == {
        "kind": "shipped",
        "message": "shipped with PostNZ, tracking TRK-o-300",
    }
    assert totals == {"confirmed": 1, "cancelled": 0, "shipped": 1, "revenue_cents": 1200}
    assert placed.status_code == 200
    assert stuck["dead_lettered"] == 1
    assert before_retry["status"] == "requested"
    assert zone.status_code == 200
    assert retried.returncode == 0, retried.stderr
    assert shipment == {
        "order_id": "o-300",
        "status": "booked",
        "carrier": "PostNZ",
        "tracking_number": "TRK-o-300",
    }
    assert drained["dead_lettered"] == 0


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
            lambda: client.get("/orders/o-200").json(), has_status("confirmed", "cancelled")
        )
        on_hand = client.get("/inventory/SKU-MUG").json()
        notifications = eventually(
            lambda: client.get("/notifications/o-200").json(), lambda found: len(found) == 2
        )
        totals = eventually(
            lambda: client.get("/reporting/summary").json(), lambda found: found["cancelled"] == 1
        )

    assert cancelled == {"order_id": "o-200", "status": "cancelled", "reason": "payment declined"}
    assert on_hand == {"sku": "SKU-MUG", "on_hand": 10}
    assert notifications == [
        {"kind": "received", "message": "order received"},
        {"kind": "cancelled", "message": "order cancelled: payment declined"},
    ]
    assert totals == {"confirmed": 0, "cancelled": 1, "shipped": 0, "revenue_cents": 0}


def test_a_redelivered_payment_request_captures_once(
    database: str, modulith_app: ModulithTestApp, scenario: Scenario
) -> None:
    list_mug(10)

    from marketplace.contracts import PaymentCaptured, PaymentRequested
    from marketplace.main import app

    with TestClient(app) as client:
        client.post("/orders", json=order_body("o-100", 2, "tok_visa"))
        eventually(lambda: client.get("/orders/o-100").json(), has_status("confirmed", "cancelled"))
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
            lambda: client.get("/orders/o-200").json(), has_status("confirmed", "cancelled")
        )
        notifications = eventually(
            lambda: client.get("/notifications/o-200").json(), lambda found: len(found) == 2
        )
        totals = eventually(
            lambda: client.get("/reporting/summary").json(), lambda found: found["cancelled"] == 1
        )

    assert placed.status_code == 200
    assert placed.json() == {"order_id": "o-200", "status": "placed"}
    assert cancelled == {"order_id": "o-200", "status": "cancelled", "reason": "out of stock"}
    assert notifications == [
        {"kind": "received", "message": "order received"},
        {"kind": "cancelled", "message": "order cancelled: out of stock"},
    ]
    assert totals == {"confirmed": 0, "cancelled": 1, "shipped": 0, "revenue_cents": 0}
