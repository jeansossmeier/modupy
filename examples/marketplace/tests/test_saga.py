import time
from collections.abc import Callable
from typing import Any

from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp

from .conftest import run_python


def eventually(read: Callable[[], Any], done: Callable[[Any], bool], seconds: float = 20) -> Any:
    deadline = time.monotonic() + seconds
    while True:
        value = read()
        if done(value) or time.monotonic() > deadline:
            return value
        time.sleep(0.1)


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
