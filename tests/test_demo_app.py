"""End-to-end test of the bundled demo application (``examples/demo_app``).

The demo is a small shop split into modules — ``orders`` publishes
``OrderPlaced``, ``inventory`` reserves stock and publishes ``StockReserved``,
``notifications`` reacts to that. This test bootstraps the real modulith
runtime against the demo package and drives the full cross-module event chain,
proving the example actually works (it is living documentation, not decoration)
and that auto-discovery + manifest verification + the in-memory event bus wire
the modules together with no explicit registration.

The demo lives on disk under ``examples/demo_app`` and is not on the default
import path, so the fixture prepends it and tears down the imported ``shop.*``
modules + the runtime singleton afterwards (mirroring conftest's make_fake_app).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

DEMO_ROOT = Path(__file__).resolve().parent.parent / "examples" / "demo_app"


@pytest.fixture
def demo_app(monkeypatch):
    """Bootstrap the modulith runtime against the on-disk demo package."""
    from modulith import configure
    from modulith.runtime import _runtime

    # Defensive: clear any runtime/manifest state leaked by a prior test.
    _runtime._reset_for_testing()
    from modulith import manifest as _manifest_mod

    _manifest_mod._reset_for_testing()

    monkeypatch.syspath_prepend(str(DEMO_ROOT))
    configure(package="shop", auto_discover=True)
    _runtime.ensure_bootstrapped()

    yield

    for name in list(sys.modules):
        if name == "shop" or name.startswith("shop."):
            del sys.modules[name]
    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()


async def test_demo_event_chain_crosses_three_modules(demo_app) -> None:
    from shop import inventory, notifications
    from shop.orders import place_order

    order_id = await place_order(customer_id="c-42", total=19.99)

    # orders → OrderPlaced → inventory reserves → StockReserved → notifications
    assert any(r.order_id == order_id for r in inventory.reserved)
    assert any(n.order_id == order_id for n in notifications.sent)


async def test_demo_modules_are_auto_discovered(demo_app) -> None:
    from modulith.runtime import _runtime

    names = {m.name for m in _runtime.modules}
    # The three business modules are discovered as subpackages of `shop`.
    assert {"orders", "inventory", "notifications"} <= names


def test_demo_http_endpoint_triggers_chain(demo_app) -> None:
    from shop import inventory, notifications
    from shop.main import app

    with TestClient(app) as client:
        resp = client.post("/orders", json={"customer_id": "c-7", "total": 5.0})

    assert resp.status_code == 200
    order_id = resp.json()["order_id"]
    # Both hops of the chain fire through the HTTP entry point: orders →
    # inventory (reserve) → notifications (notify).
    assert any(r.order_id == order_id for r in inventory.reserved)
    assert any(n.order_id == order_id for n in notifications.sent)
