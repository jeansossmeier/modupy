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

import asyncio
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


def _flaky_publish(monkeypatch: pytest.MonkeyPatch, failures: int) -> list[object]:
    """Replace inventory's ``publish`` with a broker send that fails ``failures`` times.

    On the process topology a broker send failure propagates out of
    ``publish`` into the listener (``modulith.runtime.Runtime.publish``), and
    the consumer redelivers the event; this double stands in for that send.
    """
    from shop import inventory

    published: list[object] = []
    remaining = [failures]

    async def publish(evt: object) -> None:
        if remaining[0] > 0:
            remaining[0] -= 1
            raise ConnectionError("broker send failed")
        published.append(evt)

    monkeypatch.setattr(inventory, "publish", publish)
    return published


async def test_reserve_stock_republishes_after_failed_publish(
    demo_app, monkeypatch: pytest.MonkeyPatch
) -> None:
    from shop import inventory
    from shop.contracts.events import OrderPlaced, StockReserved

    published = _flaky_publish(monkeypatch, failures=1)
    evt = OrderPlaced(order_id="o-1", customer_id="c-1", total=1.0)

    with pytest.raises(ConnectionError):
        await inventory.reserve_stock(evt)
    await inventory.reserve_stock(evt)

    assert published == [StockReserved(order_id="o-1")]
    assert [r.order_id for r in inventory.reserved] == ["o-1"]


async def test_reserve_stock_suppresses_redelivery_after_successful_publish(
    demo_app, monkeypatch: pytest.MonkeyPatch
) -> None:
    from shop import inventory
    from shop.contracts.events import OrderPlaced, StockReserved

    published = _flaky_publish(monkeypatch, failures=0)
    evt = OrderPlaced(order_id="o-2", customer_id="c-2", total=2.0)

    await inventory.reserve_stock(evt)
    await inventory.reserve_stock(evt)

    assert published == [StockReserved(order_id="o-2")]
    assert [r.order_id for r in inventory.reserved] == ["o-2"]


async def test_reserve_stock_publishes_once_for_concurrent_deliveries(
    demo_app, monkeypatch: pytest.MonkeyPatch
) -> None:
    from shop import inventory
    from shop.contracts.events import OrderPlaced, StockReserved

    release = asyncio.Event()
    published: list[object] = []

    async def publish(evt: object) -> None:
        await release.wait()
        published.append(evt)

    monkeypatch.setattr(inventory, "publish", publish)
    evt = OrderPlaced(order_id="o-3", customer_id="c-3", total=3.0)

    first = asyncio.create_task(inventory.reserve_stock(evt))
    second = asyncio.create_task(inventory.reserve_stock(evt))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert published == [StockReserved(order_id="o-3")]
    assert [r.order_id for r in inventory.reserved] == ["o-3"]


async def test_reserve_stock_guard_memory_is_bounded(
    demo_app, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard remembers the last ``GUARD_MEMORY`` orders and forgets older ones."""
    from shop import inventory
    from shop.contracts.events import OrderPlaced

    published = _flaky_publish(monkeypatch, failures=0)
    for i in range(inventory.GUARD_MEMORY + 1):
        await inventory.reserve_stock(OrderPlaced(order_id=f"o-{i}", customer_id="c", total=1.0))

    await inventory.reserve_stock(OrderPlaced(order_id="o-1", customer_id="c", total=1.0))
    assert len(published) == inventory.GUARD_MEMORY + 1

    await inventory.reserve_stock(OrderPlaced(order_id="o-0", customer_id="c", total=1.0))
    assert len(published) == inventory.GUARD_MEMORY + 2
