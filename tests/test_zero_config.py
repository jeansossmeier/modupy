"""End-to-end test of the zero-config experience.

This is the test that decides whether the framework is good. A user who
has never read the docs should be able to:

  1. Create subpackages under their app
  2. Decorate events with @event and listeners with @listener
  3. Call publish() and have everything just work

Without writing a config file, without setting env vars, without calling
any setup function. This test simulates that flow end-to-end.

The ``fake_app`` fixture lives in ``tests/conftest.py`` so other test
files can reuse the same two-module shape; new tests should prefer the
``make_fake_app`` factory in conftest for custom module structures.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

# ----- The core test: end-to-end zero-config dispatch -----------------------


def test_zero_config_event_flow(fake_app: str) -> None:
    """Define modules, publish event, listener receives it. No config."""
    # Set the package explicitly via configure() since pytest's call
    # stack doesn't include the fake app — auto-detect would land in
    # pytest internals. Real applications import their own code, so
    # the call stack walking does the right thing for them.
    from modulith import configure

    configure(package=fake_app)

    # Import the orders module. This triggers the @event registration.
    # We don't import inventory yet — discovery should find and import
    # it for us when bootstrap runs.
    orders = __import__(f"{fake_app}.orders", fromlist=["create_order"])

    # Trigger an event. This call is what triggers bootstrap, which in
    # turn discovers all modules (including inventory) and wires up
    # the listener.
    asyncio.run(orders.create_order("order-123"))

    # The listener in inventory should have received the event.
    inventory = sys.modules[f"{fake_app}.inventory"]
    assert len(inventory.received) == 1
    assert inventory.received[0].order_id == "order-123"


def test_listener_registered_before_bootstrap_still_fires(fake_app: str) -> None:
    """Listeners decorated during import (pre-bootstrap) flush correctly."""
    from modulith import configure

    configure(package=fake_app)

    # Importing inventory FIRST registers the @listener while modulith
    # hasn't bootstrapped yet. The runtime must queue the registration
    # and flush it when bootstrap happens.
    inventory = __import__(f"{fake_app}.inventory", fromlist=["received"])
    orders = __import__(f"{fake_app}.orders", fromlist=["create_order"])

    asyncio.run(orders.create_order("order-456"))

    assert len(inventory.received) == 1
    assert inventory.received[0].order_id == "order-456"


def test_runtime_logs_friendly_banner(fake_app: str, caplog) -> None:
    """First-run experience: users see what modulith is doing."""
    import logging

    from modulith import configure

    configure(package=fake_app)
    caplog.set_level(logging.INFO, logger="modulith")

    # Trigger bootstrap.
    from modulith.runtime import _runtime

    _runtime.ensure_bootstrapped()

    # The banner should mention the package, the modules, and the config.
    messages = [record.message for record in caplog.records]
    full_log = "\n".join(messages)
    assert "fakeapp" in full_log
    assert "orders" in full_log
    assert "inventory" in full_log
    assert "outbox=memory" in full_log
    assert "ready" in full_log


def test_configure_after_bootstrap_raises(fake_app: str) -> None:
    """Configuration is locked once bootstrap runs."""
    from modulith import ConfigurationError, configure
    from modulith.runtime import _runtime

    configure(package=fake_app)
    _runtime.ensure_bootstrapped()

    with pytest.raises(ConfigurationError, match="bootstrapped"):
        configure(outbox="postgres")
