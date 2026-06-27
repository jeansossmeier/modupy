"""Manifest for the inventory module.

``listeners`` is verified at startup: if ``reserve_stock`` ever fails to
register (e.g. the module silently fails to import), bootstrap aborts with a
clear error instead of quietly dropping events.
"""

from __future__ import annotations

from modulith import declare_module
from shop.inventory import reserve_stock

declare_module(
    consumes=["OrderPlaced"],
    publishes=["StockReserved"],
    listeners=[reserve_stock],
    declared_dependencies=["contracts"],
)
