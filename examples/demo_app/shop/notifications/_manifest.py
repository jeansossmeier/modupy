"""Manifest for the notifications module."""

from __future__ import annotations

from modulith import declare_module
from shop.notifications import notify_customer

declare_module(
    consumes=["StockReserved"],
    listeners=[notify_customer],
    declared_dependencies=["contracts"],
)
