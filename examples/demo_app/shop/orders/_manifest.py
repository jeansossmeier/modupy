"""Manifest for the orders module.

Declares the module's contract — what it publishes and what it may import.
modulith verifies this at startup: if ``OrderPlaced`` stopped being defined in
the module namespace, bootstrap would fail loudly rather than silently drift.
"""

from __future__ import annotations

from modulith import declare_module

declare_module(
    publishes=["OrderPlaced"],
    declared_dependencies=["contracts"],
)
