"""Fixtures for the demo shop's own test suite.

``shop`` lives on disk at ``examples/demo_app/shop`` and is not on the default
import path (mirroring ``tests/test_demo_app.py``'s ``demo_app`` fixture), so
this conftest prepends the demo root and resets the modulith runtime/manifest
singletons around every test — no test order dependency, no state leaking
into the main suite.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

DEMO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _demo_import_path(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make ``shop`` importable and reset runtime/manifest state per test."""
    from modulith import manifest as _manifest_mod
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()

    monkeypatch.syspath_prepend(str(DEMO_ROOT))

    yield

    for name in list(sys.modules):
        if name == "shop" or name.startswith("shop."):
            del sys.modules[name]
    _runtime._reset_for_testing()
    _manifest_mod._reset_for_testing()
