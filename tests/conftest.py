"""Shared pytest fixtures for the modulith test suite.

This file provides the ``make_fake_app`` factory used across integration
tests (zero-config, manifest, outbox, verifier, etc.) plus the legacy
``fake_app`` fixture that test_zero_config.py was originally written
against.

Why a factory: each subsystem test wants a different module shape
(manifest tests need ``_manifest.py`` files, verifier tests need
``_internal/`` packages, outbox tests need handlers that touch a session,
etc.). Hardcoding one shape would force every test to either reuse it
even when wrong or build its own from scratch — both are bad. The factory
takes a dict of ``module_name -> source_code`` so each test declares
exactly what it needs.

Cleanup is automatic: at fixture teardown, all modules under the fake app
are removed from ``sys.modules`` and the modulith runtime singleton is
reset, so test order can never matter.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from textwrap import dedent

import pytest


@pytest.fixture
def make_fake_app(tmp_path: Path, monkeypatch) -> Callable[..., str]:
    """Factory fixture: build an importable fake app on disk.

    Usage::

        def test_thing(make_fake_app):
            pkg = make_fake_app({
                "orders": "from modulith import event\\n@event\\nclass X: ...",
                "inventory": "from modulith import listener\\n...",
            }, package_name="myapp")  # default: "fakeapp"
            # `pkg` is the importable package name.

    Module sources are dedent'd before being written so callers can use
    triple-quoted strings with leading indentation. Each named module
    becomes a subpackage with an ``__init__.py`` containing the source.

    Files beyond the module's ``__init__.py`` (e.g. ``_manifest.py``,
    ``_internal/persistence.py``) can be created via the optional
    ``extra_files`` argument: a dict of relative path -> source code.
    """
    created_packages: list[str] = []

    def _make(
        modules: dict[str, str],
        *,
        package_name: str = "fakeapp",
        extra_files: dict[str, str] | None = None,
    ) -> str:
        app_dir = tmp_path / package_name
        if not app_dir.exists():
            app_dir.mkdir()
            (app_dir / "__init__.py").write_text("")
            created_packages.append(package_name)

        for module_name, source in modules.items():
            mod_dir = app_dir / module_name
            mod_dir.mkdir(exist_ok=True)
            (mod_dir / "__init__.py").write_text(dedent(source))

        if extra_files:
            for rel_path, source in extra_files.items():
                target = app_dir / rel_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(dedent(source))

        # Make the fake app importable and set cwd so pyproject auto-detect
        # (when relevant) lands in the test's tmp_path rather than walking
        # up to the modulith repo's own pyproject.toml.
        monkeypatch.syspath_prepend(str(tmp_path))
        monkeypatch.chdir(tmp_path)

        return package_name

    yield _make

    # Cleanup: drop the fake-app modules from sys.modules and reset the
    # modulith runtime so the next test starts from a clean slate.
    for pkg in created_packages:
        for mod_name in list(sys.modules):
            if mod_name == pkg or mod_name.startswith(f"{pkg}."):
                del sys.modules[mod_name]

    # Imported here so test files that don't use modulith state don't pay
    # the import cost just by depending on this fixture.
    from modulith.runtime import _runtime

    _runtime._reset_for_testing()


@pytest.fixture
def fake_app(make_fake_app: Callable[..., str]) -> str:
    """Two-module fake app (orders + inventory) for zero-config tests.

    Provides a stable shape used by ``tests/test_zero_config.py``: orders
    publishes ``OrderCreated``; inventory listens and records receipts.
    New test files should prefer ``make_fake_app`` directly so they can
    declare exactly the modules they need.
    """
    return make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, publish

                @event
                @dataclass(frozen=True)
                class OrderCreated:
                    order_id: str

                # Module-level state for tests to inspect.
                published: list[OrderCreated] = []

                async def create_order(order_id: str) -> None:
                    evt = OrderCreated(order_id=order_id)
                    published.append(evt)
                    await publish(evt)
            """,
            "inventory": """
                from modulith import listener
                from fakeapp.orders import OrderCreated

                # Module-level state for tests to inspect.
                received: list[OrderCreated] = []

                @listener
                async def reserve_stock(event: OrderCreated) -> None:
                    received.append(event)
            """,
        }
    )
