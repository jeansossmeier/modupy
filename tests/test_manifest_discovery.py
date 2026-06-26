"""Tests for automatic _manifest.py import during module discovery (T1.1.4).

These tests verify that the discovery plugin auto-imports _manifest.py when
present and silently skips modules that have none.
"""

from __future__ import annotations

import pytest

from modulith import manifest as manifest_module


@pytest.fixture(autouse=True)
def reset_manifests():
    manifest_module._reset_for_testing()
    yield
    manifest_module._reset_for_testing()


class TestManifestDiscovery:
    def test_manifest_registered_after_bootstrap(self, make_fake_app):
        """_manifest.py is auto-imported during discovery, populating the registry."""
        make_fake_app(
            {
                "orders": """
                    from dataclasses import dataclass
                    from modulith import event

                    @event
                    @dataclass(frozen=True)
                    class OrderCreated:
                        order_id: str
                """,
            },
            extra_files={
                "orders/_manifest.py": """
                    from modulith.manifest import declare_module
                    declare_module(publishes=["OrderCreated"])
                """,
            },
        )
        from modulith.decorators import configure
        from modulith.runtime import _runtime

        configure(package="fakeapp", verify_manifests=False)
        _runtime.ensure_bootstrapped()

        manifests = manifest_module.all_manifests()
        assert "fakeapp.orders" in manifests
        assert manifests["fakeapp.orders"].publishes == ("OrderCreated",)

    def test_module_without_manifest_does_not_pollute_registry(self, make_fake_app):
        """A module without _manifest.py causes no registry entry and no error."""
        make_fake_app(
            {
                "orders": """
                    from dataclasses import dataclass
                    from modulith import event

                    @event
                    @dataclass(frozen=True)
                    class OrderCreated:
                        order_id: str
                """,
            }
            # No extra_files — no _manifest.py
        )
        from modulith.decorators import configure
        from modulith.runtime import _runtime

        configure(package="fakeapp", verify_manifests=False)
        _runtime.ensure_bootstrapped()

        assert manifest_module.all_manifests() == {}

    def test_full_e2e_discovery_verification_bootstrap(self, make_fake_app):
        """End-to-end: _manifest.py discovered, listener registered, verification passes."""
        make_fake_app(
            {
                "orders": """
                    from dataclasses import dataclass
                    from modulith import event, listener

                    @event
                    @dataclass(frozen=True)
                    class OrderCreated:
                        order_id: str

                    @listener
                    async def on_order_created(event: OrderCreated) -> None:
                        pass
                """,
            },
            extra_files={
                "orders/_manifest.py": """
                    from modulith.manifest import declare_module
                    from fakeapp.orders import on_order_created, OrderCreated

                    declare_module(
                        publishes=["OrderCreated"],
                        listeners=[on_order_created],
                    )
                """,
            },
        )
        from modulith import publish
        from modulith.decorators import configure

        configure(package="fakeapp")

        import asyncio

        # Should succeed: manifest reality matches declaration.
        asyncio.get_event_loop().run_until_complete(publish(object()))

        # Manifest IS in registry.
        assert "fakeapp.orders" in manifest_module.all_manifests()

    def test_multiple_modules_only_manifested_ones_registered(self, make_fake_app):
        """With two modules, only the one with _manifest.py appears in the registry."""
        make_fake_app(
            {
                "orders": """
                    from dataclasses import dataclass
                    from modulith import event

                    @event
                    @dataclass(frozen=True)
                    class OrderCreated:
                        order_id: str
                """,
                "inventory": """
                    from dataclasses import dataclass
                    from modulith import event

                    @event
                    @dataclass(frozen=True)
                    class StockReserved:
                        sku: str
                """,
            },
            extra_files={
                "orders/_manifest.py": """
                    from modulith.manifest import declare_module
                    declare_module(publishes=["OrderCreated"])
                """,
                # inventory has NO _manifest.py
            },
        )
        from modulith.decorators import configure
        from modulith.runtime import _runtime

        configure(package="fakeapp", verify_manifests=False)
        _runtime.ensure_bootstrapped()

        manifests = manifest_module.all_manifests()
        assert "fakeapp.orders" in manifests
        assert "fakeapp.inventory" not in manifests
