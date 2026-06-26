"""Integration tests for manifest verification wired into runtime bootstrap (T1.1.3).

Uses make_fake_app to write real module files and trigger the full bootstrap
path: discovery → manifest import → verify_manifest → ConfigurationError or pass.
"""

from __future__ import annotations

import pytest

from modulith import ConfigurationError
from modulith import manifest as manifest_module


@pytest.fixture(autouse=True)
def reset_manifests():
    """Ensure the manifest registry is clean around each test."""
    manifest_module._reset_for_testing()
    yield
    manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# T1.1.3 — bootstrap verification
# ---------------------------------------------------------------------------


class TestManifestBootstrap:
    def test_unregistered_listener_fails_bootstrap(self, make_fake_app):
        """An app whose manifest declares a listener that never registered → ConfigurationError."""
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

                    async def orphan_listener(e: object) -> None:
                        pass

                    # This listener is declared but never @registered via @listener.
                    declare_module(listeners=[orphan_listener])
                """,
            },
        )
        from modulith import publish
        from modulith.decorators import configure

        configure(package="fakeapp")

        with pytest.raises(ConfigurationError, match="Manifest verification failed"):
            import asyncio

            asyncio.get_event_loop().run_until_complete(
                publish(object())  # triggers bootstrap
            )

    def test_matching_manifest_passes_bootstrap(self, make_fake_app):
        """An app whose manifest matches reality → bootstrap succeeds."""
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

        # Should not raise.
        import asyncio

        asyncio.get_event_loop().run_until_complete(publish(object()))

    def test_verify_manifests_false_bypasses_check(self, make_fake_app):
        """Setting verify_manifests=False skips verification even with a bad manifest."""
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

                    async def orphan_listener(e: object) -> None:
                        pass

                    declare_module(listeners=[orphan_listener])
                """,
            },
        )
        from modulith import publish
        from modulith.decorators import configure

        configure(package="fakeapp", verify_manifests=False)

        # Should not raise — verification is disabled.
        import asyncio

        asyncio.get_event_loop().run_until_complete(publish(object()))

    def test_error_message_includes_package_prefix(self, make_fake_app):
        """ConfigurationError message includes the [package] prefix from the violating module."""
        make_fake_app(
            {
                "payments": """
                    from dataclasses import dataclass
                    from modulith import event

                    @event
                    @dataclass(frozen=True)
                    class PaymentReceived:
                        amount: float
                """,
            },
            extra_files={
                "payments/_manifest.py": """
                    from modulith.manifest import declare_module

                    async def missing(e: object) -> None:
                        pass

                    declare_module(listeners=[missing])
                """,
            },
        )
        from modulith import publish
        from modulith.decorators import configure

        configure(package="fakeapp")

        with pytest.raises(ConfigurationError, match=r"\[fakeapp\.payments\]"):
            import asyncio

            asyncio.get_event_loop().run_until_complete(publish(object()))
