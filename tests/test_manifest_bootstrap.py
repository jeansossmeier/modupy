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
    async def test_unregistered_listener_fails_bootstrap(self, make_fake_app):
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
            await publish(object())  # triggers bootstrap

    async def test_matching_manifest_passes_bootstrap(self, make_fake_app):
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
        await publish(object())

    async def test_sync_listener_in_manifest_passes_bootstrap(self, make_fake_app):
        """Regression (D1): a SYNC listener declared in a manifest must verify.

        Sync listeners register as async wrappers (see sync.wrap_sync_listener); the
        wrapper carries __modulith_sync_wrapped__ pointing at the original the manifest
        references. verify_manifest must match through that marker rather than falsely
        report the listener as 'not registered' and abort bootstrap.
        """
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
                    def on_order_created(event: OrderCreated) -> None:  # SYNC listener
                        pass
                """,
            },
            extra_files={
                "orders/_manifest.py": """
                    from modulith.manifest import declare_module
                    from fakeapp.orders import on_order_created

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

        # Should not raise — the sync listener IS registered (as an async wrapper).
        await publish(object())

    async def test_verify_manifests_false_bypasses_check(self, make_fake_app):
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
        await publish(object())

    async def test_error_message_includes_package_prefix(self, make_fake_app):
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
            await publish(object())


# ---------------------------------------------------------------------------
# verify_manifest scope (declared_dependencies / owns_tables are NOT bootstrap
# checks — they belong to the AST boundary verifier)
# ---------------------------------------------------------------------------


class TestManifestVerifyScope:
    async def test_bogus_dependencies_and_tables_do_not_fail_bootstrap(self, make_fake_app):
        """Bootstrap verifies only listeners + publishes.

        `declared_dependencies` and `owns_tables` need static source analysis
        and are enforced by the AST verifier (`modulith verify`), NOT by the
        in-process bootstrap check. A manifest with deliberately bogus deps and
        tables (but correct listeners/publishes) must still boot — pinning the
        documented split so the two surfaces never silently merge.
        """
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
                        owns_tables=["a_table_that_does_not_exist"],
                        declared_dependencies=["a_module_never_imported"],
                    )
                """,
            },
        )
        from modulith import publish
        from modulith.decorators import configure

        configure(package="fakeapp")

        # Should NOT raise: bogus deps/tables are out of bootstrap's scope.
        await publish(object())


# ---------------------------------------------------------------------------
# Registry lifecycle: runtime reset must also clear the manifest registry
# ---------------------------------------------------------------------------


def test_runtime_reset_clears_manifest_registry():
    """`_runtime._reset_for_testing()` clears the module-global manifest registry.

    Regression: the registry leaked across re-bootstraps, so a re-imported
    `_manifest.py` either hit declare_module's 'already declared' guard or
    verify_manifest validated a stale module against a freshly-built bus.
    """
    from modulith.runtime import _runtime

    # declare_module derives the package from this frame's module name.
    manifest_module.declare_module(publishes=["X"])
    assert manifest_module.all_manifests()  # one entry now present

    _runtime._reset_for_testing()
    assert manifest_module.all_manifests() == {}

    # Re-declaring the same package must not hit the "already declared" guard.
    manifest_module.declare_module(publishes=["X"])  # no ConfigurationError
