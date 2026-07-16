"""Tests for the manifest system (T1.1.1 — T1.1.2).

Pattern: Each test runs against a fresh registry via the autouse fixture.
We construct fake caller frames using types.ModuleType + exec() so that
declare_module() sees the __name__ we want without writing real files.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Callable
from dataclasses import replace

import pytest

from modulith import ConfigurationError
from modulith import manifest as manifest_module
from modulith.manifest import Manifest, verify_manifest


@pytest.fixture(autouse=True)
def reset_manifests():
    """Clear the manifest registry before (and after) every test."""
    manifest_module._reset_for_testing()
    yield
    manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_declare_in_fake_module(module_name: str, **kwargs) -> None:
    """Execute declare_module(**kwargs) as if from a module named `module_name`.

    Creates a throwaway module object, sets __name__ on its globals, and
    exec()s the call inside that globals dict. This makes sys._getframe(1)
    in declare_module() see the correct __name__ without touching the
    filesystem.
    """
    fake_mod = types.ModuleType(module_name)
    fake_mod.__name__ = module_name

    # Build the call source. We pass kwargs by injecting them as locals.
    kwargs_repr = ", ".join(f"{k}=_{k}" for k in kwargs)
    src = f"from modulith.manifest import declare_module\ndeclare_module({kwargs_repr})\n"

    globs = vars(fake_mod)
    globs.update({f"_{k}": v for k, v in kwargs.items()})
    exec(src, globs)


# ---------------------------------------------------------------------------
# T1.1.1 — declare_module()
# ---------------------------------------------------------------------------


class TestDeclareModule:
    def test_strips_manifest_suffix(self):
        """Caller from fakeapp.orders._manifest → stored under 'fakeapp.orders'."""
        _run_declare_in_fake_module("fakeapp.orders._manifest")
        manifests = manifest_module.all_manifests()
        assert "fakeapp.orders" in manifests
        assert manifests["fakeapp.orders"].package == "fakeapp.orders"

    def test_bare_manifest_suffix_stripped(self):
        """The bare string '_manifest' is stripped to an empty package name — edge case."""
        # The bare _manifest module name (no parent package) — accept as-is per spec.
        _run_declare_in_fake_module("_manifest")
        manifests = manifest_module.all_manifests()
        # Per spec: bare '_manifest' is accepted as-is (no suffix stripping possible)
        assert "_manifest" in manifests

    def test_non_manifest_name_accepted_as_is(self):
        """A module name that doesn't end with '._manifest' is accepted as-is."""
        _run_declare_in_fake_module("myapp.orders")
        manifests = manifest_module.all_manifests()
        assert "myapp.orders" in manifests

    def test_all_fields_propagate_correctly(self):
        """Every kwarg is stored and lists are converted to tuples."""

        async def handler(e: object) -> None: ...

        _run_declare_in_fake_module(
            "fakeapp.payments._manifest",
            publishes=["PaymentProcessed", "PaymentFailed"],
            consumes=["OrderCreated"],
            listeners=[handler],
            owns_tables=["payments", "refunds"],
            declared_dependencies=["orders"],
            broker_targets=["redis-streams:payments", "amqp:events:payments"],
        )
        m = manifest_module.get_manifest("fakeapp.payments")
        assert m is not None
        assert m.package == "fakeapp.payments"
        assert m.publishes == ("PaymentProcessed", "PaymentFailed")
        assert m.consumes == ("OrderCreated",)
        assert m.listeners == (handler,)
        assert m.owns_tables == ("payments", "refunds")
        assert m.declared_dependencies == ("orders",)
        assert m.dependencies_declared is True
        assert m.broker_targets == (
            "redis-streams:payments",
            "amqp:events:payments",
        )

    def test_double_call_raises_configuration_error(self):
        """Second declare_module() for the same package raises ConfigurationError."""
        _run_declare_in_fake_module("fakeapp.orders._manifest")
        with pytest.raises(ConfigurationError, match="already declared"):
            _run_declare_in_fake_module("fakeapp.orders._manifest")

    def test_double_call_error_message_includes_package(self):
        """Error message names the offending package."""
        _run_declare_in_fake_module("fakeapp.inventory._manifest")
        with pytest.raises(ConfigurationError, match=r"fakeapp\.inventory"):
            _run_declare_in_fake_module("fakeapp.inventory._manifest")

    def test_listeners_stored_by_identity(self):
        """Listener function references are stored as-is, comparable by identity."""

        async def on_order_created(e: object) -> None: ...

        _run_declare_in_fake_module(
            "fakeapp.orders._manifest",
            listeners=[on_order_created],
        )
        m = manifest_module.get_manifest("fakeapp.orders")
        assert m is not None
        assert m.listeners[0] is on_order_created

    def test_empty_defaults_produce_empty_tuples(self):
        """Omitted dependencies stay iterable without enabling enforcement."""
        _run_declare_in_fake_module("fakeapp.billing._manifest")
        m = manifest_module.get_manifest("fakeapp.billing")
        assert m is not None
        assert m.publishes == ()
        assert m.consumes == ()
        assert m.listeners == ()
        assert m.owns_tables == ()
        assert m.declared_dependencies == ()
        assert isinstance(m.declared_dependencies, tuple)
        assert m.dependencies_declared is False
        assert m.broker_targets == ()

    @pytest.mark.parametrize("dependencies", [[], ()])
    def test_explicit_empty_dependencies_preserve_declared_state(
        self, dependencies: list[str] | tuple[str, ...]
    ) -> None:
        _run_declare_in_fake_module(
            "fakeapp.shipping._manifest",
            declared_dependencies=dependencies,
        )

        m = manifest_module.get_manifest("fakeapp.shipping")

        assert m is not None
        assert m.declared_dependencies == ()
        assert m.dependencies_declared is True

    def test_direct_manifest_explicit_empty_dependencies_are_declared(self) -> None:
        m = Manifest(package="fakeapp.shipping", declared_dependencies=())

        assert m.declared_dependencies == ()
        assert m.dependencies_declared is True

    def test_direct_manifest_none_dependencies_remain_iterable(self) -> None:
        m = Manifest(package="fakeapp.shipping", declared_dependencies=None)  # type: ignore[arg-type]

        assert m.declared_dependencies == ()
        assert m.dependencies_declared is False

    def test_dependency_declaration_state_cannot_be_set_by_caller(self) -> None:
        with pytest.raises(TypeError, match="dependencies_declared"):
            Manifest(package="fakeapp.shipping", dependencies_declared=True)  # type: ignore[call-arg]

    @pytest.mark.parametrize("explicit_none", [False, True])
    def test_replace_preserves_omitted_dependency_state(self, explicit_none: bool) -> None:
        kwargs = {"declared_dependencies": None} if explicit_none else {}
        original = Manifest(package="fakeapp.shipping", **kwargs)  # type: ignore[arg-type]

        copied = replace(original, package="fakeapp.shipping_copy")

        assert copied.declared_dependencies == ()
        assert isinstance(copied.declared_dependencies, tuple)
        assert copied.dependencies_declared is False

    @pytest.mark.parametrize(
        "broker_targets",
        [
            "redis-streams:orders",
            1,
            [""],
            ["redis-streams"],
            [":orders"],
            ["redis-streams:"],
            [1],
        ],
    )
    def test_broker_targets_require_non_empty_scheme_and_destination(
        self, broker_targets: object
    ) -> None:
        with pytest.raises(ConfigurationError, match="broker_targets"):
            _run_declare_in_fake_module(
                "fakeapp.orders._manifest",
                broker_targets=broker_targets,
            )

    def test_manifest_is_frozen_dataclass(self):
        """Manifest instances must be immutable (frozen=True)."""
        _run_declare_in_fake_module("fakeapp.catalog._manifest")
        m = manifest_module.get_manifest("fakeapp.catalog")
        assert m is not None
        with pytest.raises((AttributeError, TypeError)):
            m.package = "other"  # type: ignore[misc]

    def test_get_manifest_returns_none_for_unknown_package(self):
        """get_manifest() returns None when no manifest has been declared."""
        assert manifest_module.get_manifest("nonexistent.package") is None

    def test_all_manifests_returns_copy(self):
        """Mutating the returned dict does not affect the registry."""
        _run_declare_in_fake_module("fakeapp.pricing._manifest")
        result = manifest_module.all_manifests()
        result["injected"] = object()  # type: ignore[assignment]
        assert "injected" not in manifest_module.all_manifests()


# ---------------------------------------------------------------------------
# T1.1.2 — verify_manifest()
# ---------------------------------------------------------------------------


class TestVerifyManifest:
    def test_clean_manifest_returns_empty_list(self):
        """No errors when all listeners are registered and all events defined."""

        async def on_order(e: object) -> None: ...

        # Build a module in sys.modules so importlib.import_module can find it.
        fake_pkg = types.ModuleType("fakeapp_verify")
        fake_pkg.OrderCreated = object()  # type: ignore[attr-defined]
        sys.modules["fakeapp_verify"] = fake_pkg

        try:
            m = Manifest(
                package="fakeapp_verify",
                publishes=("OrderCreated",),
                listeners=(on_order,),
            )
            registered: set[Callable] = {on_order}
            errors = verify_manifest(m, registered)
            assert errors == []
        finally:
            del sys.modules["fakeapp_verify"]

    def test_unregistered_listener_produces_error(self):
        """A listener in the manifest that is NOT in the registered set → one error."""

        async def missing_listener(e: object) -> None: ...

        m = Manifest(package="fakeapp.orders", listeners=(missing_listener,))
        errors = verify_manifest(m, registered_listeners=set())
        assert len(errors) == 1
        assert "missing_listener" in errors[0]

    def test_unregistered_listener_error_mentions_package(self):
        """Error string includes the module package name."""

        async def handler(e: object) -> None: ...

        m = Manifest(package="fakeapp.shipping", listeners=(handler,))
        errors = verify_manifest(m, registered_listeners=set())
        assert "fakeapp.shipping" in errors[0]

    def test_undefined_event_type_produces_error(self):
        """An event type name listed in publishes but absent from the module → error."""
        fake_pkg = types.ModuleType("fakeapp_events")
        # Deliberately NOT adding "MissingEvent" to the namespace.
        sys.modules["fakeapp_events"] = fake_pkg

        try:
            m = Manifest(package="fakeapp_events", publishes=("MissingEvent",))
            errors = verify_manifest(m, registered_listeners=set())
            assert len(errors) == 1
            assert "MissingEvent" in errors[0]
        finally:
            del sys.modules["fakeapp_events"]

    def test_skips_when_package_import_fails(self):
        """An ImportError on the package is silently skipped for event-type checks."""
        # "definitely.not.a.real.package" doesn't exist → ImportError on import.
        m = Manifest(
            package="definitely.not.a.real.package",
            publishes=("SomeEvent",),
        )
        # Should not raise; import error is swallowed for publishes check.
        errors = verify_manifest(m, registered_listeners=set())
        # No error from the publishes check (skipped), none from listeners (none declared).
        assert errors == []

    def test_multiple_unregistered_listeners_all_reported(self):
        """Each unregistered listener produces its own error entry."""

        async def listener_a(e: object) -> None: ...

        async def listener_b(e: object) -> None: ...

        m = Manifest(package="fakeapp.billing", listeners=(listener_a, listener_b))
        errors = verify_manifest(m, registered_listeners=set())
        assert len(errors) == 2

    def test_partially_registered_listeners(self):
        """Only the unregistered listener is flagged; the registered one is not."""

        async def registered(e: object) -> None: ...

        async def unregistered(e: object) -> None: ...

        m = Manifest(package="fakeapp.catalog", listeners=(registered, unregistered))
        errors = verify_manifest(m, registered_listeners={registered})
        assert len(errors) == 1
        assert "unregistered" in errors[0]

    def test_listener_qualname_in_error(self):
        """Error message uses __qualname__ of the listener."""

        async def my_special_listener(e: object) -> None: ...

        m = Manifest(package="fakeapp.orders", listeners=(my_special_listener,))
        errors = verify_manifest(m, registered_listeners=set())
        assert "my_special_listener" in errors[0]
