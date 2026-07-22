"""Tests for the per-module worker factory (``modulith._worker.create_app``).

In process-per-module topology the supervisor spawns one uvicorn process per
module, each calling ``create_app()`` via ``--factory``. The defining property
is *selective import*: a worker imports ONLY its configured module (plus the
shared ``contracts`` module) — never its siblings — which is what gives each
worker its own process, import graph, and GIL.

These tests build a fake app on disk and drive ``create_app`` through a real
FastAPI ``TestClient``, asserting the health endpoint, router mounting, the
required-env contract, and the selective-import guarantee. Cross-process broker
routing is a separate concern (covered with the topology/proxy work).
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

import modulith._worker as worker_module
from modulith import ConfigurationError, Consumer, ConsumerSpec
from modulith._worker import _build_consumer, create_app
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.protocols import ConsumerHealth, ConsumerStatus
from modulith.runtime import _runtime


class _NoopBroker:
    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        pass

    async def close(self) -> None:
        pass


class _NoopConsumer:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def _set_worker_env(monkeypatch, module: str, package: str = "fakeapp") -> None:
    monkeypatch.setenv("MODULITH_MODULE", module)
    monkeypatch.setenv("MODULITH_APP_PACKAGE", package)
    # create_app() configures topology="processes", which now (correctly)
    # requires a real cross-process broker — the in-memory default can't carry
    # events between worker processes. These tests exercise only HTTP app
    # construction (no publishing), so a non-memory broker *name* satisfies the
    # config invariant; no adapter claims this scheme, so it stays unregistered
    # and inert (no connection ever attempted). Cross-process broker routing is
    # covered in test_cross_process.py.
    monkeypatch.setenv("MODULITH_BROKER", "test-noop-broker")


# ---------------------------------------------------------------------------
# env contract
# ---------------------------------------------------------------------------


def test_missing_env_raises(monkeypatch) -> None:
    monkeypatch.delenv("MODULITH_MODULE", raising=False)
    monkeypatch.delenv("MODULITH_APP_PACKAGE", raising=False)

    with pytest.raises(RuntimeError, match="MODULITH_MODULE"):
        create_app()


# ---------------------------------------------------------------------------
# health endpoint
# ---------------------------------------------------------------------------


def test_health_endpoint_reports_module(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "module": "orders"}


@pytest.mark.parametrize("status", ["starting", "degraded", "failed", "stopped"])
def test_health_endpoint_returns_503_when_consumer_is_not_ready(
    make_fake_app,
    monkeypatch,
    status: ConsumerStatus,
) -> None:
    class UnreadyConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        def health(self) -> ConsumerHealth:
            return ConsumerHealth(ready=False, status=status, detail="not ready")

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: UnreadyConsumer(),
    )

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": status,
        "module": "orders",
        "ready": False,
        "detail": "not ready",
    }


def test_health_endpoint_returns_200_when_consumer_is_ready(make_fake_app, monkeypatch) -> None:
    class ReadyConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        def health(self) -> ConsumerHealth:
            return ConsumerHealth(ready=True, status="ready")

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: ReadyConsumer(),
    )

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "module": "orders",
        "ready": True,
    }


def test_health_endpoint_keeps_legacy_consumer_usable(make_fake_app, monkeypatch, caplog) -> None:
    class LegacyConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: LegacyConsumer(),
    )

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/health")
        repeated_response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "unknown",
        "module": "orders",
        "warning": "consumer does not expose health",
    }
    assert repeated_response.json() == response.json()
    warnings = [
        record
        for record in caplog.records
        if record.name == "modulith.worker" and "readiness is unknown" in record.message
    ]
    assert len(warnings) == 1


def test_worker_identity_is_unique_per_app_construction(make_fake_app, monkeypatch) -> None:
    identities: list[str] = []

    def capture_identity(_module: str, consumer_name: str) -> None:
        identities.append(consumer_name)

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(worker_module, "_build_consumer", capture_identity)

    first_app = create_app()
    with TestClient(first_app):
        pass

    _runtime._reset_for_testing()
    second_app = create_app()
    with TestClient(second_app):
        pass

    assert len(identities) == 2
    assert identities[0] != identities[1]
    for identity in identities:
        module_name, separator, unique_id = identity.partition(":")
        assert module_name == "orders"
        assert separator == ":"
        assert len(unique_id) == 32
        int(unique_id, 16)


def test_health_state_is_scoped_to_each_app_lifespan(make_fake_app, monkeypatch) -> None:
    class StatefulConsumer:
        def __init__(self, health: ConsumerHealth) -> None:
            self._health = health

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        def health(self) -> ConsumerHealth:
            return self._health

    consumers = iter(
        [
            StatefulConsumer(ConsumerHealth(ready=True, status="ready")),
            StatefulConsumer(
                ConsumerHealth(ready=False, status="degraded", detail="broker unavailable")
            ),
        ]
    )
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: next(consumers),
    )

    first_app = create_app()
    _runtime._reset_for_testing()
    second_app = create_app()

    with TestClient(first_app) as first_client, TestClient(second_app) as second_client:
        assert second_client.get("/health").status_code == 503
        first_response = first_client.get("/health")

    assert first_response.status_code == 200
    assert first_response.json()["status"] == "ready"


# ---------------------------------------------------------------------------
# selective import — the core correctness property
# ---------------------------------------------------------------------------


def test_only_configured_module_is_imported(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": "", "inventory": ""})
    _set_worker_env(monkeypatch, "orders")

    create_app()

    assert "fakeapp.orders" in sys.modules
    assert "fakeapp.inventory" not in sys.modules  # sibling NOT imported


# ---------------------------------------------------------------------------
# router mounting
# ---------------------------------------------------------------------------


def test_module_router_is_mounted_under_module_prefix(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {
            "orders": """
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("/ping")
                async def ping() -> dict[str, bool]:
                    return {"pong": True}
            """
        }
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/orders/ping")

    assert resp.status_code == 200
    assert resp.json() == {"pong": True}


# ---------------------------------------------------------------------------
# contracts module import
# ---------------------------------------------------------------------------


def test_contracts_module_imported_when_present(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {"orders": ""},
        extra_files={"contracts/__init__.py": "SHARED = 'event-types-live-here'\n"},
    )
    _set_worker_env(monkeypatch, "orders")

    create_app()

    assert "fakeapp.contracts" in sys.modules


def test_missing_contracts_module_is_tolerated(make_fake_app, monkeypatch) -> None:
    # No contracts package — create_app must not blow up.
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    app = create_app()  # should not raise
    assert app is not None


# ---------------------------------------------------------------------------
# selected-module manifest import
# ---------------------------------------------------------------------------


def test_manifest_only_target_builds_consumer(make_fake_app, monkeypatch) -> None:
    built_specs: list[ConsumerSpec] = []

    def build_consumer(spec: ConsumerSpec) -> Consumer:
        built_specs.append(spec)
        return _NoopConsumer()

    make_fake_app(
        {"orders": ""},
        extra_files={
            "orders/_manifest.py": """
                from modulith import declare_module

                declare_module(broker_targets=("test-noop-broker:events.orders",))
            """
        },
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    _runtime.broker_registry.register("test-noop-broker", _NoopBroker())
    _runtime.consumer_registry.register("test-noop-broker", build_consumer)

    with TestClient(app):
        pass

    assert [spec.targets for spec in built_specs] == [("events.orders",)]


@pytest.mark.parametrize("missing_adapter", ["broker", "consumer"])
def test_manifest_only_target_requires_adapters(
    make_fake_app,
    monkeypatch,
    missing_adapter: str,
) -> None:
    make_fake_app(
        {"orders": ""},
        extra_files={
            "orders/_manifest.py": """
                from modulith import declare_module

                declare_module(broker_targets=("test-noop-broker:events.orders",))
            """
        },
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    if missing_adapter == "broker":
        _runtime.consumer_registry.register(
            "test-noop-broker",
            lambda _spec: _NoopConsumer(),
        )
    else:
        _runtime.broker_registry.register("test-noop-broker", _NoopBroker())

    with pytest.raises(ConfigurationError, match=f"{missing_adapter} adapter"):
        with TestClient(app):
            pass


def test_selected_manifest_nested_import_failure_propagates(make_fake_app, monkeypatch) -> None:
    make_fake_app(
        {"orders": ""},
        extra_files={"orders/_manifest.py": "import missing_manifest_dependency\n"},
    )
    _set_worker_env(monkeypatch, "orders")

    with pytest.raises(ModuleNotFoundError, match="missing_manifest_dependency"):
        create_app()


# ---------------------------------------------------------------------------
# consumer adapter requirements
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("missing_adapter", ["broker", "consumer"])
def test_listener_worker_requires_broker_and_consumer_adapters(
    make_fake_app,
    monkeypatch,
    missing_adapter: str,
) -> None:
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event, listener

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    pass
            """
        }
    )
    _set_worker_env(monkeypatch, "orders")
    create_app()

    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    if missing_adapter == "broker":
        _runtime.consumer_registry.register("test-noop-broker", lambda _spec: _NoopConsumer())
    else:
        _runtime.broker_registry.register("test-noop-broker", _NoopBroker())

    with pytest.raises(ConfigurationError, match=missing_adapter):
        _build_consumer("orders")


def test_listener_free_worker_does_not_require_broker_adapters(
    make_fake_app,
    monkeypatch,
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    create_app()

    assert _build_consumer("orders") is None


def test_shm_worker_without_listeners_reconciles_previous_deployment(
    make_fake_app,
    monkeypatch,
    tmp_path,
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    database_path = tmp_path / "worker.db"
    monkeypatch.setenv("MODULITH_BROKER", "shm")
    monkeypatch.setenv("MODULITH_BROKER_SQLITE_PATH", str(database_path))
    monkeypatch.setenv("MODULITH_BROKER_HINT_PATH", str(tmp_path / "worker.hints"))

    app = create_app()
    assert _runtime.broker_registry is not None
    broker = _runtime.broker_registry.get("shm")
    assert isinstance(broker, ShmBroker)
    asyncio.run(broker._cold.get_subscriptions())
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        INSERT INTO shm_subscription (target, consumer_group)
        VALUES ('events.PreviousListener', 'modulith-orders')
        """
    )
    connection.commit()
    connection.close()

    # Starting the listener-free redeployment must reconcile the stale set.
    # It needs no poll task because there are no current delivery targets.
    with TestClient(app):
        consumer = app.state.consumer
        assert isinstance(consumer, ShmConsumer)
        assert consumer._task is None
        connection = sqlite3.connect(database_path)
        try:
            subscriptions = connection.execute(
                "SELECT target, consumer_group FROM shm_subscription"
            ).fetchall()
        finally:
            connection.close()
        assert subscriptions == []


# ---------------------------------------------------------------------------
# Task 6 (runtime-sync) — module lifecycle fires for the isolated worker's
# own module, and lifespan teardown always reaches runtime/broker shutdown
# ---------------------------------------------------------------------------


def test_worker_fires_after_module_load_for_its_own_module(make_fake_app, monkeypatch) -> None:
    """lifecycle-location: the monolith's bootstrap fires
    modulith_after_module_load once per discovered module (see
    test_runtime_hooks.py::test_bootstrap_fires_after_module_load_once_per_module).
    A process-per-module worker bootstraps with auto_discover=False, so that
    same loop in Runtime._bootstrap() sees an EMPTY module list and never
    fires the hook for the module the worker actually imports — plugins that
    rely on it (startup metrics, module-scoped resources) silently never ran
    for ANY process-topology worker. The worker must fire it itself for its
    own isolated module."""
    from modulith import ModuleInfo, hookimpl
    from modulith.runtime import _runtime

    seen: list[ModuleInfo] = []

    class _Recorder:
        @hookimpl
        def modulith_after_module_load(self, module: ModuleInfo) -> None:
            seen.append(module)

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    _runtime._extra_plugins.append(_Recorder())

    create_app()

    assert [m.name for m in seen] == ["orders"]
    assert seen[0].package == "fakeapp.orders"


async def test_lifespan_teardown_runs_runtime_shutdown_when_consumer_stop_fails(
    make_fake_app, monkeypatch
) -> None:
    """simultaneous-error (worker half): a consumer.stop() failure during
    lifespan teardown must not skip runtime.shutdown() — otherwise every
    broker connection the worker registered leaks whenever the consumer
    fails to stop cleanly (a very ordinary shutdown-race occurrence, not an
    exotic edge case)."""

    class _FailingStopConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            raise RuntimeError("consumer refused to stop")

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: _FailingStopConsumer(),
    )

    app = create_app()

    shutdown_called = False
    original_shutdown = _runtime.shutdown

    async def _tracking_shutdown() -> None:
        nonlocal shutdown_called
        shutdown_called = True
        await original_shutdown()

    monkeypatch.setattr(_runtime, "shutdown", _tracking_shutdown)

    with pytest.raises(RuntimeError, match="consumer refused to stop"):
        with TestClient(app):
            pass

    assert shutdown_called, "runtime.shutdown() was skipped because consumer.stop() raised"


async def test_lifespan_teardown_combines_both_failures_into_exception_group(
    make_fake_app, monkeypatch
) -> None:
    """simultaneous-error (worker half, both sides failing): when
    consumer.stop() AND runtime.shutdown() both fail, neither error may
    silently displace the other. A bare re-raise of whichever failed last
    would hide the first failure from whoever is debugging the teardown."""

    class _FailingStopConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            raise RuntimeError("consumer refused to stop")

    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: _FailingStopConsumer(),
    )

    app = create_app()

    async def _failing_shutdown() -> None:
        raise RuntimeError("runtime shutdown also failed")

    monkeypatch.setattr(_runtime, "shutdown", _failing_shutdown)

    with pytest.raises(ExceptionGroup) as exc_info:
        with TestClient(app):
            pass

    messages = {str(exc) for exc in exc_info.value.exceptions}
    assert messages == {"consumer refused to stop", "runtime shutdown also failed"}
