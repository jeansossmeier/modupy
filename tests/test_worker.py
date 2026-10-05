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
import hashlib
import hmac
import logging
import sqlite3
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import modulith._worker as worker_module
from modulith import ConfigurationError, Consumer, ConsumerSpec, EventPublication, configure
from modulith._worker import (
    AccessLogQueryFilter,
    LogTargetQueryFilter,
    _build_consumer,
    create_app,
    health_identity,
    identity_proof,
    install_access_log_filter,
)
from modulith.adapters.shm_broker import ShmBroker, ShmConsumer
from modulith.builtin import outbox
from modulith.config import DEFAULT_MAX_PAYLOAD_BYTES
from modulith.protocols import ConsumerHealth, ConsumerStatus
from modulith.runtime import _runtime
from modulith.serializers import JsonEventSerializer


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
# uvicorn access log: no query strings
# ---------------------------------------------------------------------------

# uvicorn's access record: client, method, request target, HTTP version, status.
_ACCESS_MSG = '%s - "%s %s HTTP/%s" %d'


class _Lines(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def _capturing(name: str) -> Iterator[_Lines]:
    logger = logging.getLogger(name)
    filters, handlers = logger.filters[:], logger.handlers[:]
    level, propagate = logger.level, logger.propagate
    capture = _Lines()
    logger.filters.clear()
    logger.handlers[:] = [capture]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield capture
    logger.filters[:] = filters
    logger.handlers[:] = handlers
    logger.setLevel(level)
    logger.propagate = propagate


@pytest.fixture
def access_log() -> Iterator[_Lines]:
    """uvicorn's access logger with no filters and one capturing handler.

    Every ``create_app()`` in this module leaves the filters installed, so without
    the reset a test would see a filter an earlier test put there.
    """
    yield from _capturing("uvicorn.access")


@pytest.fixture
def error_log() -> Iterator[_Lines]:
    """The same for ``uvicorn.error``, which carries the WebSocket handshake lines."""
    yield from _capturing("uvicorn.error")


def _access_record(target: str) -> logging.LogRecord:
    args = ("127.0.0.1:51234", "GET", target, "1.1", 200)
    return logging.LogRecord("uvicorn.access", logging.INFO, __file__, 0, _ACCESS_MSG, args, None)


@pytest.mark.parametrize(
    ("target", "logged_target"),
    [
        ("/orders/42?token=hunter2&page=2", "/orders/42"),
        ("/orders/42", "/orders/42"),
        ("/orders?next=/a?b=c", "/orders"),
        ("/a%3Fb?token=hunter2", "/a%3Fb"),
    ],
    ids=[
        "with-query",
        "without-query",
        "question-mark-only-in-query",
        "encoded-question-mark-in-path",
    ],
)
def test_access_log_filter_drops_the_query_string_and_keeps_the_rest(
    target: str, logged_target: str
) -> None:
    record = _access_record(target)

    assert AccessLogQueryFilter().filter(record) is True
    assert record.getMessage() == f'127.0.0.1:51234 - "GET {logged_target} HTTP/1.1" 200'


def test_access_log_filter_leaves_a_record_that_is_not_an_access_line_alone() -> None:
    record = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 0, "why?", (), None)

    assert AccessLogQueryFilter().filter(record) is True
    assert record.getMessage() == "why?"


def test_installing_the_access_log_filter_twice_keeps_one_filter_per_logger(
    access_log: _Lines, error_log: _Lines
) -> None:
    install_access_log_filter()
    install_access_log_filter()

    assert [type(f) for f in logging.getLogger("uvicorn.access").filters] == [AccessLogQueryFilter]
    assert [type(f) for f in logging.getLogger("uvicorn.error").filters] == [LogTargetQueryFilter]


def test_create_app_hides_query_strings_in_the_access_log(
    make_fake_app, monkeypatch, access_log: _Lines
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    create_app()
    logging.getLogger("uvicorn.access").info(
        _ACCESS_MSG, "127.0.0.1:51234", "GET", "/orders/42?token=hunter2", "1.1", 200
    )

    assert access_log.lines == ['127.0.0.1:51234 - "GET /orders/42 HTTP/1.1" 200']


# uvicorn's three WebSocket protocols log the handshake on ``uvicorn.error`` with
# these templates: client, request target and, for a rejection that carries a
# response, its status.
@pytest.mark.parametrize(
    ("message", "extra", "logged"),
    [
        ('%s - "WebSocket %s" [accepted]', (), '"WebSocket {target}" [accepted]'),
        ('%s - "WebSocket %s" 403', (), '"WebSocket {target}" 403'),
        ('%s - "WebSocket %s" %d', (404,), '"WebSocket {target}" 404'),
    ],
    ids=["accepted", "closed-403", "rejected-with-status"],
)
@pytest.mark.parametrize(
    ("target", "logged_target"),
    [
        ("/ws/42?token=hunter2&room=a", "/ws/42"),
        ("/ws/42", "/ws/42"),
        ("/ws?next=/a?b=c", "/ws"),
    ],
    ids=["with-query", "without-query", "question-mark-only-in-query"],
)
def test_websocket_handshake_lines_lose_the_query_string(
    error_log: _Lines,
    message: str,
    extra: tuple[int, ...],
    logged: str,
    target: str,
    logged_target: str,
) -> None:
    install_access_log_filter()

    logging.getLogger("uvicorn.error").info(message, "127.0.0.1:51234", target, *extra)

    assert error_log.lines == [f"127.0.0.1:51234 - {logged.format(target=logged_target)}"]


def test_other_uvicorn_error_lines_keep_their_question_marks(error_log: _Lines) -> None:
    install_access_log_filter()

    logging.getLogger("uvicorn.error").error(
        "ASGI callable should return None, but returned '%s'.", "what?x"
    )

    assert error_log.lines == ["ASGI callable should return None, but returned 'what?x'."]


def test_create_app_hides_query_strings_in_websocket_handshake_lines(
    make_fake_app, monkeypatch, error_log: _Lines
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    create_app()
    logging.getLogger("uvicorn.error").info(
        '%s - "WebSocket %s" [accepted]', "127.0.0.1:51234", "/orders/ws?token=hunter2"
    )

    assert error_log.lines == ['127.0.0.1:51234 - "WebSocket /orders/ws" [accepted]']


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


_SIBLING_IMPORT_APP = {
    "contracts": """
        from dataclasses import dataclass
        from modulith import event

        RUNS: list[str] = []

        @event
        @dataclass(frozen=True)
        class PaymentReceived:
            payment_id: str

        @event
        @dataclass(frozen=True)
        class NoteSent:
            note_id: str

        @event
        @dataclass(frozen=True)
        class Audited:
            audit_id: str
    """,
    "orders": """
        from modulith import listener
        from fakeapp.contracts import RUNS, NoteSent, PaymentReceived

        def order_label(order_id: str) -> str:
            return f"order-{order_id}"

        @listener
        async def on_payment(event: PaymentReceived) -> None:
            RUNS.append("orders.on_payment")

        @listener
        async def on_note(event: NoteSent) -> None:
            RUNS.append("orders.on_note")
    """,
    "notifications": """
        from modulith import listener
        from fakeapp.contracts import RUNS, PaymentReceived
        from fakeapp.orders import order_label
        import fakeapp.shared_listeners

        @listener
        async def notify_payment(event: PaymentReceived) -> None:
            RUNS.append("notifications.notify_payment")
    """,
}

_SHARED_LISTENERS = {
    "shared_listeners.py": """
        from modulith import listener
        from fakeapp.contracts import RUNS, Audited

        @listener
        async def audit(event: Audited) -> None:
            RUNS.append("shared.audit")
    """
}


class _CapturingBroker(_NoopBroker):
    def __init__(self) -> None:
        self.targets: list[str] = []

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.targets.append(target)


def _sibling_importing_worker(make_fake_app, monkeypatch) -> tuple[Any, _CapturingBroker]:
    make_fake_app(_SIBLING_IMPORT_APP, extra_files=_SHARED_LISTENERS)
    _set_worker_env(monkeypatch, "notifications")
    create_app()
    broker = _CapturingBroker()
    assert _runtime.broker_registry is not None
    _runtime.broker_registry.register("test-noop-broker", broker)
    return sys.modules["fakeapp.contracts"], broker


def test_sibling_listener_imported_by_module_is_not_dispatched_by_worker(
    make_fake_app, monkeypatch
) -> None:
    contracts, _ = _sibling_importing_worker(make_fake_app, monkeypatch)
    assert "fakeapp.orders" in sys.modules  # the sibling really was imported

    asyncio.run(_runtime.dispatch_local(contracts.PaymentReceived("p1"), _runtime.event_bus))

    assert contracts.RUNS == ["notifications.notify_payment"]


def test_sibling_owned_event_published_by_worker_routes_to_broker(
    make_fake_app, monkeypatch
) -> None:
    contracts, broker = _sibling_importing_worker(make_fake_app, monkeypatch)

    asyncio.run(_runtime.publish(contracts.NoteSent("n1")))

    assert contracts.RUNS == []
    assert broker.targets == ["fakeapp.contracts.NoteSent"]


def test_worker_consumes_only_its_own_modules_event_types(make_fake_app, monkeypatch) -> None:
    contracts, _ = _sibling_importing_worker(make_fake_app, monkeypatch)
    specs: list[ConsumerSpec] = []

    def _capture(spec: ConsumerSpec) -> _NoopConsumer:
        specs.append(spec)
        return _NoopConsumer()

    assert _runtime.consumer_registry is not None
    _runtime.consumer_registry.register("test-noop-broker", _capture)

    assert _build_consumer("notifications") is not None

    assert sorted(specs[0].targets) == [
        "fakeapp.contracts.Audited",
        "fakeapp.contracts.PaymentReceived",
    ]
    serializer = specs[0].serializer
    note = serializer.serialize(contracts.NoteSent("n1"))
    with pytest.raises(Exception, match="NoteSent"):
        serializer.deserialize(note, "fakeapp.contracts.NoteSent")
    payment = serializer.serialize(contracts.PaymentReceived("p1"))
    assert serializer.deserialize(payment, "fakeapp.contracts.PaymentReceived") == (
        contracts.PaymentReceived("p1")
    )


def test_listener_outside_module_packages_still_runs_in_importing_worker(
    make_fake_app, monkeypatch
) -> None:
    contracts, _ = _sibling_importing_worker(make_fake_app, monkeypatch)

    async def plugin_listener(event: Any) -> None:
        contracts.RUNS.append("plugin")

    _runtime.register_listener(contracts.Audited, plugin_listener)
    asyncio.run(_runtime.dispatch_local(contracts.Audited("a1"), _runtime.event_bus))

    assert sorted(contracts.RUNS) == ["plugin", "shared.audit"]


def test_module_imported_by_entry_point_plugin_at_bootstrap_keeps_its_listeners(
    make_fake_app, monkeypatch, tmp_path
) -> None:
    dist_info = tmp_path / "fakeapp_routing-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: fakeapp-routing\nVersion: 1.0\n"
    )
    (dist_info / "entry_points.txt").write_text("[modulith]\nfakeapp_routing = fakeapp.routing\n")
    make_fake_app(
        {
            "contracts": """
                from dataclasses import dataclass
                from modulith import event

                RUNS: list[str] = []

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str
            """,
            "orders": """
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    RUNS.append("orders.on_placed")
            """,
            "inventory": """
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced
                import fakeapp.orders

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    RUNS.append("inventory.on_placed")
            """,
        },
        extra_files={
            "routing.py": """
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced
                import fakeapp.orders

                @listener
                async def audit(event: OrderPlaced) -> None:
                    RUNS.append("routing.audit")
            """
        },
    )
    _set_worker_env(monkeypatch, "inventory")

    create_app()

    assert _runtime.plugin_manager.has_plugin("fakeapp_routing")
    contracts = sys.modules["fakeapp.contracts"]
    asyncio.run(_runtime.dispatch_local(contracts.OrderPlaced("o1"), _runtime.event_bus))
    assert sorted(contracts.RUNS) == ["inventory.on_placed", "routing.audit"]


def _namespace_helper_worker(
    make_fake_app, monkeypatch, *, sibling_imports_helpers: bool = False
) -> Any:
    sibling_imports = (
        "import fakeapp.shared\n                import fakeapp.common.audit"
        if sibling_imports_helpers
        else ""
    )
    make_fake_app(
        {
            "contracts": """
                from dataclasses import dataclass
                from modulith import event

                RUNS: list[str] = []

                @event
                @dataclass(frozen=True)
                class OrderPlaced:
                    order_id: str

                @event
                @dataclass(frozen=True)
                class Audited:
                    audit_id: str
            """,
            "orders": f"""
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced
                {sibling_imports}

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    RUNS.append("orders.on_placed")
            """,
            "notifications": """
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced
                import fakeapp.orders
                import fakeapp.shared
                import fakeapp.common.audit

                @listener
                async def on_placed(event: OrderPlaced) -> None:
                    RUNS.append("notifications.on_placed")
            """,
        },
        extra_files={
            "common/audit.py": """
                from modulith import listener
                from fakeapp.contracts import RUNS, Audited, OrderPlaced

                @listener
                async def audit_placed(event: OrderPlaced) -> None:
                    RUNS.append("common.audit_placed")

                @listener
                async def audit(event: Audited) -> None:
                    RUNS.append("common.audit")
            """,
            "shared.py": """
                from modulith import listener
                from fakeapp.contracts import RUNS, OrderPlaced

                @listener
                async def share_placed(event: OrderPlaced) -> None:
                    RUNS.append("shared.share_placed")
            """,
        },
    )
    _set_worker_env(monkeypatch, "notifications")
    create_app()
    assert _runtime.broker_registry is not None
    _runtime.broker_registry.register("test-noop-broker", _NoopBroker())
    return sys.modules["fakeapp.contracts"]


def test_namespace_folder_listener_runs_in_importing_worker(make_fake_app, monkeypatch) -> None:
    contracts = _namespace_helper_worker(make_fake_app, monkeypatch)
    assert sys.modules["fakeapp.common"].__file__ is None  # a PEP 420 namespace folder
    assert "fakeapp.orders" in sys.modules  # the sibling really was imported

    asyncio.run(_runtime.dispatch_local(contracts.OrderPlaced("o1"), _runtime.event_bus))

    assert sorted(contracts.RUNS) == [
        "common.audit_placed",
        "notifications.on_placed",
        "shared.share_placed",
    ]


def test_helper_listener_belongs_to_sibling_whose_import_loaded_it_first(
    make_fake_app, monkeypatch
) -> None:
    contracts = _namespace_helper_worker(make_fake_app, monkeypatch, sibling_imports_helpers=True)
    audit = sys.modules["fakeapp.common.audit"]
    shared = sys.modules["fakeapp.shared"]
    notifications = sys.modules["fakeapp.notifications"]
    bus = _runtime.event_bus
    assert bus is not None

    assert _runtime._listener_owners[audit.audit_placed] == "fakeapp.orders"
    assert _runtime._listener_owners[shared.share_placed] == "fakeapp.orders"
    assert _runtime.local_listeners(bus.listeners_for(contracts.OrderPlaced)) == [
        notifications.on_placed
    ]
    assert _runtime.local_listeners(bus.listeners_for(contracts.Audited)) == []

    asyncio.run(_runtime.dispatch_local(contracts.OrderPlaced("o1"), bus))

    assert contracts.RUNS == ["notifications.on_placed"]


def test_worker_consumes_event_types_of_namespace_folder_listeners(
    make_fake_app, monkeypatch
) -> None:
    _namespace_helper_worker(make_fake_app, monkeypatch)
    specs: list[ConsumerSpec] = []

    def _capture(spec: ConsumerSpec) -> _NoopConsumer:
        specs.append(spec)
        return _NoopConsumer()

    assert _runtime.consumer_registry is not None
    _runtime.consumer_registry.register("test-noop-broker", _capture)

    assert _build_consumer("notifications") is not None

    assert sorted(specs[0].targets) == [
        "fakeapp.contracts.Audited",
        "fakeapp.contracts.OrderPlaced",
    ]


class _SessionScopedStore:
    def __init__(self) -> None:
        self.rows: dict[Any, EventPublication] = {}

    async def save(self, publication: EventPublication) -> None:
        self.rows[publication.id] = publication

    async def mark_complete(self, publication_id: Any) -> None:
        self.rows.pop(publication_id, None)


def test_transactional_publish_in_worker_skips_sibling_listeners(
    make_fake_app, monkeypatch
) -> None:
    contracts, _ = _sibling_importing_worker(make_fake_app, monkeypatch)
    store = _SessionScopedStore()
    outbox.configure(store, JsonEventSerializer(), start_loop=False)  # type: ignore[arg-type]
    orders_on_note = sys.modules["fakeapp.orders"].on_note
    persisted: list[str | None] = []

    async def scenario() -> None:
        token = outbox._current_session.set(object())
        try:
            await _runtime.publish(contracts.NoteSent("n1"))
        finally:
            outbox._current_session.reset(token)
        persisted.extend(row.listener for row in store.rows.values())
        sibling_row = EventPublication(
            id=uuid4(),
            payload=JsonEventSerializer().serialize(contracts.NoteSent("n2")),
            event_type="fakeapp.contracts.NoteSent",
            listener=outbox._listener_id(orders_on_note),
            published_at=datetime.now(UTC),
        )
        await outbox._dispatch_publication(sibling_row)

    asyncio.run(scenario())

    assert (persisted, contracts.RUNS) == (
        ["__modulith.broker_route__:test-noop-broker:fakeapp.contracts.NoteSent"],
        [],
    )


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


def test_module_without_router_warns_naming_module_and_attribute(
    make_fake_app, monkeypatch, caplog
) -> None:
    """A module with no ``router`` attribute produced a worker that booted,
    reported healthy, and 404'd every request under its prefix without a word
    anywhere. The worker must say what it looked for and where, at WARNING —
    uvicorn leaves the root logger handler-less, so an INFO line would be
    dropped in exactly the deployment that needs it."""
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    caplog.set_level("WARNING", logger="modulith.worker")

    app = create_app()
    with TestClient(app) as client:
        assert client.get("/orders/anything").status_code == 404

    warnings = [r.getMessage() for r in caplog.records if r.name == "modulith.worker"]
    assert any("'router'" in m and "fakeapp.orders" in m and "404" in m for m in warnings), (
        f"no diagnosable warning about the missing router: {warnings}"
    )


def test_module_with_router_does_not_warn(make_fake_app, monkeypatch, caplog) -> None:
    """The companion guard: a module that DOES expose a router must stay
    silent at WARNING, so the missing-router warning keeps its signal."""
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
    caplog.set_level("WARNING", logger="modulith.worker")

    create_app()

    assert [r.getMessage() for r in caplog.records if r.name == "modulith.worker"] == []


# ---------------------------------------------------------------------------
# docs / OpenAPI URLs
#
# The reverse proxy forwards /<module>/* to this worker and nothing else, so
# FastAPI's default doc paths are unreachable from the public port. The worker
# serves them under its own module prefix instead. tests/test_worker_docs_urls.py
# proves the same URLs answer through a real proxy on a real socket.
# ---------------------------------------------------------------------------


def test_docs_and_schema_are_served_under_the_module_prefix(make_fake_app, monkeypatch) -> None:
    """FastAPI defaults /docs, /redoc, /openapi.json and /docs/oauth2-redirect
    to app-root paths, all of which sit outside the /<module> prefix the proxy
    forwards — every one of them 404s on the public port. The worker serves all
    four under its own prefix instead, and moves rather than duplicates them:
    the root copies answer only on the internal port, where a second set of URLs
    for the same schema is just a way to document the wrong one."""
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
        schema = client.get("/orders/openapi.json")
        prefixed = {
            path: client.get(path).status_code
            for path in ("/orders/docs", "/orders/redoc", "/orders/docs/oauth2-redirect")
        }
        unprefixed = {
            path: client.get(path).status_code
            for path in ("/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect")
        }

    assert schema.status_code == 200
    assert "/orders/ping" in schema.json()["paths"]
    assert prefixed == {
        "/orders/docs": 200,
        "/orders/redoc": 200,
        "/orders/docs/oauth2-redirect": 200,
    }
    assert unprefixed == {
        "/openapi.json": 404,
        "/docs": 404,
        "/redoc": 404,
        "/docs/oauth2-redirect": 404,
    }


def test_doc_pages_reference_prefixed_urls(make_fake_app, monkeypatch) -> None:
    """Reaching /<module>/docs is only half of it: the page is useless unless
    the URLs it embeds resolve from the public port too. Swagger UI fetches the
    schema from the openapi_url baked into the page and posts OAuth2 back to
    swagger_ui_oauth2_redirect_url, which FastAPI does NOT derive from docs_url
    — it defaults to the literal "/docs/oauth2-redirect"."""
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        swagger = client.get("/orders/docs").text
        redoc = client.get("/orders/redoc").text

    assert "url: '/orders/openapi.json'" in swagger
    assert "'/orders/docs/oauth2-redirect'" in swagger
    assert 'spec-url="/orders/openapi.json"' in redoc


@pytest.mark.parametrize("module_name", ["orders", "docs"])
def test_module_route_wins_over_generated_doc_route(
    make_fake_app, monkeypatch, module_name: str
) -> None:
    """Serving docs under /<module> puts them inside the module's own URL
    namespace, where they can collide with the module's routes: a module named
    "docs" gets its UI at /docs/docs, and any module defining its own /docs
    route lands on the same path. The application's route must win — the
    reverse is a real route silently shadowed by a generated UI page."""
    make_fake_app(
        {
            module_name: """
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("/docs")
                async def module_docs() -> dict[str, str]:
                    return {"served_by": "module"}
            """
        }
    )
    _set_worker_env(monkeypatch, module_name)

    app = create_app()
    with TestClient(app) as client:
        response = client.get(f"/{module_name}/docs")

    assert response.status_code == 200
    assert response.json() == {"served_by": "module"}


def test_path_parameter_route_does_not_capture_doc_paths(make_fake_app, monkeypatch) -> None:
    """GET /{item_id} matches /orders/openapi.json with item_id="openapi.json",
    so a doc route registered after the module's router never gets a request:
    the module's handler answers 404 instead. Most CRUD modules have such a
    route, which made their worker docs unreachable through the proxy."""
    make_fake_app(
        {
            "orders": """
                from fastapi import APIRouter, HTTPException

                router = APIRouter()

                @router.get("/{item_id}")
                async def get_item(item_id: str) -> dict[str, str]:
                    raise HTTPException(status_code=404, detail="no such item")
            """
        }
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        schema = client.get("/orders/openapi.json")
        swagger = client.get("/orders/docs")
        redoc = client.get("/orders/redoc")
        item = client.get("/orders/42")

    assert schema.status_code == 200
    assert "/orders/{item_id}" in schema.json()["paths"]
    assert swagger.status_code == 200
    assert "swagger-ui" in swagger.text
    assert redoc.status_code == 200
    assert "redoc" in redoc.text
    assert item.status_code == 404
    assert item.json() == {"detail": "no such item"}


@pytest.mark.parametrize("doc_path", ["openapi.json", "docs", "redoc"])
def test_exact_module_route_wins_over_doc_route_beside_path_parameter(
    make_fake_app, monkeypatch, doc_path: str
) -> None:
    """Registering the doc routes first must not hand them a path the module
    defines exactly: its own /<doc path> route still answers, while the other
    doc paths keep working next to the module's GET /{item_id}."""
    make_fake_app(
        {
            "orders": f"""
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("/{doc_path}")
                async def module_doc() -> dict[str, str]:
                    return {{"served_by": "module"}}

                @router.get("/{{item_id}}")
                async def get_item(item_id: str) -> dict[str, str]:
                    return {{"item": item_id}}
            """
        }
    )
    _set_worker_env(monkeypatch, "orders")

    app = create_app()
    with TestClient(app) as client:
        own = client.get(f"/orders/{doc_path}")
        others = {
            other: client.get(f"/orders/{other}")
            for other in ("openapi.json", "docs", "redoc")
            if other != doc_path
        }

    assert own.json() == {"served_by": "module"}
    assert {name: response.status_code for name, response in others.items()} == {
        name: 200 for name in others
    }
    assert not any(
        "served_by" in response.text or '"item"' in response.text for response in others.values()
    )


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


def test_consumer_serializer_defaults_to_default_max_payload_bytes(
    make_fake_app, monkeypatch
) -> None:
    """A consumer built with no cap configured must still carry the same
    16 MiB default the broker adapters fall back to — never an unbounded
    serializer that silently disagrees with the broker's own default."""
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
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)

    app = create_app()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    _runtime.broker_registry.register("test-noop-broker", _NoopBroker())
    _runtime.consumer_registry.register("test-noop-broker", build_consumer)

    with TestClient(app):
        pass

    (spec,) = built_specs
    assert spec.serializer._max_payload_bytes == DEFAULT_MAX_PAYLOAD_BYTES


def test_consumer_serializer_carries_configured_broker_options_cap(
    make_fake_app, monkeypatch
) -> None:
    """A deployment whose broker cap is raised via broker_options must reach
    the consumer's deserializer too — otherwise the broker accepts a payload
    the consumer then dead-letters as oversized."""
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
    monkeypatch.delenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", raising=False)
    configure(broker_options={"max_payload_bytes": 33554432})

    app = create_app()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    _runtime.broker_registry.register("test-noop-broker", _NoopBroker())
    _runtime.consumer_registry.register("test-noop-broker", build_consumer)

    with TestClient(app):
        pass

    (spec,) = built_specs
    assert spec.serializer._max_payload_bytes == 33554432


def test_consumer_serializer_carries_env_override_cap(make_fake_app, monkeypatch) -> None:
    """MODULITH_BROKER_MAX_PAYLOAD_BYTES must reach the consumer's
    deserializer with the same precedence it has for every broker adapter:
    highest priority, overriding any broker_options value."""
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
    monkeypatch.setenv("MODULITH_BROKER_MAX_PAYLOAD_BYTES", "1048576")
    configure(broker_options={"max_payload_bytes": 33554432})

    app = create_app()
    assert _runtime.broker_registry is not None
    assert _runtime.consumer_registry is not None
    _runtime.broker_registry.register("test-noop-broker", _NoopBroker())
    _runtime.consumer_registry.register("test-noop-broker", build_consumer)

    with TestClient(app):
        pass

    (spec,) = built_specs
    assert spec.serializer._max_payload_bytes == 1048576


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


def test_shm_worker_broker_applies_forwarded_orphan_retention(
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
    monkeypatch.setenv("MODULITH_BROKER_ORPHAN_RETENTION_SECONDS", "900")

    create_app()
    assert _runtime.broker_registry is not None
    broker = _runtime.broker_registry.get("shm")
    assert isinstance(broker, ShmBroker)
    asyncio.run(broker.publish("events.Created", b"{}"))

    connection = sqlite3.connect(database_path)
    try:
        windows = connection.execute(
            "SELECT retained_until - created_at FROM shm_publication"
        ).fetchall()
    finally:
        connection.close()
    assert windows == [(pytest.approx(900.0),)]


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


def test_database_worker_without_listeners_warns_about_its_previous_subscriptions(
    make_fake_app,
    monkeypatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    from modulith.adapters.db_broker import DatabaseBroker, DatabaseConsumer

    url = f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}"

    async def _previous_deployment() -> None:
        engine = create_async_engine(url, poolclass=NullPool)
        try:
            previous = DatabaseBroker(engine=engine)
            await previous.subscribe(["events.PreviousListener"], "modulith-orders")
            await previous.publish("events.PreviousListener", b"{}", {"event_type": "x"})
        finally:
            await engine.dispose()

    asyncio.run(_previous_deployment())
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_BROKER", "database")
    monkeypatch.setenv("MODULITH_BROKER_URL", url)
    caplog.set_level("WARNING")

    app = create_app()
    with TestClient(app):
        assert isinstance(app.state.consumer, DatabaseConsumer)

    warnings = [
        r.getMessage()
        for r in caplog.records
        if "modulith broker drop-group modulith-orders --target events.PreviousListener"
        in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "1 undelivered" in warnings[0]


async def _db_rows(url: str, group: str) -> list[tuple[str, str]]:
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    from modulith.adapters.db_broker import broker_schema

    _, _, message = broker_schema()
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                select(message.c.target, message.c.status)
                .where(message.c.consumer_group == group)
                .order_by(message.c.target)
            )
            return [(str(row[0]), str(row[1])) for row in result]
    finally:
        await engine.dispose()


def test_upgraded_database_worker_leaves_a_siblings_stale_target_to_explicit_cleanup(
    make_fake_app,
    monkeypatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The earlier release subscribed notifications to the sibling-owned NoteSent."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool
    from typer.testing import CliRunner

    from modulith.adapters.db_broker import DatabaseBroker, DatabaseConsumer
    from modulith.cli import app as cli_app

    url = f"sqlite+aiosqlite:///{tmp_path / 'upgrade.db'}"
    note = "fakeapp.contracts.NoteSent"
    payment = "fakeapp.contracts.PaymentReceived"
    group = "modulith-notifications"

    async def _previous_release_subscribes() -> None:
        engine = create_async_engine(url, poolclass=NullPool)
        try:
            previous = DatabaseBroker(engine=engine)
            await previous.subscribe([note, payment, "fakeapp.contracts.Audited"], group)
            await previous.subscribe([note], "modulith-orders")
        finally:
            await engine.dispose()

    asyncio.run(_previous_release_subscribes())
    make_fake_app(_SIBLING_IMPORT_APP, extra_files=_SHARED_LISTENERS)
    _set_worker_env(monkeypatch, "notifications")
    monkeypatch.setenv("MODULITH_BROKER", "database")
    monkeypatch.setenv("MODULITH_BROKER_URL", url)
    monkeypatch.setenv("MODULITH_BROKER_POLL_INTERVAL_MS", "10")
    caplog.set_level("WARNING")

    app = create_app()
    contracts = sys.modules["fakeapp.contracts"]
    serializer = JsonEventSerializer()
    with TestClient(app):
        assert isinstance(app.state.consumer, DatabaseConsumer)
        assert _runtime.broker_registry is not None
        broker = _runtime.broker_registry.get("database")

        async def _publish_after_upgrade() -> None:
            for index in range(3):
                await broker.publish(
                    note,
                    serializer.serialize(contracts.NoteSent(f"n{index}")),
                    {"event_type": note},
                )
            await broker.publish(
                payment,
                serializer.serialize(contracts.PaymentReceived("p1")),
                {"event_type": payment},
            )

        asyncio.run(_publish_after_upgrade())
        deadline = time.monotonic() + 5.0
        while contracts.RUNS != ["notifications.notify_payment"]:
            assert time.monotonic() < deadline, contracts.RUNS
            time.sleep(0.02)
        time.sleep(0.2)  # several more polls over the pending NoteSent rows
        rows = asyncio.run(_db_rows(url, group))

    assert [status for target, status in rows if target == note] == ["pending"] * 3
    assert "dead" not in {status for _target, status in rows}
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    warnings = [
        r.getMessage()
        for r in caplog.records
        if f"modulith broker drop-group {group} --target {note}" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "0 undelivered" in warnings[0]  # counted at start, before these publishes

    _runtime._reset_for_testing()
    monkeypatch.setenv("MODULITH_PACKAGE", "fakeapp")
    dropped = CliRunner().invoke(
        cli_app, ["broker", "drop-group", group, "--target", note, "--yes"]
    )
    assert dropped.exit_code == 0, dropped.output
    assert "1 subscription(s)" in dropped.output
    assert "3 pending or claimed delivery(ies)" in dropped.output

    async def _publish_after_cleanup() -> None:
        engine = create_async_engine(url, poolclass=NullPool)
        try:
            await DatabaseBroker(engine=engine).publish(note, b"{}", {"event_type": note})
        finally:
            await engine.dispose()

    asyncio.run(_publish_after_cleanup())
    assert [row for row in asyncio.run(_db_rows(url, group)) if row[0] == note] == []
    assert asyncio.run(_db_rows(url, "modulith-orders")) == [(note, "pending")] * 4


# ---------------------------------------------------------------------------
# Module lifecycle fires for the isolated worker's own module, and lifespan
# teardown always reaches runtime/broker shutdown
# ---------------------------------------------------------------------------


def test_worker_fires_after_module_load_for_its_own_module(make_fake_app, monkeypatch) -> None:
    """The monolith's bootstrap fires
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
    """A consumer.stop() failure during
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
    """When both sides fail —
    consumer.stop() AND runtime.shutdown() — neither error may
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


async def test_lifespan_startup_failure_still_runs_runtime_shutdown(
    make_fake_app, monkeypatch
) -> None:
    """A consumer.start() failure (broker down, group creation denied, bad
    credentials) must still reach runtime.shutdown(). uvicorn exits the
    process on a lifespan-startup error so the OS reclaims the sockets, but
    an embedder that keeps the process alive — a test harness, a retry loop —
    leaks every broker client the runtime registered on each attempt."""

    class _FailingStartConsumer:
        def __init__(self) -> None:
            self.stopped = False

        async def start(self) -> None:
            raise RuntimeError("broker unreachable at startup")

        async def stop(self) -> None:
            self.stopped = True

    consumer = _FailingStartConsumer()
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setattr(
        worker_module,
        "_build_consumer",
        lambda _module, _consumer_name: consumer,
    )

    app = create_app()

    shutdown_called = False
    original_shutdown = _runtime.shutdown

    async def _tracking_shutdown() -> None:
        nonlocal shutdown_called
        shutdown_called = True
        await original_shutdown()

    monkeypatch.setattr(_runtime, "shutdown", _tracking_shutdown)

    with pytest.raises(RuntimeError, match="broker unreachable at startup"):
        with TestClient(app):
            pass

    assert shutdown_called, "runtime.shutdown() was skipped because consumer.start() raised"
    assert consumer.stopped, "consumer.stop() was skipped because consumer.start() raised"


async def test_lifespan_consumer_build_failure_still_runs_runtime_shutdown(
    make_fake_app, monkeypatch
) -> None:
    """Same leak, one step earlier: _build_consumer() raising (a missing
    broker/consumer adapter for the configured scheme) must not bypass
    runtime.shutdown() either — bootstrap has already run by then, so the
    runtime owns registrations that need releasing."""
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")

    def _explode(_module, _consumer_name):
        raise ConfigurationError("no consumer adapter registered")

    monkeypatch.setattr(worker_module, "_build_consumer", _explode)

    app = create_app()

    shutdown_called = False
    original_shutdown = _runtime.shutdown

    async def _tracking_shutdown() -> None:
        nonlocal shutdown_called
        shutdown_called = True
        await original_shutdown()

    monkeypatch.setattr(_runtime, "shutdown", _tracking_shutdown)

    with pytest.raises(ConfigurationError, match="no consumer adapter registered"):
        with TestClient(app):
            pass

    assert shutdown_called, "runtime.shutdown() was skipped because _build_consumer() raised"


def test_health_endpoint_never_returns_the_deployment_token(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_DEPLOYMENT_TOKEN", "deployment-b")

    app = create_app()
    with TestClient(app) as client:
        plain = client.get("/health")
        challenged = client.get("/health", headers={"x-modulith-nonce": "n-1"})

    assert (plain.status_code, plain.json()) == (200, {"status": "ok", "module": "orders"})
    assert "deployment-b" not in plain.text
    assert "deployment-b" not in challenged.text
    assert "deployment" not in challenged.json()


def test_health_answers_a_nonce_with_an_hmac_over_nonce_module_and_arrival_port(
    make_fake_app, monkeypatch
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_DEPLOYMENT_TOKEN", "deployment-b")

    app = create_app()
    with TestClient(app, base_url="http://testserver:8123") as client:
        resp = client.get(
            "/health",
            headers={
                "x-modulith-nonce": "n-1",
                # Headers a client controls must not move the port the proof binds.
                "x-forwarded-port": "9999",
                "x-forwarded-host": "evil.example:9999",
            },
        )

    expected = hmac.new(b"deployment-b", b"n-1\norders\n8123", hashlib.sha256).hexdigest()
    assert resp.json() == {"status": "ok", "module": "orders", "proof": expected}


def test_identity_proof_differs_per_token_nonce_module_and_port() -> None:
    base = identity_proof("deployment-b", "n-1", "orders", 8123)

    assert base == identity_proof("deployment-b", "n-1", "orders", 8123)
    others = {
        identity_proof("deployment-a", "n-1", "orders", 8123),
        identity_proof("deployment-b", "n-2", "orders", 8123),
        identity_proof("deployment-b", "n-1", "inventory", 8123),
        identity_proof("deployment-b", "n-1", "orders", 8124),
    }
    assert base not in others and len(others) == 4


@pytest.mark.parametrize("server", [None, ("testserver", None), ("/run/worker.sock", None)])
def test_health_identity_offers_no_proof_without_a_tcp_port(server) -> None:
    assert health_identity("orders", "deployment-b", "n-1", server) == {"module": "orders"}


def test_health_without_a_token_still_serves_and_offers_no_proof(
    make_fake_app, monkeypatch
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.delenv("MODULITH_DEPLOYMENT_TOKEN", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/health", headers={"x-modulith-nonce": "n-1"})

    assert (resp.status_code, resp.json()) == (200, {"status": "ok", "module": "orders"})


def test_health_without_a_nonce_offers_no_proof(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_DEPLOYMENT_TOKEN", "deployment-b")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/health")

    assert resp.json() == {"status": "ok", "module": "orders"}


def test_a_module_named_health_cannot_shadow_the_workers_own_health(
    make_fake_app, monkeypatch
) -> None:
    make_fake_app(
        {
            "health": """
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("")
                async def root():
                    return {"module_says": "hi"}

                @router.get("/detail")
                async def detail():
                    return {"module_says": "detail"}
            """
        }
    )
    _set_worker_env(monkeypatch, "health")
    monkeypatch.setenv("MODULITH_DEPLOYMENT_TOKEN", "deployment-b")

    app = create_app()
    with TestClient(app) as client:
        own = client.get("/health")
        module_route = client.get("/health/detail")

    assert own.json() == {"status": "ok", "module": "health"}
    assert module_route.json() == {"module_says": "detail"}


def test_a_module_route_the_workers_own_health_hides_is_warned_about_at_startup(
    make_fake_app, monkeypatch, caplog
) -> None:
    make_fake_app(
        {
            "health": """
                from fastapi import APIRouter

                router = APIRouter()

                @router.get("")
                async def root():
                    return {"module_says": "hi"}

                @router.get("/detail")
                async def detail():
                    return {"module_says": "detail"}
            """
        }
    )
    _set_worker_env(monkeypatch, "health")

    with caplog.at_level(logging.WARNING, logger="modulith.worker"):
        create_app()

    shadowed = [r.getMessage() for r in caplog.records if "unreachable" in r.getMessage()]
    assert len(shadowed) == 1
    assert "GET /health" in shadowed[0]
    assert "/health/detail" not in shadowed[0]


# ---------------------------------------------------------------------------
# durable outbox wiring
# ---------------------------------------------------------------------------

_MODULE_WIRING_OUTBOX = """
    from datetime import timedelta

    from modulith.builtin import outbox
    from modulith.serializers import JsonEventSerializer

    SWEEPS: list[timedelta] = []

    class Store:
        async def save(self, publication):
            pass

        async def find_incomplete(self, older_than):
            SWEEPS.append(older_than)
            return []

    outbox.configure(Store(), JsonEventSerializer(), retry_interval_seconds=60)
"""


@pytest.fixture
def _fresh_outbox() -> Any:
    outbox._reset_for_testing()
    yield
    outbox._reset_for_testing()


def test_worker_refuses_durable_outbox_without_a_bound_store(
    make_fake_app, monkeypatch, _fresh_outbox
) -> None:
    make_fake_app({"orders": ""})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")

    with pytest.raises(ConfigurationError) as exc_info:
        create_app()

    message = str(exc_info.value)
    assert ("'postgres'" in message, "main.py" in message, "MODULITH_OUTBOX_URL" in message) == (
        True,
        True,
        True,
    )


def test_worker_binds_store_from_outbox_url_for_its_module_events(
    make_fake_app, monkeypatch, tmp_path, _fresh_outbox
) -> None:
    from modulith.adapters import postgres_outbox

    make_fake_app(_SIBLING_IMPORT_APP)
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")

    create_app()
    serializer = outbox._serializer
    store_type = type(outbox._store).__name__
    asyncio.run(_runtime.shutdown())

    assert (
        store_type,
        sorted(serializer._allowed_event_types),
        postgres_outbox._active_store,
    ) == (
        "PostgresPublicationStore",
        ["fakeapp.contracts.NoteSent", "fakeapp.contracts.PaymentReceived"],
        None,
    )


def test_worker_starts_retry_loop_for_store_bound_at_module_import(
    make_fake_app, monkeypatch, _fresh_outbox
) -> None:
    make_fake_app({"orders": _MODULE_WIRING_OUTBOX})
    _set_worker_env(monkeypatch, "orders")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")

    app = create_app()
    sweeps = sys.modules["fakeapp.orders"].SWEEPS
    before_startup = list(sweeps)
    with TestClient(app):
        deadline = time.monotonic() + 2.0
        while not sweeps and time.monotonic() < deadline:
            time.sleep(0.01)

    assert (before_startup, sweeps[:1]) == ([], [timedelta(0)])


_SHARED_LISTENER_CLASS_APP = {
    "contracts": """
        from dataclasses import dataclass

        from modulith import event

        RUNS: list[tuple[str, str]] = []

        @event
        @dataclass(frozen=True)
        class OrderPlaced:
            order_id: str

        class Notifier:
            def __init__(self, name: str) -> None:
                self.name = name

            async def __call__(self, evt: OrderPlaced) -> None:
                RUNS.append((self.name, evt.order_id))
    """,
    "orders": """
        from modulith import listener
        from fakeapp.contracts import Notifier

        listener(Notifier("orders"))
    """,
    "billing": """
        from modulith import listener
        from fakeapp.contracts import RUNS, Notifier, OrderPlaced

        listener(Notifier("billing"))

        @listener
        async def on_placed(evt: OrderPlaced) -> None:
            RUNS.append(("billing.on_placed", evt.order_id))
    """,
}


@pytest.mark.parametrize("claim_strategy", ["lease", "none"])
def test_worker_sweep_delivers_only_rows_its_module_owns(
    make_fake_app, monkeypatch, tmp_path, _fresh_outbox, claim_strategy
) -> None:
    from sqlalchemy.ext.asyncio import create_async_engine

    from modulith.adapters import postgres_outbox

    make_fake_app(_SHARED_LISTENER_CLASS_APP)
    _set_worker_env(monkeypatch, "orders")
    create_app()
    contracts = sys.modules["fakeapp.contracts"]
    payload = JsonEventSerializer().serialize(contracts.OrderPlaced("o1"))
    listeners = [
        "fakeapp.billing:fakeapp.contracts.Notifier",
        "fakeapp.billing.on_placed",
        "fakeapp.orders:fakeapp.contracts.Notifier",
    ]

    async def scenario() -> list[tuple[str, bool, int]]:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'shared.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(postgres_outbox.Base.metadata.create_all)
        store = postgres_outbox.PostgresPublicationStore(engine)
        outbox.configure(
            store,
            JsonEventSerializer(allowed_event_types=[contracts.OrderPlaced]),
            start_loop=False,
            claim_strategy=claim_strategy,
        )
        for listener_id in listeners:
            await store.save(
                EventPublication(
                    id=uuid4(),
                    payload=payload,
                    event_type="fakeapp.contracts.OrderPlaced",
                    listener=listener_id,
                    published_at=datetime.now(UTC),
                )
            )
        await outbox._sweep(timedelta(0))
        rows = await store.find_incomplete(timedelta(0))
        await store.dispose()
        await engine.dispose()
        return sorted((p.listener or "", p.completed_at is None, p.attempt_count) for p in rows)

    pending = asyncio.run(scenario())

    assert (pending, contracts.RUNS) == (
        [
            ("fakeapp.billing.on_placed", True, 0),
            ("fakeapp.billing:fakeapp.contracts.Notifier", True, 0),
        ],
        [("orders", "o1")],
    )
