"""Per-module worker: a FastAPI app containing only one module's surface.

Used by uvicorn in process-per-module mode. The supervisor spawns one
of these per module; uvicorn calls create_app() to get the ASGI app.

Invoked as:
    uvicorn modulith._worker:create_app --factory \
        --host 127.0.0.1 --port 9001

with MODULITH_MODULE / MODULITH_APP_PACKAGE in the worker's environment.

Critical correctness: the worker imports ONLY the configured module
(plus the contracts module). Other modules are NOT imported. This is
what gives each worker its own GIL — it has its own process, its own
import graph, its own event loop, its own memory.

Cross-module events flow through the broker, not in-memory dispatch. This
factory wires BOTH halves: the runtime routes cross-module *publishes* to the
broker, and the worker's lifespan starts a ``BrokerConsumer`` (see
``modulith._consumer``) that *subscribes* to this module's consumed-event
streams, deserializes each message via its ``event_type`` header, and
dispatches it to the local listeners. The consumer is skipped (HTTP-only
worker) in single topology, when no broker is registered, or when the module
consumes nothing.
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger("modulith.worker")


def create_app() -> FastAPI:
    """Build a FastAPI app exposing only this worker's module.

    Called by uvicorn via ``--factory``. Reads configuration from env vars so
    each worker process has its own context:

      * ``MODULITH_MODULE``       — the single module this worker hosts (required)
      * ``MODULITH_APP_PACKAGE``  — the application package (required)

    Configures modulith with ``auto_discover=False`` so bootstrap imports no
    modules, then selectively imports the contracts module (shared event types,
    if present) and this worker's module. Mounts the module's ``router`` (if it
    exposes one) under ``/<module>`` and adds a ``/health`` endpoint.
    """
    module_name = os.environ.get("MODULITH_MODULE")
    app_package = os.environ.get("MODULITH_APP_PACKAGE")
    if not module_name or not app_package:
        raise RuntimeError(
            "process-per-module worker requires the MODULITH_MODULE and "
            "MODULITH_APP_PACKAGE environment variables to be set"
        )

    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from . import ModuleInfo, bootstrap, configure
    from .protocols import HealthAwareConsumer
    from .runtime import _runtime

    # auto_discover=False: bootstrap must NOT walk and import sibling modules —
    # selective import is the whole point of an isolated worker.
    configure(package=app_package, auto_discover=False, topology="processes")
    bootstrap()

    contracts_module = _runtime.config.contracts_module if _runtime.config else "contracts"
    _import_contracts(app_package, contracts_module)
    module_package = f"{app_package}.{module_name}"
    module = importlib.import_module(module_package)
    _import_manifest(module_package)
    # auto_discover=False means the bootstrap loop above never populates a
    # module list, so it never fires modulith_after_module_load for the
    # module THIS worker imports — plugins that rely on it (startup metrics,
    # module-scoped resources) silently never ran for any process-topology
    # worker. Fire it here, after import + manifest so the hookspec's
    # "after all listeners and event types are wired" contract still holds.
    if _runtime.plugin_manager is not None:
        _runtime.plugin_manager.hook.modulith_after_module_load(
            module=ModuleInfo(name=module_name, package=module_package)
        )
    consumer_name = f"{module_name}:{uuid4().hex}"

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        # Startup: subscribe to this module's consumed-event streams so
        # cross-process events actually get delivered (the consumer half of the
        # process-per-module topology). Teardown: stop the consumer and release
        # the runtime's broker connections so the worker doesn't leak its client.
        consumer = _build_consumer(module_name, consumer_name)
        _app.state.consumer = consumer
        _app.state.legacy_health_warning_emitted = False
        if consumer is not None:
            await consumer.start()
        try:
            yield
        finally:
            # consumer.stop() and _runtime.shutdown() must both be attempted
            # regardless of each other's outcome — a stop() failure used to
            # skip shutdown() entirely, leaking every broker connection the
            # runtime registered on every ordinary stop-time error.
            consumer_error: BaseException | None = None
            if consumer is not None:
                try:
                    await consumer.stop()
                except BaseException as exc:
                    consumer_error = exc
            _app.state.consumer = None
            try:
                await _runtime.shutdown()
            except BaseException as shutdown_error:
                if consumer_error is not None:
                    if isinstance(consumer_error, Exception) and isinstance(
                        shutdown_error, Exception
                    ):
                        raise ExceptionGroup(
                            "worker teardown failed: consumer.stop() and "
                            "runtime.shutdown() both failed",
                            [consumer_error, shutdown_error],
                        ) from None
                    # Prefer propagating BaseException (e.g. CancelledError).
                    raise (
                        consumer_error
                        if not isinstance(consumer_error, Exception)
                        else shutdown_error
                    ) from None
                raise
            if consumer_error is not None:
                raise consumer_error

    app = FastAPI(title=f"modulith-{module_name}", lifespan=lifespan)

    router = getattr(module, "router", None)
    if router is not None:
        app.include_router(router, prefix=f"/{module_name}")
        logger.info("mounted router for module %r under /%s", module_name, module_name)

    @app.get("/health")
    async def health() -> Any:
        consumer = getattr(app.state, "consumer", None)
        if consumer is None:
            return {"status": "ok", "module": module_name}
        if not isinstance(consumer, HealthAwareConsumer):
            warning = "consumer does not expose health"
            if not app.state.legacy_health_warning_emitted:
                logger.warning("worker %r readiness is unknown: %s", module_name, warning)
                app.state.legacy_health_warning_emitted = True
            return {"status": "unknown", "module": module_name, "warning": warning}

        snapshot = consumer.health()
        response: dict[str, Any] = {
            "status": snapshot.status,
            "module": module_name,
            "ready": snapshot.ready,
        }
        if snapshot.detail is not None:
            response["detail"] = snapshot.detail
        if snapshot.ready:
            return response
        return JSONResponse(status_code=503, content=response)

    logger.info("worker app built for module %r (package %r)", module_name, app_package)
    return app


def _build_consumer(module_name: str, consumer_name: str | None = None) -> Any:
    """Build this worker's cross-process consumer, or None when there's nothing to do.

    Returns None — and the worker runs HTTP-only — when topology is not
    ``processes`` or the module has no subscription targets. A process worker
    with targets requires both broker and consumer adapters so delivery cannot
    be silently disabled by incomplete configuration.

    The concrete consumer is built by the scheme's registered factory
    (``modulith_register_consumers``), not hardcoded here — the redis-streams
    factory wraps ``BrokerConsumer``; a DB broker registers its own polling
    consumer.
    """
    from ._consumer import consumer_targets
    from .brokers import ConsumerSpec
    from .config import ConfigurationError
    from .runtime import _runtime
    from .serializers import JsonEventSerializer

    cfg = _runtime.config
    bus = _runtime.event_bus
    broker_registry = _runtime.broker_registry
    consumer_registry = _runtime.consumer_registry
    if cfg is None or bus is None:
        return None
    if cfg.topology == "single":
        return None

    targets = consumer_targets(bus, cfg, module_name)
    if not targets:
        return None
    if broker_registry is None or cfg.broker not in broker_registry.schemes():
        raise ConfigurationError(
            f"module {module_name!r} has broker targets, but no broker adapter "
            f"is registered for scheme {cfg.broker!r}"
        )
    if consumer_registry is None or cfg.broker not in consumer_registry.schemes():
        raise ConfigurationError(
            f"module {module_name!r} has broker targets, but no consumer adapter "
            f"is registered for scheme {cfg.broker!r}"
        )
    if consumer_name is None:
        consumer_name = f"{module_name}:{uuid4().hex}"

    spec = ConsumerSpec(
        scheme=cfg.broker,
        module_name=module_name,
        consumer_name=consumer_name,
        group=f"modulith-{module_name}",
        targets=tuple(targets),
        bus=bus,
        serializer=JsonEventSerializer(allowed_event_types=bus.registered_event_types()),
        broker_registry=broker_registry,
    )
    return consumer_registry.build(cfg.broker, spec)


def _import_contracts(app_package: str, contracts_module: str = "contracts") -> None:
    """Import ``<app_package>.<contracts_module>`` if it exists; tolerate absence.

    Only swallows the "no contracts package" case — a ModuleNotFoundError for
    something the contracts module itself imports must still surface.
    """
    contracts = f"{app_package}.{contracts_module}"
    try:
        importlib.import_module(contracts)
    except ModuleNotFoundError as exc:
        if exc.name == contracts:
            logger.debug("no contracts module under %s", app_package)
            return
        raise


def _import_manifest(module_package: str) -> None:
    """Import the selected module's optional manifest without hiding its failures."""
    manifest_module = f"{module_package}._manifest"
    try:
        importlib.import_module(manifest_module)
    except ModuleNotFoundError as exc:
        if exc.name == manifest_module:
            logger.debug("no manifest module under %s", module_package)
            return
        raise


__all__ = ["create_app"]
