"""The modulith runtime singleton.

Holds global state: configuration, plugin manager, event bus, modules.
Lazily initializes on first use — importing modulith does no work, but
the first @listener registration or publish() call triggers bootstrap.

This pattern is what makes "just import and use" feel natural. The user
doesn't construct anything; the framework constructs itself when needed.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .brokers import BrokerRegistry
from .config import Configuration, ConfigurationError, load_configuration
from .discovery import detect_application_package
from .event_bus import InMemoryEventBus
from .manager import create_plugin_manager
from .types import EventPublication, ModuleInfo

logger = logging.getLogger("modulith")


class Runtime:
    """The lazily-initialized modulith runtime.

    A single instance lives at module scope (`_runtime` below); decorators
    and publish() reach through it. Bootstrap happens once, the first
    time any operation requires it. Subsequent calls are fast — the
    bootstrapped flag short-circuits the lock.
    """

    def __init__(self) -> None:
        # Bootstrap state.
        self._bootstrapped = False
        # Lock around the bootstrap path so concurrent first-uses don't
        # race. After bootstrap, the flag is read without locking.
        self._lock = threading.Lock()

        # Caller-supplied overrides via configure(). Applied during boot.
        self._config_overrides: dict[str, Any] = {}

        # Populated during _bootstrap().
        self._config: Configuration | None = None
        self._plugin_manager: Any = None
        self._event_bus: InMemoryEventBus | None = None
        self._broker_registry: BrokerRegistry | None = None
        self._modules: list[ModuleInfo] = []

        # Listeners registered before bootstrap go here, then flush
        # into the real event bus once it exists.
        self._pending_listeners: list[tuple[type, Callable[..., Any]]] = []

        # Plugins injected programmatically (not via entry points), registered
        # at bootstrap. Embedding contexts — most notably the pytest plugin's
        # event-capturing spy — append here before triggering bootstrap.
        self._extra_plugins: list[Any] = []

    # ----- Accessors (read-only views for plugins and tooling) -------------

    @property
    def broker_registry(self) -> BrokerRegistry | None:
        """The broker dispatch registry, or None before bootstrap."""
        return self._broker_registry

    @property
    def plugin_manager(self) -> Any:
        """The pluggy PluginManager, or None before bootstrap."""
        return self._plugin_manager

    @property
    def event_bus(self) -> InMemoryEventBus | None:
        """The in-memory event bus, or None before bootstrap."""
        return self._event_bus

    @property
    def config(self) -> Configuration | None:
        """The resolved configuration, or None before bootstrap."""
        return self._config

    @property
    def modules(self) -> list[ModuleInfo]:
        """Discovered application modules (empty before bootstrap).

        A read-only snapshot for tooling (the CLI's ``info``/``verify``/
        ``docs`` commands) — mutating the returned list does not affect
        the runtime's own module set.
        """
        return list(self._modules)

    # ----- Public API -------------------------------------------------------

    def configure(self, **overrides: Any) -> None:
        """Set configuration overrides. Must be called before bootstrap.

        Holds the bootstrap lock while checking the flag and recording the
        override, mirroring ensure_bootstrapped's double-checked locking. Read
        the flag without the lock and a configure() racing a concurrent
        first-publish could pass the guard *after* _bootstrap already consumed
        _config_overrides — silently dropping the override instead of raising.
        """
        with self._lock:
            if self._bootstrapped:
                raise ConfigurationError(
                    "configure() cannot be called after modulith has bootstrapped. "
                    "Call it at application startup, before any @listener "
                    "registration or publish() call."
                )
            self._config_overrides.update(overrides)

    def register_listener(self, event_type: type, handler: Callable[..., Any]) -> None:
        """Register a listener. Works before, during, and after bootstrap.

        Pre-bootstrap (no bus yet): queue for flush during bootstrap.
        Mid-bootstrap (bus exists, discovery running): register directly.
        Post-bootstrap: register directly.

        Gating on the bus's existence (not the bootstrapped flag) is
        what makes module-level @listener decorators work correctly when
        they fire during the discovery import phase.
        """
        if self._event_bus is not None:
            self._event_bus.register(event_type, handler)
        else:
            self._pending_listeners.append((event_type, handler))

    async def publish(self, event: Any) -> None:
        """Publish an event. Triggers bootstrap if not yet done.

        Sequence:
          1. Run ``modulith_before_event_published`` — the outbox plugin
             persists the event here when a transaction session is bound;
             observability starts a publish span.
          2. If the durable path captured this publish (an outbox store is
             configured AND a session is bound), return now — dispatch
             happens after the business transaction commits, via the
             adapter's after-commit hook. Dispatching in-memory too would
             both double-fire listeners and run them before commit.
          3. Otherwise dispatch in-memory, firing the per-listener
             observability hooks around each listener invocation.
        """
        self.ensure_bootstrapped()
        assert self._plugin_manager is not None  # for type-checker
        pm = self._plugin_manager

        pm.hook.modulith_before_event_published(event=event)

        if self._outbox_owns_dispatch():
            # Durable path: persist one record per listener inside the bound
            # transaction. Dispatch happens after commit via the adapter's
            # after-commit hook — never in-memory here (that would double-fire
            # local listeners and run them before the business commit).
            # Broker routing is separate: process-mode remote consumers still
            # need the serialized event even when local listener dispatch is
            # delayed by the transactional outbox.
            from .builtin import outbox

            assert self._event_bus is not None
            handlers = self._event_bus.listeners_for(type(event))
            await outbox.persist(event)
            await self._maybe_route_to_broker(event, has_local_handler=bool(handlers))
            # Fire the post-publish hook on the durable path too: the event is
            # now persisted, which is exactly what the hookspec documents
            # ("after an event has been persisted to the outbox"). Omitting it
            # here made metrics/tracing plugins miss every transactional
            # publish — the production path the outbox exists for.
            event_type = f"{type(event).__module__}.{type(event).__qualname__}"
            publish_pub = EventPublication(
                id=uuid4(), payload=b"", event_type=event_type, published_at=datetime.now(UTC)
            )
            pm.hook.modulith_after_event_published(event=event, publication=publish_pub)
            return

        await self._dispatch_with_hooks(event)

    def _outbox_owns_dispatch(self) -> bool:
        """True when the durable outbox path will dispatch this publish.

        The runtime stays storage-agnostic: it consults the first-party
        outbox plugin's module state rather than knowing any adapter. The
        durable path owns dispatch only when a store is configured *and* a
        transaction session is bound to the current context.
        """
        from .builtin import outbox

        return outbox._store is not None and outbox._current_session.get() is not None

    async def _dispatch_with_hooks(self, event: Any) -> None:
        """Dispatch in-memory, wrapping each listener with lifecycle hooks.

        Behaviorally equivalent to ``InMemoryEventBus.publish`` (listeners
        run concurrently; every failure is logged; the first exception is
        re-raised after all complete) but additionally fires the
        per-listener observability hooks and the post-publish hook.
        """
        assert self._event_bus is not None  # for type-checker
        assert self._plugin_manager is not None
        bus = self._event_bus
        pm = self._plugin_manager

        event_type = f"{type(event).__module__}.{type(event).__qualname__}"
        publish_pub = EventPublication(
            id=uuid4(), payload=b"", event_type=event_type, published_at=datetime.now(UTC)
        )

        handlers = bus.listeners_for(type(event))
        if not handlers:
            # No local listener. In process-per-module topology this is a
            # cross-module event bound for a worker in another process —
            # route it to the broker. In single topology it simply has no
            # consumers.
            await self._maybe_route_to_broker(event, has_local_handler=False)
            pm.hook.modulith_after_event_published(event=event, publication=publish_pub)
            return

        async def _run_one(handler: Callable[..., Any]) -> None:
            name = getattr(handler, "__qualname__", repr(handler))
            pub = EventPublication(
                id=uuid4(),
                payload=b"",
                event_type=event_type,
                listener=name,
                published_at=datetime.now(UTC),
            )
            pm.hook.modulith_on_listener_dispatch(event=event, listener_name=name, publication=pub)
            try:
                await handler(event)
            except BaseException as exc:
                pm.hook.modulith_on_listener_error(
                    event=event, listener_name=name, publication=pub, exception=exc
                )
                pm.hook.modulith_on_listener_complete(
                    event=event, listener_name=name, publication=pub, exception=exc
                )
                raise
            pm.hook.modulith_on_listener_complete(
                event=event, listener_name=name, publication=pub, exception=None
            )

        results = await asyncio.gather(*(_run_one(h) for h in handlers), return_exceptions=True)

        # Fan-out: an externalized event also crosses to the broker even though
        # it has local listeners here — remote workers consume it too. Routing
        # is independent of local delivery, so a local listener failure (raised
        # below) must not suppress it. No-op unless the event resolves to a
        # remote target (see _maybe_route_to_broker).
        await self._maybe_route_to_broker(event, has_local_handler=True)

        pm.hook.modulith_after_event_published(event=event, publication=publish_pub)

        first_error: BaseException | None = None
        for handler, result in zip(handlers, results, strict=False):
            if isinstance(result, BaseException):
                logger.error(
                    "listener %s failed for %s: %s",
                    getattr(handler, "__qualname__", repr(handler)),
                    type(event).__name__,
                    result,
                )
                if first_error is None:
                    first_error = result
        if first_error is not None:
            raise first_error

    def _resolve_broker_target(self, event: Any) -> str | None:
        """Resolve an event's explicit broker target, or None for the default.

        Priority (highest first):
          1. ``modulith_resolve_event_target`` hook (firstresult) — dynamic
             routing: tenant-aware topics, A/B channels, feature-flag reroutes.
          2. ``@externalized(target="scheme:dest")`` — a static per-event
             override stored as ``__modulith_broker_target__``.
          3. ``@externalized`` (bare) — the event opts into the *default*
             scheme; returns ``{broker}:{fully-qualified-name}``.

        Returns None when the event carries no externalization signal at all —
        the caller decides whether the listener-less default still applies.
        """
        assert self._plugin_manager is not None
        cfg = self._config
        assert cfg is not None

        resolved = self._plugin_manager.hook.modulith_resolve_event_target(event=event)
        if resolved is not None:
            return str(resolved)

        cls = type(event)
        explicit = getattr(cls, "__modulith_broker_target__", None)
        if explicit is not None:
            return str(explicit)

        if getattr(cls, "__modulith_externalized__", False):
            fqn = f"{cls.__module__}.{cls.__qualname__}"
            return f"{cfg.broker}:{fqn}"

        return None

    async def _maybe_route_to_broker(self, event: Any, *, has_local_handler: bool) -> None:
        """Route an event to the configured broker when it crosses processes.

        Only fires in non-``single`` topology. An event reaches the broker when
        EITHER:
          * it resolves to an explicit/dynamic target (the
            ``modulith_resolve_event_target`` hook or an ``@externalized``
            annotation) — routed *regardless* of local listeners, so a fan-out
            event consumed both locally and remotely still reaches remote
            workers; OR
          * it has no local listener — a cross-module event whose only consumer
            lives in another worker process — routed under the default scheme.

        An event with a local listener and no externalization signal stays
        in-process (no broker traffic). The destination is the fully-qualified
        event name (module + qualname), matching the ``event_type`` header so a
        future consumer keys producer and consumer identically.

        No-ops when topology is ``single`` or no broker is registered for the
        configured scheme (the event then simply has no remote consumers).
        """
        cfg = self._config
        if cfg is None or cfg.topology == "single":
            return
        registry = self._broker_registry
        if registry is None or cfg.broker not in registry.schemes():
            return

        target = self._resolve_broker_target(event)
        if target is None:
            # No externalization signal. Route only a listener-less event, by
            # the default scheme; an in-process-only event isn't broker traffic.
            if has_local_handler:
                return
            fqn = f"{type(event).__module__}.{type(event).__qualname__}"
            target = f"{cfg.broker}:{fqn}"

        from .serializers import JsonEventSerializer

        event_type = f"{type(event).__module__}.{type(event).__qualname__}"
        payload = JsonEventSerializer().serialize(event)
        await registry.publish(target, payload, {"event_type": event_type})
        logger.debug("routed %s to broker target %s", event_type, target)

    def ensure_bootstrapped(self) -> None:
        """Run bootstrap if not yet done. Safe to call repeatedly."""
        # Fast path: already bootstrapped, no lock needed.
        if self._bootstrapped:
            return
        # Slow path: take the lock and re-check (double-checked locking).
        with self._lock:
            # mypy's flow analysis can't see that another thread may have
            # set _bootstrapped between the fast-path check and acquiring
            # the lock, so it flags the inner return as unreachable. The
            # branch is the entire reason DCL exists — keep it.
            if self._bootstrapped:
                return  # type: ignore[unreachable]
            self._bootstrap()

    # ----- Bootstrap --------------------------------------------------------

    def _bootstrap(self) -> None:
        """One-time initialization. Runs under the lock."""
        # 1. Resolve configuration from all sources.
        self._config = load_configuration(**self._config_overrides)

        # 2. Detect application package if it wasn't configured.
        if self._config.package is None:
            detected = detect_application_package()
            self._config = replace(self._config, package=detected)

        # 3. Create the plugin manager (loads built-ins + entry points, plus
        # any programmatically-injected extras such as the test spy).
        self._plugin_manager = create_plugin_manager(extra_plugins=self._extra_plugins)

        # 4. Build the event bus. Future versions may swap this for a
        # broker-backed bus when topology != "single".
        self._event_bus = InMemoryEventBus()

        # 4.5. Build the broker registry and let plugins register adapters.
        # Brokers must be available before any events flow, hence before
        # discovery imports module code that may publish on import.
        self._broker_registry = BrokerRegistry()
        self._plugin_manager.hook.modulith_register_brokers(registry=self._broker_registry)

        # 5. Flush listeners that were registered before bootstrap.
        for event_type, handler in self._pending_listeners:
            self._event_bus.register(event_type, handler)
        self._pending_listeners.clear()

        # 6. Trigger module discovery via the plugin hook. This imports
        # each discovered module, which fires any @listener decorators
        # they contain (those flush into the bus we just built).
        if self._config.auto_discover:
            result = self._plugin_manager.hook.modulith_discover_modules(
                app_package=self._config.package,
            )
            # firstresult=True returns the winning plugin's value directly.
            self._modules = result if result else []

        # 6.5. Verify manifests against observed reality.
        if self._config.verify_manifests:
            from . import manifest as _manifest_module
            from .config import ConfigurationError

            manifests = _manifest_module.all_manifests()
            if manifests:
                registered: set[Callable[..., Any]] = set()
                for handlers in self._event_bus._handlers.values():
                    registered.update(handlers)
                all_errors: list[str] = []
                for pkg, m in manifests.items():
                    errors = _manifest_module.verify_manifest(m, registered)
                    all_errors.extend(f"[{pkg}] {e}" for e in errors)
                if all_errors:
                    raise ConfigurationError(
                        "Manifest verification failed:\n  - " + "\n  - ".join(all_errors)
                    )

        # 6.6. Notify plugins that each module is loaded. Fires AFTER discovery
        # (modules imported, @listener decorators run) and manifest verification
        # so the hookspec's "after all listeners and event types are wired"
        # contract holds. Previously declared but never invoked — plugins that
        # implement it (startup metrics, module-scoped resources, doc canvases)
        # silently never ran.
        for module in self._modules:
            self._plugin_manager.hook.modulith_after_module_load(module=module)

        # 7. Friendly startup banner so users see what's active.
        self._log_banner()

        # 8. Mark complete. Future calls take the fast path.
        self._bootstrapped = True

    def _log_banner(self) -> None:
        """Log active configuration so users see what modulith is doing."""
        cfg = self._config
        assert cfg is not None  # we just set it
        logger.info("detected application package %r", cfg.package)
        if self._modules:
            module_list = ", ".join(m.name for m in self._modules)
            logger.info("discovered %d module(s): %s", len(self._modules), module_list)
        else:
            logger.info("no modules discovered under %r", cfg.package)
        logger.info("outbox=%s, broker=%s, topology=%s", cfg.outbox, cfg.broker, cfg.topology)
        if cfg.outbox == "memory" and not cfg.production:
            logger.info(
                "outbox disabled — set [tool.modulith].outbox = 'postgres' "
                "for durable event delivery"
            )
        logger.info("ready")

    # ----- Shutdown ---------------------------------------------------------

    async def shutdown(self) -> None:
        """Release runtime-owned resources — closes every registered broker.

        Fulfils the Broker.close() / BrokerRegistry.close_all() contract
        ("called once on application shutdown"), which previously had no caller:
        a redis-streams client's connection leaked for the process lifetime.
        Also stops the first-party outbox retry loop when the runtime owns the
        process lifecycle. Idempotent and safe to call when no broker/outbox was
        ever registered. Invoke from a worker's ASGI lifespan teardown and the
        supervisor's shutdown.
        """
        registry = self._broker_registry
        if registry is not None:
            await registry.close_all()
        from .builtin import outbox

        await outbox.shutdown()

    # ----- Test support -----------------------------------------------------

    def _reset_for_testing(self) -> None:
        """Reset to uninitialized state. ONLY for tests."""
        self._bootstrapped = False
        self._config_overrides = {}
        self._config = None
        self._plugin_manager = None
        self._event_bus = None
        self._broker_registry = None
        self._modules = []
        self._pending_listeners = []
        self._extra_plugins = []
        # The manifest registry is a separate module-global, populated by
        # declare_module at import time. Resetting the runtime without clearing
        # it leaks manifests across re-bootstraps: a re-imported _manifest.py
        # hits declare_module's "already declared" guard, or verify_manifest
        # validates a stale module against the fresh bus. Reset them together.
        from . import manifest as _manifest_module

        _manifest_module._reset_for_testing()


# The module-level singleton. Decorators and publish() reach through this.
_runtime = Runtime()
