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
import os
import pkgutil
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import modulith

from .brokers import BrokerRegistry, ConsumerRegistry
from .config import Configuration, ConfigurationError, load_configuration
from .discovery import detect_application_package
from .event_bus import InMemoryEventBus, _require_async_handler
from .manager import create_plugin_manager
from .types import EventPublication, EventPublishReceipt, ModuleInfo

logger = logging.getLogger("modulith")

# Strong references for fire-and-forget background tasks (currently just the
# best-effort provisional-broker close below). asyncio only holds a weak
# reference to a scheduled task — without this, the task can be garbage
# collected before it runs.
_background_tasks: set[asyncio.Task[Any]] = set()

# The hooks that hand plugins an ``EventPublication``. None of modulith's own
# hookimpls reads the publication's ``payload`` — observability uses only
# ``id``, and the outbox's after-publish hookimpl is a no-op on the in-memory
# path — so when every registered implementation of all four lives inside
# modulith, the bytes are provably unobservable and need never be produced.
_PUBLICATION_HOOKS = (
    "modulith_after_event_published",
    "modulith_on_listener_dispatch",
    "modulith_on_listener_error",
    "modulith_on_listener_complete",
)

# Every top-level name in the ``modulith`` package, as a dotted prefix
# (``modulith.builtin``, ``modulith.adapters``, …). Used to tell a
# hookimpl shipped with this package from a third-party one: ``modulith``
# is a regular package, so its ``__path__`` is its own directory alone —
# no external code can ever become a real top-level subpackage of it.
# ``pkgutil.iter_modules`` lists without importing anything, so computing
# this has no import side effects (no SQLAlchemy, no adapters).
_BUILTIN_PLUGIN_PREFIXES = tuple(
    sorted(f"modulith.{m.name}" for m in pkgutil.iter_modules(modulith.__path__))
)


def _is_builtin_plugin_module(name: str) -> bool:
    """True when a hookimpl's ``__module__`` belongs to the modulith package.

    Matches the package itself and any module under a real top-level
    subpackage. A third-party plugin that merely *names* itself after the
    package (``modulith_extras.*``, or even ``modulith.plugins.custom``)
    is external by this test — its dotted path is not one of the
    package's own directories.
    """
    return name == "modulith" or name.startswith(_BUILTIN_PLUGIN_PREFIXES)


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
        # REENTRANT: bootstrap imports application code (discovery) and runs
        # plugin hooks while holding it; a module-level @listener decorator or
        # a plugin calling back into the runtime on the same thread must not
        # self-deadlock (a plain Lock froze the process forever here).
        self._lock = threading.RLock()
        # The thread currently running _bootstrap(), or None. Lets re-entrant
        # calls (configure()/ensure_bootstrapped()/publish_sync() reached from
        # code imported or hooked *during* bootstrap) fail fast with a clear
        # error instead of recursing into a second bootstrap or silently
        # mutating state the current bootstrap already consumed.
        self._bootstrapping_thread: int | None = None

        # Caller-supplied overrides via configure(). Applied during boot.
        self._config_overrides: dict[str, Any] = {}

        # Populated during _bootstrap().
        self._config: Configuration | None = None
        self._plugin_manager: Any = None
        self._event_bus: InMemoryEventBus | None = None
        self._broker_registry: BrokerRegistry | None = None
        self._consumer_registry: ConsumerRegistry | None = None
        self._modules: list[ModuleInfo] = []

        # Listeners registered before bootstrap go here, then flush
        # into the real event bus once it exists.
        self._pending_listeners: list[tuple[type, Callable[..., Any]]] = []

        # Plugins injected programmatically (not via entry points), registered
        # at bootstrap. Embedding contexts — most notably the pytest plugin's
        # event-capturing spy — append here before triggering bootstrap.
        self._extra_plugins: list[Any] = []

        # Plugin names to skip at bootstrap — forwarded to
        # create_plugin_manager(disable=...). Set via
        # configure(disable_plugins=[...]); the extra_plugins counterpart.
        # This is what makes the documented escape hatches (e.g.
        # disable=['modulith.observe-shield'], or replacing a built-in)
        # reachable from application configuration.
        self._disabled_plugins: list[str] = []

    # ----- Accessors (read-only views for plugins and tooling) -------------

    @property
    def broker_registry(self) -> BrokerRegistry | None:
        """The broker dispatch registry, or None before bootstrap."""
        return self._broker_registry

    @property
    def consumer_registry(self) -> ConsumerRegistry | None:
        """The consumer factory registry, or None before bootstrap."""
        return self._consumer_registry

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

        Beyond Configuration fields, one runtime-level kwarg is accepted:
        ``disable_plugins`` — a list/tuple of plugin names to skip at
        bootstrap (built-in module paths, entry-point names, or the
        observe-shield's ``modulith.observe-shield``), forwarded to
        ``create_plugin_manager(disable=...)``.
        """
        with self._lock:
            if self._bootstrapping_thread == threading.get_ident():
                # Re-entered from code running *inside* bootstrap (a module
                # imported by discovery, or a plugin hook). The configuration
                # was already resolved in step 1, so the override would be
                # silently dropped — fail loudly instead. (The reentrant lock
                # lets us get here rather than deadlocking forever.)
                raise ConfigurationError(
                    "configure() called while modulith is bootstrapping (from "
                    "module import code or a plugin hook). Configuration is "
                    "already resolved at this point — call configure() at "
                    "application startup, before the first @listener "
                    "registration or publish() call."
                )
            if self._bootstrapped:
                raise ConfigurationError(
                    "configure() cannot be called after modulith has bootstrapped. "
                    "Call it at application startup, before any @listener "
                    "registration or publish() call."
                )
            # Runtime-level (non-Configuration) override: plugin names to skip
            # at bootstrap. Validated here — a bare string would silently
            # iterate as characters, disabling nothing.
            if "disable_plugins" in overrides:
                disable = overrides.pop("disable_plugins")
                if (
                    isinstance(disable, str)
                    or not isinstance(disable, list | tuple)
                    or not all(isinstance(name, str) for name in disable)
                ):
                    raise ConfigurationError(
                        "disable_plugins must be a list/tuple of plugin-name "
                        "strings, e.g. configure(disable_plugins="
                        f"['modulith.observe-shield']); got {disable!r}"
                    )
                self._disabled_plugins = list(disable)
            self._config_overrides.update(overrides)

    def register_listener(self, event_type: type, handler: Callable[..., Any]) -> None:
        """Register a listener. Works before, during, and after bootstrap.

        Pre-bootstrap and mid-bootstrap (no *published* bus yet): queue for
        the single flush that runs near the end of bootstrap.
        Post-bootstrap: register directly on the live bus.

        The check-then-act runs under the runtime lock, so a registration
        racing _bootstrap()'s flush can never land in a pending list that
        has already been consumed (which silently lost the listener
        forever): another thread's registration either arrives before the
        flush, or blocks until bootstrap finishes and registers directly.
        The lock is reentrant, so module-level @listener decorators firing
        during the discovery import phase (on the bootstrap thread itself)
        enter without deadlocking. A registration made on the bootstrap
        thread *after* the flush — from a ``modulith_verify_module`` or
        ``modulith_after_module_load`` hookimpl — cannot be covered by the
        lock at all; _bootstrap()'s commit point drains that tail before
        clearing the queue.
        """
        # Validate here — not only inside the bus — so a sync handler is
        # rejected at the registration call site instead of surfacing as a
        # confusing failure when the queued listener is flushed at bootstrap.
        _require_async_handler(event_type, handler)
        with self._lock:
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

        Failure contract on the DIRECT (non-transactional) path: when the
        event routes to a cross-process broker, the broker send is awaited
        inline and a broker publish failure PROPAGATES to this caller —
        fail-loud by design, never swallowed: with no outbox
        row persisted, a swallowed send would lose the event for every
        remote consumer with zero trace. Callers that need publish() to be
        decoupled from broker availability should use the transactional
        outbox path (bind a session + durable store), where the send
        happens after commit with retry/dead-letter handling. Pinned by
        tests/test_cross_process.py::
        test_broker_publish_failure_propagates_to_publisher.
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
            # Broker routing is commit-gated the same way: the route is
            # persisted as its own publication row in the bound session and
            # sent after commit. Sending synchronously here handed the event
            # to remote consumers *before* the business transaction committed
            # — a rollback could not un-send it, the exact inconsistency the
            # outbox exists to prevent.
            from .builtin import outbox

            assert self._event_bus is not None
            handlers = self._event_bus.listeners_for(type(event))
            try:
                records = list(await outbox.persist(event))
                target = self._broker_route_target(event, has_local_handler=bool(handlers))
                if target is not None:
                    records.append(await outbox.persist_broker_route(event, target))
            except BaseException as exc:
                # The publish failed BETWEEN the paired publish hooks.
                # modulith_after_event_published is contractually scoped to
                # successful persistence, so it must NOT fire — but the
                # observability publish span started in the before hook would
                # then leak (never ended, stale ContextVar).
                # modulith_on_publish_error gives plugins (observability
                # included) a paired hook to close out whatever they opened.
                self._fire_publish_error(event, exc)
                raise
            # Fire the post-publish hook on the durable path too: the event is
            # now persisted, which is exactly what the hookspec documents
            # ("after an event has been persisted to the outbox"). Omitting it
            # here made metrics/tracing plugins miss every transactional
            # publish — the production path the outbox exists for. Hand hook
            # consumers the records actually saved (EventPublishReceipt)
            # instead of a fabricated EventPublication matching neither the
            # persisted id nor the configured storage serializer's bytes.
            receipt = EventPublishReceipt(records=tuple(records))
            pm.hook.modulith_after_event_published(event=event, publication=receipt)
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

    async def _route_to_broker_guarded(self, event: Any, *, has_local_handler: bool) -> None:
        """``_maybe_route_to_broker`` + publish-span cleanup on failure.

        The inline broker route runs between the paired publish hooks, so a
        route failure must close the observability publish span before it
        propagates — the direct-path leg of that pairing.
        """
        try:
            await self._maybe_route_to_broker(event, has_local_handler=has_local_handler)
        except BaseException as exc:
            self._fire_publish_error(event, exc)
            raise

    def _fire_publish_error(self, event: Any, exc: BaseException) -> None:
        """Fire the observe-only publish-error hook for a failed publish.

        A publish that raises between ``modulith_before_event_published`` and
        ``modulith_after_event_published`` (outbox persist/serialize, inline
        broker route) fires no further hook — the hookspec scopes the after
        hook to success — so any cleanup a plugin started in the before hook
        (the built-in observability plugin's publish span, most notably)
        leaked: never ended, with a stale ContextVar mis-parenting the next
        dispatch span in the same context. Purely observational
        — the ``_ObserveContractShield`` in ``manager.py`` guarantees a
        raising hookimpl is logged and swallowed rather than masking the
        original publish failure, which the caller always re-raises
        unchanged right after this call.
        """
        if self._plugin_manager is None:
            return
        self._plugin_manager.hook.modulith_on_publish_error(event=event, exception=exc)

    def _payload_is_observed(self) -> bool:
        """True when a hookimpl defined outside modulith receives a publication.

        Every ``_PUBLICATION_HOOKS`` implementation shipped in this package
        ignores ``publication.payload``, so with only built-ins registered the
        serialized bytes can never be read by anyone.
        """
        pm = self._plugin_manager
        if pm is None:
            return False
        for hook_name in _PUBLICATION_HOOKS:
            for impl in getattr(pm.hook, hook_name).get_hookimpls():
                if not _is_builtin_plugin_module(impl.function.__module__ or ""):
                    return True
        return False

    def _serialized_payload(self, event: Any) -> bytes:
        """Best-effort serialized bytes for hook-facing EventPublications.

        The lifecycle/observability hooks (dead-letter handlers, audit logs)
        receive real payload bytes on the durable outbox path; the in-memory
        path used to hand them a hardcoded ``b""``, silently defeating those
        use cases. In-memory dispatch must not *require* serializability
        (only the durable outbox does), so failures degrade back to ``b""``
        with a debug log instead of failing the publish. Serialization uses
        the default JSON serializer; these publications are observational
        only — they are never persisted or redelivered.

        Skipped — back to ``b""`` — when no hookimpl outside modulith is
        registered for any publication-carrying hook: a full recursive
        dataclass walk dominates the cost of an in-memory publish, and on the
        default plugin set not one byte of it is ever read.
        """
        if not self._payload_is_observed():
            return b""

        from .serializers import JsonEventSerializer

        try:
            return JsonEventSerializer().serialize(event)
        except Exception:
            logger.debug(
                "could not serialize %s for hook publications; hooks receive "
                "an empty payload for this event",
                type(event).__name__,
                exc_info=True,
            )
            return b""

    async def _dispatch_with_hooks(self, event: Any) -> None:
        """Dispatch in-memory, wrapping each listener with lifecycle hooks.

        Behaviorally equivalent to ``InMemoryEventBus.publish`` (listeners
        run concurrently; every failure is logged; the first exception is
        re-raised after all complete) but additionally fires the
        per-listener observability hooks and the post-publish hook.

        The one divergence: a fan-out broker route failing after the listeners
        ran propagates instead of the first listener error — the fail-loud
        broker contract in ``publish()``'s docstring outranks it. The listener
        failures are already logged by then, so they stay diagnosable.
        """
        assert self._event_bus is not None  # for type-checker
        assert self._plugin_manager is not None
        bus = self._event_bus
        pm = self._plugin_manager

        event_type = f"{type(event).__module__}.{type(event).__qualname__}"
        # Serialize once per publish; every hook-facing publication below
        # shares the same bytes (real data for dead-letter/audit hooks, not
        # the hardcoded b"" they used to receive on this path).
        payload = self._serialized_payload(event)
        publish_pub = EventPublication(
            id=uuid4(), payload=payload, event_type=event_type, published_at=datetime.now(UTC)
        )

        handlers = bus.listeners_for(type(event))
        if not handlers:
            # No local listener. In process-per-module topology this is a
            # cross-module event bound for a worker in another process —
            # route it to the broker. In single topology it simply has no
            # consumers.
            await self._route_to_broker_guarded(event, has_local_handler=False)
            pm.hook.modulith_after_event_published(event=event, publication=publish_pub)
            return

        try:
            first_error = await self._run_listeners(
                event, handlers, payload=payload, event_type=event_type
            )
        except BaseException as exc:
            # Cancellation is the only way out of _run_listeners — listener
            # failures come back as a return value — and it lands BETWEEN the
            # paired publish hooks: modulith_after_event_published below never
            # fires, so the observability publish span opened in the before
            # hook would leak (never ended, stale ContextVar).
            # Same pairing as the durable branch in publish() and as
            # _route_to_broker_guarded.
            self._fire_publish_error(event, exc)
            raise

        # Fan-out: an externalized event also crosses to the broker even though
        # it has local listeners here — remote workers consume it too. Routing
        # is independent of local delivery, so a local listener failure (raised
        # below) must not suppress it. No-op unless the event resolves to a
        # remote target (see _maybe_route_to_broker).
        await self._route_to_broker_guarded(event, has_local_handler=True)

        pm.hook.modulith_after_event_published(event=event, publication=publish_pub)

        if first_error is not None:
            raise first_error

    async def dispatch_local(self, event: Any, bus: Any) -> None:
        """Deliver an event received from another process to local listeners.

        The cross-process consumers call this instead of ``bus.publish`` so a
        remotely-delivered event fires the same per-listener lifecycle hooks
        (``modulith_on_listener_dispatch`` / ``_error`` / ``_complete``) an
        in-memory publish does. Without it every listener invocation in a
        worker process is untraced and a plugin using
        ``modulith_on_listener_error`` for alerting never sees a cross-process
        listener failure, even though the hookspec scopes those hooks to every
        ``(event, listener)`` pair unconditionally.

        The *publish* hooks and broker routing stay out on purpose: this
        process did not publish the event, and re-routing a consumed event to
        the target it was consumed from is an infinite redelivery loop.
        """
        if self._plugin_manager is None:
            # No bootstrapped plugin manager (a consumer driven directly by a
            # test harness): there are no hookimpls to fire, so plain bus
            # delivery already is the whole contract.
            await bus.publish(event)
            return
        handlers = bus.listeners_for(type(event))
        if not handlers:
            return
        first_error = await self._run_listeners(
            event,
            handlers,
            payload=self._serialized_payload(event),
            event_type=f"{type(event).__module__}.{type(event).__qualname__}",
        )
        if first_error is not None:
            raise first_error

    async def _run_listeners(
        self,
        event: Any,
        handlers: list[Callable[..., Any]],
        *,
        payload: bytes,
        event_type: str,
    ) -> BaseException | None:
        """Run every listener concurrently, wrapped in the per-listener hooks.

        Returns the first listener failure rather than raising it: the caller
        still has work to finish (broker fan-out, the post-publish hook) that a
        local listener failure must not skip. Every failure is logged HERE,
        before returning, so a caller that goes on to raise for its own reasons
        — a broker route failing during an outage — can never swallow the
        listener diagnostics along with it.
        """
        assert self._plugin_manager is not None
        pm = self._plugin_manager

        async def _run_one(handler: Callable[..., Any]) -> None:
            name = getattr(handler, "__qualname__", repr(handler))
            pub = EventPublication(
                id=uuid4(),
                payload=payload,
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
        return first_error

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

    def _broker_route_target(self, event: Any, *, has_local_handler: bool) -> str | None:
        """The validated broker target for this publish, or None (in-process).

        Only resolves in non-``single`` topology. An event routes to the
        broker when EITHER:
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

        Returns None when topology is ``single`` or when the event carries no
        cross-process signal. When the event DOES resolve to a broker target
        but no broker is registered for that target's scheme, raises
        ConfigurationError — uniformly for the default scheme and for
        explicit ``@externalized(target=...)`` overrides. (The old behavior
        was the worst of both worlds: the default-scheme case silently
        dropped the event forever, while an explicit target crashed with an
        undocumented UnknownBrokerError.)
        """
        cfg = self._config
        if cfg is None or cfg.topology == "single":
            return None
        registry = self._broker_registry
        if registry is None:  # pre-bootstrap safety; publish() bootstraps first
            return None

        target = self._resolve_broker_target(event)
        if target is None:
            # No externalization signal. Route only a listener-less event, by
            # the default scheme; an in-process-only event isn't broker traffic.
            if has_local_handler:
                return None
            fqn = f"{type(event).__module__}.{type(event).__qualname__}"
            target = f"{cfg.broker}:{fqn}"

        event_type = f"{type(event).__module__}.{type(event).__qualname__}"
        scheme = target.partition(":")[0]
        if scheme not in registry.schemes():
            raise ConfigurationError(
                f"cannot route event {event_type} to broker target {target!r}: "
                f"no broker adapter is registered for scheme {scheme!r} "
                f"(registered schemes: {registry.schemes() or 'none'}). In "
                f"topology={cfg.topology!r} this event crosses processes, so "
                f"dropping it silently would lose it for every remote "
                f"consumer. Install or register the broker adapter, or fix "
                f"the configured scheme."
            )
        return target

    async def _maybe_route_to_broker(self, event: Any, *, has_local_handler: bool) -> None:
        """Send an event to the broker when it crosses processes (direct path).

        Routing rules and failure modes live in ``_broker_route_target``.
        Used ONLY by the non-durable dispatch path — inside a transaction the
        runtime persists the route via ``outbox.persist_broker_route`` instead
        and the send happens after commit (see ``publish``).
        """
        target = self._broker_route_target(event, has_local_handler=has_local_handler)
        if target is None:
            return
        registry = self._broker_registry
        assert registry is not None  # _broker_route_target resolved a target

        from .serializers import JsonEventSerializer

        event_type = f"{type(event).__module__}.{type(event).__qualname__}"
        payload = JsonEventSerializer().serialize(event)
        await registry.publish(target, payload, {"event_type": event_type})
        logger.debug("routed %s to broker target %s", event_type, target)

    def ensure_bootstrapped(self) -> None:
        """Run bootstrap if not yet done. Safe to call repeatedly.

        A *failed* bootstrap leaves the runtime un-bootstrapped and otherwise
        untouched: _bootstrap() builds all state in local variables and
        installs it on ``self`` only after every step succeeds, so a retry
        after fixing the problem starts from a clean slate. Listeners whose
        modules were already imported by the failed attempt stay queued in
        the pending list (Python caches the imports, so their decorators
        never re-fire) and are flushed by the next attempt.

        Re-entrant calls from inside bootstrap itself (module import code or
        a plugin hook calling back into publish()/ensure_bootstrapped())
        raise ConfigurationError instead of recursing into a second
        bootstrap.
        """
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
            if self._bootstrapping_thread == threading.get_ident():
                raise ConfigurationError(
                    "modulith bootstrap re-entered from within bootstrap "
                    "itself (publish()/ensure_bootstrapped() called from "
                    "module import code or a plugin hook). Defer runtime use "
                    "until after startup."
                )
            self._bootstrapping_thread = threading.get_ident()
            try:
                self._bootstrap()
            finally:
                self._bootstrapping_thread = None

    # ----- Bootstrap --------------------------------------------------------

    def _bootstrap(self) -> None:
        """One-time initialization. Runs under the lock.

        ATOMIC: every piece of state is built in local variables and
        installed on ``self`` only after all steps succeed. An exception at
        any step — a discovery import error, a failed manifest verification —
        leaves the runtime exactly as it was, so ensure_bootstrapped() can
        genuinely be retried. The previous in-place version left a half-built
        bus behind and cleared the pending-listener queue, so a retry
        "succeeded" with every already-imported module's listeners silently
        gone (imports are cached; their @listener decorators never re-fire).

        One deliberate exception: the resolved configuration is installed
        just before the broker-registration hook (step 4.5) because adapter
        hookimpls and discovery-imported module code read it lazily via
        ``_runtime.config`` — and it is rolled back to None if any later
        step fails, preserving the clean-retry guarantee.
        """
        # 1. Resolve configuration from all sources.
        config = load_configuration(**self._config_overrides)

        # 2. Detect application package if it wasn't configured.
        if config.package is None:
            detected = detect_application_package()
            config = replace(config, package=detected)

        # 3. Create the plugin manager (loads built-ins + entry points, plus
        # any programmatically-injected extras such as the test spy), skipping
        # anything named in configure(disable_plugins=[...]) — the documented
        # escape hatch for e.g. the observe-shield or replacing a built-in.
        plugin_manager = create_plugin_manager(
            extra_plugins=self._extra_plugins,
            disable=self._disabled_plugins,
        )

        # 4. Build the event bus — kept LOCAL until bootstrap succeeds, so
        # register_listener() keeps queueing into _pending_listeners for the
        # whole bootstrap (including the discovery import phase) and a failed
        # attempt can't strand listeners in a discarded bus. Future versions
        # may swap this for a broker-backed bus when topology != "single".
        event_bus = InMemoryEventBus()

        # 4.5. Build the broker registry and let plugins register adapters.
        # Brokers must be available before any events flow, hence before
        # discovery imports module code that may publish on import.
        # The resolved configuration is installed on ``self`` HERE, before the
        # hook fires: adapter hookimpls (the redis-streams broker) and module
        # code imported by discovery read it lazily via ``_runtime.config``,
        # so keeping it local until the commit point silently turned broker
        # registration into a no-op. It is the ONE piece of state exposed
        # early; the except-block rolls it back so a failed bootstrap still
        # leaves the runtime pristine for a clean retry.
        broker_registry = BrokerRegistry()
        consumer_registry = ConsumerRegistry()
        self._config = config
        try:
            plugin_manager.hook.modulith_register_brokers(registry=broker_registry)
            # Consumer factories register right after brokers: a consumer
            # factory typically reuses the backend object its broker just
            # registered (pulled from broker_registry by scheme), so brokers
            # must land first. Config-free like the broker hook.
            plugin_manager.hook.modulith_register_consumers(registry=consumer_registry)

            # 5. Trigger module discovery via the plugin hook. This imports
            # each discovered module; the @listener decorators they contain
            # queue into _pending_listeners (the bus above isn't published yet).
            modules: list[ModuleInfo] = []
            if config.auto_discover:
                result = plugin_manager.hook.modulith_discover_modules(
                    app_package=config.package,
                )
                # firstresult=True returns the winning plugin's value directly.
                modules = result if result else []

            # 6. Flush every queued listener — those registered before bootstrap
            # AND those the discovery imports just queued, in registration order —
            # into the local bus. The queue is NOT cleared until the very end:
            # a failure below must leave it intact for the next attempt.
            for event_type, handler in list(self._pending_listeners):
                event_bus.register(event_type, handler)
            flushed = len(self._pending_listeners)

            # 6.5. Verify manifests against observed reality. Iterates the LOCAL
            # bus (unpublished, and register_listener blocks on the runtime lock
            # for the duration of bootstrap), so no concurrent registration can
            # mutate the handler dict mid-iteration.
            if config.verify_manifests:
                from . import manifest as _manifest_module
                from .config import ConfigurationError

                manifests = _manifest_module.all_manifests()
                if manifests:
                    registered: set[Callable[..., Any]] = set()
                    for handlers in list(event_bus._handlers.values()):
                        registered.update(handlers)
                    all_errors: list[str] = []
                    for pkg, m in manifests.items():
                        errors = _manifest_module.verify_manifest(m, registered)
                        all_errors.extend(f"[{pkg}] {e}" for e in errors)
                    if all_errors:
                        raise ConfigurationError(
                            "Manifest verification failed:\n  - " + "\n  - ".join(all_errors)
                        )

            # 6.55. Enforce boundary violations when strict_boundaries is enabled.
            # strict_boundaries=True re-runs the boundary scan here and aborts
            # bootstrap with ConfigurationError on any violation, so a violating
            # app can never start. The default (False) skips this block
            # entirely — no scan, no warning, nothing logged; `modulith verify`
            # is where boundaries get checked, typically as a CI gate. That is
            # the trade-off: enabling it re-parses every .py file under every
            # application module (see modulith/builtin/verifier.py) on every
            # process start, which the CLI gate pays once per pipeline run.
            #
            # Single-process `modulith dev` uses this marker so it survives
            # uvicorn's reload child. It must not weaken process topology.
            if config.strict_boundaries:
                from .builtin import verifier
                from .config import ConfigurationError

                violations: list[Any] = []
                for module in modules:
                    for result in plugin_manager.hook.modulith_verify_module(
                        module=module, all_modules=modules
                    ):
                        violations.extend(result)
                violations.extend(verifier.detect_cycles(modules))
                if violations:
                    details = "\n  - ".join(
                        f"[{v.severity.value.upper()}] {v.module}: {v.rule}: {v.message}"
                        + (f" ({v.location})" if v.location else "")
                        for v in violations
                    )
                    warn_only = (
                        config.topology == "single"
                        and os.environ.get("MODULITH_DEV_WARN_ONLY") == "1"
                        and not config.production
                    )
                    if warn_only:
                        logger.warning(
                            "boundary violations detected with strict_boundaries=True "
                            "(warn-only: single-process `modulith dev`):\n  - %s",
                            details,
                        )
                    else:
                        raise ConfigurationError(
                            f"boundary violations detected with strict_boundaries=True:\n  - {details}"
                        )

            # 6.6. Notify plugins that each module is loaded. Fires AFTER
            # discovery (modules imported, @listener decorators run) and
            # manifest verification so the hookspec's "after all listeners and
            # event types are wired" contract holds. Previously declared but
            # never invoked — plugins that implement it (startup metrics,
            # module-scoped resources, doc canvases) silently never ran.
            for module in modules:
                plugin_manager.hook.modulith_after_module_load(module=module)
        except BaseException:
            # Roll back the early config install (see step 4.5) — a failed
            # bootstrap must leave the runtime exactly as it was. The local
            # broker_registry is never installed on self, but any adapter
            # that registered into it during step 4.5 (e.g. a redis client)
            # is live and must still be closed — otherwise a LATER step
            # failing (discovery, manifests, modulith_after_module_load)
            # leaked it for the process lifetime.
            self._config = None
            self._close_provisional_brokers(broker_registry)
            raise

        # 7. Commit point — install all state on self. Nothing above mutated
        # the runtime, so an exception in steps 1-6.6 left it pristine.
        self._config = config
        self._plugin_manager = plugin_manager
        self._event_bus = event_bus
        self._broker_registry = broker_registry
        self._consumer_registry = consumer_registry
        self._modules = modules
        # Drain whatever queued AFTER the step-6 flush. A hookimpl for
        # modulith_verify_module (6.55) or modulith_after_module_load (6.6)
        # runs on this very thread, and the lock is reentrant, so a plugin
        # registering a listener from one of them takes register_listener's
        # pending branch (self._event_bus is still None up here) and the
        # clear() below would destroy it with no error, no warning, no log.
        # Only the tail is drained: the bus appends without dedupe, so
        # re-flushing the whole list would double-register step 6's listeners.
        for event_type, handler in self._pending_listeners[flushed:]:
            event_bus.register(event_type, handler)
        self._pending_listeners.clear()

        # 7.5. Friendly startup banner so users see what's active.
        self._log_banner()

        # 8. Mark complete. Future calls take the fast path.
        self._bootstrapped = True

    @staticmethod
    def _close_provisional_brokers(registry: BrokerRegistry) -> None:
        """Best-effort close for a broker_registry orphaned by a failed bootstrap.

        Mirrors ``_teardown_test_resources``'s loop detection: with no loop
        running (the common case — bootstrap failing from a sync caller)
        ``close_all()`` is awaited directly via ``asyncio.run()``. Inside a
        running loop (bootstrap failing from the async ``publish()`` path,
        which calls ``ensure_bootstrapped()`` synchronously) a sync method
        can't await, so the close is scheduled as a background task instead —
        best-effort, but ``close_all()`` already logs-and-swallows adapter
        errors internally, so this never risks masking the original bootstrap
        failure that's about to propagate.
        """
        if not registry.schemes():
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(registry.close_all())
            except RuntimeError:  # pragma: no cover - loop-policy edge cases
                logger.debug(
                    "could not close provisional brokers after failed bootstrap",
                    exc_info=True,
                )
        else:
            task = asyncio.ensure_future(registry.close_all())
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)

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
        registry = self._broker_registry
        if (
            cfg.topology != "single"
            and registry is not None
            and cfg.broker not in registry.schemes()
        ):
            # A cross-process topology whose configured scheme has no adapter
            # is almost certainly a typo or a missing plugin. Publishing a
            # cross-process event in this state raises ConfigurationError
            # (see _maybe_route_to_broker); warn at startup too so the
            # mismatch is visible before the first publish. Not fatal here:
            # embedders and tests legitimately register brokers after
            # bootstrap via runtime.broker_registry.
            logger.warning(
                "topology=%r but no broker adapter is registered for the "
                "configured scheme %r (registered: %s) — cross-process "
                "publishes will fail until one is registered",
                cfg.topology,
                cfg.broker,
                registry.schemes() or "none",
            )
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

        Order matters: the outbox retry loop stops first (no new dispatches),
        then the active publication store's in-flight after-commit dispatch
        tasks are drained when the store exposes ``wait_for_dispatch()``
        (duck-typed; the Postgres adapter does) — so a listener already
        mid-flight finishes instead of being abandoned to a future crash
        sweep — and brokers close last, after any fan-out those listeners
        route.

        TERMINAL, not reversible: the bootstrapped flag stays set and the
        (now closed) brokers stay registered, so this runtime never
        re-bootstraps in-process. A publish() after shutdown that reaches a
        closed broker raises that adapter's own error, not a modulith error.
        Restart the process to get a working runtime (tests use
        ``_reset_for_testing`` instead).

        The local step (outbox stop + store drain) and the broker step
        (``registry.close_all()``) run INDEPENDENTLY: a failure in one no
        longer skips the other — a store whose ``wait_for_dispatch()`` raises
        used to abort before brokers ever got a chance to close, leaking
        their connections on every failed drain. When only one step fails,
        that single exception propagates unchanged (no behavior change for
        the common case); when BOTH fail, they're combined into an
        ``ExceptionGroup`` so neither failure silently displaces the other.
        """
        from .builtin import outbox

        local_error: BaseException | None = None
        try:
            await outbox.shutdown()
            store = outbox._store
            waiter = getattr(store, "wait_for_dispatch", None) if store is not None else None
            if waiter is not None:
                await waiter()
        except BaseException as exc:
            local_error = exc

        broker_error: BaseException | None = None
        registry = self._broker_registry
        if registry is not None:
            try:
                await registry.close_all()
            except BaseException as exc:
                broker_error = exc

        if local_error is not None and broker_error is not None:
            # ExceptionGroup requires Exception instances; BaseException
            # (CancelledError / KeyboardInterrupt) must propagate unwrapped.
            if isinstance(local_error, Exception) and isinstance(broker_error, Exception):
                raise ExceptionGroup(
                    "runtime shutdown failed: local drain and broker close both failed",
                    [local_error, broker_error],
                )
            raise local_error if not isinstance(local_error, Exception) else broker_error
        if local_error is not None:
            raise local_error
        if broker_error is not None:
            raise broker_error

    # ----- Test support -----------------------------------------------------

    def _reset_for_testing(self) -> None:
        """Reset to uninitialized state. ONLY for tests.

        Beyond nulling the runtime's own fields, this tears down the
        cross-cutting machinery the old implementation leaked between
        tests: registered brokers are closed (their connections otherwise
        outlive the "fresh runtime") and the outbox plugin's module state —
        store, serializer, retry task — is cleared, so a store configured by
        one test can't silently flip a later test's publish() onto the
        durable dispatch path. Closing brokers needs an event loop: with no
        loop running (the sync fixtures this method serves) a temporary one
        is used; inside a running loop the retry task is cancelled in place
        and broker close() is skipped (close_all() logs-and-swallows, so a
        loop-bound client can't fail the reset either way).
        """
        self._teardown_test_resources()
        self._bootstrapped = False
        self._config_overrides = {}
        self._config = None
        self._plugin_manager = None
        self._event_bus = None
        self._broker_registry = None
        self._consumer_registry = None
        self._modules = []
        self._pending_listeners = []
        self._extra_plugins = []
        self._disabled_plugins = []
        # The manifest registry is a separate module-global, populated by
        # declare_module at import time. Resetting the runtime without clearing
        # it leaks manifests across re-bootstraps: a re-imported _manifest.py
        # hits declare_module's "already declared" guard, or verify_manifest
        # validates a stale module against the fresh bus. Reset them together.
        from . import manifest as _manifest_module

        _manifest_module._reset_for_testing()

    def _teardown_test_resources(self) -> None:
        """Close brokers and clear outbox module state for _reset_for_testing."""
        from .builtin import outbox

        registry = self._broker_registry
        retry_task = outbox._retry_task
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            if registry is not None and registry.schemes():
                try:
                    asyncio.run(registry.close_all())
                except RuntimeError:  # pragma: no cover - loop-policy edge cases
                    logger.debug("could not close brokers during test reset", exc_info=True)
            if retry_task is not None and not retry_task.done():
                try:
                    retry_task.cancel()
                except RuntimeError:  # pragma: no cover - task's loop already closed
                    pass
        else:
            if retry_task is not None and not retry_task.done():
                retry_task.cancel()
        outbox._reset_for_testing()


# The module-level singleton. Decorators and publish() reach through this.
_runtime = Runtime()
