"""The modulith runtime singleton.

Holds global state: configuration, plugin manager, event bus, modules.
Lazily initializes on first use — importing modulith does no work, but
the first @listener registration or publish() call triggers bootstrap.

This pattern is what makes "just import and use" feel natural. The user
doesn't construct anything; the framework constructs itself when needed.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from .config import Configuration, ConfigurationError, load_configuration
from .discovery import detect_application_package
from .event_bus import InMemoryEventBus
from .manager import create_plugin_manager
from .types import ModuleInfo

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
        self._modules: list[ModuleInfo] = []

        # Listeners registered before bootstrap go here, then flush
        # into the real event bus once it exists.
        self._pending_listeners: list[tuple[type, Callable[..., Any]]] = []

    # ----- Public API -------------------------------------------------------

    def configure(self, **overrides: Any) -> None:
        """Set configuration overrides. Must be called before bootstrap."""
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
        """Publish an event. Triggers bootstrap if not yet done."""
        self.ensure_bootstrapped()
        assert self._event_bus is not None  # for type-checker
        await self._event_bus.publish(event)

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

        # 3. Create the plugin manager (loads built-ins + entry points).
        self._plugin_manager = create_plugin_manager()

        # 4. Build the event bus. Future versions may swap this for a
        # broker-backed bus when topology != "single".
        self._event_bus = InMemoryEventBus()

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

    # ----- Test support -----------------------------------------------------

    def _reset_for_testing(self) -> None:
        """Reset to uninitialized state. ONLY for tests."""
        self._bootstrapped = False
        self._config_overrides = {}
        self._config = None
        self._plugin_manager = None
        self._event_bus = None
        self._modules = []
        self._pending_listeners = []


# The module-level singleton. Decorators and publish() reach through this.
_runtime = Runtime()
