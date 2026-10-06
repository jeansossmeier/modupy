"""Plugin manager construction and entry-point discovery.

Wires pluggy together with the modulith hookspecs and discovers
third-party plugins via the "modulith" entry point group. Built-in
plugins (verifier, outbox, observability, docs) load through the same
mechanism as external plugins — the only difference is they ship in
the modulith package.

This is the architectural test that matters: built-ins are not
privileged, just first-party. Users can disable any of them and
replace them with their own implementations using the same hookspec
contract.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Generator
from typing import Any

import pluggy

from . import hooks
from .markers import hookimpl

logger = logging.getLogger(__name__)

# Entry point group plugins declare in their pyproject.toml:
#
#   [project.entry-points."modulith"]
#   my_plugin = "my_package.plugin_module"
#
# pluggy's load_setuptools_entrypoints() resolves these via
# importlib.metadata at startup.
ENTRY_POINT_GROUP = "modulith"

# Built-in plugin module paths. Each is loaded as a regular plugin.
# Users who want to replace one (e.g. swap the default verifier) pass
# its name in the ``disable`` argument and register their replacement
# via ``extra_plugins`` or an entry point.
#
# The list is the single source of truth for which built-ins load.
BUILTIN_PLUGINS = (
    "modulith.builtin.discovery",
    "modulith.builtin.outbox",
    "modulith.builtin.verifier",
    "modulith.builtin.docs",
    "modulith.builtin.observability",
    "modulith.adapters.redis_broker",
    "modulith.adapters.db_broker",
    "modulith.adapters.shm_broker",
)

# Name the observe-shield registers under. Listed in ``disable`` it can be
# switched off like any other plugin (the escape hatch for debugging a
# misbehaving observability plugin by letting its exceptions propagate).
OBSERVE_SHIELD_NAME = "modulith.observe-shield"


class _ObserveContractShield:
    """Enforces the observe-only contract of the listener lifecycle hooks.

    The hookspecs for ``modulith_on_listener_dispatch``,
    ``modulith_on_listener_complete``, ``modulith_on_listener_error``, and
    ``modulith_on_publish_error`` promise that they observe, they don't
    gate: a plugin's failure must never prevent a listener from running,
    mask the listener's own exception, or suppress the runtime's re-raise
    (for the publish-error hook: never mask the original publish failure).
    Enforcing that promise
    here — as pluggy wrappers around each hook — covers every call site
    (in-memory dispatch, the durable outbox path, and any future one)
    instead of requiring each caller to remember a try/except.

    Exceptions from hookimpls are logged and swallowed. ``Exception`` (not
    ``BaseException``) is caught so KeyboardInterrupt/SystemExit still
    abort. One raising hookimpl still short-circuits later-registered
    impls of the same hook for that call — pluggy stops the impl loop on
    the first exception — but the listener itself always proceeds.
    """

    @staticmethod
    def _swallow(hook_name: str, label: str, exc: Exception) -> None:
        logger.exception(
            "%s hookimpl raised for %r — swallowed to honor the "
            "observe-only hook contract (the original outcome is unaffected)",
            hook_name,
            label,
            exc_info=exc,
        )

    @hookimpl(wrapper=True)
    def modulith_on_listener_dispatch(
        self, listener_name: str
    ) -> Generator[None, list[object], list[object]]:
        try:
            return (yield)
        except Exception as exc:
            self._swallow("modulith_on_listener_dispatch", listener_name, exc)
            return []

    @hookimpl(wrapper=True)
    def modulith_on_listener_complete(
        self, listener_name: str
    ) -> Generator[None, list[object], list[object]]:
        try:
            return (yield)
        except Exception as exc:
            self._swallow("modulith_on_listener_complete", listener_name, exc)
            return []

    @hookimpl(wrapper=True)
    def modulith_on_listener_error(
        self, listener_name: str
    ) -> Generator[None, list[object], list[object]]:
        try:
            return (yield)
        except Exception as exc:
            self._swallow("modulith_on_listener_error", listener_name, exc)
            return []

    @hookimpl(wrapper=True)
    def modulith_on_publish_error(self, event: Any) -> Generator[None, list[object], list[object]]:
        try:
            return (yield)
        except Exception as exc:
            self._swallow("modulith_on_publish_error", type(event).__name__, exc)
            return []


def _load_builtin_plugins(
    pm: pluggy.PluginManager,
    disable: list[str] | tuple[str, ...],
) -> None:
    """Import and register the built-in plugins, honoring ``disable``.

    Skipping a missing built-in is a real bug — surface it rather than
    papering over it.
    """
    for plugin_path in BUILTIN_PLUGINS:
        if plugin_path in disable:
            logger.debug("skipping disabled built-in plugin: %s", plugin_path)
            continue
        try:
            module = importlib.import_module(plugin_path)
            pm.register(module, name=plugin_path)
        except ImportError as exc:
            # Built-ins ship with the package. Import failure here
            # means a real packaging or installation problem.
            raise RuntimeError(f"failed to load built-in plugin {plugin_path!r}: {exc}") from exc


def _load_entrypoint_plugins(
    pm: pluggy.PluginManager,
    disable: list[str] | tuple[str, ...],
) -> None:
    """Load entry-point plugins, skipping disabled ones *before* import.

    pluggy handles the importlib.metadata machinery and reports the count
    for diagnostics. Disabled names are blocked FIRST so pluggy's own
    is_blocked() check skips them inside load_setuptools_entrypoints —
    before their modules are imported. Unregistering after loading would
    remove the hookimpls only after the module's import-time side effects
    had run, breaking the "skip during loading" promise in
    create_plugin_manager's docstring. The names are unblocked
    afterwards so an explicitly-passed extra plugin can still register
    under the same canonical name — extras are intentional and take final
    precedence.
    """
    for name in disable:
        pm.set_blocked(name)
    loaded = pm.load_setuptools_entrypoints(ENTRY_POINT_GROUP)
    logger.debug("loaded %d third-party plugin(s) from entry points", loaded)
    for name in disable:
        pm.unblock(name)


def create_plugin_manager(
    *,
    extra_plugins: list[Any] | tuple[Any, ...] = (),
    disable: list[str] | tuple[str, ...] = (),
    load_entrypoints: bool = True,
    load_builtins: bool = True,
) -> pluggy.PluginManager:
    """Construct a configured PluginManager for a modulith application.

    Args:
        extra_plugins: Plugin instances or modules to register explicitly.
            Useful for tests and for plugins that shouldn't be discovered
            via entry points.
        disable: Plugin names to skip during loading. Matches both
            built-in module paths and entry-point names.
        load_entrypoints: When False, skip entry-point discovery entirely.
            Tests use this for full control over registered plugins.
        load_builtins: When False, skip loading built-in plugins. Lets
            tests run against a clean slate without listing every
            built-in in ``disable``.

    Returns:
        A PluginManager with hookspecs registered and selected plugins
        loaded. Invoke hooks via ``pm.hook.<hookname>(**kwargs)``.

    Raises:
        pluggy.PluginValidationError: A registered plugin implements a hook
            name that matches no declared hookspec (typically a typo, e.g.
            ``modulith_verfy_module``). Surfacing this at startup beats a
            plugin that silently never fires.
    """
    pm = pluggy.PluginManager(ENTRY_POINT_GROUP)

    # Register the hookspec module so pluggy knows the contract. pluggy
    # validates hookimpl *signatures* against the spec at registration
    # time; misspelled hook *names* are caught by check_pending() at the
    # end of this function — either way, mistakes surface at startup, not
    # as a plugin that silently never fires.
    pm.add_hookspecs(hooks)

    # The observe-shield enforces the non-gating contract of the listener
    # lifecycle hooks (see _ObserveContractShield). Registered first, as
    # part of the manager's own wiring — but under a public name so
    # ``disable`` can switch it off like any plugin.
    if OBSERVE_SHIELD_NAME not in disable:
        pm.register(_ObserveContractShield(), name=OBSERVE_SHIELD_NAME)

    # Load built-ins first so external plugins can extend or override
    # via tryfirst/trylast hook ordering.
    if load_builtins:
        _load_builtin_plugins(pm, disable)

    # Then discover third-party plugins via entry points.
    if load_entrypoints:
        _load_entrypoint_plugins(pm, disable)

    # Safety net for anything the block above couldn't cover (e.g. a
    # plugin registered by another plugin's import side effects under a
    # disabled name). Accepts both module paths and entry-point names for
    # a uniform interface.
    for name in disable:
        if pm.has_plugin(name):
            pm.unregister(name=name)

    # Register explicit extras last so tests can take final precedence
    # over both built-ins and entry-point discoveries.
    for plugin in extra_plugins:
        pm.register(plugin)

    # Reject hookimpls whose names match no declared hookspec (typos).
    # Without this, pluggy accepts the registration and the misspelled
    # hook is simply never invoked — the plugin silently does nothing,
    # forever. Loud beats silent (see the markers.py docstring).
    pm.check_pending()

    return pm
