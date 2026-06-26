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
from typing import Any

import pluggy

from . import hooks

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
)


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
    """
    pm = pluggy.PluginManager(ENTRY_POINT_GROUP)

    # Register the hookspec module so pluggy knows the contract. Plugin
    # implementations not matching a declared hookspec fail validation
    # at registration time — typos surface immediately, not silently.
    pm.add_hookspecs(hooks)

    # Load built-ins first so external plugins can extend or override
    # via tryfirst/trylast hook ordering. Skipping a missing built-in
    # is a real bug — surface it rather than papering over it.
    if load_builtins:
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
                raise RuntimeError(
                    f"failed to load built-in plugin {plugin_path!r}: {exc}"
                ) from exc

    # Discover third-party plugins via entry points. pluggy handles the
    # importlib.metadata machinery and reports the count for diagnostics.
    if load_entrypoints:
        loaded = pm.load_setuptools_entrypoints(ENTRY_POINT_GROUP)
        logger.debug("loaded %d third-party plugin(s) from entry points", loaded)

    # Apply the disable list to entry-point plugins after loading.
    # We accept both module paths and entry-point names here for a
    # uniform interface.
    for name in disable:
        if pm.has_plugin(name):
            pm.unregister(name=name)

    # Register explicit extras last so tests can take final precedence
    # over both built-ins and entry-point discoveries.
    for plugin in extra_plugins:
        pm.register(plugin)

    return pm
