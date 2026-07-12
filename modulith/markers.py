"""Marker re-exports for plugin authors.

Plugin authors import ``hookimpl`` from here to decorate their hook
implementations:

    from modulith import hookimpl

    @hookimpl
    def modulith_register_brokers(registry):
        registry.register("my-scheme", MyBroker(...))

This indirection is deliberate: by re-exporting from a tiny module
(rather than from pluggy directly), we can swap the underlying plugin
system in the future without breaking existing plugins. They keep
importing from ``modulith``; we keep the import surface stable.

The project name string ("modulith") must match the one passed to
HookspecMarker in modulith.hooks — pluggy uses it to recognize which
functions on a registered plugin are modulith hook implementations.
"""

import pluggy

# The decorator plugin authors use to mark their hook implementations.
# Validation is two-staged: pluggy checks the *signature* (argument
# names) against the declared hookspec at registration time, while
# misspelled hook *names* are caught by the pm.check_pending() call at
# the end of modulith.manager.create_plugin_manager(). Both surface as
# PluginValidationError at startup, rather than a plugin that silently
# never fires.
hookimpl = pluggy.HookimplMarker("modulith")
