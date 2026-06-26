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
HookspecMarker in modulith.hooks — pluggy uses it to validate that a
hook implementation matches a known spec.
"""

import pluggy

# The decorator plugin authors use to mark their hook implementations.
# Pluggy validates at registration time that the function name matches
# a declared hookspec, catching typos at startup rather than silently
# failing to invoke the hook.
hookimpl = pluggy.HookimplMarker("modulith")
