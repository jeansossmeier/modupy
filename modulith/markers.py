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


# A subclass only so the instance has a docstring of its own: pluggy's marker
# defines __slots__, so ``hookimpl.__doc__`` cannot be assigned, and
# inspect.getdoc (what scripts/gen_api_reference.py renders) would fall back to
# pluggy's class docstring. pluggy declares the class @final, which only the
# type checker enforces; the subclass adds no behavior.
class _HookimplMarker(pluggy.HookimplMarker):  # type: ignore[misc]
    """Mark a function as a modulith hook implementation.

    ``hookimpl`` is already an instance bound to the ``modulith`` project
    name, so use it directly as ``@hookimpl`` or with pluggy's options as
    ``@hookimpl(tryfirst=True)``; there is nothing to instantiate. Name the
    function after the hook it implements (see ``modulith.hooks``) and give
    it only arguments that hook declares.
    """


# The decorator plugin authors use to mark their hook implementations.
# Validation is two-staged: pluggy checks the *signature* (argument
# names) against the declared hookspec at registration time, while
# misspelled hook *names* are caught by the pm.check_pending() call at
# the end of modulith.manager.create_plugin_manager(). Both surface as
# PluginValidationError at startup, rather than a plugin that silently
# never fires.
hookimpl = _HookimplMarker("modulith")
