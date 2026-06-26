"""Default module discovery: walk subpackages of the application root.

Every direct subpackage that doesn't start with an underscore becomes a
module. Single-file modules (.py files at the top level) are not modules
in the modulith sense — modules are packages with their own internal
structure.

This implementation is registered as a built-in plugin. Users who want
custom discovery (namespace packages, multi-root projects, manifest
files) can disable this and supply their own modulith_discover_modules
hook implementation.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import pkgutil

from modulith import ModuleInfo, hookimpl

logger = logging.getLogger("modulith.discovery")


@hookimpl
def modulith_discover_modules(app_package: str) -> list[ModuleInfo]:
    """Discover modules as direct subpackages of `app_package`.

    Subpackages starting with underscore are private and skipped.
    Each surviving subpackage is imported eagerly so any @listener
    decorators it contains register against the event bus.

    Returns an empty list if the application package can't be imported
    or contains no subpackages — never raises, so a misconfigured app
    still gets a clean error from the runtime banner ("0 modules
    discovered") rather than a cryptic ImportError.
    """
    try:
        root = importlib.import_module(app_package)
    except ImportError as exc:
        logger.error("could not import application package %r: %s", app_package, exc)
        return []

    # Single-file modules (no __path__) have no subpackages to discover.
    if not hasattr(root, "__path__"):
        logger.debug("%r is a module, not a package — nothing to discover", app_package)
        return []

    modules: list[ModuleInfo] = []
    # iter_modules walks one level deep — exactly what we want for
    # the "direct subpackages are modules" convention.
    for _finder, name, is_pkg in pkgutil.iter_modules(root.__path__):
        # Skip private subpackages. The underscore convention is the
        # Python-native way to mark something as internal.
        if name.startswith("_"):
            continue
        # Skip single-file submodules — modules are packages.
        if not is_pkg:
            continue

        package_name = f"{app_package}.{name}"
        modules.append(ModuleInfo(name=name, package=package_name))

        # Import eagerly so @listener decorators run and register their
        # handlers. Failure here is a real bug in the user's code —
        # log it loudly but don't crash discovery for other modules.
        try:
            importlib.import_module(package_name)
        except Exception as exc:
            logger.exception("module %r failed to import: %s", package_name, exc)

        # Auto-import _manifest.py if present. find_spec avoids an import
        # attempt (and its noise) when the file simply doesn't exist.
        manifest_name = f"{package_name}._manifest"
        spec = importlib.util.find_spec(manifest_name)
        if spec is not None:
            try:
                importlib.import_module(manifest_name)
            except Exception:
                logger.exception("module %r failed to import its manifest", package_name)

    return modules
