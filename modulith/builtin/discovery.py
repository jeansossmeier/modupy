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

from modulith import ConfigurationError, ModuleInfo, hookimpl

logger = logging.getLogger("modulith.discovery")

# Side channel: package name -> one-line failure summary for every module
# whose own code or _manifest.py raised during the most recent discovery
# walk. Discovery itself must not raise for these (one broken module must
# not abort discovery of its siblings — see the loop below), but swallowing
# them entirely used to exempt exactly those modules from manifest
# verification: a module that never imports never registers a manifest, so
# the "module silently failed to import" safety net could not fire for the
# very failure mode it documents (A4-r1-10, A4-r3-133). The runtime calls
# modulith_after_module_load for every discovered module during bootstrap;
# the hookimpl below turns any recorded failure into a loud, aggregated
# ConfigurationError there.
_import_failures: dict[str, str] = {}


@hookimpl
def modulith_discover_modules(app_package: str) -> list[ModuleInfo]:
    """Discover modules as direct subpackages of `app_package`.

    Subpackages starting with underscore are private and skipped.
    Each surviving subpackage is imported eagerly so any @listener
    decorators it contains register against the event bus.

    Failure semantics (W3 R3-F1):

    * The APPLICATION PACKAGE's own import failure — missing package and a
      bug in the app's ``__init__`` alike (A4-r5-207 uniformity) — is a
      fatal misconfiguration: it raises ConfigurationError with the original
      exception chained as the cause, so bootstrap (and thus ``modulith
      verify``/``doctor``) fails loudly instead of going CI-green on an app
      that cannot even be imported.
    * Individual MODULES whose own import (or whose ``_manifest.py``) fails
      do not abort the walk; they are recorded and surfaced as a
      ConfigurationError when bootstrap fires modulith_after_module_load —
      one broken module must not hide its healthy siblings.
    """
    # Fresh walk, fresh failure ledger — stale entries from a previous
    # bootstrap in the same process must not fail an app that has been
    # fixed and re-discovered since.
    _import_failures.clear()

    try:
        root = importlib.import_module(app_package)
    except Exception as exc:
        # Deliberately broad (A4-r5-207): a NameError inside the app's
        # __init__ must fail exactly like a missing package (ImportError) —
        # but LOUDLY (W3 R3-F1). Returning [] here made `modulith verify`
        # print "✓ no boundary violations" and exit 0 on an unimportable
        # application package.
        logger.exception("could not import application package %r: %s", app_package, exc)
        raise ConfigurationError(
            f"the application package {app_package!r} failed to import "
            f"({type(exc).__name__}: {exc}). Nothing can be discovered or "
            "verified until it imports — fix the error above (full traceback "
            "logged) or correct [tool.modulith].package / MODULITH_PACKAGE."
        ) from exc

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
        # Recorded unconditionally: a module whose import fails below must
        # still appear here so bootstrap fires modulith_after_module_load
        # for it — that is where the recorded failure becomes a loud error.
        modules.append(ModuleInfo(name=name, package=package_name))

        # Import eagerly so @listener decorators run and register their
        # handlers. Failure here is a real bug in the user's code — record
        # it for bootstrap, but don't crash discovery for other modules.
        imported_ok = True
        try:
            importlib.import_module(package_name)
        except Exception as exc:
            imported_ok = False
            logger.exception("module %r failed to import: %s", package_name, exc)
            _import_failures[package_name] = f"module {package_name!r} failed to import: {exc!r}"

        # Auto-import _manifest.py if present. Only probe when the parent
        # imported cleanly: find_spec re-imports the parent to read its
        # __path__, so on a broken parent it would raise ModuleNotFoundError
        # and crash discovery for every *other* module — violating this hook's
        # "never raises" contract. Guard find_spec itself too, defensively.
        if imported_ok:
            manifest_name = f"{package_name}._manifest"
            try:
                spec = importlib.util.find_spec(manifest_name)
            except (ImportError, AttributeError, ValueError) as exc:
                logger.debug("could not probe for %r manifest: %s", package_name, exc)
                spec = None
            if spec is not None:
                try:
                    importlib.import_module(manifest_name)
                except Exception as exc:
                    logger.exception("module %r failed to import its manifest", package_name)
                    # A broken _manifest.py never reaches declare_module(),
                    # so the module would be silently exempt from manifest
                    # verification — record it for bootstrap instead.
                    _import_failures[package_name] = (
                        f"manifest {manifest_name!r} failed to import: {exc!r}"
                    )

    return modules


@hookimpl
def modulith_after_module_load(module: ModuleInfo) -> None:
    """Fail bootstrap loudly when a discovered module never actually loaded.

    The runtime fires this hook once per discovered module during bootstrap.
    If discovery recorded an import failure for the module (its own package
    or its ``_manifest.py`` raised), the module's listeners and manifest
    never registered — events it should handle would be dropped with no
    trace beyond a log line. Raising here turns that into the same loud
    startup failure that manifest verification produces, closing the gap
    where the exact bug the manifest check documents ("module silently
    failed to import") also prevented the check from running (A4-r1-10,
    A4-r3-133). All recorded failures are aggregated into one message so a
    multi-module breakage surfaces in a single startup error.
    """
    if module.package not in _import_failures:
        return
    details = "\n  - ".join(_import_failures[pkg] for pkg in sorted(_import_failures))
    raise ConfigurationError(
        "module discovery recorded import failure(s):\n  - "
        + details
        + "\nA module that fails to import cannot register its listeners or "
        "manifest, so its events would be silently dropped. Fix the import "
        "error(s) above — full tracebacks were logged during discovery."
    )
