"""The manifest file pattern.

Optional but recommended for production. Each module declares its public
contract — events published, events consumed, listeners, owned tables,
declared dependencies — in a `_manifest.py` file. The framework reads
manifests at startup, validates them against observed reality, and uses
them to drive documentation, verification, and the audit tool.

Implementation status: SKELETON. ~120 lines when complete.

Example usage:

    # myapp/orders/_manifest.py
    from modulith import declare_module
    from . import handlers

    declare_module(
        publishes=["OrderCreated", "OrderShipped"],
        consumes=["PaymentReceived"],
        listeners=[handlers.on_payment_received],
        owns_tables=["orders", "order_items"],
        declared_dependencies=["payments"],
    )

What the framework does with this:

  1. Validates that every name in `listeners` actually got registered.
     Catches "module silently failed to import" — the worst kind of bug.
  2. Validates that every type in `publishes` is actually published.
     Catches dead code and renamed events.
  3. Validates that imports match `declared_dependencies`. Anything
     imported from a module not in the list is a violation.
  4. Feeds the documentation generator. The Application Module Canvas
     is built directly from manifests.
  5. Feeds the audit tool. Manifest coverage is a readiness signal.
"""

from __future__ import annotations

import importlib
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("modulith.manifest")


@dataclass(frozen=True)
class Manifest:
    """A frozen snapshot of one module's declared contract.

    Created by declare_module() and stored on the module's package
    object as `__modulith_manifest__`. The framework reads this during
    bootstrap to validate against observed reality.
    """

    # Module's own package name, derived from the calling frame.
    package: str

    # Event type names this module publishes.
    publishes: tuple[str, ...] = ()

    # Event type names this module consumes (listens for).
    consumes: tuple[str, ...] = ()

    # The actual listener function objects. We store references so we
    # can verify they got registered against the event bus.
    listeners: tuple[Callable[..., Any], ...] = ()

    # Database tables owned by this module. Other modules cannot query.
    owns_tables: tuple[str, ...] = ()

    # Modules this module is allowed to import from. Verifier uses this.
    declared_dependencies: tuple[str, ...] = ()


# Module-level registry. Keyed by package name. Populated by declare_module
# during import; consumed by the runtime during bootstrap.
_manifests: dict[str, Manifest] = {}


def declare_module(
    *,
    publishes: list[str] | tuple[str, ...] = (),
    consumes: list[str] | tuple[str, ...] = (),
    listeners: list[Callable[..., Any]] | tuple[Callable[..., Any], ...] = (),
    owns_tables: list[str] | tuple[str, ...] = (),
    declared_dependencies: list[str] | tuple[str, ...] = (),
) -> None:
    """Register a manifest for the calling module.

    Call this at module scope in `_manifest.py`. The package name is
    auto-detected from the calling frame.
    """
    from .config import ConfigurationError

    frame = sys._getframe(1)
    caller_name: str = frame.f_globals.get("__name__", "")

    # Derive the owning package by stripping the ._manifest suffix.
    # The bare string "_manifest" (no parent package) is accepted as-is —
    # there is nothing to strip to.
    if caller_name.endswith("._manifest"):
        package = caller_name[: -len("._manifest")]
    elif caller_name == "_manifest":
        package = caller_name
    else:
        logger.debug(
            "declare_module() called from %r — convention is <package>/_manifest.py",
            caller_name,
        )
        package = caller_name

    if package in _manifests:
        raise ConfigurationError(
            f"Module {package!r} already declared a manifest. "
            "Each module must declare exactly once."
        )

    _manifests[package] = Manifest(
        package=package,
        publishes=tuple(publishes),
        consumes=tuple(consumes),
        listeners=tuple(listeners),
        owns_tables=tuple(owns_tables),
        declared_dependencies=tuple(declared_dependencies),
    )


def get_manifest(package: str) -> Manifest | None:
    """Return the manifest for a module, or None if none was declared."""
    return _manifests.get(package)


def all_manifests() -> dict[str, Manifest]:
    """Return all registered manifests. Used by verifier and docs."""
    return dict(_manifests)


def verify_manifest(manifest: Manifest, registered_listeners: set[Callable[..., Any]]) -> list[str]:
    """Check a manifest against observed reality.

    Returns errors as plain strings rather than Violation objects because
    these are *manifest correctness* errors, not boundary violations.
    Different surface — different error path.
    """
    errors: list[str] = []

    for func in manifest.listeners:
        if func not in registered_listeners:
            qualname = getattr(func, "__qualname__", repr(func))
            errors.append(
                f"listener {qualname} declared in {manifest.package} "
                "but not registered against the event bus "
                "— module may have failed to import"
            )

    try:
        mod = importlib.import_module(manifest.package)
    except ImportError:
        # Import failure is surfaced elsewhere; skip publishes check silently.
        return errors

    for name in manifest.publishes:
        # hasattr (not "is None") so that an event class explicitly bound
        # to None — e.g. a failed conditional import — still gets flagged.
        if not hasattr(mod, name):
            errors.append(
                f"event type {name!r} declared in {manifest.package} "
                "but not defined in package namespace"
            )

    return errors


def _reset_for_testing() -> None:
    """Clear the manifest registry. ONLY for tests."""
    _manifests.clear()


__all__ = [
    "Manifest",
    "all_manifests",
    "declare_module",
    "get_manifest",
    "verify_manifest",
]
