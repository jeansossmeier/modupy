"""The manifest file pattern.

Optional but recommended for production. Each module declares its public
contract — events published, events consumed, listeners, owned tables,
declared dependencies — in a `_manifest.py` file. The framework reads
manifests at startup, validates them against observed reality, and uses
them to drive documentation, verification, and the audit tool.

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

What the framework does with this — split across two surfaces by cost:

  Bootstrap manifest verification (`verify_manifest`, runs every startup
  when ``verify_manifests`` is on — cheap, in-process, no AST):
    1. Validates that every name in `listeners` actually got registered.
       Catches "module silently failed to import" — the worst kind of bug.
    2. Validates that every type in `publishes` is defined in the package
       namespace. Catches dead code and renamed events.

  AST boundary verifier (`builtin/verifier.py`, optional — runs via the
  ``modulith verify`` CLI / pre-commit, NOT during bootstrap):
    3. Validates that cross-module imports match `declared_dependencies`
       and that table access respects `owns_tables`. These need static
       analysis of the source tree, so they deliberately live outside the
       hot bootstrap path — run the verifier in CI to enforce them.

  Documentation + audit (read-only consumers):
    4. Feeds the documentation generator. The Application Module Canvas
       is built directly from manifests (`publishes`/`consumes`/etc.).
    5. Feeds the audit tool. Manifest coverage is a readiness signal.

`consumes` is currently descriptive only — it drives docs/audit and is not
cross-validated at bootstrap (no single process knows every publisher).
"""

from __future__ import annotations

import importlib
import inspect
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("modulith.manifest")


class _UndeclaredDependencies(tuple[str, ...]):
    """Tuple-shaped sentinel for an omitted dependency declaration."""


_UNDECLARED_DEPENDENCIES = _UndeclaredDependencies()


def _normalize_broker_targets(targets: object) -> tuple[str, ...]:
    """Validate and normalize broker targets declared by a module."""
    from .config import ConfigurationError

    if isinstance(targets, str) or not isinstance(targets, list | tuple):
        raise ConfigurationError(
            "broker_targets must be a list or tuple of 'scheme:destination' strings"
        )

    normalized: list[str] = []
    for target in targets:
        if type(target) is not str:
            raise ConfigurationError(
                f"broker_targets must contain only 'scheme:destination' strings; got {target!r}"
            )
        scheme, separator, destination = target.partition(":")
        if not separator or not scheme.strip() or not destination.strip():
            raise ConfigurationError(
                "broker_targets must contain non-empty 'scheme:destination' "
                f"strings; got {target!r}"
            )
        normalized.append(f"{scheme.strip()}:{destination.strip()}")
    return tuple(normalized)


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

    # Modules this module is allowed to import from. Always a tuple so callers
    # can iterate it; dependencies_declared preserves omitted vs explicit empty.
    declared_dependencies: tuple[str, ...] = _UNDECLARED_DEPENDENCIES
    dependencies_declared: bool = field(init=False, default=False)

    # Static broker destinations this module consumes from.
    broker_targets: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        dependencies: tuple[str, ...]
        if self.declared_dependencies is None or isinstance(
            self.declared_dependencies, _UndeclaredDependencies
        ):
            dependencies = _UNDECLARED_DEPENDENCIES
        else:
            dependencies = tuple(self.declared_dependencies)
        object.__setattr__(self, "declared_dependencies", dependencies)
        object.__setattr__(
            self,
            "dependencies_declared",
            not isinstance(dependencies, _UndeclaredDependencies),
        )
        object.__setattr__(self, "broker_targets", _normalize_broker_targets(self.broker_targets))


# Module-level registry. Keyed by package name. Populated by declare_module
# during import; consumed by the runtime during bootstrap.
_manifests: dict[str, Manifest] = {}


def declare_module(
    *,
    publishes: list[str] | tuple[str, ...] = (),
    consumes: list[str] | tuple[str, ...] = (),
    listeners: list[Callable[..., Any]] | tuple[Callable[..., Any], ...] = (),
    owns_tables: list[str] | tuple[str, ...] = (),
    declared_dependencies: list[str] | tuple[str, ...] | None = None,
    broker_targets: list[str] | tuple[str, ...] = (),
) -> None:
    """Register a manifest for the calling module.

    Call this at module scope in `_manifest.py`. The package name is
    auto-detected from the calling frame.

    ``declared_dependencies=None`` leaves dependency enforcement off. An
    explicit empty sequence means "this module depends on nothing".
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

    manifest_kwargs: dict[str, Any] = {
        "package": package,
        "publishes": tuple(publishes),
        "consumes": tuple(consumes),
        "listeners": tuple(listeners),
        "owns_tables": tuple(owns_tables),
        "broker_targets": _normalize_broker_targets(broker_targets),
    }
    if declared_dependencies is not None:
        manifest_kwargs["declared_dependencies"] = tuple(declared_dependencies)

    _manifests[package] = Manifest(**manifest_kwargs)


def get_manifest(package: str) -> Manifest | None:
    """Return the manifest for a module, or None if none was declared."""
    return _manifests.get(package)


def all_manifests() -> dict[str, Manifest]:
    """Return all registered manifests. Used by verifier and docs."""
    return dict(_manifests)


# Sentinel distinguishing "attribute missing" from "attribute bound to None"
# in the publishes check below.
_MISSING: Any = object()


def _listener_location(func: Callable[..., Any]) -> str | None:
    """Best-effort ``file:line`` of a listener's definition site.

    SPEC 5.4 promises manifest-verification failures 'with a clear error
    and file:line' (A4-r1-11). Returns None for objects the inspect module
    cannot resolve (builtins, C extensions) — the error stays useful
    without a location rather than failing the failure path.
    """
    try:
        source_file = inspect.getsourcefile(func)
        _, line = inspect.getsourcelines(func)
    except (OSError, TypeError):
        return None
    if source_file is None:
        return None
    return f"{source_file}:{line}"


def verify_manifest(manifest: Manifest, registered_listeners: set[Callable[..., Any]]) -> list[str]:
    """Check a manifest against observed reality.

    Scope is deliberately narrow — the two checks that are cheap and need no
    source analysis: (1) declared `listeners` are registered, (2) declared
    `publishes` names exist in the package namespace. `declared_dependencies`
    and `owns_tables` require AST analysis and are enforced by the boundary
    verifier (``builtin/verifier.py``), not here; `consumes` is descriptive
    (docs/audit). See the module docstring for the full split.

    Returns errors as plain strings rather than Violation objects because
    these are *manifest correctness* errors, not boundary violations.
    Different surface — different error path.
    """
    errors: list[str] = []

    # Listener adapters register async wrappers, while manifests reference the
    # original returned handlers. Fold the established origin marker into the
    # identity set so adapted listeners still verify.
    effective_listeners: set[Callable[..., Any]] = set(registered_listeners)
    for registered in registered_listeners:
        original = getattr(registered, "__modulith_sync_wrapped__", None)
        if original is not None:
            effective_listeners.add(original)

    for func in manifest.listeners:
        if func not in effective_listeners:
            qualname = getattr(func, "__qualname__", repr(func))
            location = _listener_location(func)
            where = f" ({location})" if location else ""
            errors.append(
                f"listener {qualname}{where} declared in {manifest.package} "
                "but not registered against the event bus "
                "— module may have failed to import"
            )

    try:
        mod = importlib.import_module(manifest.package)
    except ImportError:
        # Import failure is surfaced elsewhere; skip publishes check silently.
        return errors

    errors.extend(_publishes_errors(manifest, mod))
    return errors


def _publishes_errors(manifest: Manifest, mod: Any) -> list[str]:
    """Errors for declared ``publishes`` names missing from the package."""
    errors: list[str] = []
    mod_file = getattr(mod, "__file__", None)
    where = f" ({mod_file})" if mod_file else ""
    for name in manifest.publishes:
        # getattr with a sentinel (not hasattr) so that an event class
        # explicitly bound to None — e.g. a failed conditional import —
        # gets flagged too, not just a missing name (A4-r2-81).
        value = getattr(mod, name, _MISSING)
        if value is _MISSING:
            errors.append(
                f"event type {name!r} declared in {manifest.package} "
                f"but not defined in package namespace{where}"
            )
        elif value is None:
            errors.append(
                f"event type {name!r} declared in {manifest.package} "
                f"is bound to None in the package namespace{where} "
                "— failed conditional import or renamed event"
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
