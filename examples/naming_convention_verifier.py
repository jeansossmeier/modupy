"""Example plugin: custom verification rule for event naming.

Demonstrates the aggregate-hook shape — multiple plugins implement
``modulith_verify_module`` and their results combine into the full
violation report. Built-in rules and this rule both run; a module
with both boundary issues and naming issues reports all of them.

Package this as ``modupy-naming-rules`` with entry point:

    [project.entry-points."modulith"]
    naming_rules = "modulith_naming_rules.plugin"

After install, ``modulith verify`` includes these checks automatically.
No code changes needed in the application — the plugin loads via the
entry point and contributes its rule.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil

from modulith import (
    ModuleInfo,
    Violation,
    ViolationSeverity,
    hookimpl,
)


@hookimpl
def modulith_verify_module(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Flag domain events not named in past tense.

    Domain events describe something that already happened —
    ``OrderCreated``, not ``CreateOrder``. The latter is a command, a
    different concept that should live in a request/response API not
    in the event stream. Catching this at verify time prevents the
    common confusion before it spreads through the codebase.
    """
    violations: list[Violation] = []

    # Import the module to inspect its public namespace. Discovery has
    # already imported it once at startup, so this is essentially free.
    package = importlib.import_module(module.package)

    # Events usually live in submodules (``contracts/events.py``) that the
    # package ``__init__`` need not re-export, so walk those too. A submodule
    # that fails to import raises, exactly like the package import above.
    namespaces = [package]
    for info in pkgutil.walk_packages(getattr(package, "__path__", []), f"{module.package}."):
        namespaces.append(importlib.import_module(info.name))

    seen: set[int] = set()
    for name, obj in (member for ns in namespaces for member in inspect.getmembers(ns)):
        # Heuristic: events are classes with a marker attribute set by
        # the @event decorator. Real implementation would use a more
        # robust check (e.g. registration in a per-module event list).
        # ``seen`` collapses an event re-exported by ``__init__``.
        if not getattr(obj, "__modulith_event__", False) or id(obj) in seen:
            continue
        seen.add(id(obj))

        # Only flag events this module actually defines. Cross-module events
        # live in the shared contracts package and get imported into the
        # namespace of every module that uses them, so without this check one
        # badly-named event is reported once per importer — each time blaming
        # a module that cannot rename it.
        owner = getattr(obj, "__module__", "")
        if owner != module.package and not owner.startswith(module.package + "."):
            continue

        if not _looks_past_tense(name):
            violations.append(
                Violation(
                    rule="event-past-tense",
                    message=(
                        f"event {name!r} should be named in past tense "
                        f"(e.g. 'OrderCreated', not 'CreateOrder'). Events "
                        f"describe what happened; commands describe intent."
                    ),
                    module=module.name,
                    # Warning rather than error — naming is a soft rule.
                    # Teams that want it strict can override the severity.
                    severity=ViolationSeverity.WARNING,
                )
            )

    return violations


# Crude past-tense detector. A production version would use a proper
# lexicon (NLTK's WordNet, or a curated allowlist of known event
# suffixes). For an example, this captures the common cases.
_PAST_TENSE_SUFFIXES = (
    "ed",  # Created, Updated, Deleted
    "wn",  # Drawn, Shown, Withdrawn
    "en",  # Taken, Broken, Hidden
    "ought",  # Brought, Bought, Thought
    "aught",  # Caught, Taught
)


def _looks_past_tense(name: str) -> bool:
    """True if name ends with a recognized past-tense suffix."""
    return any(name.endswith(suffix) for suffix in _PAST_TENSE_SUFFIXES)
