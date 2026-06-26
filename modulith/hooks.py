"""Public extension contract for modulith plugins.

Defines every point where third-party code can extend or override
modulith's behavior. Each hook is a stable, versioned contract — adding
hooks is fine, changing existing signatures is a breaking change for
every plugin ever published.

Plugins implement these hooks using @hookimpl from modulith.markers and
are discovered via the "modulith" entry point group in pyproject.toml.

Hook shapes:
  * Aggregating (default): every plugin runs, results combine into a list.
  * firstresult=True: plugins run until one returns non-None; that wins.
  * Side-effect (return None): every plugin runs for its effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pluggy

from .types import EventPublication, ModuleInfo, Violation

# Avoid a circular import: BrokerRegistry imports from .protocols which
# imports from .types, and we only need the type for annotations here.
if TYPE_CHECKING:
    from .brokers import BrokerRegistry


# The hookspec marker. Plugin authors use the matching hookimpl marker
# from modulith.markers — the project name string must match.
hookspec = pluggy.HookspecMarker("modulith")


# ---------------------------------------------------------------------------
# Module lifecycle
# ---------------------------------------------------------------------------


@hookspec(firstresult=True)
def modulith_discover_modules(app_package: str) -> list[ModuleInfo] | None:
    """Discover application modules within a root package.

    The first plugin to return a non-None list wins. The built-in
    implementation (modulith.builtin.discovery) walks subpackages of
    ``app_package`` and treats each non-underscore-prefixed subpackage
    as a module. Override this hook for custom layouts: namespace
    packages, multi-root projects, or filesystem-based module manifests.

    Returns None to defer to the next plugin.
    """


@hookspec
def modulith_after_module_load(module: ModuleInfo) -> None:
    """Notify plugins that a module has been imported and registered.

    Called once per module at startup, after all listeners and event
    types in the module have been wired into the registry. Use this for
    plugin behavior that needs to inspect module contents — emitting
    startup metrics, building documentation, registering module-scoped
    resources.
    """


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


@hookspec
def modulith_verify_module(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Run verification rules against a module.

    All plugins run; their returned lists are concatenated to form the
    full violation report. Built-in rules check boundary respect,
    cyclic dependencies, and declared-dependency consistency. Custom
    plugins can add naming conventions, layering rules, or any other
    static check.

    Receives ``all_modules`` because some rules are cross-module
    (cycles, missing dependencies). Return an empty list when the
    module passes.
    """


# ---------------------------------------------------------------------------
# Event lifecycle
# ---------------------------------------------------------------------------


@hookspec
def modulith_before_event_published(event: Any) -> None:
    """Run immediately before an event is added to the outbox.

    Use for validation, enrichment, or audit logging at the publish
    boundary. Raising an exception aborts publication; if the publish
    call was inside a transaction, the transaction will roll back.

    This hook does **not** run for direct in-process dispatch outside
    a transaction context — it's specifically about the durable path.
    """


@hookspec
def modulith_after_event_published(
    event: Any,
    publication: EventPublication,
) -> None:
    """Run after an event has been persisted to the outbox.

    The event is durably recorded at this point. Failures in listeners
    will not undo the publication — they trigger retries via
    modulith_on_listener_error. This is the right hook for metrics
    counting events produced, distributed tracing of publish spans,
    and structured audit logs.
    """


@hookspec
def modulith_on_listener_dispatch(
    event: Any,
    listener_name: str,
    publication: EventPublication,
) -> None:
    """Run when a listener is about to be invoked.

    Fires once per (event, listener) pair, so an event with three
    listeners triggers this hook three times. Use for per-listener
    span creation, log correlation, or rate-limit decisions. The
    listener invocation happens regardless of what this hook does —
    it observes, it doesn't gate.
    """


@hookspec
def modulith_on_listener_error(
    event: Any,
    listener_name: str,
    publication: EventPublication,
    exception: BaseException,
) -> None:
    """Run when a listener raises an exception.

    The publication remains incomplete after this hook; the retry loop
    will pick it up on its next pass. Use this hook for alerting,
    structured error logging, or feeding dead-letter handlers. Do not
    re-raise — exceptions from this hook are swallowed to prevent one
    plugin's failure from masking another's.
    """


# ---------------------------------------------------------------------------
# Externalization
# ---------------------------------------------------------------------------


@hookspec(firstresult=True)
def modulith_resolve_event_target(event: Any) -> str | None:
    """Resolve a broker target for an event, overriding @externalized.

    First plugin to return a non-None target wins. Targets follow the
    "scheme:destination" format consumed by BrokerRegistry. Return None
    to defer to the next plugin or fall through to the static
    @externalized annotation on the event class. Use this hook for
    dynamic routing — tenant-aware topics, A/B-test channels, or
    feature-flag-controlled rerouting.
    """


@hookspec
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    """Register external broker adapters at startup.

    Plugins shipping broker integrations (Kafka, RabbitMQ, SQS, NATS,
    Redis Streams) register their adapters against URI schemes here.
    Called once during application bootstrap, before any events flow.
    Plugins should read configuration from environment variables or a
    plugin-specific config object — the registry itself is config-free.
    """


# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------


@hookspec
def modulith_render_documentation(
    modules: list[ModuleInfo],
    output_dir: str,
) -> list[str]:
    """Generate documentation artifacts for the module arrangement.

    Each plugin writes its outputs into ``output_dir`` and returns the
    list of paths it produced (relative to that directory). Built-ins
    generate Mermaid component diagrams, per-module Markdown canvases,
    and an event-flow diagram. Custom plugins can add OpenAPI specs,
    dependency reports, ADR templates, or any other generated artifact.
    """
