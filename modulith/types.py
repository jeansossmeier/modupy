"""Shared data types used across the modulith plugin contract.

These types appear in hookspec signatures and protocol method signatures,
so they form part of the public ABI for plugin authors. Keep them simple,
frozen where possible, and avoid coupling them to any particular storage
or transport backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID


@dataclass(frozen=True)
class ModuleInfo:
    """Describes a discovered application module.

    A module is a top-level subpackage of the application package
    (e.g. ``myapp.orders``, ``myapp.inventory``). The discovery hook
    produces these; the verifier and documentation hooks consume them.
    """

    # Logical name used in events, traces, and docs (e.g. "orders").
    # By default this is the last segment of the package path.
    name: str

    # Fully-qualified Python package, e.g. "myapp.orders".
    package: str

    # Optional explicit allowlist of module names this module may depend
    # on. Empty tuple means "infer from imports" — the verifier derives
    # the dependency graph from observed cross-module imports.
    declared_dependencies: tuple[str, ...] = ()


@dataclass
class EventPublication:
    """Outbox record for a published event.

    Created when an event is published inside a transaction; persisted
    via a PublicationStore so delivery survives crashes. Mutable because
    the registry updates ``completed_at`` and ``attempt_count`` over the
    record's lifetime.

    Field requirements: ``id`` and ``payload`` are unconditionally required.
    ``event_type``, ``listener``, and ``published_at`` are populated by the
    outbox plugin at save time (after the listener set is resolved); they
    default to None so partial-construction call sites (tests, in-flight
    transformations) don't need to know them upfront.
    """

    # Stable identifier for the publication record. Different from any
    # event-level ID the application might use — this is the outbox row.
    id: UUID

    # Serialized event body. Bytes (not str) so binary serializers like
    # Avro or Protobuf work without an extra encoding step.
    payload: bytes

    # Fully-qualified type name, e.g. "myapp.orders.events.OrderCreated".
    # Used to resolve the deserialization target on retry. Set by the
    # outbox plugin when it constructs the publication record. When the
    # record can originate outside the trusted process boundary (a shared
    # outbox table, a broker), treat this as untrusted input: serializers
    # resolve it via importlib, so deserialization should be restricted to
    # an allowlist (see EventSerializer.deserialize and
    # JsonEventSerializer's allowed_event_types).
    event_type: str | None = None

    # Identifier of the listener this publication targets. One event
    # with three listeners produces three EventPublication rows. Set by
    # the outbox plugin per-listener at save time.
    listener: str | None = None

    # When the event was first persisted to the outbox. Set by the outbox
    # plugin at save time using a UTC-aware timestamp.
    published_at: datetime | None = None

    # When the listener completed successfully. None means in-flight or
    # failed — the registry's retry loop only considers completed=None.
    completed_at: datetime | None = None

    # Incremented on every dispatch attempt. Plugins can use this for
    # backoff calculations and to surface "stuck" events in dashboards.
    attempt_count: int = 0

    # Last error message if the most recent attempt failed. Truncated
    # to a reasonable length by the store implementation.
    last_error: str | None = None

    # When the most recent delivery attempt ran. None until the first
    # *retry* (the after-commit/crash-sweep first delivery leaves it None).
    # Retry backoff is measured from this, not from ``published_at`` — so a
    # persistently-failing listener actually backs off instead of being
    # retried on every sweep once the record ages past the (capped) backoff.
    last_attempt_at: datetime | None = None


class ViolationSeverity(Enum):
    """Severity level for verification findings."""

    # Allows the build to proceed but is surfaced in reports.
    WARNING = "warning"

    # Fails the verifier — CI should reject the change.
    ERROR = "error"


@dataclass(frozen=True)
class Violation:
    """A boundary or rule violation detected during verification.

    Verifier plugins return lists of these. The aggregated result drives
    CLI exit codes, IDE markers, and documentation reports.
    """

    # Stable rule identifier, e.g. "no-cyclic-dependency". Used so
    # downstream tooling can suppress specific rules per module.
    rule: str

    # Human-readable description of the violation.
    message: str

    # Module where the violation was detected.
    module: str

    # Default severity is ERROR — make warnings explicit.
    severity: ViolationSeverity = ViolationSeverity.ERROR

    # Optional source location in "path/to/file.py:line" format.
    # Lets editors jump directly to the offending line.
    location: str | None = None
