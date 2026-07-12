"""Driver-shaped adapter contracts.

Drivers are "one wins" adapters — exactly one PublicationStore is active
per application, exactly one EventSerializer. They're wired explicitly
at startup: application setup passes instances to
``modulith.builtin.outbox.configure(store, serializer)``. There is no
entry-point auto-discovery for drivers — only hook plugins are
discovered via entry points (the ``modulith`` group).

Brokers are different: multiple can be active simultaneously, routed by
URI scheme. The Broker protocol defines the contract; the BrokerRegistry
in modulith.brokers handles dispatch.

All three protocols use ``runtime_checkable`` so applications can do
``isinstance(thing, PublicationStore)`` for diagnostics, but adapters
do **not** need to inherit from these — duck typing via Protocol is the
intended pattern.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from .types import EventPublication


@runtime_checkable
class PublicationStore(Protocol):
    """Storage backend for the transactional outbox.

    Implementations must integrate with the application's transaction
    lifecycle: ``save`` is called inside the business transaction so the
    publication record commits atomically with the data that produced it.

    Completion is tracked separately. ``mark_complete`` runs after the
    listener succeeds; ``find_incomplete`` drives the retry loop on
    startup and on schedule.

    All methods are async because real outbox stores are network-bound.
    Implementations may use any async DB driver (asyncpg, motor, aioredis).
    """

    async def save(self, publication: EventPublication) -> None:
        """Persist a new publication record.

        Called inside the business transaction. The store must use the
        same connection/session as the surrounding work so a rollback
        of the business transaction also rolls back this record.
        """
        ...

    async def mark_complete(self, publication_id: UUID) -> None:
        """Mark a publication as successfully delivered.

        The completion mode (UPDATE / DELETE / ARCHIVE) determines the
        physical effect — the protocol only requires that subsequent
        ``find_incomplete`` calls do not return this id.
        """
        ...

    async def find_incomplete(self, older_than: timedelta) -> list[EventPublication]:
        """Find publications still pending past the staleness threshold.

        ``older_than`` is measured from ``published_at``. Used by the
        retry loop to find work without thrashing on freshly-published
        events that simply haven't been dispatched yet.
        """
        ...

    async def archive(self, publication_id: UUID) -> None:
        """Move a completed publication to archive storage.

        Called when the configured completion mode is ARCHIVE. Stores
        that don't support archiving may implement this as a no-op or
        as a hard delete — but be explicit in adapter documentation.
        """
        ...

    async def delete(self, publication_id: UUID) -> None:
        """Hard-delete a publication record.

        Called when the completion mode is DELETE. Also used by the
        purge maintenance job for archive entries past their retention.
        """
        ...


@runtime_checkable
class EventSerializer(Protocol):
    """Strategy for converting events to/from bytes.

    The default serializer is JSON via ``json.dumps`` on a dataclass
    ``__dict__``. Real applications may want Pydantic, MessagePack,
    Avro, or Protobuf. The serializer is a single shared instance —
    swap it once, all events use the new format.
    """

    def serialize(self, event: Any) -> bytes:
        """Encode an event instance to bytes for storage and transport."""
        ...

    def deserialize(self, data: bytes, event_type: str) -> Any:
        """Decode bytes back to an event instance.

        ``event_type`` is the fully-qualified class name from the
        EventPublication record. Serializers that need the runtime class
        (most do) resolve it via ``importlib.import_module`` plus
        ``getattr``. Serializers that work from the bytes alone (some
        Avro setups) may ignore this argument.

        Treat ``event_type`` as UNTRUSTED input whenever records can
        originate outside the trusted process boundary (a shared outbox
        table, a broker): importlib-based resolution turns a forged value
        into an arbitrary-module import. Implementations should restrict
        the resolvable types — the default ``JsonEventSerializer``
        accepts an ``allowed_event_types`` allowlist and raises
        ``ValueError`` for anything else before resolving the class.
        """
        ...


@runtime_checkable
class Broker(Protocol):
    """External message broker adapter.

    Brokers are registered against URI schemes (``kafka``, ``sqs``,
    ``rabbitmq``) and invoked when an event's externalization target
    starts with that scheme. Implementations should be async and tolerate
    redelivery — the outbox guarantees at-least-once, not exactly-once,
    so consumers must be idempotent.
    """

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send a serialized message to the broker.

        ``target`` is the destination *within* the broker — topic name
        for Kafka, queue ARN for SQS, exchange + routing key for AMQP.
        The scheme prefix has already been stripped by the registry.

        ``headers`` are optional metadata, surfaced to consumers where
        the broker supports it (Kafka headers, AMQP properties, etc.).
        Brokers without header support should silently drop them.
        """
        ...

    async def close(self) -> None:
        """Release broker resources during application shutdown.

        Called once by the BrokerRegistry. Implementations should be
        idempotent — close-after-close should not raise.
        """
        ...
