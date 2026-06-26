"""The transactional outbox plugin.

This is the technically hardest piece in modulith and the feature that
justifies the project's existence over "FastAPI plus folders." When
enabled (via outbox != "memory"), publish() calls inside a transaction
are durably stored and delivered at-least-once after commit.

Implementation status: SKELETON. ~150 lines for the plugin itself; the
storage adapter (e.g. modulith/adapters/postgres_outbox.py) is a separate
~180 lines. Together: ~330 lines total.

Critical correctness properties:
  1. Atomic — publication record commits with the business transaction.
  2. Crash-safe — process death before delivery leaves the record
     incomplete; retry loop picks it up on restart.
  3. At-least-once — listener may be called more than once if a delivery
     completes but completion-marking fails. Listeners must be idempotent.
  4. Ordered per-publication — retries for a given publication are
     serialized; never two concurrent attempts.

Reference implementation: Spring Modulith's Event Publication Registry.
We mirror its semantics closely, including completion modes (UPDATE,
DELETE, ARCHIVE).
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from datetime import timedelta
from typing import Any
from uuid import UUID

from modulith import EventPublication, PublicationStore, hookimpl

logger = logging.getLogger("modulith.outbox")

# The current transaction's session, set by adapter integration code.
# Plugins consult this in modulith_before_event_published.
_current_session: ContextVar[Any | None] = ContextVar("_modulith_current_session", default=None)

# Module-level state. Bound during plugin initialization.
_store: PublicationStore | None = None
_serializer: Any = None  # the EventSerializer instance
_dead_letter_after_attempts: int = 10
_retry_interval_seconds: int = 30
_max_retry_backoff_seconds: int = 300


# ---------------------------------------------------------------------------
# Plugin initialization
# ---------------------------------------------------------------------------


def configure(
    store: PublicationStore,
    serializer: Any,
    *,
    dead_letter_after_attempts: int = 10,
    retry_interval_seconds: int = 30,
) -> None:
    """Wire up the outbox at startup.

    Called by the runtime when configuration enables the outbox. The
    store and serializer are resolved from the configured outbox name
    via entry points (e.g. outbox="postgres" → modulith-postgres adapter).

    IMPLEMENTATION TODO:
    1. Bind globals: _store, _serializer, etc.
    2. Start the retry loop as a background asyncio task. Store a
       reference so we can cancel it on shutdown.
    3. On startup, call _store.find_incomplete(timedelta(0)) to catch
       any in-flight events from a previous process — these are the
       crash-recovery cases.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


# ---------------------------------------------------------------------------
# The publish-time hook
# ---------------------------------------------------------------------------


@hookimpl
def modulith_before_event_published(event: Any) -> None:
    """Persist the event to the outbox if a transaction is active.

    IMPLEMENTATION TODO:
    1. Check _current_session.get() — if None, no transaction context;
       just return (the in-memory bus will dispatch directly).
    2. Resolve which listeners are registered for type(event). One
       publication record per listener (Spring's per-listener model).
    3. For each listener:
       a. Build EventPublication with id=uuid4(), event_type=
          f"{type(event).__module__}.{type(event).__qualname__}",
          payload=_serializer.serialize(event), listener=name,
          published_at=now_utc(), completed_at=None.
       b. Await _store.save(publication). The store uses the same
          session as the business transaction, so this commits atomically.
       c. Tag the session with a pending-dispatch list so the
          after_commit hook can find it.

    The session integration is in the adapter file (postgres_outbox.py).
    This plugin is storage-agnostic; it just calls the store.

    NOTE: this hook runs during async publish() calls. publish_sync()
    propagates the contextvar via run_coroutine_threadsafe, so this
    hook runs in the same context whether the original call was sync
    or async.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


@hookimpl
def modulith_after_event_published(event: Any, publication: EventPublication) -> None:
    """Observability hook (spans, metrics) — outbox itself does nothing here.

    The outbox plugin doesn't need this hook; it's listed in the SPEC
    for symmetry. Kept as a stub so the hookspec is exercised.
    """


# ---------------------------------------------------------------------------
# The dispatch loop
# ---------------------------------------------------------------------------


async def _dispatch_publication(publication: EventPublication) -> None:
    """Deliver one publication to its listener and mark complete.

    IMPLEMENTATION TODO:
    1. Deserialize the event via _serializer.deserialize(payload, event_type).
    2. Resolve the listener function by name (the runtime maintains a
       name -> function map).
    3. Call await listener(event).
    4. On success: await _store.mark_complete(publication.id) — or
       _store.delete / _store.archive depending on configured completion mode.
    5. On failure:
       a. Increment publication.attempt_count.
       b. Record publication.last_error = str(exc)[:500].
       c. If attempt_count >= _dead_letter_after_attempts, mark
          dead-lettered (store-specific; Postgres sets a column flag).
       d. Otherwise, leave incomplete — retry loop picks it up.
       e. Call modulith_on_listener_error hook for observability.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


async def _retry_loop() -> None:
    """Background task: poll for incomplete publications and retry them.

    IMPLEMENTATION TODO:
    Forever:
    1. Sleep _retry_interval_seconds (asyncio.sleep, cancellable).
    2. Get incomplete: await _store.find_incomplete(timedelta(seconds=30))
       — the threshold avoids picking up freshly-published events that
       just haven't dispatched yet.
    3. For each: await _dispatch_publication(pub) with backoff:
         backoff = min(2 ** pub.attempt_count, _max_retry_backoff_seconds)
         if (now - pub.published_at).total_seconds() < backoff: skip
    4. Repeat.

    Cancellation: the runtime cancels this task on shutdown. We catch
    asyncio.CancelledError, log "outbox retry loop stopping", and re-raise.
    """
    raise NotImplementedError("Phase 1 — see TODO above")


# ---------------------------------------------------------------------------
# Maintenance APIs (used by the CLI)
# ---------------------------------------------------------------------------


async def status() -> dict[str, int]:
    """Return counts: incomplete, completed, dead-lettered."""
    raise NotImplementedError("Phase 1")


async def force_retry(publication_id: UUID) -> None:
    """Immediately retry a specific publication, bypassing backoff."""
    raise NotImplementedError("Phase 1")


async def purge_completed(older_than: timedelta) -> int:
    """Delete completed publications older than threshold; return count deleted."""
    raise NotImplementedError("Phase 1")


__all__ = [
    "_current_session",  # exported for adapters to bind
    "configure",
    "force_retry",
    "modulith_after_event_published",
    "modulith_before_event_published",
    "purge_completed",
    "status",
]
