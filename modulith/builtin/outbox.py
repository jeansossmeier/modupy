"""The transactional outbox plugin.

This is the technically hardest piece in modulith and the feature that
justifies the project's existence over "FastAPI plus folders." When
enabled (via outbox != "memory"), publish() calls inside a transaction
are durably stored and delivered at-least-once after commit.

Critical correctness properties:
  1. Atomic — the publication record commits with the business transaction.
     ``_store.save`` adds the row to the *bound* session, so a rollback of
     the business transaction also discards the publication.
  2. Crash-safe — process death before delivery leaves the record
     incomplete; the retry loop picks it up on restart (crash sweep).
  3. At-least-once — a listener may be called more than once if delivery
     completes but completion-marking fails. Listeners must be idempotent.
  4. Non-reentrant *within a process* — the after-commit dispatch task and the
     crash-recovery sweep never run two concurrent attempts for the same row
     in one process (the ``_inflight_ids`` guard below). Across processes
     (the process-per-module topology), delivery is at-least-once and two
     workers' retry loops CAN dispatch the same row concurrently. The SQL
     store's ``SELECT ... FOR UPDATE SKIP LOCKED`` narrows that window but does
     not close it — which is exactly why property #3 holds and listeners must
     be idempotent.

Reference implementation: Spring Modulith's Event Publication Registry. We
mirror its semantics, including completion modes (update / delete / archive).

Two deliberate design decisions, where the storage-agnostic architecture
(SPEC §7.2: "this plugin is storage-agnostic; it just calls the store")
takes precedence over the granular plan:

  * **Persistence runs in async code, driven by the runtime** — not inside
    the synchronous ``modulith_before_event_published`` pluggy hook. pluggy
    hooks cannot ``await``, but ``PublicationStore.save`` is async, so the
    runtime calls ``await outbox.persist(event)`` when the outbox owns
    dispatch (see ``Runtime.publish``). ``modulith_before_event_published``
    stays the validation/enrichment slot the SPEC describes.
  * **session.info bookkeeping lives in the adapter's ``save``**, not here —
    ``session.info`` is SQLAlchemy-specific, so a storage-agnostic plugin
    must not touch it. The plugin just builds records and calls ``save``.

The dead-letter decision is plugin-side: ``PublicationStore`` exposes only
five methods (no ``mark_failed``), so ``save`` is an upsert-by-id used to
persist a failed attempt's ``attempt_count``/``last_error``, and a record
is "dead-lettered" once ``attempt_count`` reaches the threshold. The retry
loop skips such records; ``status`` counts them separately.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from modulith import EventPublication, PublicationStore, hookimpl

logger = logging.getLogger("modulith.outbox")

# The current transaction's session, set by adapter integration code
# (``bind_session``). The plugin only reads it to decide whether a publish
# is transactional; the adapter's ``save`` uses it to enlist the record.
_current_session: ContextVar[Any | None] = ContextVar("_modulith_current_session", default=None)

# Module-level state. Bound during configure().
_store: PublicationStore | None = None
_serializer: Any = None  # the EventSerializer instance
_completion_mode: str = "update"
_dead_letter_after_attempts: int = 10
_retry_interval_seconds: float = 30.0
_max_retry_backoff_seconds: float = 300.0
_retry_stale_seconds: float = 30.0
_retry_loop_enabled: bool = True
_retry_task: asyncio.Task[None] | None = None

# Guards check-then-set access to ``_retry_task``. The module slot is
# process-global while event loops are not: the main loop and sync.py's
# persistent daemon-thread loop can race _ensure_retry_loop() from two OS
# threads, and without a threading-level lock each would spawn its own
# retry-loop task, orphaning one with no cancellation path.
_retry_task_lock = threading.Lock()

# Publication ids currently being dispatched in THIS process. The after-commit
# dispatch task and the crash-recovery sweep can both pick up the same freshly
# committed row; this set makes a publication's delivery non-reentrant within a
# process (the cross-process case is handled by the store's row-level claim).
_inflight_ids: set[UUID] = set()

# Guards the check-then-add on ``_inflight_ids``. The non-reentrancy guarantee
# is documented at *process* granularity, but a bare set is only safe against
# interleaving on a single event loop — two loops on two OS threads (main loop
# + sync.py's daemon-thread loop) could both pass the membership check before
# either added, delivering the same publication concurrently. The lock is only
# ever held across the synchronous check+add / discard, never across an await.
_inflight_lock = threading.Lock()

# Listener-column sentinel marking a publication row as a *deferred broker
# send* rather than a local listener delivery. The durable path must not hand
# an event to the broker inside publish() — the business transaction hasn't
# committed yet and a broker send cannot be un-sent on rollback — so the route
# is persisted as its own row (enlisted in the bound session, atomic with the
# business work) and dispatched to the broker after commit, with the same
# at-least-once retry machinery local listeners get. The double-underscore
# namespace can't collide with a real listener id (module-qualified qualnames
# never start with it).
_BROKER_ROUTE_LISTENER_PREFIX = "__modulith.broker_route__:"


def _listener_id(handler: Any) -> str:
    """Stable, module-qualified identity for a listener.

    ``__qualname__`` alone is not unique: two modules can each define a
    module-level ``def on_order_created`` for the same event, and the bare
    qualname would collide — one listener delivered twice, the other never.
    Qualifying with ``__module__`` disambiguates them.
    """
    qualname = getattr(handler, "__qualname__", None)
    if qualname is None:
        return repr(handler)
    module = getattr(handler, "__module__", None)
    return f"{module}.{qualname}" if module else qualname


# ---------------------------------------------------------------------------
# Plugin initialization
# ---------------------------------------------------------------------------


def configure(
    store: PublicationStore,
    serializer: Any,
    *,
    completion_mode: str = "update",
    dead_letter_after_attempts: int = 10,
    retry_interval_seconds: float = 30.0,
    max_retry_backoff_seconds: float = 300.0,
    retry_stale_seconds: float = 30.0,
    start_loop: bool = True,
) -> None:
    """Wire up the outbox at startup.

    Binds the store/serializer and (when a loop is running and ``start_loop``
    is true) kicks off the background retry loop plus a one-shot crash sweep
    that re-dispatches anything left incomplete by a previous process.

    ``completion_mode`` is one of ``"update"`` (set ``completed_at``),
    ``"delete"`` (remove the row), or ``"archive"`` (move to archive). A
    record is dead-lettered once ``attempt_count`` reaches
    ``dead_letter_after_attempts``.

    ``start_loop=False`` binds state without starting the loop — used by
    tests that drive ``_dispatch_publication`` directly, and by callers that
    start the loop later on their own running loop.

    ``configure()`` and ``shutdown()`` are a paired lifecycle. Re-configuring
    while a previous retry loop is still alive cancels that stale task first:
    letting it live meant it silently kept polling the NEW store without ever
    running the new configuration's one-shot crash sweep.
    """
    global _store, _serializer, _completion_mode
    global _dead_letter_after_attempts, _retry_interval_seconds
    global _max_retry_backoff_seconds, _retry_stale_seconds, _retry_loop_enabled

    if completion_mode not in ("update", "delete", "archive"):
        raise ValueError(
            f"completion_mode must be 'update', 'delete', or 'archive', got {completion_mode!r}"
        )
    if dead_letter_after_attempts < 1:
        # 0 or negative would make every record — including never-attempted
        # crash-recovered ones — count as already dead-lettered: the sweep
        # would skip them all forever, silently blackholing publications.
        raise ValueError(
            f"dead_letter_after_attempts must be >= 1, got {dead_letter_after_attempts!r}"
        )

    _cancel_retry_task()

    _store = store
    _serializer = serializer
    _completion_mode = completion_mode
    _dead_letter_after_attempts = dead_letter_after_attempts
    _retry_interval_seconds = retry_interval_seconds
    _max_retry_backoff_seconds = max_retry_backoff_seconds
    _retry_stale_seconds = retry_stale_seconds
    _retry_loop_enabled = start_loop

    if start_loop:
        _ensure_retry_loop()


def _ensure_retry_loop() -> None:
    """Start the retry loop + crash sweep if a loop is running and none runs.

    No-ops outside a running loop (e.g. synchronous bootstrap). The loop is
    then started lazily on the first transactional publish, which always
    happens inside a running loop.

    The check-then-create runs under ``_retry_task_lock``: without it, two
    event loops on two OS threads could both see the slot empty and each
    spawn a retry-loop task, with the single-slot module global orphaning
    the loser (no reference, no cancellation path).
    """
    global _retry_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    with _retry_task_lock:
        if _retry_task is not None and not _retry_task.done():
            return
        _retry_task = loop.create_task(_retry_loop())


def _cancel_retry_task() -> None:
    """Best-effort, non-blocking cancellation of the current retry task.

    Used by the synchronous teardown paths (``configure()`` replacing a live
    loop, ``_reset_for_testing()``) that cannot ``await`` the cancellation
    like ``shutdown()`` does. The task may live on another thread's loop
    (sync.py's daemon loop), so cancellation is scheduled thread-safely; a
    task whose loop is already closed has nothing left to cancel.
    """
    global _retry_task
    with _retry_task_lock:
        task = _retry_task
        _retry_task = None
    if task is None or task.done():
        return
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if task.get_loop() is running:
        task.cancel()
    else:
        try:
            task.get_loop().call_soon_threadsafe(task.cancel)
        except RuntimeError:
            pass  # the task's loop is already closed


# ---------------------------------------------------------------------------
# The publish-time path
# ---------------------------------------------------------------------------


@hookimpl
def modulith_before_event_published(event: Any) -> None:
    """Validation/enrichment slot (SPEC §4.2). The outbox does not persist
    here — persistence is async and is driven by ``persist`` from the runtime
    (pluggy hooks cannot ``await``). Kept so the hookspec stays exercised and
    so other plugins composing on this hook see a registered implementation.
    """


@hookimpl
def modulith_after_event_published(event: Any, publication: EventPublication) -> None:
    """Observability hook — the outbox itself does nothing here."""


async def persist(event: Any) -> None:
    """Persist one publication per registered listener, inside the txn.

    Called by ``Runtime.publish`` when the outbox owns dispatch (a store is
    configured AND a session is bound). Each ``_store.save`` enlists the
    record in the bound session so it commits atomically with the business
    work; after commit the adapter's after-commit hook fires dispatch.
    """
    from .. import runtime as _rt

    assert _store is not None  # owns-dispatch guarantees this
    if _retry_loop_enabled:
        _ensure_retry_loop()
    bus = _rt._runtime.event_bus
    if bus is None:
        return
    handlers = bus.listeners_for(type(event))
    if not handlers:
        return

    fqcn = f"{type(event).__module__}.{type(event).__qualname__}"
    payload = _serializer.serialize(event)
    now = datetime.now(UTC)
    for handler in handlers:
        pub = EventPublication(
            id=uuid4(),
            payload=payload,
            event_type=fqcn,
            listener=_listener_id(handler),
            published_at=now,
        )
        await _store.save(pub)


async def persist_broker_route(event: Any, target: str) -> None:
    """Persist a deferred broker send for this transaction's publish.

    Called by ``Runtime.publish`` on the durable path when the event resolves
    to a broker target (cross-process topology). The route commits — or rolls
    back — atomically with the business transaction; the after-commit dispatch
    (``_dispatch_publication``) recognizes the sentinel listener id and sends
    the already-serialized payload to the broker. See
    ``_BROKER_ROUTE_LISTENER_PREFIX`` for why the send must not happen inside
    publish() itself.
    """
    assert _store is not None  # owns-dispatch guarantees this
    if _retry_loop_enabled:
        _ensure_retry_loop()
    fqcn = f"{type(event).__module__}.{type(event).__qualname__}"
    pub = EventPublication(
        id=uuid4(),
        payload=_serializer.serialize(event),
        event_type=fqcn,
        listener=_BROKER_ROUTE_LISTENER_PREFIX + target,
        published_at=datetime.now(UTC),
    )
    await _store.save(pub)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _resolve_listener(publication: EventPublication, event: Any) -> Any:
    """Find the registered handler whose qualname matches the publication."""
    from .. import runtime as _rt

    bus = _rt._runtime.event_bus
    if bus is None:
        return None
    for handler in bus.listeners_for(type(event)):
        if _listener_id(handler) == publication.listener:
            return handler
    return None


async def _complete(publication: EventPublication) -> None:
    """Apply the configured completion mode to a delivered publication."""
    assert _store is not None
    publication.completed_at = datetime.now(UTC)
    if _completion_mode == "delete":
        await _store.delete(publication.id)
    elif _completion_mode == "archive":
        await _store.archive(publication.id)
    else:
        await _store.mark_complete(publication.id)


async def _record_failure(publication: EventPublication, exc: Exception) -> None:
    """Persist one failed delivery attempt without hiding the publication."""
    assert _store is not None
    publication.attempt_count += 1
    publication.last_error = str(exc)[:500]
    publication.last_attempt_at = datetime.now(UTC)
    await _store.save(publication)
    if publication.attempt_count >= _dead_letter_after_attempts:
        logger.error(
            "publication %s dead-lettered after %d attempt(s): %s",
            publication.id,
            publication.attempt_count,
            publication.last_error,
        )


async def _dispatch_publication(publication: EventPublication) -> None:
    """Deliver one publication to its listener and complete or fail it.

    On success: completion mode is applied (mark_complete / delete / archive).
    On failure: ``attempt_count`` is incremented, ``last_error`` recorded, the
    record re-saved (upsert) so the failure persists, and observability error
    hooks fire. The record stays incomplete for the retry loop — or is
    considered dead-lettered once it reaches the attempt threshold.
    """
    from .. import runtime as _rt

    assert _store is not None and _serializer is not None
    assert publication.event_type is not None

    with _inflight_lock:
        if publication.id in _inflight_ids:
            # Already being delivered by a concurrent task in this process (the
            # after-commit dispatch racing the crash-recovery sweep, say). Skip so
            # a publication is never delivered twice concurrently within one
            # process — the lock makes the check+add atomic across OS threads,
            # not just across tasks on one event loop.
            return
        _inflight_ids.add(publication.id)
    try:
        if (publication.listener or "").startswith(_BROKER_ROUTE_LISTENER_PREFIX):
            # A deferred broker send (see persist_broker_route) — no local
            # listener to resolve; the serialized payload goes to the wire.
            await _dispatch_broker_route(publication)
            return

        try:
            event = _serializer.deserialize(publication.payload, publication.event_type)
        except Exception as exc:
            logger.exception(
                "could not deserialize publication %s (%s) — recording failed attempt",
                publication.id,
                publication.event_type,
            )
            await _record_failure(publication, exc)
            return

        handler = _resolve_listener(publication, event)
        pm = _rt._runtime.plugin_manager

        if handler is None:
            err = LookupError(
                f"no registered listener {publication.listener!r} for publication {publication.id}"
            )
            logger.warning("%s", err)
            await _record_failure(publication, err)
            return

        if pm is not None:
            pm.hook.modulith_on_listener_dispatch(
                event=event, listener_name=publication.listener, publication=publication
            )

        try:
            await handler(event)
        except Exception as exc:
            await _record_failure(publication, exc)
            if pm is not None:
                pm.hook.modulith_on_listener_error(
                    event=event,
                    listener_name=publication.listener,
                    publication=publication,
                    exception=exc,
                )
                pm.hook.modulith_on_listener_complete(
                    event=event,
                    listener_name=publication.listener,
                    publication=publication,
                    exception=exc,
                )
            return

        try:
            await _complete(publication)
        except Exception as exc:
            # Completion-marking failed AFTER a successful delivery. Route it
            # through _record_failure like a listener failure: otherwise the
            # record stays at attempt_count == 0 forever, _backoff_elapsed
            # treats it as immediately eligible on every sweep, and it can
            # never dead-letter — unbounded duplicate listener invocations.
            # The re-invocation this schedules is the documented at-least-once
            # property #3; listeners must be idempotent.
            logger.exception(
                "completion (%s) failed for publication %s after successful "
                "delivery — recording the attempt so backoff and dead-lettering "
                "still apply",
                _completion_mode,
                publication.id,
            )
            await _record_failure(publication, exc)
        # The listener itself succeeded, so the dispatch/complete span pairing
        # reports exception=None regardless of the completion write's outcome.
        if pm is not None:
            pm.hook.modulith_on_listener_complete(
                event=event,
                listener_name=publication.listener,
                publication=publication,
                exception=None,
            )
    finally:
        with _inflight_lock:
            _inflight_ids.discard(publication.id)


async def _dispatch_broker_route(publication: EventPublication) -> None:
    """Deliver a deferred broker send (see ``persist_broker_route``).

    Runs after commit (or from the retry loop / crash sweep). Success applies
    the configured completion mode; failure records the attempt so the retry
    loop re-sends — broker delivery gets the same at-least-once guarantee as
    local listeners, and a crash between commit and send is recovered by the
    sweep instead of losing the event for every remote consumer.
    """
    from .. import runtime as _rt

    assert publication.listener is not None  # caller matched the prefix
    target = publication.listener[len(_BROKER_ROUTE_LISTENER_PREFIX) :]
    try:
        registry = _rt._runtime.broker_registry
        scheme = target.partition(":")[0]
        if registry is None or scheme not in registry.schemes():
            registered = registry.schemes() if registry is not None else []
            raise LookupError(
                f"no broker adapter registered for scheme {scheme!r} "
                f"(publication {publication.id} targeting {target!r}; "
                f"registered schemes: {registered or 'none'})"
            )
        await registry.publish(
            target, publication.payload, {"event_type": publication.event_type or ""}
        )
    except Exception as exc:
        logger.warning("broker route %s failed for publication %s: %s", target, publication.id, exc)
        await _record_failure(publication, exc)
        return
    try:
        await _complete(publication)
    except Exception as exc:
        # Same rationale as the local-listener path: a failed completion write
        # must age the record so backoff/dead-lettering engage — the broker
        # send already happened, so the retry this schedules is the documented
        # at-least-once delivery, not a lost event.
        logger.exception(
            "completion (%s) failed for broker-routed publication %s — "
            "recording the attempt so backoff and dead-lettering still apply",
            _completion_mode,
            publication.id,
        )
        await _record_failure(publication, exc)
        return
    logger.debug("routed publication %s to broker target %s", publication.id, target)


# ---------------------------------------------------------------------------
# The retry loop
# ---------------------------------------------------------------------------


def _backoff_elapsed(publication: EventPublication) -> bool:
    """True when enough time has passed since publish to retry this record.

    A never-attempted record (count 0) dispatches immediately — this is the
    crash-recovery case, where a committed-but-undelivered publication must go
    out as soon as the sweep finds it. Backoff applies only to *re*-tries:
    exponential in ``attempt_count``, capped at the configured maximum, and
    measured from the *last attempt* (not the original publish) so a
    persistently-failing record actually backs off instead of being retried on
    every sweep once it ages past the cap.
    """
    if publication.attempt_count == 0:
        return True
    anchor = publication.last_attempt_at or publication.published_at
    if anchor is None:
        return True
    if anchor.tzinfo is None:
        # The plugin always WRITES UTC-aware timestamps, but a custom store's
        # find_incomplete() may round-trip them naive (SQLite, for one, drops
        # the tz). Interpret naive as UTC instead of letting the naive/aware
        # subtraction below raise TypeError and kill the sweep.
        anchor = anchor.replace(tzinfo=UTC)
    backoff = min(2.0 ** (publication.attempt_count - 1), _max_retry_backoff_seconds)
    age = (datetime.now(UTC) - anchor).total_seconds()
    return age >= backoff


async def _sweep(older_than: timedelta) -> None:
    """One pass: dispatch every eligible incomplete publication."""
    assert _store is not None
    for pub in await _store.find_incomplete(older_than):
        if pub.attempt_count >= _dead_letter_after_attempts:
            continue  # dead-lettered — no further retries
        if not _backoff_elapsed(pub):
            continue
        await _dispatch_publication(pub)


async def _guarded_sweep(older_than: timedelta) -> None:
    """Run one sweep, containing failures so the retry loop survives them.

    A transient store error (connection blip, failover) must only cost the
    one sweep it hit — without this containment it killed the retry-loop
    task outright, permanently stalling retries for EVERY pending
    publication until the next transactional publish happened to restart it.
    ``asyncio.CancelledError`` (a BaseException) still propagates for clean
    shutdown.
    """
    try:
        await _sweep(older_than)
    except Exception:
        logger.exception("outbox sweep failed — retrying on the next interval")


async def _retry_loop() -> None:
    """Background task: poll for incomplete publications and retry them.

    On entry, runs a crash-recovery sweep (``older_than=0``) to catch records
    left in flight by a previous process. Then polls on the configured
    interval with a staleness threshold so freshly-published-but-not-yet-
    committed-dispatched events aren't thrashed. Cancels cleanly on shutdown.
    """
    # The task copied the *creating* call site's contextvars (PEP 567). On the
    # lazy-start path that call site is a live request with a bound session —
    # frozen into this task forever, so a cascading publish() from a
    # retry-dispatched listener would enlist in that stale, already-closed
    # session and never be committed. Every dispatch this loop drives must run
    # session-less (a listener's own transactional work rebinds explicitly).
    _current_session.set(None)
    try:
        await _guarded_sweep(timedelta(0))  # crash recovery
        while True:
            await asyncio.sleep(_retry_interval_seconds)
            await _guarded_sweep(timedelta(seconds=_retry_stale_seconds))
    except asyncio.CancelledError:
        logger.debug("outbox retry loop stopping")
        raise


async def shutdown() -> None:
    """Cancel the retry loop and wait for it to stop. Idempotent.

    The module slot keeps pointing at the task until cancellation has
    actually completed: nulling it up front opened a window (cancel() only
    *requests*; the task needs another loop turn to unwind) where a
    concurrent transactional publish's ``_ensure_retry_loop()`` saw "no
    loop" and spawned a second retry task that survived shutdown entirely.
    """
    global _retry_task
    task = _retry_task
    if task is None:
        return
    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    with _retry_task_lock:
        # Clear the slot only if no concurrent configure()/_ensure_retry_loop()
        # installed a fresh task while we awaited the cancellation.
        if _retry_task is task:
            _retry_task = None


# ---------------------------------------------------------------------------
# Maintenance APIs (used by the CLI)
# ---------------------------------------------------------------------------


async def status() -> dict[str, int]:
    """Return counts: incomplete, completed, dead-lettered.

    Counts come from the store's *unbounded* ``count_open``/
    ``count_dead_lettered`` capabilities when available — the SQL store's
    ``find_incomplete`` is capped (LIMIT 100) and must NOT be the source of
    operational counts (a 50k backlog would otherwise read as 100). Stores
    without those capabilities (the in-memory test double) fall back to
    partitioning the incomplete set by the attempt threshold. ``completed``
    comes from ``count_completed``; stores that delete on completion report
    zero, which is correct for them.
    """
    assert _store is not None
    count_open = getattr(_store, "count_open", None)
    count_dead = getattr(_store, "count_dead_lettered", None)
    if count_open is not None and count_dead is not None:
        incomplete = await count_open()
        dead = await count_dead()
    else:
        pubs = await _store.find_incomplete(timedelta(0))
        incomplete = sum(1 for p in pubs if p.attempt_count < _dead_letter_after_attempts)
        dead = sum(1 for p in pubs if p.attempt_count >= _dead_letter_after_attempts)
    completed = 0
    counter = getattr(_store, "count_completed", None)
    if counter is not None:
        completed = await counter()
    return {"incomplete": incomplete, "completed": completed, "dead_lettered": dead}


async def force_retry(publication_id: UUID) -> None:
    """Immediately retry a specific publication, bypassing backoff.

    Searches both the retryable set and the dead-letter set: now that the SQL
    store excludes dead-letters from ``find_incomplete``, an operator forcing a
    retry of an exhausted publication (e.g. after fixing the listener) must
    still be able to reach it.
    """
    assert _store is not None
    candidates = list(await _store.find_incomplete(timedelta(0)))
    candidates += await list_dead_lettered()
    for pub in candidates:
        if pub.id == publication_id:
            await _dispatch_publication(pub)
            return
    logger.warning("force_retry: publication %s not found or already complete", publication_id)


async def purge_completed(older_than: timedelta) -> int:
    """Delete completed publications older than threshold; return count deleted.

    Delegates to an optional ``purge_completed`` store capability (a bulk
    delete is inherently storage-specific). Stores without it report zero.
    """
    assert _store is not None
    purger = getattr(_store, "purge_completed", None)
    if purger is None:
        return 0
    result: int = await purger(older_than)
    return result


async def list_dead_lettered() -> list[EventPublication]:
    """Return publications that have exhausted their retry budget.

    Uses the store's dedicated ``find_dead_lettered`` capability when available
    (the SQL store excludes dead-letters from ``find_incomplete`` so its capped
    retry window isn't starved). Stores without it fall back to partitioning the
    incomplete set by the attempt threshold.
    """
    assert _store is not None
    finder = getattr(_store, "find_dead_lettered", None)
    if finder is not None:
        return list(await finder())
    pubs = await _store.find_incomplete(timedelta(0))
    return [p for p in pubs if p.attempt_count >= _dead_letter_after_attempts]


async def retry_all_dead_lettered() -> int:
    """Resubmit every dead-lettered publication with a fresh retry budget.

    Each record's ``attempt_count``/``last_error`` is reset and re-saved before
    dispatch, so a transient failure that exhausted the budget gets a clean
    start rather than immediately re-dead-lettering. Returns the number of
    publications resubmitted.
    """
    assert _store is not None
    dead = await list_dead_lettered()
    for pub in dead:
        pub.attempt_count = 0
        pub.last_error = None
        await _store.save(pub)
        await _dispatch_publication(pub)
    return len(dead)


# ---------------------------------------------------------------------------
# Test support
# ---------------------------------------------------------------------------


def _reset_for_testing() -> None:
    """Reset module state to uninitialized. ONLY for tests.

    Cancels any live retry task (best-effort, like ``shutdown()`` but
    synchronous): merely dropping the reference leaked ghost retry loops
    that kept polling — and dispatching against — whatever store a later
    ``configure()`` bound.
    """
    global _store, _serializer, _completion_mode
    global _dead_letter_after_attempts, _retry_interval_seconds
    global _max_retry_backoff_seconds, _retry_stale_seconds, _retry_loop_enabled
    _cancel_retry_task()
    _store = None
    _serializer = None
    _completion_mode = "update"
    _dead_letter_after_attempts = 10
    _retry_interval_seconds = 30.0
    _max_retry_backoff_seconds = 300.0
    _retry_stale_seconds = 30.0
    _retry_loop_enabled = True
    with _inflight_lock:
        _inflight_ids.clear()


__all__ = [
    "_current_session",  # exported for adapters to bind
    "configure",
    "force_retry",
    "list_dead_lettered",
    "modulith_after_event_published",
    "modulith_before_event_published",
    "persist",
    "persist_broker_route",
    "purge_completed",
    "retry_all_dead_lettered",
    "shutdown",
    "status",
]
