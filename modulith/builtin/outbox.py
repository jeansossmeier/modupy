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
     incomplete; the retry loop picks it up on restart (crash sweep). A row
     the dead process was delivering under a lease (``claim_strategy=
     "lease"``) stays claimed until that lease expires, so it is recovered by
     the first sweep after expiry, up to ``claim_lease_seconds`` plus
     ``retry_interval_seconds`` after the crash. An advisory lock is released
     with the dead process's connection, so the crash sweep recovers those
     rows at once.
  3. At-least-once — a listener may be called more than once if delivery
     completes but completion-marking fails. Listeners must be idempotent.
  4. Non-reentrant *within a process* — the after-commit dispatch task and the
     crash-recovery sweep never run two concurrent attempts for the same row
     in one process (the ``_inflight_ids`` guard below). Across processes
     (the process-per-module topology), delivery is at-least-once and two
     workers' retry loops CAN dispatch the same row concurrently. With
     ``claim_strategy="lease"`` (default) or ``"advisory_lock"``, the store's
     claim/lock fencing closes that cross-process window for the retry sweep.
     ``claim_strategy="none"`` opts out of that fencing (SKIP LOCKED
     narrows but does not close it) — which is why property #3 still holds
     and listeners must be idempotent under that mode.

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
import inspect
import logging
import math
import threading
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from modulith import EventPublication, PublicationStore, hookimpl
from modulith._claims import (
    DEFAULT_CLAIM_BATCH_SIZE,
    DEFAULT_CLAIM_LEASE_SECONDS,
    DEFAULT_CLAIM_STRATEGY,
    VALID_CLAIM_STRATEGIES,
)
from modulith.brokers import _split_broker_target
from modulith.config import ConfigurationError
from modulith.serializers import JsonEventSerializer, _event_type_name

logger = logging.getLogger("modulith.outbox")

# The broker WIRE serializer. The wire format is fixed JSON in v1: the worker
# consumer and the direct (non-durable) publish path both speak
# JsonEventSerializer, so the durable path must put the same bytes on the
# wire. The *configured* outbox serializer (``configure(serializer=...)``)
# governs STORAGE of local-listener publication rows only — a binary storage
# serializer (Avro, Protobuf, pickle) must not leak onto the wire, where the
# consumer would dead-letter every event as poison.
_WIRE_SERIALIZER = JsonEventSerializer()

# The current transaction's session. Private, and deliberately absent from
# ``__all__``: the supported way to bind and release one is
# ``modulith.adapters.postgres_outbox.bind_session`` / ``unbind_session``,
# which restore the previous value on exit so nested binds don't clobber the
# outer session. The plugin only reads this to decide whether a publish is
# transactional; the adapter's ``save`` uses it to enlist the record. Read it
# through ``_bound_session()``: ``bind_session`` stores a ``_SessionBinding``
# holder rather than the session itself.
_current_session: ContextVar[Any | None] = ContextVar("_modulith_current_session", default=None)


class _SessionBinding:
    """Mutable holder ``bind_session`` puts in ``_current_session``.

    A task created inside the bound scope copies the context and so shares
    this very object. ``unbind_session`` clears ``session``, which ends the
    binding for those tasks too: their later publishes take the unbound path
    instead of enlisting in a session nobody will commit again.
    """

    __slots__ = ("session",)

    def __init__(self, session: Any) -> None:
        self.session: Any | None = session


def _bound_session() -> Any | None:
    """The session bound to the current context, or None if unbound or the
    binding has ended."""
    value = _current_session.get()
    if isinstance(value, _SessionBinding):
        return value.session
    return value


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

# Claim coordination (see modulith._claims). Bound in configure().
# Default ``"lease"``; third-party stores without ClaimingStore fall back to
# the original find_incomplete path at sweep time (capability duck-typing).
_claim_strategy: str = DEFAULT_CLAIM_STRATEGY
_claim_lease_seconds: float = DEFAULT_CLAIM_LEASE_SECONDS
_claim_batch_size: int = DEFAULT_CLAIM_BATCH_SIZE
_claim_owner: str = ""

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

    A callable instance or bound method is named after its class, so one class
    registered from two application modules would give both the same id; its
    id is prefixed with the registering module package (``orders:shared.X``)
    so each module's rows reach only that module's instance, in whichever
    worker sweeps them. A plain function's id stays module-qualified only.
    """
    from .. import runtime as _rt

    qualname = getattr(handler, "__qualname__", None)
    if qualname is None:
        return repr(handler)
    module = getattr(handler, "__module__", None)
    base = f"{module}.{qualname}" if module else qualname
    owner = None
    if inspect.ismethod(handler) or getattr(handler, "__modulith_instance_listener__", False):
        owner = _rt._runtime._listener_owners.get(handler)
    return f"{owner}:{base}" if owner else base


def _foreign_to_this_worker(publication: EventPublication) -> bool:
    """True for a listener row a sibling process-topology worker owns.

    Workers share one outbox table, but each dispatches only its own module's
    listeners. A sweep that claimed a sibling's row must hand it back
    untouched: dispatching it here would find no handler and charge the row
    an attempt, pushing it toward dead-letter before its owner delivers it.
    Broker-route rows carry no listener and any worker may send them.
    """
    from .. import runtime as _rt

    rt = _rt._runtime
    listener_id = publication.listener or ""
    if rt._hosted_module is None or listener_id.startswith(_BROKER_ROUTE_LISTENER_PREFIX):
        return False
    bus = rt.event_bus
    assert bus is not None  # sweeps dispatch nothing until bootstrap installs the bus
    local_ids = {
        _listener_id(handler)
        for event_type in bus.registered_event_types()
        for handler in rt.local_listeners(bus.listeners_for(event_type))
    }
    return listener_id not in local_ids


def _require_distinct_listener_ids(event_type: type, handlers: list[Any]) -> None:
    """Refuse to persist rows under a listener id two handlers share.

    ``_resolve_listener`` hands every row stamped with that id to the first
    matching handler, so the other would silently never run — two instances
    of one callable class, or bound methods of two instances, collide this way.
    """
    seen: dict[str, Any] = {}
    for handler in handlers:
        target = getattr(handler, "__modulith_sync_wrapped__", handler)
        listener_id = _listener_id(handler)
        other = seen.setdefault(listener_id, target)
        if other is not target:
            raise ConfigurationError(
                f"listeners {other!r} and {target!r} for {event_type.__qualname__} share "
                f"the outbox listener id {listener_id!r}, so durable delivery cannot "
                "tell them apart. Register each as a distinct class or module-level "
                "function: the stored id is the handler's module-qualified name."
            )


# ---------------------------------------------------------------------------
# Plugin initialization
# ---------------------------------------------------------------------------


def _resolve_dead_letter_threshold(store: Any, configured: int | None) -> int:
    """Unify the plugin's dead-letter threshold with the store's own.

    A store MAY duck-type expose ``dead_letter_after_attempts`` (e.g.
    ``PostgresPublicationStore`` writes an ``is_dead_lettered`` flag from ITS
    OWN threshold at ``save()`` time) plus a ``dead_letter_after_attempts_
    explicit`` marker. Stores without either attribute (the in-memory test
    double, third-party stores with no threshold of their own) simply defer
    entirely to this plugin's value — unchanged behavior for them.

    Precedence: if BOTH sides were explicitly set and disagree, that is a
    genuine misconfiguration — fail loudly here rather than silently picking
    one and leaving the store's ``is_dead_lettered`` flag and the plugin's own
    skip-check disagreeing on which rows are dead. If only one side is
    explicit, that value wins and is pushed onto the store so both agree.
    """
    store_value = getattr(store, "dead_letter_after_attempts", None)
    store_explicit = getattr(store, "dead_letter_after_attempts_explicit", False)
    if configured is not None and store_explicit and store_value != configured:
        raise ConfigurationError(
            "conflicting dead_letter_after_attempts: outbox.configure() got "
            f"{configured!r} but the store was constructed with {store_value!r}. "
            "Set it in exactly one place and let the other default, or set the "
            "same value in both."
        )
    if configured is not None:
        resolved = configured
    elif store_explicit and isinstance(store_value, int) and not isinstance(store_value, bool):
        resolved = store_value
    else:
        resolved = 10
    if resolved < 1:
        # 0 or negative would make every record — including never-attempted
        # crash-recovered ones — count as already dead-lettered: the sweep
        # would skip them all forever, silently blackholing publications.
        raise ValueError(f"dead_letter_after_attempts must be >= 1, got {resolved!r}")
    if hasattr(store, "dead_letter_after_attempts"):
        store.dead_letter_after_attempts = resolved
    return resolved


def configure(
    store: PublicationStore,
    serializer: Any,
    *,
    completion_mode: str = "update",
    dead_letter_after_attempts: int | None = None,
    retry_interval_seconds: float = 30.0,
    max_retry_backoff_seconds: float = 300.0,
    retry_stale_seconds: float = 30.0,
    start_loop: bool = True,
    claim_strategy: str = DEFAULT_CLAIM_STRATEGY,
    claim_lease_seconds: float = DEFAULT_CLAIM_LEASE_SECONDS,
    claim_batch_size: int = DEFAULT_CLAIM_BATCH_SIZE,
) -> None:
    """Wire up the outbox at startup.

    Binds the store/serializer and (when a loop is running and ``start_loop``
    is true) kicks off the background retry loop plus a one-shot crash sweep
    that re-dispatches anything left incomplete by a previous process.

    ``completion_mode`` is one of ``"update"`` (set ``completed_at``),
    ``"delete"`` (remove the row), or ``"archive"`` (move to archive). A
    record is dead-lettered once ``attempt_count`` reaches
    ``dead_letter_after_attempts``. ``None`` (the default) means "unset here";
    the effective value is unified with the store's own setting if it has one
    (see ``_resolve_dead_letter_threshold``), else defaults to 10.

    ``claim_strategy`` coordinates concurrent sweepers (see
    ``modulith._claims``): ``"lease"`` (default), ``"advisory_lock"``, or
    ``"none"``. Stores without the matching capability fall back to the
    original unclaimed ``find_incomplete`` path at sweep time.

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
    global _claim_strategy, _claim_lease_seconds, _claim_batch_size, _claim_owner

    if completion_mode not in ("update", "delete", "archive"):
        raise ValueError(
            f"completion_mode must be 'update', 'delete', or 'archive', got {completion_mode!r}"
        )
    if claim_strategy not in VALID_CLAIM_STRATEGIES:
        raise ValueError(
            f"claim_strategy must be one of {VALID_CLAIM_STRATEGIES}, got {claim_strategy!r}"
        )
    if not isinstance(claim_lease_seconds, (int, float)) or not (
        math.isfinite(claim_lease_seconds) and claim_lease_seconds > 0
    ):
        # Reject NaN, +/-inf, and non-positive leases — matching
        # config.py's _validate_outbox_options, which already does this.
        raise ValueError(
            f"claim_lease_seconds must be a positive finite number, got {claim_lease_seconds!r}"
        )
    if (
        not isinstance(claim_batch_size, int)
        or isinstance(claim_batch_size, bool)
        or claim_batch_size < 1
    ):
        raise ValueError(f"claim_batch_size must be a positive integer, got {claim_batch_size!r}")
    if claim_strategy == "advisory_lock" and not getattr(store, "supports_advisory_lock", False):
        raise ConfigurationError(
            "claim_strategy='advisory_lock' requires a store with "
            "supports_advisory_lock=True (Postgres pg_try_advisory_lock)"
        )
    if claim_strategy == "none":
        # Intentional contract: concurrent sweepers MAY double-dispatch.
        # Log once per configure so operators see the tradeoff.
        logger.warning(
            "outbox claim_strategy='none': concurrent sweepers may double-dispatch "
            "the same publication; listeners must be idempotent"
        )

    resolved_dead_letter = _resolve_dead_letter_threshold(store, dead_letter_after_attempts)

    _cancel_retry_task()

    _store = store
    _serializer = serializer
    _completion_mode = completion_mode
    _dead_letter_after_attempts = resolved_dead_letter
    _retry_interval_seconds = retry_interval_seconds
    _max_retry_backoff_seconds = max_retry_backoff_seconds
    _retry_stale_seconds = retry_stale_seconds
    _retry_loop_enabled = start_loop
    _claim_strategy = claim_strategy
    _claim_lease_seconds = float(claim_lease_seconds)
    _claim_batch_size = claim_batch_size
    # Unique per configure() so two processes / reconfigs don't share an owner id.
    _claim_owner = uuid4().hex

    if start_loop:
        _ensure_retry_loop()


def start() -> None:
    """Start the retry loop and its one-shot crash sweep on the running loop.

    Idempotent, and a no-op when no store is bound or no loop is running.
    ``configure()`` at module import time runs before the server's event loop
    exists, so a server calls this from its ASGI startup; without it, rows a
    crashed process left undelivered wait for the first transactional
    publish. CLI processes never call it, so they never sweep or dispatch.
    """
    if _store is not None:
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

    A task whose loop has since been closed (e.g. ``sync._run_nested_dispatch``
    ran a transactional publish on a throwaway loop, then closed it) is
    treated as absent rather than merely "not done" — it will never run
    another step, so the ``not _retry_task.done()`` guard alone would block
    every future retry loop for the rest of the process.
    """
    global _retry_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    with _retry_task_lock:
        if (
            _retry_task is not None
            and not _retry_task.done()
            and not _retry_task.get_loop().is_closed()
        ):
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


async def persist(event: Any) -> list[EventPublication]:
    """Persist one publication per registered listener, inside the txn.

    Called by ``Runtime.publish`` when the outbox owns dispatch (a store is
    configured AND a session is bound). Each ``_store.save`` enlists the
    record in the bound session so it commits atomically with the business
    work; after commit the adapter's after-commit hook fires dispatch.

    Returns the actual persisted records (empty when the event has no
    registered listener) — the runtime wraps these in an
    ``EventPublishReceipt`` for ``modulith_after_event_published`` instead
    of fabricating a placeholder unrelated to what was really saved.
    """
    from .. import runtime as _rt

    assert _store is not None  # owns-dispatch guarantees this
    if _retry_loop_enabled:
        _ensure_retry_loop()
    bus = _rt._runtime.event_bus
    if bus is None:
        return []
    handlers = _rt._runtime.local_listeners(bus.listeners_for(type(event)))
    if not handlers:
        return []
    _require_distinct_listener_ids(type(event), handlers)

    fqcn = f"{type(event).__module__}.{type(event).__qualname__}"
    payload = _serializer.serialize(event)
    now = datetime.now(UTC)
    saved: list[EventPublication] = []
    for handler in handlers:
        pub = EventPublication(
            id=uuid4(),
            payload=payload,
            event_type=fqcn,
            listener=_listener_id(handler),
            published_at=now,
        )
        await _store.save(pub)
        saved.append(pub)
    return saved


async def persist_broker_route(event: Any, target: str) -> EventPublication:
    """Persist a deferred broker send for this transaction's publish.

    Called by ``Runtime.publish`` on the durable path when the event resolves
    to a broker target (cross-process topology). The route commits — or rolls
    back — atomically with the business transaction; the after-commit dispatch
    (``_dispatch_publication``) recognizes the sentinel listener id and sends
    the already-serialized payload to the broker. See
    ``_BROKER_ROUTE_LISTENER_PREFIX`` for why the send must not happen inside
    publish() itself.

    The payload is serialized with the WIRE serializer (fixed JSON in v1),
    not the configured storage serializer — the row's payload goes to the
    broker verbatim, and the worker consumer decodes the wire format (see
    ``_WIRE_SERIALIZER``).

    Returns the persisted record — folded into the runtime's
    ``EventPublishReceipt`` alongside any per-listener records ``persist()``
    saved for the same publish.
    """
    assert _store is not None  # owns-dispatch guarantees this
    if _retry_loop_enabled:
        _ensure_retry_loop()
    fqcn = f"{type(event).__module__}.{type(event).__qualname__}"
    pub = EventPublication(
        id=uuid4(),
        payload=_WIRE_SERIALIZER.serialize(event),
        event_type=fqcn,
        listener=_BROKER_ROUTE_LISTENER_PREFIX + target,
        published_at=datetime.now(UTC),
    )
    await _store.save(pub)
    return pub


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _resolve_listener(publication: EventPublication, event: Any) -> Any:
    """Find the registered handler whose qualname matches the publication.

    Only reached for a publication whose event type is registered, which
    cannot be true without a bound bus — so the bus is never None here.
    """
    from .. import runtime as _rt

    bus = _rt._runtime.event_bus
    assert bus is not None
    for handler in _rt._runtime.local_listeners(bus.listeners_for(type(event))):
        if _listener_id(handler) == publication.listener:
            return handler
    return None


async def _complete(publication: EventPublication) -> None:
    """Apply the configured completion mode to a delivered publication.

    ``completed_at`` is stamped only AFTER the store call returns — not
    before. Setting it optimistically first (then having the store call
    fail) left the in-memory record looking completed while _record_failure
    went on to persist attempt_count/last_error on it: an inconsistent row
    that is simultaneously "completed" and "failed". If the store call below
    raises, the caller (_dispatch_publication) routes to _record_failure,
    which must see completed_at still None.

    Lease mode: when ``publication.claim_token`` is set and the store
    exposes ``complete_claim``, fence the write by token. A stale token
    means another sweeper already owns the row — abandon quietly without
    burning retry attempts (do not raise).
    """
    assert _store is not None
    token = publication.claim_token
    complete_claim = getattr(_store, "complete_claim", None) if token else None
    if token is not None and complete_claim is not None:
        ok = await complete_claim(publication.id, token, _completion_mode)
        if not ok:
            logger.warning(
                "stale claim token on complete for publication %s — abandoning "
                "(another sweeper owns this row)",
                publication.id,
            )
            return
        publication.completed_at = datetime.now(UTC)
        return

    if _completion_mode == "delete":
        await _store.delete(publication.id)
    elif _completion_mode == "archive":
        await _store.archive(publication.id)
    else:
        await _store.mark_complete(publication.id)
    publication.completed_at = datetime.now(UTC)


async def _record_failure(publication: EventPublication, exc: Exception) -> None:
    """Persist one failed delivery attempt without hiding the publication.

    Lease mode: fence the failure write with ``fail_claim`` when a claim
    token is present. A stale token means we lost the lease mid-dispatch —
    revert the in-memory attempt bump so we don't pretend the failure was
    recorded, and leave the peer claimant alone.
    """
    assert _store is not None
    publication.attempt_count += 1
    publication.last_error = str(exc)[:500]
    publication.last_attempt_at = datetime.now(UTC)

    token = publication.claim_token
    fail_claim = getattr(_store, "fail_claim", None) if token else None
    if token is not None and fail_claim is not None:
        ok = await fail_claim(publication, token)
        if not ok:
            # Revert optimistic bump — the failure was not persisted.
            publication.attempt_count -= 1
            logger.warning(
                "stale claim token on fail for publication %s — abandoning "
                "(another sweeper owns this row)",
                publication.id,
            )
            return
    else:
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

        # Fail closed on unregistered event types — regardless of whether
        # ``_serializer`` was itself constructed with an allowlist. Deserializing
        # an attacker-controlled ``event_type`` (a forged outbox row) resolves and
        # instantiates ANY importable class (see JsonEventSerializer.deserialize /
        # _resolve_class), so this is the shared seam that must gate it, not
        # something left to the caller's serializer configuration.
        bus = _rt._runtime.event_bus
        registered_type_names = (
            {_event_type_name(t) for t in _rt._runtime.local_event_types(bus)}
            if bus is not None
            else set()
        )
        if publication.event_type not in registered_type_names:
            unregistered_err = ValueError(
                f"event type {publication.event_type!r} is not a registered event type "
                f"for publication {publication.id} — refusing to deserialize"
            )
            logger.warning("%s", unregistered_err)
            await _record_failure(publication, unregistered_err)
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
        scheme = _split_broker_target(target)[0]
        if registry is None or scheme not in registry.schemes():
            registered = registry.schemes() if registry is not None else []
            raise LookupError(
                f"no broker adapter registered for scheme {scheme!r} "
                f"(publication {publication.id} targeting {target!r}; "
                f"registered schemes: {registered or 'none'})"
            )
        await registry.publish(
            target,
            publication.payload,
            {
                "event_type": publication.event_type or "",
                "publication_id": str(publication.id),
            },
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


# Largest exponent ``2.0 ** n`` accepts before overflowing a float64.
_MAX_BACKOFF_EXPONENT = 1023


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
    # Cap the exponent BEFORE exponentiating. ``2.0 ** 1024`` overflows the
    # float range and raises OverflowError, and that raise escapes the whole
    # sweep cycle rather than skipping one row — a permanent head-of-line
    # block, since the offending row sorts first (oldest last attempt) on the
    # very next cycle. Reachable whenever ``dead_letter_after_attempts`` is set
    # high enough for a row to keep accumulating attempts past 1024. 1023 is
    # the largest non-overflowing exponent and ``2.0 ** 1023`` already dwarfs
    # any finite backoff cap, so the min() below is unchanged.
    exponent = min(publication.attempt_count - 1, _MAX_BACKOFF_EXPONENT)
    backoff = min(2.0**exponent, _max_retry_backoff_seconds)
    age = (datetime.now(UTC) - anchor).total_seconds()
    return age >= backoff


async def _sweep(older_than: timedelta) -> None:
    """One pass: dispatch every eligible incomplete publication.

    Pre-dispatch guard: when the runtime isn't bootstrapped (startup
    ordering, or a bootstrap that is currently failing), dispatch cannot
    succeed for ANY row — no event bus to resolve listeners, no broker
    registry for routes. That is an infrastructure outage, not a poison
    message: skip the cycle WITHOUT incrementing attempts or touching
    backoff bookkeeping, so committed rows aren't burned toward the
    dead-letter threshold while the outage lasts. Genuine dispatch failures
    (listener raised, broker rejected) still record attempts and can
    dead-letter.

    The configured claim strategy selects the concurrency path:
      * lease + ClaimingStore → claim_batch, renew during dispatch, fence
      * advisory_lock + AdvisoryLockingStore → try_lock around dispatch
      * none, or missing capability → original find_incomplete path
    """
    from .. import runtime as _rt

    assert _store is not None

    if _claim_strategy == "lease" and hasattr(_store, "claim_batch"):
        await _sweep_lease(older_than, runtime_ready=_rt._runtime.event_bus is not None)
        return
    if _claim_strategy == "advisory_lock" and hasattr(_store, "try_lock_publication"):
        await _sweep_advisory(older_than, runtime_ready=_rt._runtime.event_bus is not None)
        return
    await _sweep_unclaimed(older_than, runtime_ready=_rt._runtime.event_bus is not None)


async def _sweep_unclaimed(older_than: timedelta, *, runtime_ready: bool) -> None:
    """Original find_incomplete path (claim_strategy=none or no capability)."""
    assert _store is not None
    pending = await _store.find_incomplete(older_than)
    if pending and not runtime_ready:
        logger.info(
            "outbox sweep: runtime is not bootstrapped — skipping %d pending "
            "publication(s) this cycle without burning retry attempts",
            len(pending),
        )
        return
    for pub in pending:
        if pub.attempt_count >= _dead_letter_after_attempts:
            continue  # dead-lettered — no further retries
        if not _backoff_elapsed(pub) or _foreign_to_this_worker(pub):
            continue
        await _dispatch_publication(pub)


async def _sweep_lease(older_than: timedelta, *, runtime_ready: bool) -> None:
    """Lease mode: claim a batch, re-arm each row's lease before its turn,
    renew during dispatch, fence complete/fail."""
    assert _store is not None
    claimed = await _store.claim_batch(  # type: ignore[attr-defined]
        owner=_claim_owner,
        batch_size=_claim_batch_size,
        lease_seconds=_claim_lease_seconds,
        older_than=older_than,
    )
    if claimed and not runtime_ready:
        logger.info(
            "outbox sweep: runtime is not bootstrapped — releasing %d claimed "
            "publication(s) this cycle without burning retry attempts",
            len(claimed),
        )
        # Release immediately so another process can reclaim once ready.
        for pub in claimed:
            if pub.claim_token:
                await _store.renew_claim(pub.id, pub.claim_token, 0.0)  # type: ignore[attr-defined]
        return
    for pub in claimed:
        if pub.attempt_count >= _dead_letter_after_attempts:
            if pub.claim_token:
                await _store.renew_claim(pub.id, pub.claim_token, 0.0)  # type: ignore[attr-defined]
            continue
        if not _backoff_elapsed(pub) or _foreign_to_this_worker(pub):
            # Release early: holding a full lease on a not-yet-due row, or on
            # a row only a sibling worker can deliver, would block every other
            # sweeper from picking it up sooner.
            # ponytail: foreign rows still occupy claim_batch slots, so a large
            # sibling backlog can delay this worker's own rows; filter the
            # claim query by local listener ids if that shows up.
            if pub.claim_token:
                await _store.renew_claim(pub.id, pub.claim_token, 0.0)  # type: ignore[attr-defined]
            continue
        # ``claim_batch`` stamps ONE shared expiry on the whole batch, but the
        # rows dispatch serially: a slow head of the batch can leave the tail's
        # lease expired before its turn, and a peer sweeper reclaims it. Re-arm
        # this row's lease immediately before dispatching it, and dispatch only
        # while the row is still ours — a lost re-arm means the peer owns the
        # row now, so delivering it here would be a second delivery under a
        # dead lease.
        still_ours = not pub.claim_token or await _store.renew_claim(  # type: ignore[attr-defined]
            pub.id, pub.claim_token, _claim_lease_seconds
        )
        if still_ours:
            await _dispatch_with_lease_renewal(pub)


async def _sweep_advisory(older_than: timedelta, *, runtime_ready: bool) -> None:
    """Advisory-lock mode: hold a PG advisory lock through each dispatch."""
    assert _store is not None
    pending = await _store.find_incomplete(older_than)
    if pending and not runtime_ready:
        logger.info(
            "outbox sweep: runtime is not bootstrapped — skipping %d pending "
            "publication(s) this cycle without burning retry attempts",
            len(pending),
        )
        return
    for pub in pending:
        if pub.attempt_count >= _dead_letter_after_attempts:
            continue
        if not _backoff_elapsed(pub) or _foreign_to_this_worker(pub):
            continue
        await _dispatch_under_advisory_lock(pub)


async def _dispatch_under_advisory_lock(publication: EventPublication) -> None:
    """Deliver ``publication`` while holding its advisory lock; skip it when
    another dispatcher (a peer's sweep or after-commit task) holds the lock.

    ``publication`` may have been read before locking, and a peer may have
    delivered or failed the row and released its lock since, so it is re-read
    under the lock and delivered only if still pending and past its backoff.
    Both the advisory sweep and the Postgres adapter's after-commit dispatch
    route through here.
    """
    assert _store is not None
    store_any: Any = _store  # AdvisoryLockingStore capability
    handle = await store_any.try_lock_publication(publication.id)
    if handle is None:
        return
    try:
        finder = getattr(_store, "find_by_id", None)
        current = await finder(publication.id) if finder is not None else publication
        if (
            current is not None
            and current.completed_at is None
            and current.attempt_count < _dead_letter_after_attempts
            and _backoff_elapsed(current)
        ):
            await _dispatch_publication(current)
    finally:
        await store_any.unlock_publication(handle, publication.id)


async def _dispatch_with_lease_renewal(publication: EventPublication) -> None:
    """Dispatch under an active lease, renewing at one-third of the lease.

    A lost lease (stale token) stops the renew loop but does not cancel
    dispatch — fencing happens in ``_complete`` / ``_record_failure``. A
    renewal that raises is logged and retried on the next interval until the
    lease it protects has expired; it never fails the delivery.
    """
    token = publication.claim_token
    if not token or not hasattr(_store, "renew_claim"):
        await _dispatch_publication(publication)
        return

    stop = asyncio.Event()
    renew_interval = _claim_lease_seconds / 3.0

    async def _renew_loop() -> None:
        assert _store is not None and token is not None
        # ClaimingStore capability — not on the base PublicationStore Protocol.
        store_any: Any = _store
        loop = asyncio.get_running_loop()
        lease_deadline = loop.time() + _claim_lease_seconds
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=renew_interval)
                return
            except TimeoutError:
                pass
            try:
                ok = await store_any.renew_claim(publication.id, token, _claim_lease_seconds)
            except Exception:
                expired = loop.time() >= lease_deadline
                logger.warning(
                    "lease renewal for publication %s raised — %s",
                    publication.id,
                    "the lease has expired; stopping renewals, completion will be fenced"
                    if expired
                    else "retrying until the lease expires",
                    exc_info=True,
                )
                if expired:
                    return
                continue
            if not ok:
                logger.warning(
                    "lost lease on publication %s during dispatch — "
                    "stopping renewals; completion will be fenced",
                    publication.id,
                )
                return
            lease_deadline = loop.time() + _claim_lease_seconds

    renew_task = asyncio.create_task(_renew_loop())
    try:
        await _dispatch_publication(publication)
    finally:
        stop.set()
        renew_task.cancel()
        try:
            await renew_task
        except asyncio.CancelledError:
            # This may be renew_task's own expected exit, or a cancellation
            # aimed at the ENCLOSING task (e.g. outbox.shutdown()) landing at
            # this exact suspension point. Re-raise only the latter, matching
            # _polling_consumer.PollingConsumer._cancel's idiom — otherwise
            # shutdown()'s cancellation is silently discarded here and its
            # poll loop spins forever.
            current = asyncio.current_task()
            if current is not None and current.cancelling() > 0:
                raise


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

    Cross-loop safe: the retry task may live on a DIFFERENT event loop than
    the one ``shutdown()`` is awaited from — sync.py's persistent
    daemon-thread loop runs the retry task while the application's main loop
    awaits ``shutdown()`` during teardown. Cancelling directly (``task.
    cancel()``) is only safe from the task's own loop; from any other loop it
    must go through ``call_soon_threadsafe``. Likewise ``await task`` on a
    foreign-loop task raises ("Task got Future attached to a different
    loop"), so completion is observed by polling ``task.done()`` instead.
    """
    global _retry_task
    task = _retry_task
    if task is None:
        return
    if not task.done():
        if task.get_loop().is_closed():
            # Stranded on a closed foreign loop — it will never take another
            # step, so cancelling or waiting for it would hang forever.
            logger.warning(
                "outbox retry task was stranded on a closed event loop; "
                "clearing it without waiting for it to finish"
            )
        else:
            stranded = False
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
                    # The loop closed between the check above and this call.
                    stranded = True
            if not stranded:
                while not task.done():
                    await asyncio.sleep(0.01)
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
    comes from ``count_completed`` plus the store's optional
    ``count_archived`` (rows moved out of the primary table under
    ``completion_mode="archive"`` are gone from ``count_completed`` by
    construction, so a store using that mode must expose ``count_archived``
    for its archived rows to be counted at all). Stores that delete on
    completion report zero, which is correct for them.
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
    archived_counter = getattr(_store, "count_archived", None)
    if archived_counter is not None:
        completed += await archived_counter()
    return {"incomplete": incomplete, "completed": completed, "dead_lettered": dead}


async def force_retry(publication_id: UUID) -> None:
    """Immediately retry a specific publication, bypassing backoff.

    Uses the store's ``find_by_id`` capability (a direct point lookup) when
    available: ``find_incomplete``/``find_dead_lettered`` are both capped
    windows (LIMIT 100), so scanning them could never reach a targeted row
    sitting further back in a large backlog. Stores without ``find_by_id``
    (third-party stores implementing only the paged finders) fall back to the
    bounded scan.
    """
    assert _store is not None
    finder = getattr(_store, "find_by_id", None)
    if finder is not None:
        pub = await finder(publication_id)
        if pub is None or pub.completed_at is not None:
            logger.warning(
                "force_retry: publication %s not found or already complete", publication_id
            )
            return
        await _dispatch_publication(pub)
        return
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
    """Return ALL publications that have exhausted their retry budget.

    Uses the store's dedicated ``find_dead_lettered`` capability when available
    (the SQL store excludes dead-letters from ``find_incomplete`` so its capped
    retry window isn't starved). Stores without it fall back to partitioning the
    incomplete set by the attempt threshold.

    Pages through the store's keyset-pagination capability
    (``find_dead_lettered(after=..., limit=...)``) so a backlog past a single
    100-row page is fully returned rather than silently truncated. A
    third-party store predating that signature — ``find_dead_lettered()``
    taking no arguments — raises ``TypeError`` on the first paginated call,
    which is caught to fall back to its single unbounded/capped result
    unchanged.
    """
    assert _store is not None
    finder = getattr(_store, "find_dead_lettered", None)
    if finder is None:
        pubs = await _store.find_incomplete(timedelta(0))
        return [p for p in pubs if p.attempt_count >= _dead_letter_after_attempts]
    results: list[EventPublication] = []
    after: tuple[datetime, UUID] | None = None
    page_size = 100
    while True:
        try:
            page = await finder(after=after, limit=page_size)
        except TypeError:
            return list(await finder())
        if not page:
            break
        results.extend(page)
        if len(page) < page_size:
            break
        last = page[-1]
        assert last.published_at is not None
        after = (last.published_at, last.id)
    return results


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
    global _claim_strategy, _claim_lease_seconds, _claim_batch_size, _claim_owner
    _cancel_retry_task()
    _store = None
    _serializer = None
    _completion_mode = "update"
    _dead_letter_after_attempts = 10
    _retry_interval_seconds = 30.0
    _max_retry_backoff_seconds = 300.0
    _retry_stale_seconds = 30.0
    _retry_loop_enabled = True
    _claim_strategy = DEFAULT_CLAIM_STRATEGY
    _claim_lease_seconds = DEFAULT_CLAIM_LEASE_SECONDS
    _claim_batch_size = DEFAULT_CLAIM_BATCH_SIZE
    _claim_owner = ""
    with _inflight_lock:
        _inflight_ids.clear()


__all__ = [
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
    "start",
    "status",
]
