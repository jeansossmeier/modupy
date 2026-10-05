"""Internal claim abstraction for outbox delivery concurrency control.

Three ``claim_strategy`` values govern how concurrent outbox sweepers —
multiple processes/workers each running the retry loop against the same
durable store — avoid dispatching the same publication twice:

  * ``"lease"`` (default) — a sweeper atomically claims a batch of rows
    (writes ``claim_owner``/``claim_token``/``claim_until``, committing
    before dispatch), renews the lease at one-third of ``claim_lease_seconds``
    while dispatch is in flight, and fences its completion/failure write by
    ``claim_token`` so a sweeper whose lease already expired cannot silently
    clobber a newer claimant's row.
  * ``"advisory_lock"`` — a sweeper holds a PostgreSQL advisory lock (one
    exclusive per-connection lock keyed by the publication id) for the
    duration of dispatch. Postgres-only: a store backed by a non-Postgres
    engine must reject this strategy at construction time.
  * ``"none"`` — no claim coordination at all. Two sweepers CAN dispatch the
    same row concurrently. This is a documented, intentional tradeoff
    (lower coordination overhead) — logged once at configure() time so
    operators see it.

This module is intentionally storage- and runtime-agnostic (no SQLAlchemy,
no import of ``modulith.builtin.outbox`` or ``modulith.config``) so it can be
imported cheaply from both the config-validation path and any adapter.

``modulith.builtin.outbox`` (the storage-agnostic plugin) checks a store for
the ``ClaimingStore``/``AdvisoryLockingStore`` capability via
``isinstance``/``getattr`` duck typing before using any of this — third-party
stores (or the in-memory test double) that omit these methods simply keep
the original non-claiming dispatch path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable
from uuid import UUID

from .types import EventPublication


class LockConnectionTimeout(Exception):
    """An advisory-locking store found no free lock connection within its
    pool timeout. The row was not locked, read or charged an attempt."""


VALID_CLAIM_STRATEGIES = ("lease", "advisory_lock", "none")
DEFAULT_CLAIM_STRATEGY = "lease"
DEFAULT_CLAIM_LEASE_SECONDS = 60.0
DEFAULT_CLAIM_BATCH_SIZE = 100

# Listener-column prefix of a row that is a deferred broker send, not a local
# listener delivery (see ``modulith.builtin.outbox``). Any worker may send one.
BROKER_ROUTE_LISTENER_PREFIX = "__modulith.broker_route__:"


@dataclass(frozen=True)
class Claim:
    """A granted claim on one publication row (lease-mode bookkeeping).

    ``token`` fences completion/failure writes: a write is only honored if
    the row's current ``claim_token`` still matches. ``until`` is the lease
    expiry, renewed by the dispatcher at one-third of the lease interval.
    """

    publication_id: UUID
    owner: str
    token: str
    until: datetime


@runtime_checkable
class ClaimingStore(Protocol):
    """Optional lease-mode capability a PublicationStore may implement.

    Stores without this get the outbox plugin's original non-claiming
    dispatch path (``find_incomplete`` + direct dispatch).
    """

    async def claim_batch(
        self, *, owner: str, batch_size: int, lease_seconds: float, older_than: timedelta
    ) -> list[EventPublication]:
        """Atomically claim up to ``batch_size`` unclaimed/expired-lease rows.

        Returned publications carry ``claim_owner``/``claim_token``/
        ``claim_until`` populated so the caller can renew and fence on them.

        Optional keywords: the sweep passes one only to a store whose
        ``claim_batch`` names it as a parameter (``**kwargs`` does not count),
        so a store written against the four required keywords keeps working.

        * ``exclude_ids`` (a collection of publication ids) — rows this
          process is delivering right now. They are neither claimed nor
          charged, however long their lease has lapsed.
        * ``listeners`` (a collection of listener ids) — the listeners this
          worker hosts, passed only by a process-per-module worker. Only rows
          whose listener is in it, plus broker-route rows (any worker may send
          those), are claimed; a sibling worker's rows are left uncharged for
          it. Omitted means every row is claimable.
        """
        ...

    async def renew_claim(self, publication_id: UUID, token: str, lease_seconds: float) -> bool:
        """Extend a still-held claim's lease. Returns False if ``token`` is
        stale (the lease already expired and/or another owner re-claimed the
        row) — the caller must stop dispatching and abandon it."""
        ...

    async def complete_claim(self, publication_id: UUID, token: str, mode: str) -> bool:
        """Fenced completion write (update/delete/archive per ``mode``).
        Returns False without applying anything if ``token`` is stale."""
        ...

    async def fail_claim(self, publication: EventPublication, token: str) -> bool:
        """Fenced failure-record write (persists attempt_count/last_error).
        Returns False without applying anything if ``token`` is stale."""
        ...


@runtime_checkable
class AdvisoryLockingStore(Protocol):
    """Optional advisory-lock-mode capability a PublicationStore may implement.

    Postgres-only in practice (backed by ``pg_try_advisory_lock``); a store
    should refuse to be constructed with ``claim_strategy="advisory_lock"``
    on a non-Postgres engine rather than expose this capability unusably.

    A store must also implement ``find_by_id``: the plugin re-reads the row
    under the lock to see what a peer did since the sweep's snapshot, and
    ``outbox.configure()`` refuses ``claim_strategy="advisory_lock"`` for a
    store without it. A store may implement ``check_advisory_lock_config()``,
    which ``configure()`` calls and which raises ``ConfigurationError`` for a
    setup that cannot hold one lock per connection safely.
    """

    async def try_lock_publication(self, publication_id: UUID) -> object | None:
        """Attempt to acquire the advisory lock for one publication.

        Returns an opaque handle (truthy) on success, or ``None`` if another
        connection already holds it. The caller must pass the SAME handle to
        ``unlock_publication`` when done, win or lose.

        Raises ``LockConnectionTimeout`` when no connection for the lock
        frees up within the store's pool timeout: the row was not locked, read
        or charged an attempt, and the caller leaves it to a later sweep."""
        ...

    async def unlock_publication(self, handle: object, publication_id: UUID) -> None:
        """Release a lock handle returned by ``try_lock_publication``."""
        ...


__all__ = [
    "DEFAULT_CLAIM_BATCH_SIZE",
    "DEFAULT_CLAIM_LEASE_SECONDS",
    "DEFAULT_CLAIM_STRATEGY",
    "VALID_CLAIM_STRATEGIES",
    "AdvisoryLockingStore",
    "Claim",
    "ClaimingStore",
    "LockConnectionTimeout",
]
