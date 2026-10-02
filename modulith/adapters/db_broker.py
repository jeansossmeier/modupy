"""Database-backed broker adapter (relational DB as cross-module transport).

Lets process-per-module topology use a relational database (Postgres / MySQL
/ SQLite) as the cross-module message transport, so Redis is not required.
SQLite doubles as a zero-dependency bootstrap broker (embedded file or
``:memory:``).

Distributed via the ``modupy[database]`` extra (async SQLAlchemy + the
asyncpg / aiomysql / aiosqlite drivers, plus ``cryptography`` for MySQL 8
caching_sha2_password auth). Because this adapter is registered as a BUILTIN
plugin (loaded at
every bootstrap, see ``modulith.manager.BUILTIN_PLUGINS``), **SQLAlchemy is
never imported at module scope** — every import is lazy, inside a function,
exactly like ``redis_broker`` lazy-imports ``redis``. An application that
never selects ``broker = "database"`` never pays the SQLAlchemy import cost
and never needs the extra installed.

Single scheme ``database``; the concrete SQL dialect (Postgres / MySQL /
SQLite) is inferred from the SQLAlchemy URL, mirroring how
``modulith.adapters.postgres_outbox`` is dialect-aware despite being named
for Postgres.

Configuration resolves ``MODULITH_BROKER_<KEY>`` env var (blank == unset) >
``[tool.modulith.broker_options]`` subtable, per key:
  url / dsn                   SQLAlchemy URL (absent -> embedded SQLite file
                               '.modulith-broker.db' in a collision-safe private
                               per-user state directory, with a startup warning)
  completion_mode             'delete' (default, keeps the table small) |
                               'mark' (sets status='done', row stays for the
                               prune job)
  pool_size / max_overflow    connection-pool sizing (Postgres / MySQL defaults
                               to QueuePool; file SQLite defaults to 1/0)
  busy_timeout_ms             SQLite only: how long a blocked writer waits for
                               the lock before SQLITE_BUSY (default 250)
  sqlite_synchronous          SQLite only: PRAGMA synchronous mode (default
                               NORMAL — the WAL pairing; set FULL for
                               power-loss durability at ~2.5ms/commit fsync)
  no_subscriber_policy        'error' (default) | 'wait' | 'store'
  no_subscriber_wait_*        wait timeout (30s) and poll interval (100ms)
  orphan_replay_policy        'ttl_all_groups' (default) | 'first_groups' |
                               'expected_groups'
  orphan_retention_seconds    retained-source lifetime (default 86400s)
  expected_consumer_groups    target -> non-empty group list for expected mode
  poll_interval_ms            consumer poll cadence when idle (default 20)
  batch_size                  claim LIMIT per poll (default 100)
  dispatch_concurrency        rows dispatched concurrently per claimed batch
                               (default 10; 1 = sequential)
  reclaim_stale_seconds       a row claimed but not ack'd/failed within this many
                               seconds is treated as orphaned (crashed consumer)
                               and reclaimed by the next claim (default 60)
  max_delivery_attempts       a message that fails to dispatch this many times is
                               dead-lettered instead of retried forever (default 5)
  retention_age_seconds       prune deletes terminal ('done'/'dead') rows older
                               than this many seconds (default 259200 = 3 days,
                               so dead letters cannot grow unboundedly)
  retention_count             prune keeps only the newest N terminal rows per
                               (target, consumer_group)
  prune_interval_seconds      how often the consumer's background prune runs
                               (default 300s; set to 0 to disable pruning)

Postgres LISTEN/NOTIFY (a low-latency alternative to polling) is a planned
opt-in and not yet implemented — the transport polls on every dialect.

Fan-out mechanism (how the producer learns the consumer groups): the
producer process (module-isolated) never imports consumer modules, so
consumers self-register their subscriptions in a persistent
``broker_subscription`` table at ``DatabaseConsumer.start()`` time (upsert,
idempotent — never auto-deleted, so a rollback still finds its backlog). A
consumer claims only rows for the targets it currently consumes; at start it
logs one WARNING per target its group still holds but no longer consumes
(``stale_targets``), and ``drop_group(group, targets=...)`` — behind
``modulith broker drop-group <group> --target <t>`` — is the only way such a
subscription and its undelivered rows are removed. ``DatabaseBroker.publish()`` looks up every
group subscribed to the target and inserts one ``broker_message`` row per
group in a single transaction. With no registered group, the default ``error``
policy raises ``NoSubscribersError`` without writing a row. ``wait`` polls in
fresh transactions until a group appears or its monotonic deadline expires;
``store`` retains one source until its replay policy has materialized the
required per-group queue rows or its database-clock TTL expires.

Claim path (competing consumers): ``FOR UPDATE SKIP LOCKED`` on Postgres,
MySQL 8.0.1+ and MariaDB 10.6+ lets concurrent consumers partition the backlog
instead of blocking or double-claiming (see ``_supports_skip_locked``). An
older MySQL-family server is rejected with a ``ConfigurationError``
(``_require_skip_locked``), never given an unlocked claim. SQLite has no row locking at
all and rejects the clause, so it degrades to a plain claim inside one
transaction — correct for sequential consumption in tests, but not a
substitute for the Postgres/MySQL concurrency guarantee. SQLite is hardened
for best-effort multi-process use: WAL journaling + ``busy_timeout`` on every
connection (``_install_sqlite_pragmas``) plus a bounded application retry on a
transient "database is locked" (``_write`` / ``_SQLITE_BUSY_MAX_RETRIES``,
needed because SQLite raises SQLITE_BUSY immediately — ignoring busy_timeout —
when a read lock upgrades to a write lock, exactly what the claim does).

Retention: terminal rows ('done' left by ``completion_mode='mark'``, and
'dead' letters) accumulate unless pruned. ``DatabaseBroker.prune()`` deletes
them by age (``retention_age_seconds``) and/or by count (``retention_count``,
newest-N per ``(target, consumer_group)``); pending/claimed rows are never
touched, so prune can never drop an undelivered message. ``DatabaseConsumer``
runs it on a background interval when either retention knob is configured.
The same prune path always removes expired retained sources and their replay
ledgers; publish/subscribe also remove them lazily before replay-sensitive work.
Corollary: a consumer group that stops consuming permanently (a module
retired without dropping its ``broker_subscription`` rows) keeps accumulating
'pending' rows that prune will never delete — undelivered work is never
pruned by design. ``modulith run --topology processes`` warns about such a
group at startup; ``modulith broker drop-group <group>`` (``drop_group``)
removes its subscription rows and undelivered messages when decommissioning
a module.

Resilience posture mirrors ``modulith._consumer.BrokerConsumer``: a
background poll loop with capped exponential backoff on backend errors,
``stop()`` that never raises, poison messages (undeserializable payload or
missing ``event_type``) dead-lettered immediately, and dispatch failures
retried with backoff up to an attempt cap before being dead-lettered. A
consumer that crashes between claim and ack leaves its row in ``claimed``;
the next claim reclaims it once ``claimed_at`` is older than the reclaim
window (``_DEFAULT_RECLAIM_STALE_S``, default 60s), the DB analogue of the
Redis adapter's XAUTOCLAIM reclaim — this is what keeps delivery at-least-once
across a consumer crash.

While a claimed batch is in flight, its owner re-stamps ``claimed_at`` every
``reclaim_stale_seconds / 3`` (``renew_claims``, owner-guarded — the same
one-third-lease cadence as the outbox claim renewal), so a batch whose total
dispatch time exceeds the reclaim window is never reclaimed and
double-dispatched mid-flight. Renewal stops the moment the process dies,
which restores the crash-reclaim guarantee above.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import math
import os
import random
import re
import weakref
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from typing import Any, Concatenate, ParamSpec, TypeVar, cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from modulith import (
    BrokerRegistry,
    ConfigurationError,
    Consumer,
    ConsumerRegistry,
    ConsumerSpec,
    hookimpl,
)

from ..config import (
    DEFAULT_BROKER_DB_FILENAME,
    DEFAULT_MAX_PAYLOAD_BYTES,
    MAX_PAYLOAD_BYTES,
    _validate_sql_schema,
)
from ._dead_letter import DeadLetter
from ._polling_consumer import PollingConsumer
from ._state_path import resolve_state_file

logger = logging.getLogger("modulith.adapters.db")

_DB_SCHEME = "database"
_DEFAULT_COMPLETION_MODE = "delete"
# Valid completion modes: 'delete' removes the row on ack (small table);
# 'mark' keeps it as 'done' for the prune job. Validated in DatabaseBroker.
_COMPLETION_MODES = frozenset({"delete", "mark"})
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_DISPATCH_CONCURRENCY = 10
_DEFAULT_POLL_INTERVAL_S = 0.02
_DEFAULT_NO_SUBSCRIBER_POLICY = "error"
_DEFAULT_NO_SUBSCRIBER_WAIT_TIMEOUT_S = 30.0
_DEFAULT_NO_SUBSCRIBER_WAIT_POLL_INTERVAL_S = 0.1
_NO_SUBSCRIBER_POLICIES = frozenset({"error", "store", "wait"})
_DEFAULT_ORPHAN_REPLAY_POLICY = "ttl_all_groups"
_ORPHAN_REPLAY_POLICIES = frozenset({"ttl_all_groups", "first_groups", "expected_groups"})
_DEFAULT_ORPHAN_RETENTION_S = 86400.0
# must match _MAX_ORPHAN_RETENTION_S in shm_broker.py; keeps the stored
# publication's expires_at (now + timedelta) far from datetime's overflow.
_MAX_ORPHAN_RETENTION_S = 100 * 365 * 86400.0

# Default terminal-row retention applied by the consumer factory when
# ``retention_age_seconds`` is not configured: dead-lettered rows (and 'done'
# rows in mark mode) are pruned after 3 days. Long enough to notice and
# inspect a Friday-night poison message on Monday; short enough that a
# chronic poison producer cannot grow the table unboundedly. Disable the
# background prune entirely with ``prune_interval_seconds = 0``.
_DEFAULT_RETENTION_AGE_S = 3 * 86400.0

# The two terminal statuses prune is allowed to delete. 'pending'/'claimed'
# rows are undelivered work and must NEVER be pruned (that would be message
# loss), so every prune query is filtered to exactly these.
_TERMINAL_STATUSES = ("done", "dead")

# VARCHAR lengths for the broker tables. MySQL rejects an unbounded VARCHAR, so
# every String column carries an explicit length. Mirror these EXACTLY in
# migrations/versions/0002_broker_message.py and 0004_broker_retained_messages.py.
_ID_LEN = 64  # UUID hex (36) + headroom for alternative id schemes
_TARGET_LEN = 255  # event FQN routing key (PK/indexed -> must be bounded)
_GROUP_LEN = 255  # "modulith-<module>" consumer group (PK/indexed)
_EVENT_TYPE_LEN = 255  # event FQN for deserialize
_STATUS_LEN = 32  # 'pending' | 'claimed' | 'done' | 'dead' (indexed)
_CLAIMED_BY_LEN = 255  # "<module>:<pid>" claimant id

# A row claimed but not ACK'd/failed within this many seconds is treated as
# orphaned (the claiming consumer crashed between claim and ack) and reclaimed
# by the next claim. The DB equivalent of the Redis adapter's
# ``reclaim_min_idle_ms`` (default 60s). Idle-time based, with NO crash/liveness
# detection: a healthy-but-slow consumer whose dispatch outlasts this window has
# its in-flight row reclaimed and double-dispatched — keep it above worst-case
# listener latency; listeners must be idempotent regardless (at-least-once).
_DEFAULT_RECLAIM_STALE_S = 60.0

# A message that fails to dispatch this many times is dead-lettered rather
# than retried forever. Mirrors modulith._consumer._MAX_DELIVERY_ATTEMPTS.
_MAX_DELIVERY_ATTEMPTS = 5

# Capped exponential backoff — same shape as modulith._consumer's constants
# (0.05s, 0.1s, 0.2s, ... capped at 5s) for both broker-error retries and
# dispatch-failure redelivery delay.
_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 5.0
_BACKOFF_MAX_EXPONENT = 7

# Dialects that support ``FOR UPDATE SKIP LOCKED``. SQLite has no row
# locking and rejects the clause outright, so it must never be issued there.
# MariaDB reports its own dialect name ("mariadb", not "mysql") and has
# supported SKIP LOCKED since 10.6 (2021) — without it here a ``mariadb://``
# URL silently degraded to the SQLite-style plain claim, losing the
# competing-consumer partitioning it is fully capable of. A MariaDB server
# reached through ``mysql+aiomysql://`` reports ``mysql`` with
# ``dialect.is_mariadb`` set, so the MySQL-family gate reads the server version.
_SKIP_LOCKED_DIALECTS = frozenset({"postgresql", "mysql", "mariadb"})
_MYSQL_FAMILY_DIALECTS = frozenset({"mysql", "mariadb"})
_MYSQL_SKIP_LOCKED_MINIMUM = (8, 0, 1)
_MARIADB_SKIP_LOCKED_MINIMUM = (10, 6)

# SQLite ``busy_timeout`` (ms) applied to every connection when none is
# configured: how long a blocked writer waits for the lock before raising
# SQLITE_BUSY. Kept short (250ms) so app-level retries stay in control of the
# total budget — a 5s timeout per attempt made ``_SQLITE_BUSY_MAX_RETRIES``
# stretch toward tens of seconds under multi-worker startup contention.
_DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 250

# App-level retry budget for a transient SQLite "database is locked" error.
# ``busy_timeout`` handles short wait-for-lock cases, but SQLite returns
# SQLITE_BUSY *immediately* (ignoring busy_timeout) when a transaction upgrades
# a read lock to a write lock under contention — exactly what claim_batch's
# SELECT-then-UPDATE does — so a bounded application retry is still needed.
# Wall budget caps the sum of (busy_timeout waits + geometric sleeps) so a
# contended fleet cannot stall a single write for 8 x busy_timeout.
_SQLITE_BUSY_MAX_RETRIES = 8
_SQLITE_BUSY_TOTAL_BUDGET_S = 2.0
# Schema create is startup-only and may wait on a peer's create_all; allow a
# longer wall budget / retry count than the hot-path write retries.
_SQLITE_SCHEMA_BUSY_BUDGET_S = 15.0
_SQLITE_SCHEMA_BUSY_MAX_RETRIES = 32

# MySQL named locks are connection-scoped, so the implementation holds them
# until the replay transaction commits and then releases them explicitly.
# Total wall budget for winning every named lock a target-locked write needs.
_TARGET_LOCK_TIMEOUT_S = 30
# How long ONE attempt waits inside GET_LOCK before giving its pooled
# connection back and retrying. Waiting the whole budget in a single call kept
# the connection checked out for the entire wait, so a handful of publishers
# contending for one target could occupy every slot in the pool and make
# unrelated claims, acks and prunes fail with a QueuePool timeout against a
# perfectly healthy database.
_TARGET_LOCK_ATTEMPT_WAIT_S = 1

# How many retained-message ids one statement may bind, and how many retained
# sources one replay page holds. Far below every driver's bind-parameter limit
# (SQLite 32,766; asyncpg 32,767) so a large retained table still deletes and
# replays; small enough that a replay page's payloads stay a bounded working
# set rather than the whole table.
_RETAINED_ID_CHUNK = 500


class NoSubscribersError(RuntimeError):
    """Raised when a database-broker target has no subscribed consumer group."""

    def __init__(self, target: str, timeout_seconds: float | None = None) -> None:
        self.target = target
        self.timeout_seconds = timeout_seconds
        message = f"database broker target {target!r} has no subscribers"
        if timeout_seconds is not None:
            message += f"; timed out after {timeout_seconds:g} seconds"
        super().__init__(message)


def _validate_choice(value: Any, option_name: str, allowed: frozenset[str]) -> str:
    if type(value) is not str or value not in allowed:
        raise ConfigurationError(f"{option_name} must be one of {sorted(allowed)}, got {value!r}")
    return value


def _positive_finite_float(value: Any, option_name: str) -> float:
    if isinstance(value, bool):
        raise ConfigurationError(f"{option_name} must be a finite number greater than 0")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"{option_name} must be a finite number greater than 0, got {value!r}"
        ) from exc
    if not math.isfinite(number) or number <= 0:
        raise ConfigurationError(
            f"{option_name} must be a finite number greater than 0, got {value!r}"
        )
    return number


def _non_negative_finite_float(value: Any, option_name: str) -> float:
    if isinstance(value, bool):
        raise ConfigurationError(
            f"{option_name} must be a finite number greater than or equal to 0"
        )
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"{option_name} must be a finite number greater than or equal to 0, got {value!r}"
        ) from exc
    if not math.isfinite(number) or number < 0:
        raise ConfigurationError(
            f"{option_name} must be a finite number greater than or equal to 0, got {value!r}"
        )
    return number


def _positive_int(value: Any, option_name: str) -> int:
    if type(value) is not int or value < 1:
        raise ConfigurationError(
            f"{option_name} must be a non-boolean integer greater than or equal to 1, got {value!r}"
        )
    return value


def _non_negative_int(value: Any, option_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ConfigurationError(
            f"{option_name} must be a non-boolean integer greater than or equal to 0, got {value!r}"
        )
    return value


def _validate_expected_consumer_groups(value: Any) -> dict[str, list[str]]:
    if type(value) is not dict:
        raise ConfigurationError(
            "expected_consumer_groups must map non-empty targets to non-empty lists "
            f"of group strings, got {value!r}"
        )
    normalized: dict[str, list[str]] = {}
    for target, groups in value.items():
        if (
            type(target) is not str
            or not target.strip()
            or type(groups) is not list
            or not groups
            or any(type(group) is not str or not group.strip() for group in groups)
        ):
            raise ConfigurationError(
                "expected_consumer_groups must map non-empty targets to non-empty lists "
                f"of group strings, got {target!r}: {groups!r}"
            )
        normalized[target] = list(groups)
    return normalized


def _backoff_delay(attempt: int) -> float:
    """Capped exponential backoff for the ``attempt``'th consecutive failure
    (1-indexed): 0.05s, 0.1s, 0.2s, ... capped at 5s."""
    exponent = min(attempt - 1, _BACKOFF_MAX_EXPONENT)
    # ``2.0 ** exponent`` (float base), not ``2**exponent``: typeshed types
    # ``int.__pow__`` as returning ``Any`` (a negative exponent would yield a
    # float at runtime), which would otherwise leak Any through this
    # function's declared ``-> float`` return.
    return min(_BACKOFF_BASE_S * (2.0**exponent), _BACKOFF_CAP_S)


def _sqlite_busy_delay(attempt: int) -> float:
    """Sleep before retrying a SQLITE_BUSY write: geometric from 2ms, capped
    at 50ms, plus 0-3ms jitter.

    Deliberately NOT ``_backoff_delay``: that scale (50ms..5s) is sized for
    backend outages, while a busy collision on a local SQLite file usually
    clears in single-digit milliseconds. Sleeping 50ms+ per collision was
    measured to push steady-state delivery p50 from ~20ms to ~200ms under
    concurrent publish + claim traffic (the claim's read->write lock upgrade
    raises SQLITE_BUSY immediately, so collisions are routine, not
    exceptional). Early attempts stay in the 2-11ms range for the common
    fast-clear case; the geometric growth (capped at 50ms) restores a ~170ms
    total budget across ``_SQLITE_BUSY_MAX_RETRIES`` for a writer holding the
    lock longer — a 100-row claim commit or a prune sweep. Jitter
    decorrelates the colliding writers.
    """
    return min(0.002 * (2.0 ** (attempt - 1)), 0.05) + random.uniform(0.0, 0.003)


def _owned(message: Any, row_id: str, consumer_name: str) -> tuple[Any, ...]:
    """WHERE predicate identifying a row THIS consumer currently owns:
    matching id AND still 'claimed' AND still claimed by ``consumer_name``.

    Every completion path (ack/fail/dead_letter) filters on this so a late
    write from a healthy-but-slow consumer whose row was already reclaimed —
    and possibly moved to a terminal state — by a peer is a no-op (a
    compare-and-swap), never resurrecting a terminal row or clobbering the row
    another consumer now owns."""
    return (
        message.c.id == row_id,
        message.c.status == "claimed",
        message.c.claimed_by == consumer_name,
    )


def _rowcount(result: Any) -> int:
    """Best-effort affected-row count from a DELETE/UPDATE ``CursorResult``.

    ``rowcount`` is well-defined for DML on the drivers this adapter targets
    (asyncpg / aiomysql / aiosqlite), but can be ``-1`` ("unknown") on some
    backends — clamp that to 0 so the prune tally stays a non-negative count
    (it is informational / logged, never load-bearing).
    """
    rc = result.rowcount
    return rc if isinstance(rc, int) and rc > 0 else 0


def _is_sqlite_locked(exc: BaseException) -> bool:
    """True for a transient SQLite lock (``OperationalError`` whose message is
    'database is locked' / 'database table is locked') — the retryable
    contention signal. Schema / integrity / programming errors are never
    retried (they can't succeed on a re-run)."""
    from sqlalchemy.exc import OperationalError

    if not isinstance(exc, OperationalError):
        return False
    orig = getattr(exc, "orig", None)
    message = (str(orig) if orig is not None else str(exc)).lower()
    return "database is locked" in message or "database table is locked" in message


def _is_already_exists(exc: BaseException) -> bool:
    """True for a 'relation/table already exists' DDL error across the three
    supported dialects — the benign loser of a cross-process CREATE TABLE race.

    ``metadata.create_all`` runs a SELECT-then-CREATE per table (checkfirst),
    so two workers bootstrapping the same fresh DB can both pass the existence
    check and both issue CREATE; the loser gets Postgres 'already exists',
    MySQL 1050 'Table ... already exists', or SQLite 'table ... already
    exists' — all of which carry the 'already exists' substring. The
    column backfill in ``_add_missing_message_columns`` races the same way;
    its loser gets 'duplicate column name' on MySQL and SQLite."""
    from sqlalchemy.exc import OperationalError, ProgrammingError

    if not isinstance(exc, OperationalError | ProgrammingError):
        return False
    orig = getattr(exc, "orig", None)
    message = (str(orig) if orig is not None else str(exc)).lower()
    return "already exists" in message or "duplicate column name" in message


def _add_missing_message_columns(connection: Any, schema: str | None) -> None:
    """Add ``broker_message`` columns newer than a self-bootstrapped table.

    ``metadata.create_all`` skips a table that already exists, so a database
    the broker bootstrapped before ``dispatch_started`` existed would never
    gain it. Migration 0006 adds the same column for alembic-managed
    databases and skips it when this backfill already ran.
    """
    from sqlalchemy import inspect
    from sqlalchemy.schema import CreateColumn

    _, _, message = broker_schema()
    column = message.c.dispatch_started
    # Reflection and raw DDL bypass schema_translate_map, which is how both the
    # ``schema`` engine option and an injected engine route the broker tables.
    translate = connection.get_execution_options().get("schema_translate_map") or {}
    schema = translate.get(None, schema)
    existing = {col["name"] for col in inspect(connection).get_columns(message.name, schema=schema)}
    if column.name in existing:
        return
    preparer = connection.dialect.identifier_preparer
    table = preparer.quote(message.name)
    if schema:
        table = f"{preparer.quote_schema(schema)}.{table}"
    column_ddl = CreateColumn(column).compile(dialect=connection.dialect)
    connection.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column_ddl}")


def _is_pg_namespace_unique_race(exc: BaseException) -> bool:
    """True only for PostgreSQL's concurrent CREATE SCHEMA unique race."""
    from sqlalchemy.exc import IntegrityError

    if not isinstance(exc, IntegrityError):
        return False
    orig = getattr(exc, "orig", None)
    cause = getattr(orig, "__cause__", None)
    sqlstate = (
        getattr(orig, "sqlstate", None)
        or getattr(orig, "pgcode", None)
        or getattr(cause, "sqlstate", None)
    )
    diag = getattr(orig, "diag", None)
    cause_diag = getattr(cause, "diag", None)
    constraint = (
        getattr(orig, "constraint_name", None)
        or getattr(diag, "constraint_name", None)
        or getattr(cause, "constraint_name", None)
        or getattr(cause_diag, "constraint_name", None)
    )
    return sqlstate == "23505" and constraint == "pg_namespace_nspname_index"


_VERSION_SEPARATOR = re.compile(r"[.\-+]")
_VERSION_TOKEN = re.compile(r"(\d+)(?:a|b|c)?|(MariaDB\w*)")


def _mysql_family_release(
    dialect: Any, reported: str | None
) -> tuple[bool, tuple[int, ...] | None]:
    """Whether the server is MariaDB, and its release (``None`` when unknown).

    ``reported`` is the server's own ``VERSION()``. The release is its first
    three numbers, after MariaDB's ``5.5.5-`` compatibility prefix, and unknown
    when no number precedes the ``MariaDB`` token. The dialect's parsed
    ``server_version_info`` is not used: SQLAlchemy 2.1 keeps only the three
    numbers just before that token, which for Enterprise builds such as
    ``10.6.12-8-MariaDB-enterprise`` are minor, patch and build.
    """
    mariadb = bool(getattr(dialect, "is_mariadb", False))
    if reported is None:
        return mariadb, None
    numbers: list[int] = []
    for token in _VERSION_SEPARATOR.split(reported):
        match = _VERSION_TOKEN.fullmatch(token)
        if match is None:
            continue
        if match.group(2):
            if numbers[:3] == [5, 5, 5]:
                numbers = numbers[3:]
            return True, tuple(numbers[:3]) or None
        numbers.append(int(match.group(1)))
    return mariadb, tuple(numbers[:3]) or None


def _skip_locked_minimum(mariadb: bool) -> tuple[int, ...]:
    return _MARIADB_SKIP_LOCKED_MINIMUM if mariadb else _MYSQL_SKIP_LOCKED_MINIMUM


def _supports_skip_locked(engine: Any, reported: str | None) -> bool:
    """True when ``engine``'s connected server supports ``FOR UPDATE SKIP LOCKED``.

    Postgres always does. MySQL does from 8.0.1 and MariaDB from 10.6, judged
    by ``reported``, the server's ``VERSION()``; older servers reject the clause
    as a syntax error, and an unknown version is never assumed to parse it.
    SQLite has no row locking and raises a CompileError if the clause is
    issued, so the claim query gates on this before adding
    ``.with_for_update(skip_locked=True)``.
    """
    dialect = engine.dialect
    if dialect.name not in _SKIP_LOCKED_DIALECTS:
        return False
    if dialect.name not in _MYSQL_FAMILY_DIALECTS:
        return True
    mariadb, release = _mysql_family_release(dialect, reported)
    return release is not None and release >= _skip_locked_minimum(mariadb)


def _require_skip_locked(engine: Any, reported: str | None) -> None:
    """Reject a MySQL-family server that cannot run the locked claim.

    The plain claim is not a fallback there: under InnoDB REPEATABLE READ two
    consumers' consistent reads return the same pending rows and both claim them.
    """
    dialect = engine.dialect
    if dialect.name not in _MYSQL_FAMILY_DIALECTS or _supports_skip_locked(engine, reported):
        return
    mariadb, release = _mysql_family_release(dialect, reported)
    server = "MariaDB" if mariadb else "MySQL"
    shown = "unknown" if release is None else ".".join(map(str, release))
    minimum = ".".join(map(str, _skip_locked_minimum(mariadb)))
    raise ConfigurationError(
        f"The database broker claims messages with FOR UPDATE SKIP LOCKED, which the "
        f"connected {server} server {shown} does not support; {server} {minimum} or "
        "newer is required."
    )


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _on_owning_loop(
    method: Callable[Concatenate[DatabaseBroker, _P], Coroutine[Any, Any, _R]],
) -> Callable[Concatenate[DatabaseBroker, _P], Coroutine[Any, Any, _R]]:
    """Run a broker coroutine on the event loop that owns the engine's pool.

    asyncpg and aiomysql connections are bound to the loop that opened them,
    and one pool serves every loop, so a call from any other running loop (the
    sync API's daemon-thread loop, for one) is submitted to the owning loop and
    awaited from here. A call already on the owning loop runs directly.

    The owning loop must stay running and unblocked: a submitted call waits on
    it, and an owner blocked in synchronous code hangs the caller until it
    unblocks.
    """

    @functools.wraps(method)
    async def run(self: DatabaseBroker, /, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        owner = self._check_cross_loop_usage()
        if owner is None:
            return await method(self, *args, **kwargs)
        # ponytail: an owner that stops between the is_running() check and
        # running this task leaves the call waiting forever; bound the wait if
        # an owner loop ever stops while other loops are still publishing.
        future = asyncio.run_coroutine_threadsafe(method(self, *args, **kwargs), owner)
        return await asyncio.wrap_future(future)

    return run


def _is_sqlite_url(url: Any) -> bool:
    """True when ``url`` names the SQLite backend (any driver), resolved via
    ``make_url`` rather than string-matching so ``sqlite+aiosqlite://`` and a
    bare ``sqlite://`` both classify correctly. Accepts a SQLAlchemy ``URL``
    object too — callers must not ``str()`` a URL whose path contains ``?``.
    """
    from sqlalchemy.engine import make_url

    return make_url(url).get_backend_name() == "sqlite"


def _is_sqlite_memory_url(url: Any) -> bool:
    """Return whether SQLAlchemy will open this SQLite URL in memory.

    SQLite treats both an empty database path and ``:memory:`` as private
    in-memory databases. URI mode also supports named/shared memory databases;
    SQLAlchemy exposes those URI controls in the parsed query mapping.
    Accepts a SQLAlchemy ``URL`` object (same reason as ``_is_sqlite_url``).
    """
    from sqlalchemy.engine import make_url

    parsed = make_url(url)
    if parsed.get_backend_name() != "sqlite":
        return False
    if parsed.database in (None, "", ":memory:"):
        return True

    uri_enabled = str(parsed.query.get("uri", "")).lower() == "true"
    if not uri_enabled:
        return False
    return (
        parsed.database == "file::memory:" or str(parsed.query.get("mode", "")).lower() == "memory"
    )


_SQLITE_SYNCHRONOUS_MODES = frozenset({"OFF", "NORMAL", "FULL", "EXTRA"})
_DEFAULT_SQLITE_SYNCHRONOUS = "NORMAL"


def _install_sqlite_pragmas(engine: Any, busy_timeout_ms: int, synchronous: str) -> None:
    """Set WAL + ``busy_timeout`` + ``synchronous`` on every new SQLite connection.

    WAL lets one writer and concurrent readers coexist (a plain rollback
    journal serializes them); ``busy_timeout`` makes a blocked writer wait
    rather than erroring immediately — together they make file-backed SQLite a
    workable best-effort multi-process broker. No-op-safe on ``:memory:`` (WAL
    silently downgrades to 'memory'). Installed on the ``sync_engine``'s
    'connect' event — the documented way to run PRAGMAs on an aiosqlite async
    engine (the event fires with the raw DBAPI connection, whose cursor runs
    synchronously by bridging to aiosqlite's connection thread).

    ``synchronous`` defaults to NORMAL — the canonical WAL pairing: commits
    skip the per-commit fsync (measured ~2.6ms each on the delivery path's
    three commits: publish, claim, ack), fsyncing only at WAL checkpoints.
    An application/process crash loses nothing (the WAL survives); only an
    OS crash or power loss can drop the last commits. Deployments that need
    power-loss durability set ``broker_options.sqlite_synchronous = "FULL"``.
    """
    from sqlalchemy import event

    timeout = int(busy_timeout_ms)
    sync_mode = synchronous.upper()
    if sync_mode not in _SQLITE_SYNCHRONOUS_MODES:
        raise ConfigurationError(
            f"sqlite_synchronous must be one of {sorted(_SQLITE_SYNCHRONOUS_MODES)}, "
            f"got {synchronous!r} (broker_options sqlite_synchronous / "
            "MODULITH_BROKER_SQLITE_SYNCHRONOUS)"
        )

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            # busy_timeout FIRST: journal_mode=WAL can itself block on a
            # contended file, and must inherit the wait budget.
            # ``timeout`` is an int and ``sync_mode`` is vetted against the
            # frozen set above -> safe to interpolate (PRAGMA takes no bind
            # params); never a raw user string.
            cursor.execute(f"PRAGMA busy_timeout={timeout}")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute(f"PRAGMA synchronous={sync_mode}")
        finally:
            cursor.close()


def _create_engine(url: Any, opts: dict[str, Any]) -> Any:
    """Build the async engine, applying dialect-appropriate options resolved
    ``MODULITH_BROKER_<KEY>`` env var > ``[tool.modulith.broker_options]``
    subtable (via ``_broker_opt``, exactly like the URL and consumer options),
    so pool sizing is a per-deployment value operators can set from the
    environment without editing pyproject.

    - Postgres / MySQL: ``pool_size`` / ``max_overflow`` size the connection
      pool (both optional; omitted -> SQLAlchemy's QueuePool defaults).
    - File-backed SQLite: defaults to ``pool_size=1, max_overflow=0`` (one
      writer); WAL + ``busy_timeout`` are installed per connection. Pass a
      SQLAlchemy ``URL`` object (not ``str(URL)``) when the path may contain
      ``?`` / ``#`` — stringifying and reparsing truncates at ``?``.
    - In-memory SQLite: ``StaticPool``, one shared connection, so the database
      lives as long as the engine.
    """
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import StaticPool
    except ImportError as exc:  # pragma: no cover — exercised via an import shim
        # The adapter is a BUILTIN plugin (lazy imports, module docstring), so a
        # missing extra only surfaces here — when an app actually selects
        # ``broker='database'`` and builds an engine from a URL. Replace the
        # opaque bare ``ModuleNotFoundError: No module named 'sqlalchemy'`` with
        # an actionable ConfigurationError the CLI maps to exit 1.
        raise ConfigurationError(
            "The 'database' broker requires SQLAlchemy (async) plus a DB driver. "
            "Install the extra: pip install 'modupy[database]'"
        ) from exc

    sqlite = _is_sqlite_url(url)
    busy_timeout_ms: int | None = None
    if sqlite:
        configured_timeout = _opt_int(_broker_opt(opts, "busy_timeout_ms", "BUSY_TIMEOUT_MS"))
        busy_timeout_ms = (
            _DEFAULT_SQLITE_BUSY_TIMEOUT_MS if configured_timeout is None else configured_timeout
        )
        if busy_timeout_ms <= 0:
            raise ConfigurationError(
                f"busy_timeout_ms must be a positive integer, got {busy_timeout_ms!r}"
            )
        maximum_timeout_ms = int(_SQLITE_BUSY_TOTAL_BUDGET_S * 1000)
        if busy_timeout_ms > maximum_timeout_ms:
            logger.warning(
                "SQLite busy_timeout_ms=%d exceeds the %.3gs write deadline; capping it to %dms",
                busy_timeout_ms,
                _SQLITE_BUSY_TOTAL_BUDGET_S,
                maximum_timeout_ms,
            )
            busy_timeout_ms = maximum_timeout_ms

    kwargs: dict[str, Any] = {}
    if sqlite and not _is_sqlite_memory_url(url):
        # File-backed SQLite has one writer. SQLAlchemy's default QueuePool
        # (5+10) only multiplies SQLITE_BUSY under concurrent dispatch.
        pool_size = _opt_int(_broker_opt(opts, "pool_size", "POOL_SIZE"))
        kwargs["pool_size"] = 1 if pool_size is None else pool_size
        max_overflow = _opt_int(_broker_opt(opts, "max_overflow", "MAX_OVERFLOW"))
        kwargs["max_overflow"] = 0 if max_overflow is None else max_overflow
    elif not sqlite:
        pool_size = _opt_int(_broker_opt(opts, "pool_size", "POOL_SIZE"))
        if pool_size is not None:
            kwargs["pool_size"] = pool_size
        max_overflow = _opt_int(_broker_opt(opts, "max_overflow", "MAX_OVERFLOW"))
        if max_overflow is not None:
            kwargs["max_overflow"] = max_overflow
    else:
        # SQLAlchemy 2.1 deprecates inferring StaticPool from a mode=memory URL;
        # a later release would hand out separate connections, each seeing an
        # empty database.
        kwargs["poolclass"] = StaticPool
    engine = create_async_engine(url, **kwargs)
    if sqlite:
        assert busy_timeout_ms is not None
        synchronous = _option_or_default(
            _broker_opt(opts, "sqlite_synchronous", "SQLITE_SYNCHRONOUS"),
            _DEFAULT_SQLITE_SYNCHRONOUS,
        )
        _install_sqlite_pragmas(engine, busy_timeout_ms, str(synchronous))
    return engine


# ---------------------------------------------------------------------------
# Lazy schema (SQLAlchemy Core, built + cached on first use)
# ---------------------------------------------------------------------------

_metadata_cache: Any | None = None
_subscription_table_cache: Any | None = None
_message_table_cache: Any | None = None


def broker_schema() -> tuple[Any, Any, Any]:
    """Return ``(metadata, broker_subscription, broker_message)``, building
    and caching them on first call so repeated calls (every publish/claim)
    reuse the same ``Table`` objects rather than re-declaring them against a
    fresh ``MetaData`` (which would raise on the second ``Table(...)`` call
    with the same name). SQLAlchemy is imported here, not at module scope —
    see the module docstring's optional-dependency rationale.
    """
    global _metadata_cache, _subscription_table_cache, _message_table_cache
    if _metadata_cache is not None:
        return _metadata_cache, _subscription_table_cache, _message_table_cache

    from sqlalchemy import (
        Boolean,
        Column,
        DateTime,
        ForeignKey,
        Index,
        Integer,
        LargeBinary,
        MetaData,
        String,
        Table,
        Text,
        UniqueConstraint,
        false,
    )
    from sqlalchemy.dialects.mysql import DATETIME as MySQLDateTime
    from sqlalchemy.dialects.mysql import LONGBLOB as MySQLLongBlob

    # Timestamp type with microsecond precision on EVERY dialect. MySQL and
    # MariaDB default DATETIME to whole-second precision (fsp=0), which loses
    # the broker's sub-second timing — available_at with
    # a 0.05-0.2s backoff, the claimed_at reclaim window — making an immediate
    # claim see available_at > now and return nothing. fsp=6 fixes both;
    # Postgres/SQLite already keep microseconds so the variant is inert there
    # (the migration mirrors this exactly). One shared instance is fine —
    # SQLAlchemy type objects are reusable across columns.
    ts = DateTime(timezone=True).with_variant(MySQLDateTime(fsp=6), "mysql", "mariadb")

    # Payload type that can actually hold a ``max_payload_bytes`` publish on
    # EVERY dialect. LargeBinary compiles to MySQL/MariaDB BLOB, which caps at
    # 65,535 bytes — a fraction of the broker's default cap — so a modest
    # payload passes publish()'s check and then dies with MySQL error 1406
    # ("Data too long for column"). LONGBLOB (4 GiB) covers the whole accepted
    # range; Postgres BYTEA and SQLite BLOB are already unbounded so the
    # variant is inert there (the migrations mirror this exactly).
    payload_type = LargeBinary().with_variant(MySQLLongBlob(), "mysql", "mariadb")

    metadata = MetaData()

    # Explicit VARCHAR lengths: MySQL rejects an unbounded VARCHAR (Postgres and
    # SQLite accept it, but this adapter supports all three). PK/indexed string
    # columns MUST be bounded so MySQL can index them; the lengths are generous
    # for the values they hold (UUID id, FQN target/event_type, modulith-<mod>
    # group, short status enum, <mod>:<pid> claimant). Keep these IN LOCKSTEP
    # with migrations/versions/0002_broker_message.py and
    # 0004_broker_retained_messages.py (the drift tests diff the schemas).
    subscription = Table(
        "broker_subscription",
        metadata,
        Column("target", String(_TARGET_LEN), primary_key=True),
        Column("consumer_group", String(_GROUP_LEN), primary_key=True),
        Column("updated_at", ts, nullable=False),
    )

    message = Table(
        "broker_message",
        metadata,
        Column("id", String(_ID_LEN), primary_key=True),
        Column("target", String(_TARGET_LEN), nullable=False),
        Column("consumer_group", String(_GROUP_LEN), nullable=False),
        # Nullable at the DB level: a publish() call with no "event_type"
        # header (or a directly-inserted test row) produces a poison message
        # that the consumer dead-letters on first claim, rather than a schema
        # violation at insert time.
        Column("event_type", String(_EVENT_TYPE_LEN), nullable=True),
        Column("payload", payload_type, nullable=False),
        Column("headers", Text, nullable=True),
        Column(
            "status",
            String(_STATUS_LEN),
            nullable=False,
            default="pending",
            server_default="pending",
        ),
        Column("attempts", Integer, nullable=False, default=0, server_default="0"),
        Column("available_at", ts, nullable=False),
        Column("claimed_at", ts, nullable=True),
        Column("claimed_by", String(_CLAIMED_BY_LEN), nullable=True),
        Column("created_at", ts, nullable=False),
        Column("last_error", Text, nullable=True),
        # Set by the pre-dispatch renewal of the current claim, so a stale
        # reclaim charges an attempt only to a row whose listener started.
        # Keep in lockstep with migrations/versions/0006_broker_dispatch_started.py.
        Column(
            "dispatch_started",
            Boolean,
            nullable=False,
            default=False,
            server_default=false(),
        ),
        Index("ix_broker_message_claim", "consumer_group", "status", "available_at"),
        Index("ix_broker_message_prune", "status", "created_at"),
        Index("ix_broker_message_target", "target"),
    )

    # These auxiliary tables intentionally stay outside broker_schema()'s
    # return tuple. Existing callers continue to receive the original
    # (metadata, subscription, message) public shape.
    retained_message = Table(
        "broker_retained_message",
        metadata,
        Column("id", String(_ID_LEN), primary_key=True),
        Column("target", String(_TARGET_LEN), nullable=False),
        Column("event_type", String(_EVENT_TYPE_LEN), nullable=True),
        Column("payload", payload_type, nullable=False),
        Column("headers", Text, nullable=True),
        Column("created_at", ts, nullable=False),
        Column("expires_at", ts, nullable=False),
        Index("ix_broker_retained_message_expiry", "expires_at"),
        Index("ix_broker_retained_message_target_expiry", "target", "expires_at"),
    )
    Table(
        "broker_retained_delivery",
        metadata,
        Column(
            "retained_message_id",
            String(_ID_LEN),
            ForeignKey(retained_message.c.id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("consumer_group", String(_GROUP_LEN), primary_key=True),
        Column("broker_message_id", String(_ID_LEN), nullable=False),
        Column("delivered_at", ts, nullable=False),
        UniqueConstraint(
            "broker_message_id",
            name="uq_broker_retained_delivery_message",
        ),
    )

    _metadata_cache = metadata
    _subscription_table_cache = subscription
    _message_table_cache = message
    return metadata, subscription, message


def _retained_tables() -> tuple[Any, Any]:
    """Return the replay tables without expanding broker_schema()'s API."""
    metadata, _, _ = broker_schema()
    return (
        metadata.tables["broker_retained_message"],
        metadata.tables["broker_retained_delivery"],
    )


def _delivery_message_id(retained_message_id: str, consumer_group: str) -> str:
    """Stable queue-row id for one retained source/group pair."""
    key = f"modulith:database:{retained_message_id}:{consumer_group}"
    return str(uuid5(NAMESPACE_URL, key))


def _postgres_target_lock_key(target: str) -> int:
    """Stable signed 64-bit key accepted by pg_advisory_xact_lock."""
    digest = hashlib.blake2b(target.encode(), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def _mysql_target_lock_name(target: str) -> str:
    """Stable named-lock key below MySQL's 64-character limit."""
    digest = hashlib.sha256(target.encode()).hexdigest()[:48]
    return f"modulith-db:{digest}"


# ---------------------------------------------------------------------------
# Broker implementation (producer + consumer-side operations)
# ---------------------------------------------------------------------------


class DatabaseBroker:
    """Broker using a relational database as the transport (fan-out on write).

    Conforms structurally to ``modulith.Broker`` (publish + close); the extra
    consumer-side methods (``subscribe``, ``claim_batch``, ``ack``, ``fail``,
    ``dead_letter``) are what ``DatabaseConsumer`` drives — one engine serves
    both halves, exactly like ``RedisStreamsBroker``.

    A pre-built async ``engine`` may be injected (tests, or callers that
    manage their own pool/pooling policy); otherwise one is created lazily
    from ``url`` so SQLAlchemy stays a soft dependency.
    """

    def __init__(
        self,
        url: Any | None = None,
        *,
        engine: Any | None = None,
        completion_mode: str = _DEFAULT_COMPLETION_MODE,
        engine_options: dict[str, Any] | None = None,
        no_subscriber_policy: str = _DEFAULT_NO_SUBSCRIBER_POLICY,
        no_subscriber_wait_timeout_seconds: float = _DEFAULT_NO_SUBSCRIBER_WAIT_TIMEOUT_S,
        no_subscriber_wait_poll_interval_ms: float = (
            _DEFAULT_NO_SUBSCRIBER_WAIT_POLL_INTERVAL_S * 1000.0
        ),
        orphan_replay_policy: str = _DEFAULT_ORPHAN_REPLAY_POLICY,
        orphan_retention_seconds: float = _DEFAULT_ORPHAN_RETENTION_S,
        expected_consumer_groups: dict[str, list[str]] | None = None,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    ) -> None:
        if engine is not None:
            self._engine = engine
        else:
            if not url:
                raise ConfigurationError(
                    "DatabaseBroker requires a SQLAlchemy URL: pass url=... "
                    "(or broker_options={'url': ...} / MODULITH_BROKER_URL) "
                    "or inject a pre-built engine=... for tests."
                )
            # Lazy engine build (SQLAlchemy stays a soft dependency): applies
            # pooling / SQLite hardening from broker_options — see _create_engine.
            # Prefer a SQLAlchemy URL object over str(URL) when the path may
            # contain '?' — stringifying then reparsing truncates there.
            self._engine = _create_engine(url, engine_options or {})
        if completion_mode not in _COMPLETION_MODES:
            raise ConfigurationError(
                f"completion_mode must be one of {sorted(_COMPLETION_MODES)}, "
                f"got {completion_mode!r} (broker_options completion_mode / "
                "MODULITH_BROKER_COMPLETION_MODE)"
            )
        self._completion_mode = completion_mode
        self._no_subscriber_policy = _validate_choice(
            no_subscriber_policy, "no_subscriber_policy", _NO_SUBSCRIBER_POLICIES
        )
        self._no_subscriber_wait_timeout_s = _positive_finite_float(
            no_subscriber_wait_timeout_seconds, "no_subscriber_wait_timeout_seconds"
        )
        wait_poll_interval_ms = _positive_finite_float(
            no_subscriber_wait_poll_interval_ms, "no_subscriber_wait_poll_interval_ms"
        )
        self._no_subscriber_wait_poll_interval_s = wait_poll_interval_ms / 1000.0
        self._orphan_replay_policy = _validate_choice(
            orphan_replay_policy, "orphan_replay_policy", _ORPHAN_REPLAY_POLICIES
        )
        self._orphan_retention_seconds = _positive_finite_float(
            orphan_retention_seconds, "orphan_retention_seconds"
        )
        if self._orphan_retention_seconds > _MAX_ORPHAN_RETENTION_S:
            raise ConfigurationError(
                f"orphan_retention_seconds must be <= {_MAX_ORPHAN_RETENTION_S:g} "
                f"(100 years), got {orphan_retention_seconds!r}"
            )
        self._expected_consumer_groups = _validate_expected_consumer_groups(
            {} if expected_consumer_groups is None else expected_consumer_groups
        )
        self._max_payload_bytes = _positive_int(max_payload_bytes, "max_payload_bytes")
        if self._max_payload_bytes > MAX_PAYLOAD_BYTES:
            raise ConfigurationError(
                f"max_payload_bytes must be <= {MAX_PAYLOAD_BYTES}, got {max_payload_bytes!r}"
            )
        # Real SQLAlchemy engines always expose a dialect. Minimal injected
        # engines without one retain the adapter's historical SQLite behavior.
        dialect_name = getattr(getattr(self._engine, "dialect", None), "name", "sqlite")
        self._is_sqlite = dialect_name == "sqlite"
        # Falling back to the migrations' own knob keeps the runtime pointed at
        # the schema `migrations/env.py::_resolve_schema` migrated the tables
        # into: without it, an operator who sets only MODULITH_DB_SCHEMA gets a
        # broker that silently create_all()s a second, untracked table set in
        # the default schema.
        schema = (
            _broker_opt(engine_options or {}, "schema", "SCHEMA")
            or os.environ.get("MODULITH_DB_SCHEMA")
            or None
        )
        if schema is not None:
            schema = _validate_sql_schema(schema, option_name="schema")
        self._schema: str | None
        if schema and dialect_name == "postgresql":
            self._schema = schema
            # execution_options() on an (Async)Engine returns a new facade
            # sharing the same connection pool, never a fresh pool of its own
            # — safe to rebind without disturbing anything already checked
            # out against self._engine.
            self._engine = self._engine.execution_options(schema_translate_map={None: schema})
        else:
            if schema:
                logger.warning(
                    "engine_options schema=%r is only supported on PostgreSQL; "
                    "ignoring it for the %r dialect",
                    schema,
                    dialect_name,
                )
            self._schema = None
        self._schema_ready = False
        # Keyed per running loop, same rationale and weak-key eviction as
        # SerialStoreExecutor._locks in _shm_executor.py: _ensure_schema is
        # reachable from more than one event loop (publish/subscribe called
        # from a sync-API daemon-thread loop as well as the app's own loop),
        # and a plain asyncio.Lock() binds to whichever loop first contends
        # it, raising or wedging every other loop thereafter.
        self._schema_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        # The loop that owns the engine's pool (see _on_owning_loop): a
        # weakref.ref to the first loop seen, same eviction rationale as
        # _schema_locks above.
        self._used_loop_ref: weakref.ReferenceType[asyncio.AbstractEventLoop] | None = None
        self._cross_loop_warned = False
        self._server_version: str | None = None

    @property
    def engine(self) -> Any:
        return self._engine

    async def _mysql_server_version(self) -> str | None:
        """The MySQL-family server's ``VERSION()``, read once per broker; ``None``
        on other dialects, which the SKIP LOCKED gate judges by name alone."""
        if self._engine.dialect.name not in _MYSQL_FAMILY_DIALECTS:
            return None
        if self._server_version is None:
            async with self._engine.connect() as conn:
                result = await conn.exec_driver_sql("SELECT VERSION()")
                self._server_version = str(result.scalar_one())
        return self._server_version

    def _schema_is_ready(self) -> bool:
        """Indirection over ``self._schema_ready`` so the double-checked-lock
        re-read below isn't statically narrowed to a constant by mypy (the
        attribute genuinely can change while a concurrent caller awaits the
        lock — a method call, unlike a bare attribute expression, isn't
        narrowed across the ``async with`` the second check sits inside)."""
        return self._schema_ready

    def _schema_lock_for_running_loop(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._schema_locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._schema_locks[loop] = lock
        return lock

    def _check_cross_loop_usage(self) -> asyncio.AbstractEventLoop | None:
        """Return the loop a call from the running loop must run on, or ``None``
        to run it here.

        The first loop to use the broker owns the engine. A later call from
        another loop goes to the owner while the owner is running, and warns
        once. When the owner has stopped or closed, the running loop takes
        ownership instead: nothing is left to run the call, and the pooled
        connections opened on the old loop are only as usable as their driver
        allows off it.
        """
        loop = asyncio.get_running_loop()
        owner = None if self._used_loop_ref is None else self._used_loop_ref()
        if owner is None or owner is loop or owner.is_closed() or not owner.is_running():
            self._used_loop_ref = weakref.ref(loop)
            return None
        if not self._cross_loop_warned:
            logger.warning(
                "DatabaseBroker engine is owned by the event loop that first used "
                "it; calls from another loop (such as publish_sync()'s) are "
                "submitted to the owning loop, and the broker runs them on that "
                "loop, which must stay running and unblocked while they wait."
            )
            self._cross_loop_warned = True
        return owner

    async def _broker_tables_present(self, *, deadline: float | None = None) -> bool:
        """True only when every table in broker metadata is queryable."""
        from sqlalchemy import select

        async def probe() -> bool:
            metadata, _, _ = broker_schema()
            async with self._engine.connect() as conn:
                for table in metadata.sorted_tables:
                    await conn.execute(select(1).select_from(table).limit(0))
            return True

        try:
            if deadline is None:
                return await probe()
            async with asyncio.timeout_at(deadline):
                return await probe()
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise
        except Exception:
            return False

    async def _ensure_schema(self) -> None:
        """Create the broker tables if absent — idempotent, cheap after the
        first call (short-circuits on the in-process flag), and safe across
        processes.

        ``create_all`` does a SELECT-then-CREATE per table, so two workers
        bootstrapping the same fresh DB at once can race: both see a table
        missing, both CREATE it, and the loser fails with 'already exists'
        or SQLITE_BUSY. Retries both; on a second 'already exists' the peer
        finished the schema and we mark ready (partial creates are reconciled
        by the first successful create_all after the race).
        """
        if self._schema_is_ready():
            return
        async with self._schema_lock_for_running_loop():
            if self._schema_is_ready():
                return
            metadata, _, _ = broker_schema()
            loop = asyncio.get_running_loop()
            deadline = loop.time() + _SQLITE_SCHEMA_BUSY_BUDGET_S if self._is_sqlite else None
            attempt = 0
            schema = self._schema

            def create_tables(connection: Any) -> None:
                metadata.create_all(connection)
                _add_missing_message_columns(connection, schema)

            async def create_schema() -> None:
                async def create() -> None:
                    async with self._engine.begin() as conn:
                        if self._schema:
                            from sqlalchemy.schema import CreateSchema

                            await conn.execute(CreateSchema(self._schema, if_not_exists=True))
                        await conn.run_sync(create_tables)

                if deadline is None:
                    await create()
                else:
                    async with asyncio.timeout_at(deadline):
                        await create()

            async def retry_delay() -> None:
                delay = _sqlite_busy_delay(attempt)
                if deadline is None:
                    await asyncio.sleep(delay)
                else:
                    async with asyncio.timeout_at(deadline):
                        await asyncio.sleep(delay)

            while True:
                attempt += 1
                try:
                    await create_schema()
                    self._schema_ready = True
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if _is_already_exists(exc) or _is_pg_namespace_unique_race(exc):
                        logger.debug("broker schema create raced with a peer — reconciling")
                        try:
                            await create_schema()
                        except Exception as exc2:
                            if _is_already_exists(exc2):
                                # Peer likely finished. Verify before marking
                                # ready — a persistent DDL error must still raise.
                                if await self._broker_tables_present(deadline=deadline):
                                    self._schema_ready = True
                                    return
                                if attempt >= _SQLITE_SCHEMA_BUSY_MAX_RETRIES or (
                                    deadline is not None and loop.time() >= deadline
                                ):
                                    raise
                                await retry_delay()
                                continue
                            if not _is_sqlite_locked(exc2):
                                raise
                            exc = exc2  # fall through to busy retry
                        else:
                            self._schema_ready = True
                            return
                    if not _is_sqlite_locked(exc):
                        raise
                    if attempt >= _SQLITE_SCHEMA_BUSY_MAX_RETRIES or (
                        deadline is not None and loop.time() >= deadline
                    ):
                        raise
                    logger.warning(
                        "SQLite busy on schema create (attempt %d/%d) — retrying",
                        attempt,
                        _SQLITE_SCHEMA_BUSY_MAX_RETRIES,
                    )
                    await retry_delay()

    async def _write(self, operation: Callable[[Any], Awaitable[Any]]) -> Any:
        """Run ``operation(conn)`` inside one transaction, retrying a transient
        SQLite lock up to ``_SQLITE_BUSY_MAX_RETRIES`` times with backoff,
        bounded by ``_SQLITE_BUSY_TOTAL_BUDGET_S`` wall time.

        Each attempt is a fresh transaction (``engine.begin()`` rolls back on
        the raised lock error), so a retry re-runs the whole operation from a
        clean state — safe for the SELECT-then-write claim path. Non-lock
        errors propagate immediately; ``CancelledError`` is never swallowed.
        On Postgres / MySQL ``_is_sqlite_locked`` never matches, so this is a
        plain single-attempt transaction there.

        The wall budget covers waiting — acquiring the connection/transaction
        and the geometric backoff sleeps — but NOT ``operation`` itself. An
        attempt that has started doing work runs to completion: capping the
        work aborted legitimately long writes (a bulk ``prune`` delete over a
        few hundred thousand rows) with a ``TimeoutError`` that
        ``_is_sqlite_locked`` cannot match, so it never retried, and could
        abandon a transaction the database had already committed.

        Exhausting that budget always surfaces as ``TimeoutError``, whether the
        lock blocked the transaction itself or the first statement inside it —
        which of the two happens depends on when the driver defers ``BEGIN``,
        and the caller should not see the outcome change with it. Exhausting
        ``_SQLITE_BUSY_MAX_RETRIES`` first keeps raising the driver's own error.
        """
        if not self._is_sqlite:
            async with self._engine.begin() as conn:
                return await operation(conn)

        attempt = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _SQLITE_BUSY_TOTAL_BUDGET_S
        while True:
            attempt += 1
            try:
                async with AsyncExitStack() as stack:
                    async with asyncio.timeout_at(deadline):
                        conn = await stack.enter_async_context(self._engine.begin())
                    return await operation(conn)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not _is_sqlite_locked(exc):
                    raise
                if loop.time() >= deadline:
                    raise TimeoutError(
                        f"SQLite write did not get its lock within {_SQLITE_BUSY_TOTAL_BUDGET_S}s"
                    ) from exc
                if attempt >= _SQLITE_BUSY_MAX_RETRIES:
                    raise
                logger.warning(
                    "SQLite busy on write (attempt %d/%d) — retrying",
                    attempt,
                    _SQLITE_BUSY_MAX_RETRIES,
                )
                async with asyncio.timeout_at(deadline):
                    await asyncio.sleep(_sqlite_busy_delay(attempt))

    async def _write_target_locked(
        self,
        targets: list[str] | tuple[str, ...],
        operation: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Serialize replay-sensitive work by target in deterministic order."""
        from sqlalchemy import text

        ordered_targets = sorted(set(targets))
        if not ordered_targets:
            return await self._write(operation)
        # Same defensive read as __init__'s _is_sqlite: a minimal injected
        # engine without a dialect keeps the adapter's historical SQLite
        # behavior instead of raising AttributeError on this path alone.
        dialect = getattr(getattr(self._engine, "dialect", None), "name", "sqlite")
        if dialect in {"mysql", "mariadb"}:
            return await self._write_mysql_target_locked(ordered_targets, operation)

        async def locked_operation(conn: Any) -> Any:
            if dialect == "postgresql":
                for target in ordered_targets:
                    await conn.execute(
                        text("SELECT pg_advisory_xact_lock(:key)"),
                        {"key": _postgres_target_lock_key(target)},
                    )
            elif dialect == "sqlite":
                # SQLite has no row/advisory locks. BEGIN IMMEDIATE obtains the
                # database write reservation before either side reads state.
                await conn.exec_driver_sql("BEGIN IMMEDIATE")
            return await operation(conn)

        return await self._write(locked_operation)

    async def _write_mysql_target_locked(
        self,
        targets: list[str],
        operation: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Hold MySQL connection locks until the transaction has committed.

        A contended waiter must not sit on a pooled connection for the whole
        ``_TARGET_LOCK_TIMEOUT_S`` budget: each attempt waits only
        ``_TARGET_LOCK_ATTEMPT_WAIT_S`` inside GET_LOCK and hands the
        connection back before retrying, so publishers queueing on one target
        cannot exhaust the pool and stall unrelated broker work.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _TARGET_LOCK_TIMEOUT_S
        while True:
            blocked, value = await self._attempt_mysql_target_locked(targets, operation)
            if blocked is None:
                return value
            if loop.time() >= deadline:
                raise RuntimeError(
                    f"timed out acquiring database broker target lock for {blocked!r}"
                )

    async def _attempt_mysql_target_locked(
        self,
        targets: list[str],
        operation: Callable[[Any], Awaitable[Any]],
    ) -> tuple[str | None, Any]:
        """Take one pooled connection and either do the work or give it back.

        Returns ``(None, result)`` once every named lock was won and the
        operation committed, or ``(blocked_target, None)`` when a lock was
        still held elsewhere — and in that case the connection is already back
        in the pool with no lock retained, so the caller can retry cheaply.
        """
        from sqlalchemy import text

        async with self._engine.connect() as conn:
            transaction = await conn.begin()
            acquired: list[str] = []
            blocked: str | None = None
            try:
                for target in targets:
                    lock_name = _mysql_target_lock_name(target)
                    result = await conn.execute(
                        text("SELECT GET_LOCK(:name, :timeout)"),
                        {"name": lock_name, "timeout": _TARGET_LOCK_ATTEMPT_WAIT_S},
                    )
                    if result.scalar_one() != 1:
                        blocked = target
                        break
                    acquired.append(lock_name)
                if blocked is not None:
                    return blocked, None
                value = await operation(conn)
                await transaction.commit()
                return None, value
            except BaseException:
                if transaction.is_active:
                    await transaction.rollback()
                raise
            finally:
                # Named locks survive commit and connection-pool return. Always
                # release them explicitly so a pooled connection cannot retain
                # ownership after this operation.
                for lock_name in reversed(acquired):
                    await conn.execute(
                        text("SELECT RELEASE_LOCK(:name)"),
                        {"name": lock_name},
                    )
                if conn.in_transaction():
                    await conn.rollback()

    async def _now(self, conn: Any) -> datetime:
        """Current time from the DATABASE server clock (not the app host's).

        Every skew-sensitive timestamp — a message's claim visibility
        (``available_at``), the reclaim cutoff (``claimed_at``), the retry
        backoff, and the prune age — is both STAMPED and COMPARED against this
        one clock, so cross-host clock skew can't make one consumer reclaim a
        peer's fresh claim early, delay a message's visibility, or prune by the
        wrong wall clock (competing consumers and the producer commonly run on
        different hosts).

        SQLite's ``CURRENT_TIMESTAMP`` is only second-resolution. ``strftime``
        keeps millisecond precision while still sourcing time from SQLite,
        which makes retained-message TTL independent of the application clock.
        """
        from sqlalchemy import text

        dialect = self._engine.dialect.name
        if dialect == "postgresql":
            result = await conn.execute(text("SELECT now()"))
            return cast(datetime, result.scalar_one())
        if dialect in {"mysql", "mariadb"}:
            # UTC_TIMESTAMP(6) (not NOW(), which is session-timezone dependent)
            # returns microsecond-precision naive UTC; tag it UTC so it
            # round-trips like the app-clock path did (columns are tz-aware).
            result = await conn.execute(text("SELECT UTC_TIMESTAMP(6)"))
            return cast(datetime, result.scalar_one()).replace(tzinfo=UTC)
        if dialect == "sqlite":
            result = await conn.execute(text("SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')"))
            raw = cast(str, result.scalar_one())
            return datetime.fromisoformat(raw).replace(tzinfo=UTC)
        return datetime.now(UTC)

    async def _delete_retained(self, conn: Any, retained_ids: list[str]) -> None:
        """Delete ledger rows explicitly before their retained sources.

        The FK also cascades on server databases. The explicit delete keeps
        cleanup correct on injected SQLite engines where foreign keys may not
        have been enabled by ``_install_sqlite_pragmas``.

        Chunked because every caller's id list is bounded only by the retained
        table's own size: one ``IN (...)`` over more ids than the driver's bind
        limit raises (SQLite "too many SQL variables" at 32,766; asyncpg at
        32,767), and since prune/publish/subscribe all delete through here, the
        only routine that can shrink the table would be the one that fails on
        it.
        """
        if not retained_ids:
            return
        from sqlalchemy import delete

        retained, delivery = _retained_tables()
        for start in range(0, len(retained_ids), _RETAINED_ID_CHUNK):
            chunk = retained_ids[start : start + _RETAINED_ID_CHUNK]
            await conn.execute(delete(delivery).where(delivery.c.retained_message_id.in_(chunk)))
            await conn.execute(delete(retained).where(retained.c.id.in_(chunk)))

    async def _prune_expired_retained(
        self,
        conn: Any,
        now: datetime,
        target: str | None = None,
    ) -> int:
        from sqlalchemy import select

        retained, _ = _retained_tables()
        expired = select(retained.c.id).where(retained.c.expires_at <= now)
        if target is not None:
            expired = expired.where(retained.c.target == target)
        ids = [row[0] for row in await conn.execute(expired)]
        await self._delete_retained(conn, ids)
        return len(ids)

    async def _fan_out_retained(
        self,
        conn: Any,
        sources: list[Any],
        groups: list[str],
    ) -> int:
        """Materialize each source/group pair once using its durable ledger."""
        from sqlalchemy import select

        if not groups:
            return 0
        _, _, message = broker_schema()
        _, delivery = _retained_tables()
        unique_groups = sorted(set(groups))
        inserted = 0

        for source in sources:
            source_id = cast(str, source["id"])
            existing_result = await conn.execute(
                select(delivery.c.consumer_group).where(
                    delivery.c.retained_message_id == source_id,
                    delivery.c.consumer_group.in_(unique_groups),
                )
            )
            existing = {row[0] for row in existing_result}
            missing = [group for group in unique_groups if group not in existing]
            if not missing:
                continue

            delivered_at = await self._now(conn)
            ledger_rows = [
                {
                    "retained_message_id": source_id,
                    "consumer_group": group,
                    "broker_message_id": _delivery_message_id(source_id, group),
                    "delivered_at": delivered_at,
                }
                for group in missing
            ]
            await conn.execute(delivery.insert(), ledger_rows)
            await conn.execute(
                message.insert(),
                [
                    {
                        "id": row["broker_message_id"],
                        "target": source["target"],
                        "consumer_group": row["consumer_group"],
                        "event_type": source["event_type"],
                        "payload": source["payload"],
                        "headers": source["headers"],
                        "status": "pending",
                        "attempts": 0,
                        "available_at": delivered_at,
                        "claimed_at": None,
                        "claimed_by": None,
                        "created_at": delivered_at,
                        "last_error": None,
                    }
                    for row in ledger_rows
                ],
            )
            inserted += len(missing)
        return inserted

    async def _publish_stored(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None,
    ) -> None:
        from sqlalchemy import select

        expected_groups: list[str] = []
        if self._orphan_replay_policy == "expected_groups":
            expected_groups = self._expected_consumer_groups.get(target, [])
            if not expected_groups:
                raise ConfigurationError(
                    "expected_consumer_groups must contain a non-empty group list "
                    f"for target {target!r} when orphan_replay_policy='expected_groups'"
                )
        _, subscription, _ = broker_schema()
        retained, _ = _retained_tables()
        event_type = (headers or {}).get("event_type", "")
        headers_blob = json.dumps(headers) if headers else None

        async def op(conn: Any) -> int:
            now = await self._now(conn)
            await self._prune_expired_retained(conn, now, target)
            source = {
                "id": str(uuid4()),
                "target": target,
                "event_type": event_type,
                "payload": payload,
                "headers": headers_blob,
                "created_at": now,
                "expires_at": now + timedelta(seconds=self._orphan_retention_seconds),
            }
            await conn.execute(retained.insert().values(**source))
            groups = [
                row[0]
                for row in await conn.execute(
                    select(subscription.c.consumer_group).where(subscription.c.target == target)
                )
            ]
            if self._orphan_replay_policy == "expected_groups":
                groups = sorted(set(groups).union(expected_groups))
            delivered = await self._fan_out_retained(conn, [source], groups)
            if self._orphan_replay_policy == "expected_groups" or (
                self._orphan_replay_policy == "first_groups" and groups
            ):
                await self._delete_retained(conn, [cast(str, source["id"])])
            return delivered

        delivered: int = await self._write_target_locked([target], op)
        logger.debug(
            "stored publish target=%s fanned out to %d current group(s)",
            target,
            delivered,
        )

    # ----- producer side -----------------------------------------------

    @_on_owning_loop
    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Fan out on write: one ``broker_message`` row per subscribed group.

        Looks up every ``consumer_group`` subscribed to ``target`` (populated
        by ``DatabaseConsumer.start()``) and inserts one row per group in a
        single transaction. With no group, applies the configured ``error``,
        ``wait``, or retained-message ``store`` policy.
        """
        if len(payload) > self._max_payload_bytes:
            raise ConfigurationError(
                f"database broker payload is {len(payload)} bytes, exceeding "
                f"max_payload_bytes={self._max_payload_bytes}. Reduce the payload "
                "or increase the limit."
            )
        await self._ensure_schema()
        if self._no_subscriber_policy == "store":
            await self._publish_stored(target, payload, headers)
            return
        from sqlalchemy import select

        _, subscription, message = broker_schema()
        event_type = (headers or {}).get("event_type", "")
        headers_blob = json.dumps(headers) if headers else None

        async def op(conn: Any) -> int:
            result = await conn.execute(
                select(subscription.c.consumer_group).where(subscription.c.target == target)
            )
            groups = [row[0] for row in result]
            if not groups:
                return 0
            now = await self._now(conn)
            rows = [
                {
                    "id": str(uuid4()),
                    "target": target,
                    "consumer_group": group,
                    "event_type": event_type,
                    "payload": payload,
                    "headers": headers_blob,
                    "status": "pending",
                    "attempts": 0,
                    "available_at": now,
                    "claimed_at": None,
                    "claimed_by": None,
                    "created_at": now,
                    "last_error": None,
                }
                for group in groups
            ]
            await conn.execute(message.insert(), rows)
            return len(groups)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._no_subscriber_wait_timeout_s
        while True:
            count = await self._write(op)
            if count:
                logger.debug("publish target=%s fanned out to %d group(s)", target, count)
                return
            if self._no_subscriber_policy == "error":
                raise NoSubscribersError(target)
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise NoSubscribersError(target, self._no_subscriber_wait_timeout_s)
            await asyncio.sleep(min(self._no_subscriber_wait_poll_interval_s, remaining))

    @_on_owning_loop
    async def close(self) -> None:
        """Dispose the engine (and its connection pool). Idempotent-friendly
        (close-after-close does not raise — ``AsyncEngine.dispose`` itself
        tolerates repeated calls)."""
        await self._engine.dispose()

    # ----- consumer side -------------------------------------------------

    @_on_owning_loop
    async def subscribe(self, targets: list[str] | tuple[str, ...], group: str) -> None:
        """Upsert ``(target, group)`` subscription rows — idempotent AND
        concurrency-safe.

        Uses the dialect-native upsert (Postgres/SQLite ``ON CONFLICT DO
        UPDATE``, MySQL/MariaDB ``ON DUPLICATE KEY UPDATE``) so two workers of the same
        replicated module registering the same ``(target, consumer_group)`` at
        once can't collide on the PK. A check-then-insert races here: both
        workers see no row, both insert, and one dies with IntegrityError —
        exactly the first-deploy hazard for a replicated module. Refreshes
        ``updated_at`` on conflict so the row doubles as a liveness marker.
        """
        if not targets:
            return
        await self._ensure_schema()
        _require_skip_locked(self._engine, await self._mysql_server_version())
        _, subscription, _ = broker_schema()
        retained, _ = _retained_tables()
        ordered_targets = sorted(set(targets))

        async def op(conn: Any) -> None:
            from sqlalchemy import select

            now = await self._now(conn)
            rows = [
                {"target": target, "consumer_group": group, "updated_at": now}
                for target in ordered_targets
            ]
            await conn.execute(self._upsert_subscription(subscription, rows))

            if self._no_subscriber_policy != "store":
                return
            for target in ordered_targets:
                await self._prune_expired_retained(conn, now, target)
                if self._orphan_replay_policy == "ttl_all_groups":
                    replay_groups = [group]
                elif self._orphan_replay_policy == "first_groups":
                    group_result = await conn.execute(
                        select(subscription.c.consumer_group).where(subscription.c.target == target)
                    )
                    replay_groups = [row[0] for row in group_result]
                else:
                    continue
                # Page the replay by id. Under ``ttl_all_groups`` the retained
                # table holds every publish for the whole retention window even
                # while consumers are healthy, and this runs on every consumer
                # start — selecting the full rows in one statement would make
                # the backlog's entire payload volume resident at once
                # (SQLAlchemy's async execute() prebuffers the whole result),
                # so a restart after a busy window OOMs on the way up.
                id_result = await conn.execute(
                    select(retained.c.id)
                    .where(
                        retained.c.target == target,
                        retained.c.expires_at > now,
                    )
                    .order_by(retained.c.created_at, retained.c.id)
                )
                source_ids = [row[0] for row in id_result]
                for start in range(0, len(source_ids), _RETAINED_ID_CHUNK):
                    page = source_ids[start : start + _RETAINED_ID_CHUNK]
                    page_result = await conn.execute(
                        select(retained)
                        .where(retained.c.id.in_(page))
                        .order_by(retained.c.created_at, retained.c.id)
                    )
                    await self._fan_out_retained(conn, list(page_result.mappings()), replay_groups)
                    if self._orphan_replay_policy == "first_groups":
                        await self._delete_retained(conn, page)

        await self._write_target_locked(ordered_targets, op)

    def _upsert_subscription(self, subscription: Any, rows: list[dict[str, Any]]) -> Any:
        """Build the dialect-native subscription upsert statement.

        Postgres/SQLite: ``INSERT ... ON CONFLICT (target, consumer_group) DO
        UPDATE SET updated_at=excluded.updated_at``. MySQL/MariaDB: ``INSERT
        ... ON DUPLICATE KEY UPDATE``. Any other dialect (unreachable for the
        supported backends) falls back to a plain insert.
        """
        name = self._engine.dialect.name
        if name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            pg_stmt = pg_insert(subscription).values(rows)
            return pg_stmt.on_conflict_do_update(
                index_elements=["target", "consumer_group"],
                set_={"updated_at": pg_stmt.excluded.updated_at},
            )
        if name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            sqlite_stmt = sqlite_insert(subscription).values(rows)
            return sqlite_stmt.on_conflict_do_update(
                index_elements=["target", "consumer_group"],
                set_={"updated_at": sqlite_stmt.excluded.updated_at},
            )
        if name in {"mysql", "mariadb"}:
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            mysql_stmt = mysql_insert(subscription).values(rows)
            return mysql_stmt.on_duplicate_key_update(updated_at=mysql_stmt.inserted.updated_at)
        from sqlalchemy import insert

        return insert(subscription).values(rows)

    @_on_owning_loop
    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
        max_attempts: int | None = None,
        targets: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Claim up to ``batch_size`` due rows for ``group``.

        ``targets``, when given, restricts the claim to rows for those
        targets. A consumer passes the targets it currently consumes, so rows
        fanned out through a subscription its module no longer consumes stay
        ``pending`` for a rollback or an explicit ``drop_group(...,
        targets=...)`` instead of being dead-lettered as undeserializable.

        Picks up both freshly ``pending`` rows AND rows stuck in ``claimed``
        whose ``claimed_at`` is older than ``reclaim_stale_seconds`` — the
        latter are orphans left by a consumer that crashed between claim and
        ack, so reclaiming them is what preserves at-least-once delivery across
        a consumer crash (the DB analogue of the Redis adapter's XAUTOCLAIM
        reclaim).

        A reclaim re-stamps ``claimed_at``/``claimed_by``, and bumps
        ``attempts`` only when the abandoned claim had started dispatching
        the row (``renew_claims(..., start_dispatch=True)`` sets
        ``dispatch_started``). It has to charge that row: a consumer that
        crashes, is OOM-killed, or wedges past the lease cap never reaches
        ``fail()``, so without the charge the row would be handed out forever
        with ``attempts`` frozen at 0. Rows claimed in the same batch whose
        listener never started are reclaimed free, so a crash loop on one
        row does not spend the retry budget of the rows queued behind it.
        When ``max_attempts`` is given, a charged reclaim that exhausts it
        moves the row straight to ``dead`` and withholds it from the batch,
        because nothing on the crash path will run the cap in ``fail()``.

        Postgres/MySQL: ``FOR UPDATE SKIP LOCKED`` lets concurrent consumers
        partition the backlog instead of blocking or double-claiming. SQLite
        (no row locking) degrades to a plain claim inside one transaction —
        see ``_supports_skip_locked`` and the module docstring.
        """
        await self._ensure_schema()
        server_version = await self._mysql_server_version()
        _require_skip_locked(self._engine, server_version)
        from sqlalchemy import and_, or_, select, update

        _, _, message = broker_schema()
        cap = None if max_attempts is None else _positive_int(max_attempts, "max_attempts")

        async def op(conn: Any) -> list[dict[str, Any]]:
            now = await self._now(conn)
            stale_cutoff = now - timedelta(seconds=reclaim_stale_seconds)
            stmt = (
                select(message)
                .where(
                    message.c.consumer_group == group,
                    message.c.available_at <= now,
                    or_(
                        message.c.status == "pending",
                        and_(
                            message.c.status == "claimed",
                            message.c.claimed_at <= stale_cutoff,
                        ),
                    ),
                )
                .order_by(message.c.available_at)
                .limit(batch_size)
            )
            if targets is not None:
                stmt = stmt.where(message.c.target.in_(list(targets)))
            if _supports_skip_locked(self._engine, server_version):
                stmt = stmt.with_for_update(skip_locked=True)
            result = await conn.execute(stmt)
            rows = [dict(row._mapping) for row in result]

            charged = [
                row for row in rows if row["status"] == "claimed" and row["dispatch_started"]
            ]
            exhausted_ids = (
                set()
                if cap is None
                else {row["id"] for row in charged if int(row["attempts"]) + 1 >= cap}
            )
            charged_ids = [row["id"] for row in charged if row["id"] not in exhausted_ids]
            uncharged_ids = [
                row["id"]
                for row in rows
                if row["status"] != "claimed" or not row["dispatch_started"]
            ]

            if uncharged_ids:
                await conn.execute(
                    update(message)
                    .where(message.c.id.in_(uncharged_ids))
                    .values(
                        status="claimed",
                        claimed_at=now,
                        claimed_by=consumer_name,
                        dispatch_started=False,
                    )
                )
            if charged_ids:
                await conn.execute(
                    update(message)
                    .where(message.c.id.in_(charged_ids))
                    .values(
                        status="claimed",
                        claimed_at=now,
                        claimed_by=consumer_name,
                        attempts=message.c.attempts + 1,
                        dispatch_started=False,
                    )
                )
            if exhausted_ids:
                await conn.execute(
                    update(message)
                    .where(message.c.id.in_(exhausted_ids))
                    .values(
                        status="dead",
                        attempts=message.c.attempts + 1,
                        available_at=now,
                        claimed_at=None,
                        claimed_by=None,
                        dispatch_started=False,
                        last_error=(
                            f"reclaimed {cap} times without completing "
                            "(consumer crashed or wedged mid-dispatch)"
                        ),
                    )
                )

            bumped = set(charged_ids)
            claimed_rows = [row for row in rows if row["id"] not in exhausted_ids]
            for row in claimed_rows:
                if row["id"] in bumped:
                    row["attempts"] = int(row["attempts"]) + 1
                row["dispatch_started"] = False
            return claimed_rows

        rows: list[dict[str, Any]] = await self._write(op)
        return rows

    @_on_owning_loop
    async def renew_claims(
        self,
        row_ids: list[str],
        *,
        consumer_name: str,
        start_dispatch: bool = False,
    ) -> int:
        """Re-stamp ``claimed_at`` = server-now for rows THIS consumer still owns.

        ``start_dispatch=True`` also marks the rows' dispatch as started, in
        the same statement: the consumer passes it once per row just before
        handing the row to its listener, so a later stale reclaim charges
        that row an attempt. Heartbeat renewals leave the mark alone.

        The consumer's in-flight heartbeat: called every
        ``reclaim_stale_seconds / 3`` while a claimed batch dispatches, so rows
        waiting behind slow listeners (or the concurrency gate) are never
        reclaimed by a peer mid-dispatch — batch size stays decoupled from the
        reclaim window. Owner-guarded like ``ack``/``fail``: rows already
        reclaimed by a peer are skipped, and the returned count tells the
        caller how many rows are still theirs. Renewal stops when the process
        dies, so crash reclaim is unaffected.
        """
        if not row_ids:
            return 0
        from sqlalchemy import update

        _, _, message = broker_schema()

        async def op(conn: Any) -> int:
            now = await self._now(conn)
            values: dict[str, Any] = {"claimed_at": now}
            if start_dispatch:
                values["dispatch_started"] = True
            result = await conn.execute(
                update(message)
                .where(
                    message.c.id.in_(row_ids),
                    message.c.status == "claimed",
                    message.c.claimed_by == consumer_name,
                )
                .values(**values)
            )
            return int(result.rowcount or 0)

        count: int = await self._write(op)
        return count

    @_on_owning_loop
    async def release_claims(self, row_ids: list[str], *, consumer_name: str) -> int:
        """Hand rows THIS consumer claimed but never dispatched back to ``pending``.

        A stopping consumer calls it for the rows its stop kept from their
        listeners, so any consumer of the group can claim them at once instead
        of after ``reclaim_stale_seconds``. Nothing is charged: ``attempts`` and
        ``available_at`` stay as they were. Owner-guarded like
        ``renew_claims``, and a row whose dispatch started keeps its claim, so
        its stale reclaim still charges the attempt. Returns how many rows
        were released.
        """
        if not row_ids:
            return 0
        from sqlalchemy import update

        _, _, message = broker_schema()

        async def op(conn: Any) -> int:
            result = await conn.execute(
                update(message)
                .where(
                    message.c.id.in_(row_ids),
                    message.c.status == "claimed",
                    message.c.claimed_by == consumer_name,
                    message.c.dispatch_started.is_(False),
                )
                .values(status="pending", claimed_at=None, claimed_by=None)
            )
            return int(result.rowcount or 0)

        count: int = await self._write(op)
        return count

    @_on_owning_loop
    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        """Complete a row THIS consumer still owns: delete it (default) or mark
        it 'done' (mark mode, which keeps the row for the prune job — see
        ``prune``).

        Guarded by ``status='claimed' AND claimed_by=:consumer_name`` (a
        compare-and-swap): a late ack from a consumer whose row was already
        reclaimed — and possibly dead-lettered — by a peer is a no-op, never
        deleting or completing a row the caller no longer owns.
        """
        from sqlalchemy import delete, update

        _, _, message = broker_schema()

        async def op(conn: Any) -> None:
            if self._completion_mode == "delete":
                await conn.execute(delete(message).where(*_owned(message, row_id, consumer_name)))
            else:
                await conn.execute(
                    update(message)
                    .where(*_owned(message, row_id, consumer_name))
                    .values(status="done")
                )

        await self._write(op)

    @_on_owning_loop
    async def fail(self, row_id: str, error: str, *, consumer_name: str, max_attempts: int) -> None:
        """Record a dispatch failure for a row THIS consumer still owns:
        attempts++ with backoff, staying 'pending' until ``max_attempts`` is
        reached, then 'dead'.

        Guarded by ``status='claimed' AND claimed_by=:consumer_name`` and the
        attempt count is read from the row INSIDE the same transaction (not a
        caller snapshot), so a late failure from a consumer whose row was
        already reclaimed by a peer — or already moved to a terminal state — is
        a no-op instead of resurrecting a dead row or writing a stale attempt
        count.
        """
        from sqlalchemy import select, update

        _, _, message = broker_schema()

        async def op(conn: Any) -> None:
            current = (
                await conn.execute(
                    select(message.c.attempts).where(*_owned(message, row_id, consumer_name))
                )
            ).scalar_one_or_none()
            if current is None:
                return  # reclaimed by a peer or already terminal — late write is a no-op
            new_attempts = current + 1
            now = await self._now(conn)
            if new_attempts >= max_attempts:
                status = "dead"
                available_at = now
            else:
                status = "pending"
                available_at = now + timedelta(seconds=_backoff_delay(new_attempts))
            await conn.execute(
                update(message)
                .where(*_owned(message, row_id, consumer_name))
                .values(
                    status=status,
                    attempts=new_attempts,
                    available_at=available_at,
                    last_error=error,
                    claimed_at=None,
                    claimed_by=None,
                )
            )

        await self._write(op)

    @_on_owning_loop
    async def dead_letter(self, row_id: str, error: str, *, consumer_name: str) -> None:
        """Mark a poison row THIS consumer still owns 'dead' immediately
        (undeserializable payload or missing ``event_type`` — retrying can never
        succeed).

        Guarded by ``status='claimed' AND claimed_by=:consumer_name`` for the
        same reason as ``ack``/``fail``: a late call after a peer reclaimed the
        row is a no-op.
        """
        from sqlalchemy import update

        _, _, message = broker_schema()

        async def op(conn: Any) -> None:
            await conn.execute(
                update(message)
                .where(*_owned(message, row_id, consumer_name))
                .values(status="dead", last_error=error)
            )

        await self._write(op)

    @_on_owning_loop
    async def prune(
        self,
        *,
        retention_age_seconds: float | None = None,
        retention_count: int | None = None,
    ) -> int:
        """Delete terminal ('done'/'dead') rows by age and/or by count.

        - ``retention_age_seconds``: delete terminal rows whose ``created_at``
          is older than ``now - retention_age_seconds``.
        - ``retention_count``: keep only the newest ``retention_count`` terminal
          rows per ``(target, consumer_group)``; delete the rest.

        Both terminal-row settings are optional. Expired retained sources are
        always pruned, including when neither setting is supplied. When both
        terminal settings are set, age runs before count in one transaction.
        **Only terminal queue rows are ever deleted** — 'pending'/'claimed'
        rows are undelivered work and are never touched. Returns the total
        number of terminal rows and retained sources deleted.

        Note: the age prune measures from ``created_at`` (publish time), not
        from when the row became terminal. A message that only dead-lettered
        after exhausting its retries is aged from when it was first published,
        so a slow-to-die message can be eligible for age-prune shortly after it
        turns terminal. This is intentional (``created_at`` is the stable,
        indexed column) and harmless — terminal rows are, by definition, no
        longer deliverable.
        """
        await self._ensure_schema()
        from sqlalchemy import delete, func, select

        _, _, message = broker_schema()
        terminal = message.c.status.in_(_TERMINAL_STATUSES)

        async def op(conn: Any) -> int:
            now = await self._now(conn)
            deleted_local = await self._prune_expired_retained(conn, now)
            if retention_age_seconds is not None:
                cutoff = now - timedelta(seconds=retention_age_seconds)
                result = await conn.execute(
                    delete(message).where(terminal, message.c.created_at < cutoff)
                )
                deleted_local += _rowcount(result)
            if retention_count is not None:
                # Rank terminal rows newest-first within each logical queue and
                # delete everything past the keep-count. The window subquery is
                # wrapped in a derived table (``.subquery()``) so the DELETE's
                # IN-subquery reads FROM that derived table, not from
                # ``broker_message`` directly — MySQL rejects a subquery that
                # references the delete target directly (error 1093); Postgres
                # and SQLite accept the derived-table form too.
                rank = (
                    func.row_number()
                    .over(
                        partition_by=(message.c.target, message.c.consumer_group),
                        order_by=message.c.created_at.desc(),
                    )
                    .label("rn")
                )
                ranked = select(message.c.id, rank).where(terminal).subquery()
                doomed = select(ranked.c.id).where(ranked.c.rn > retention_count)
                # Re-assert ``terminal`` on the outer DELETE too: the doomed ids
                # already come from a terminal-only ranking, but this makes the
                # never-touch-undelivered-rows guarantee independent of the
                # subquery — a pending/claimed row can never be deleted even if
                # the ranking logic later regresses.
                result = await conn.execute(
                    delete(message).where(message.c.id.in_(doomed), terminal)
                )
                deleted_local += _rowcount(result)
            return deleted_local

        deleted: int = await self._write(op)
        if deleted:
            logger.debug("prune deleted %d terminal row(s)", deleted)
        return deleted

    @_on_owning_loop
    async def group_backlog(self) -> dict[str, int]:
        """Map every group to its pending and claimed row count.

        A group counts when it is subscribed or still holds pending or
        claimed rows, so rows left behind by an unsubscribed group show up.
        """
        await self._ensure_schema()
        from sqlalchemy import func, select

        _, subscription, message = broker_schema()

        async def op(conn: Any) -> dict[str, int]:
            groups = await conn.execute(select(subscription.c.consumer_group).distinct())
            backlog = {str(row[0]): 0 for row in groups}
            counts = await conn.execute(
                select(message.c.consumer_group, func.count())
                .where(message.c.status.in_(("pending", "claimed")))
                .group_by(message.c.consumer_group)
            )
            for group, count in counts:
                backlog[str(group)] = int(count)
            return dict(sorted(backlog.items()))

        result: dict[str, int] = await self._write(op)
        return result

    @_on_owning_loop
    async def active_groups(self, *, within_seconds: float) -> set[str]:
        """Groups a consumer refreshed or claimed for within ``within_seconds``.

        A running consumer re-stamps its subscription rows' ``updated_at``
        periodically (``touch_subscriptions``) and every claim stamps
        ``claimed_at``, so a group served by any deployment shows up here
        even when this one's modules do not derive it. Expects the broker
        tables to exist; it never creates them.
        """
        from sqlalchemy import select, union

        _, subscription, message = broker_schema()

        async def op(conn: Any) -> set[str]:
            cutoff = await self._now(conn) - timedelta(seconds=within_seconds)
            rows = await conn.execute(
                union(
                    select(subscription.c.consumer_group).where(
                        subscription.c.updated_at >= cutoff
                    ),
                    select(message.c.consumer_group).where(message.c.claimed_at >= cutoff),
                )
            )
            return {str(row[0]) for row in rows}

        result: set[str] = await self._write(op)
        return result

    @_on_owning_loop
    async def touch_subscriptions(self, targets: list[str] | tuple[str, ...], group: str) -> None:
        """Stamp ``group``'s subscriptions to ``targets`` as served right now."""
        from sqlalchemy import update

        _, subscription, _ = broker_schema()

        async def op(conn: Any) -> None:
            await conn.execute(
                update(subscription)
                .where(
                    subscription.c.consumer_group == group,
                    subscription.c.target.in_(list(targets)),
                )
                .values(updated_at=await self._now(conn))
            )

        await self._write(op)

    @_on_owning_loop
    async def sole_subscriber_targets(self, group: str) -> list[str]:
        """Targets ``group`` subscribes to that no other group subscribes to."""
        from sqlalchemy import func, select

        _, subscription, _ = broker_schema()
        held = select(subscription.c.target).where(subscription.c.consumer_group == group)

        async def op(conn: Any) -> list[str]:
            rows = await conn.execute(
                select(subscription.c.target)
                .where(subscription.c.target.in_(held))
                .group_by(subscription.c.target)
                .having(func.count() == 1)
                .order_by(subscription.c.target)
            )
            return [str(row[0]) for row in rows]

        result: list[str] = await self._write(op)
        return result

    @property
    def store_location(self) -> str:
        """The database URL this broker uses, with any password masked."""
        return str(self._engine.url.render_as_string(hide_password=True))

    @property
    def no_subscriber_policy(self) -> str:
        """What a publish does when its target has no subscribed group."""
        return str(self._no_subscriber_policy)

    @property
    def no_subscriber_wait_timeout_seconds(self) -> float:
        """How long a publish waits for a subscriber under the ``wait`` policy."""
        return float(self._no_subscriber_wait_timeout_s)

    @property
    def orphan_replay_policy(self) -> str:
        """Which groups a stored publish reaches under the ``store`` policy."""
        return str(self._orphan_replay_policy)

    @property
    def orphan_retention_seconds(self) -> float:
        """How long a stored publish is kept for a group that subscribes later."""
        return float(self._orphan_retention_seconds)

    def expected_targets(self, group: str) -> list[str]:
        """Targets whose every publish is queued for ``group`` by configuration alone.

        Under the ``store`` policy with ``orphan_replay_policy="expected_groups"``
        a publish fans out to its ``expected_consumer_groups`` whether or not
        they subscribe, so dropping such a group's subscription does not stop it.
        """
        if self._no_subscriber_policy != "store" or self._orphan_replay_policy != "expected_groups":
            return []
        return sorted(t for t, groups in self._expected_consumer_groups.items() if group in groups)

    @_on_owning_loop
    async def has_schema(self) -> bool:
        """Whether the broker tables exist, checked without creating anything.

        A SQLite file that does not exist yet is reported absent without
        connecting, since connecting would create it.
        """
        from sqlalchemy import inspect

        url = self._engine.url
        database = url.database
        if (
            url.get_backend_name() == "sqlite"
            and database
            and database != ":memory:"
            and not database.startswith("file:")
            and not os.path.exists(database)
        ):
            return False
        _, subscription, message = broker_schema()
        names = (subscription.name, message.name)
        # The inspector ignores schema_translate_map, so resolve the schema a
        # caller-supplied engine translates the broker tables into.
        translate = self._engine.get_execution_options().get("schema_translate_map") or {}
        schema = self._schema or translate.get(None)

        def tables_exist(connection: Any) -> bool:
            inspector = inspect(connection)
            return all(inspector.has_table(name, schema=schema) for name in names)

        async with self._engine.connect() as conn:
            exists: bool = await conn.run_sync(tables_exist)
        return exists

    @_on_owning_loop
    async def stale_targets(
        self, group: str, targets: list[str] | tuple[str, ...]
    ) -> dict[str, int]:
        """Map each target ``group`` holds but ``targets`` omits to its backlog.

        A held target is one the group subscribes to or has pending or
        claimed rows for. The count is those undelivered rows. Nothing is
        removed: a rollback to a release that still consumes the target
        finds its backlog intact.
        """
        await self._ensure_schema()
        from sqlalchemy import func, select

        _, subscription, message = broker_schema()
        consumed = list(targets)

        async def op(conn: Any) -> dict[str, int]:
            subscribed = await conn.execute(
                select(subscription.c.target).where(
                    subscription.c.consumer_group == group,
                    subscription.c.target.not_in(consumed),
                )
            )
            stale = {str(row[0]): 0 for row in subscribed}
            counts = await conn.execute(
                select(message.c.target, func.count())
                .where(
                    message.c.consumer_group == group,
                    message.c.status.in_(("pending", "claimed")),
                    message.c.target.not_in(consumed),
                )
                .group_by(message.c.target)
            )
            for target, count in counts:
                stale[str(target)] = int(count)
            return dict(sorted(stale.items()))

        result: dict[str, int] = await self._write(op)
        return result

    @_on_owning_loop
    async def drop_group(
        self, group: str, *, targets: list[str] | tuple[str, ...] | None = None
    ) -> tuple[int, int]:
        """Unsubscribe a retired group and delete its undelivered rows.

        ``targets``, when given, limits the removal to those targets' rows.
        Returns ``(subscriptions, rows)`` removed. Terminal rows stay for
        ``prune``. This is the cleanup behind ``modulith broker drop-group``.
        Expects the broker tables to exist; it never creates them.
        """
        from sqlalchemy import delete

        _, subscription, message = broker_schema()

        async def op(conn: Any) -> tuple[int, int]:
            drop_subscriptions = delete(subscription).where(subscription.c.consumer_group == group)
            drop_rows = delete(message).where(
                message.c.consumer_group == group,
                message.c.status.in_(("pending", "claimed")),
            )
            if targets is not None:
                drop_subscriptions = drop_subscriptions.where(
                    subscription.c.target.in_(list(targets))
                )
                drop_rows = drop_rows.where(message.c.target.in_(list(targets)))
            subscriptions = await conn.execute(drop_subscriptions)
            rows = await conn.execute(drop_rows)
            return _rowcount(subscriptions), _rowcount(rows)

        removed: tuple[int, int] = await self._write(op)
        return removed

    @_on_owning_loop
    async def list_dead_letters(
        self, *, after: tuple[datetime, str] | None = None, limit: int = 100
    ) -> list[DeadLetter]:
        """One page of dead-lettered deliveries, oldest first by ``(created_at, id)``.

        ``after`` is the ``cursor`` of the last entry of the previous page.
        Every entry is one group's delivery of a message, so a message that
        died for two groups is listed twice. Expects the broker tables to
        exist; it never creates them.
        """
        from sqlalchemy import and_, or_, select

        _, _, message = broker_schema()

        async def op(conn: Any) -> list[DeadLetter]:
            stmt = (
                select(message)
                .where(message.c.status == "dead")
                .order_by(message.c.created_at, message.c.id)
                .limit(limit)
            )
            if after is not None:
                created_at, row_id = after
                stmt = stmt.where(
                    or_(
                        message.c.created_at > created_at,
                        and_(message.c.created_at == created_at, message.c.id > row_id),
                    )
                )
            return [
                DeadLetter(
                    id=str(row["id"]),
                    target=str(row["target"]),
                    consumer_group=str(row["consumer_group"]),
                    event_type=row["event_type"],
                    attempts=int(row["attempts"]),
                    last_error=row["last_error"],
                    created_at=row["created_at"],
                )
                for row in (await conn.execute(stmt)).mappings()
            ]

        page: list[DeadLetter] = await self._write(op)
        return page

    @_on_owning_loop
    async def retry_dead_letters(self) -> int:
        """Make every dead-lettered delivery claimable again; return how many.

        Each row is already addressed to one consumer group, so only that
        group's consumer receives it again; rows other groups completed are
        not touched. Attempts and the recorded error are cleared. Expects the
        broker tables to exist; it never creates them.
        """
        from sqlalchemy import update

        _, _, message = broker_schema()

        async def op(conn: Any) -> int:
            result = await conn.execute(
                update(message)
                .where(message.c.status == "dead")
                .values(
                    status="pending",
                    attempts=0,
                    last_error=None,
                    available_at=await self._now(conn),
                    claimed_at=None,
                    claimed_by=None,
                    dispatch_started=False,
                )
            )
            return _rowcount(result)

        count: int = await self._write(op)
        return count


# ---------------------------------------------------------------------------
# Consumer implementation
# ---------------------------------------------------------------------------


class DatabaseConsumer(PollingConsumer):
    """Compatibility wrapper over the shared durable polling lifecycle."""

    def __init__(
        self,
        *,
        broker: DatabaseBroker,
        bus: Any,
        serializer: Any,
        consumer_name: str,
        group: str,
        targets: list[str] | tuple[str, ...],
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY,
        max_attempts: int = _MAX_DELIVERY_ATTEMPTS,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
        prune_interval_s: float | None = None,
        retention_age_seconds: float | None = None,
        retention_count: int | None = None,
    ) -> None:
        super().__init__(
            broker=broker,
            bus=bus,
            serializer=serializer,
            consumer_name=consumer_name,
            group=group,
            targets=targets,
            poll_interval_s=_positive_finite_float(poll_interval_s, "poll_interval_s"),
            batch_size=_positive_int(batch_size, "batch_size"),
            dispatch_concurrency=_positive_int(dispatch_concurrency, "dispatch_concurrency"),
            max_attempts=_positive_int(max_attempts, "max_attempts"),
            reclaim_stale_seconds=_positive_finite_float(
                reclaim_stale_seconds,
                "reclaim_stale_seconds",
            ),
            prune_interval_s=(
                None
                if prune_interval_s is None
                else _non_negative_finite_float(prune_interval_s, "prune_interval_s")
            ),
            retention_age_seconds=(
                None
                if retention_age_seconds is None
                else _positive_finite_float(retention_age_seconds, "retention_age_seconds")
            ),
            retention_count=(
                None
                if retention_count is None
                else _non_negative_int(retention_count, "retention_count")
            ),
            logger=logger,
            scheme=_DB_SCHEME,
            # Every empty poll here is a network round-trip and a write
            # transaction (SELECT ... FOR UPDATE SKIP LOCKED), so a fleet idling
            # at the 20ms default poll interval spends thousands of claim
            # transactions per second finding nothing. Consecutive empty polls
            # widen the wait up to the shared cap and the first non-empty poll
            # snaps it straight back, so the cost is bounded pickup latency on
            # the first message after an idle stretch.
            idle_backoff=True,
        )


# ---------------------------------------------------------------------------
# Plugin registration hooks
# ---------------------------------------------------------------------------


@hookimpl
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    """Register the ``database`` scheme when the app selects it.

    Connection settings resolve env > ``[tool.modulith.broker_options]``
    subtable. No-op unless ``broker == "database"``. Constructing the async
    engine here does not open a connection (SQLAlchemy engines connect
    lazily on first use), so this is safe to run unconditionally at
    bootstrap once the scheme is selected.
    """
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _DB_SCHEME:
        return

    opts = cfg.broker_options or {}
    url: Any = _broker_opt(opts, "url", "URL") or _broker_opt(opts, "dsn", "DSN")
    if not url:
        # Production must never invent a per-host SQLite file — cross-host
        # delivery would silently split. Dev/test get an embedded file.
        if cfg.production:
            raise ConfigurationError(
                "broker='database' in production requires an explicit URL "
                "(set [tool.modulith.broker_options].url or MODULITH_BROKER_URL). "
                "An implicit embedded SQLite file is not durable across hosts."
            )
        try:
            from sqlalchemy.engine import URL
        except ImportError as exc:
            raise ConfigurationError(
                "The 'database' broker requires SQLAlchemy (async) plus a DB "
                "driver. Install the extra: pip install 'modupy[database]'"
            ) from exc

        # Keep the URL object (do NOT str() it): reparsing str(URL) truncates a
        # filesystem path that contains '?'.
        db_path = resolve_state_file(
            cfg.package,
            filename=DEFAULT_BROKER_DB_FILENAME,
            state_dir=_broker_opt(opts, "state_dir", "STATE_DIR"),
            label="database broker SQLite file",
        )
        url = URL.create("sqlite+aiosqlite", database=str(db_path))
        logger.warning(
            "database broker has no url configured — defaulting to embedded "
            "SQLite at %s (override with [tool.modulith.broker_options].url "
            "or MODULITH_BROKER_URL)",
            db_path,
        )
    if cfg.topology == "processes" and _is_sqlite_memory_url(url):
        raise ConfigurationError(
            "the database broker cannot use in-memory SQLite with "
            "topology='processes'; each worker would have an isolated database. "
            "Use a file-backed SQLite URL or a server database."
        )
    completion_mode = _option_or_default(
        _broker_opt(opts, "completion_mode", "COMPLETION_MODE"),
        _DEFAULT_COMPLETION_MODE,
    )
    no_subscriber_policy = _option_or_default(
        _broker_opt(opts, "no_subscriber_policy", "NO_SUBSCRIBER_POLICY"),
        _DEFAULT_NO_SUBSCRIBER_POLICY,
    )
    orphan_replay_policy = _option_or_default(
        _broker_opt(opts, "orphan_replay_policy", "ORPHAN_REPLAY_POLICY"),
        _DEFAULT_ORPHAN_REPLAY_POLICY,
    )
    expected_consumer_groups = _parse_expected_consumer_groups(
        _broker_opt(
            opts,
            "expected_consumer_groups",
            "EXPECTED_CONSUMER_GROUPS",
        )
    )
    broker = DatabaseBroker(
        url=url,
        completion_mode=completion_mode,
        engine_options=opts,
        no_subscriber_policy=no_subscriber_policy,
        no_subscriber_wait_timeout_seconds=_option_or_default(
            _broker_opt(
                opts,
                "no_subscriber_wait_timeout_seconds",
                "NO_SUBSCRIBER_WAIT_TIMEOUT_SECONDS",
            ),
            _DEFAULT_NO_SUBSCRIBER_WAIT_TIMEOUT_S,
        ),
        no_subscriber_wait_poll_interval_ms=_option_or_default(
            _broker_opt(
                opts,
                "no_subscriber_wait_poll_interval_ms",
                "NO_SUBSCRIBER_WAIT_POLL_INTERVAL_MS",
            ),
            _DEFAULT_NO_SUBSCRIBER_WAIT_POLL_INTERVAL_S * 1000.0,
        ),
        orphan_replay_policy=orphan_replay_policy,
        orphan_retention_seconds=_option_or_default(
            _broker_opt(opts, "orphan_retention_seconds", "ORPHAN_RETENTION_SECONDS"),
            _DEFAULT_ORPHAN_RETENTION_S,
        ),
        expected_consumer_groups=expected_consumer_groups,
        max_payload_bytes=_option_or_default(
            _opt_int(_broker_opt(opts, "max_payload_bytes", "MAX_PAYLOAD_BYTES")),
            DEFAULT_MAX_PAYLOAD_BYTES,
        ),
    )
    registry.register(_DB_SCHEME, broker)
    logger.info("registered database broker (completion_mode=%s)", completion_mode)


def _broker_opt(opts: dict[str, Any], key: str, env_suffix: str) -> Any:
    """Resolve one broker setting: ``MODULITH_BROKER_<ENV_SUFFIX>`` env var
    (highest priority — for deployment-time values like the URL/DSN) else the
    ``broker_options`` subtable value else None. A blank env var ('') counts as
    unset (templated deployments commonly render ``MODULITH_X=``)."""
    env_value = os.environ.get(f"MODULITH_BROKER_{env_suffix}")
    if env_value:
        return env_value
    return opts.get(key)


def _option_or_default(value: Any, default: Any) -> Any:
    return default if value is None else value


def _parse_expected_consumer_groups(value: Any) -> dict[str, list[str]]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                "expected_consumer_groups must be a JSON object when set through "
                "MODULITH_BROKER_EXPECTED_CONSUMER_GROUPS"
            ) from exc
    return _validate_expected_consumer_groups(value)


def _opt_float(value: Any) -> float | None:
    """Coerce a broker-option value (typed ``Any`` from TOML/env) to a float,
    or None when absent. A non-numeric value (e.g. a typo'd env var) raises
    ``ConfigurationError`` rather than a bare ``ValueError`` so the operator
    sees a broker-config error, not an opaque traceback."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConfigurationError(f"broker option expected a number, got {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"broker option expected a number, got {value!r}") from exc


def _opt_int(value: Any) -> int | None:
    """Coerce a broker-option value to an int, or None when absent. Same
    ``ConfigurationError``-on-bad-value contract as ``_opt_float`` (note
    ``'10.0'`` is rejected — use a bare integer)."""
    if value is None:
        return None
    if type(value) is int:
        return value
    if type(value) is not str:
        raise ConfigurationError(f"broker option expected an integer, got {value!r}")
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"broker option expected an integer, got {value!r}") from exc


def _make_db_consumer(spec: ConsumerSpec) -> Consumer:
    """Build a database consumer for one worker module from ``spec``.

    Wraps ``DatabaseConsumer`` around the ``DatabaseBroker`` already
    registered on the producer side (pulled from ``spec.broker_registry`` by
    scheme), whose consumer-side methods the loop drives. One engine serves
    both halves — no second engine is opened here.

    Consumer-loop cadence (``poll_interval_ms`` / ``batch_size`` /
    ``dispatch_concurrency``) and prune
    retention are read from ``[tool.modulith.broker_options]`` (env-overridable
    via ``MODULITH_BROKER_*``) so they are configurable per deployment.
    """
    from ..runtime import _runtime

    broker = cast(DatabaseBroker, spec.broker_registry.get(spec.scheme))
    cfg = _runtime.config
    opts = (cfg.broker_options if cfg is not None else None) or {}
    poll_interval_ms = _opt_float(_broker_opt(opts, "poll_interval_ms", "POLL_INTERVAL_MS"))
    batch_size = _opt_int(_broker_opt(opts, "batch_size", "BATCH_SIZE"))
    dispatch_concurrency = _opt_int(
        _broker_opt(opts, "dispatch_concurrency", "DISPATCH_CONCURRENCY")
    )
    reclaim_stale_seconds = _opt_float(
        _broker_opt(opts, "reclaim_stale_seconds", "RECLAIM_STALE_SECONDS")
    )
    max_delivery_attempts = _opt_int(
        _broker_opt(opts, "max_delivery_attempts", "MAX_DELIVERY_ATTEMPTS")
    )
    retention_age_seconds = _opt_float(
        _broker_opt(opts, "retention_age_seconds", "RETENTION_AGE_SECONDS")
    )
    return DatabaseConsumer(
        broker=broker,
        bus=spec.bus,
        serializer=spec.serializer,
        consumer_name=spec.consumer_name,
        group=spec.group,
        targets=list(spec.targets),
        poll_interval_s=(
            poll_interval_ms / 1000.0 if poll_interval_ms is not None else _DEFAULT_POLL_INTERVAL_S
        ),
        batch_size=batch_size if batch_size is not None else _DEFAULT_BATCH_SIZE,
        dispatch_concurrency=(
            dispatch_concurrency
            if dispatch_concurrency is not None
            else _DEFAULT_DISPATCH_CONCURRENCY
        ),
        reclaim_stale_seconds=(
            reclaim_stale_seconds if reclaim_stale_seconds is not None else _DEFAULT_RECLAIM_STALE_S
        ),
        max_attempts=(
            max_delivery_attempts if max_delivery_attempts is not None else _MAX_DELIVERY_ATTEMPTS
        ),
        prune_interval_s=_opt_float(
            _broker_opt(opts, "prune_interval_seconds", "PRUNE_INTERVAL_SECONDS")
        ),
        retention_age_seconds=(
            retention_age_seconds if retention_age_seconds is not None else _DEFAULT_RETENTION_AGE_S
        ),
        retention_count=_opt_int(_broker_opt(opts, "retention_count", "RETENTION_COUNT")),
    )


@hookimpl
def modulith_register_consumers(registry: ConsumerRegistry) -> None:
    """Register the ``database`` consumer factory when the app selects it.

    The consumer-side mirror of ``modulith_register_brokers``: no-op unless
    ``broker == "database"``. The factory reuses the broker object that hook
    registered (fetched from the broker registry at build time), so no
    second engine is created.
    """
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _DB_SCHEME:
        return
    registry.register(_DB_SCHEME, _make_db_consumer)


__all__ = [
    "DatabaseBroker",
    "DatabaseConsumer",
    "NoSubscribersError",
    "broker_schema",
    "modulith_register_brokers",
    "modulith_register_consumers",
]
