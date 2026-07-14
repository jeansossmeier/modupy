"""Database-backed broker adapter (relational DB as cross-module transport).

Lets process-per-module topology use a relational database (Postgres / MySQL
/ SQLite) as the cross-module message transport, so Redis is not required.
SQLite doubles as a zero-dependency bootstrap broker (embedded file or
``:memory:``).

Distributed via the ``modulith[postgres]`` / ``modulith[test]`` extras.
Optional dependency: SQLAlchemy (async) plus a driver (asyncpg / aiomysql /
aiosqlite). Because this adapter is registered as a BUILTIN plugin (loaded at
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
  url / dsn                   SQLAlchemy URL (required — no universal default)
  completion_mode             'delete' (default, keeps the table small) |
                               'mark' (sets status='done', row stays for the
                               prune job)
  pool_size / max_overflow    connection-pool sizing (Postgres / MySQL; ignored
                               for SQLite, whose pool rejects them)
  busy_timeout_ms             SQLite only: how long a blocked writer waits for
                               the lock before SQLITE_BUSY (default 5000)
  poll_interval_ms            consumer poll cadence (default 1000)
  batch_size                  claim LIMIT per poll (default 10)
  retention_age_seconds       prune deletes terminal ('done'/'dead') rows older
                               than this many seconds
  retention_count             prune keeps only the newest N terminal rows per
                               (target, consumer_group)
  prune_interval_seconds      how often the consumer's background prune runs
                               (defaults to 300s when either retention_* is set;
                               set to 0 to disable)

Postgres LISTEN/NOTIFY (a low-latency alternative to polling) is a planned
opt-in and not yet implemented — the transport polls on every dialect.

Fan-out mechanism (how the producer learns the consumer groups): the
producer process (module-isolated) never imports consumer modules, so
consumers self-register their subscriptions in a persistent
``broker_subscription`` table at ``DatabaseConsumer.start()`` time (upsert,
idempotent — never auto-deleted). ``DatabaseBroker.publish()`` looks up every
group subscribed to the target and inserts one ``broker_message`` row per
group in a single transaction. Zero subscribers means zero rows (no remote
consumer has ever registered interest in this target yet) — an
at-least-once nuance documented as acceptable because listeners are
idempotent regardless (same posture as the Redis adapter's startup-race gap).

Claim path (competing consumers): ``FOR UPDATE SKIP LOCKED`` on Postgres/
MySQL lets concurrent consumers partition the backlog instead of blocking or
double-claiming (see ``_supports_skip_locked``). SQLite has no row locking at
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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from modulith import (
    BrokerRegistry,
    ConfigurationError,
    Consumer,
    ConsumerRegistry,
    ConsumerSpec,
    hookimpl,
)

logger = logging.getLogger("modulith.adapters.db")

_DB_SCHEME = "database"
_DEFAULT_COMPLETION_MODE = "delete"
_DEFAULT_BATCH_SIZE = 10
_DEFAULT_POLL_INTERVAL_S = 1.0

# How often the consumer's background prune runs when retention is configured
# but ``prune_interval_seconds`` was not set explicitly. Deliberately coarse:
# prune is table maintenance, not on the delivery hot path.
_DEFAULT_PRUNE_INTERVAL_S = 300.0

# The two terminal statuses prune is allowed to delete. 'pending'/'claimed'
# rows are undelivered work and must NEVER be pruned (that would be message
# loss), so every prune query is filtered to exactly these.
_TERMINAL_STATUSES = ("done", "dead")

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
_SKIP_LOCKED_DIALECTS = frozenset({"postgresql", "mysql"})

# SQLite ``busy_timeout`` (ms) applied to every connection when none is
# configured: how long a blocked writer waits for the lock before raising
# SQLITE_BUSY. Makes file-backed SQLite usable as a best-effort multi-process
# broker instead of erroring on the first contended write.
_DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000

# App-level retry budget for a transient SQLite "database is locked" error.
# ``busy_timeout`` handles the common wait-for-lock case, but SQLite returns
# SQLITE_BUSY *immediately* (ignoring busy_timeout) when a transaction upgrades
# a read lock to a write lock under contention — exactly what claim_batch's
# SELECT-then-UPDATE does — so a bounded application retry is still needed.
_SQLITE_BUSY_MAX_RETRIES = 4


def _backoff_delay(attempt: int) -> float:
    """Capped exponential backoff for the ``attempt``'th consecutive failure
    (1-indexed): 0.05s, 0.1s, 0.2s, ... capped at 5s."""
    exponent = min(attempt - 1, _BACKOFF_MAX_EXPONENT)
    # ``2.0 ** exponent`` (float base), not ``2**exponent``: typeshed types
    # ``int.__pow__`` as returning ``Any`` (a negative exponent would yield a
    # float at runtime), which would otherwise leak Any through this
    # function's declared ``-> float`` return.
    return min(_BACKOFF_BASE_S * (2.0**exponent), _BACKOFF_CAP_S)


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


def _supports_skip_locked(engine: Any) -> bool:
    """True when ``engine``'s dialect supports ``FOR UPDATE SKIP LOCKED``.

    Postgres and MySQL (8+) support it; SQLite has no row locking whatsoever
    and raises a CompileError if the clause is issued, so the claim query
    must gate on this before adding ``.with_for_update(skip_locked=True)``
    (exactly the pattern ``postgres_outbox.py``'s ``_supports_skip_locked``
    uses, generalized to the two lockable dialects instead of one).
    """
    return engine.dialect.name in _SKIP_LOCKED_DIALECTS


def _is_sqlite_url(url: str) -> bool:
    """True when ``url`` names the SQLite backend (any driver), resolved via
    ``make_url`` rather than string-matching so ``sqlite+aiosqlite://`` and a
    bare ``sqlite://`` both classify correctly."""
    from sqlalchemy.engine import make_url

    return make_url(url).get_backend_name() == "sqlite"


def _install_sqlite_pragmas(engine: Any, busy_timeout_ms: int) -> None:
    """Set WAL + ``busy_timeout`` on every new SQLite connection.

    WAL lets one writer and concurrent readers coexist (a plain rollback
    journal serializes them); ``busy_timeout`` makes a blocked writer wait
    rather than erroring immediately — together they make file-backed SQLite a
    workable best-effort multi-process broker. No-op-safe on ``:memory:`` (WAL
    silently downgrades to 'memory'). Installed on the ``sync_engine``'s
    'connect' event — the documented way to run PRAGMAs on an aiosqlite async
    engine (the event fires with the raw DBAPI connection, whose cursor runs
    synchronously by bridging to aiosqlite's connection thread).
    """
    from sqlalchemy import event

    timeout = int(busy_timeout_ms)

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            # ``timeout`` is an int -> safe to interpolate (PRAGMA takes no bind
            # params); never a user string.
            cursor.execute(f"PRAGMA busy_timeout={timeout}")
        finally:
            cursor.close()


def _create_engine(url: str, opts: dict[str, Any]) -> Any:
    """Build the async engine, applying dialect-appropriate options from
    ``opts`` (the ``[tool.modulith.broker_options]`` subtable).

    - Postgres / MySQL: ``pool_size`` / ``max_overflow`` size the connection
      pool (both optional; omitted -> SQLAlchemy's QueuePool defaults).
    - SQLite: pool-sizing kwargs are NOT passed (SQLite's pool rejects them);
      instead WAL + ``busy_timeout`` are installed per connection.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    kwargs: dict[str, Any] = {}
    sqlite = _is_sqlite_url(url)
    if not sqlite:
        pool_size = _opt_int(opts.get("pool_size"))
        if pool_size is not None:
            kwargs["pool_size"] = pool_size
        max_overflow = _opt_int(opts.get("max_overflow"))
        if max_overflow is not None:
            kwargs["max_overflow"] = max_overflow
    engine = create_async_engine(url, **kwargs)
    if sqlite:
        busy_timeout_ms = _opt_int(opts.get("busy_timeout_ms")) or _DEFAULT_SQLITE_BUSY_TIMEOUT_MS
        _install_sqlite_pragmas(engine, busy_timeout_ms)
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
        Column,
        DateTime,
        Index,
        Integer,
        LargeBinary,
        MetaData,
        String,
        Table,
        Text,
    )

    metadata = MetaData()

    subscription = Table(
        "broker_subscription",
        metadata,
        Column("target", String, primary_key=True),
        Column("consumer_group", String, primary_key=True),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )

    message = Table(
        "broker_message",
        metadata,
        Column("id", String, primary_key=True),
        Column("target", String, nullable=False),
        Column("consumer_group", String, nullable=False),
        # Nullable at the DB level: a publish() call with no "event_type"
        # header (or a directly-inserted test row) produces a poison message
        # that the consumer dead-letters on first claim, rather than a schema
        # violation at insert time.
        Column("event_type", String, nullable=True),
        Column("payload", LargeBinary, nullable=False),
        Column("headers", Text, nullable=True),
        Column("status", String, nullable=False, default="pending", server_default="pending"),
        Column("attempts", Integer, nullable=False, default=0, server_default="0"),
        Column("available_at", DateTime(timezone=True), nullable=False),
        Column("claimed_at", DateTime(timezone=True), nullable=True),
        Column("claimed_by", String, nullable=True),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("last_error", Text, nullable=True),
        Index("ix_broker_message_claim", "consumer_group", "status", "available_at"),
        Index("ix_broker_message_prune", "status", "created_at"),
        Index("ix_broker_message_target", "target"),
    )

    _metadata_cache = metadata
    _subscription_table_cache = subscription
    _message_table_cache = message
    return metadata, subscription, message


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
        url: str | None = None,
        *,
        engine: Any | None = None,
        completion_mode: str = _DEFAULT_COMPLETION_MODE,
        engine_options: dict[str, Any] | None = None,
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
            self._engine = _create_engine(url, engine_options or {})
        self._completion_mode = completion_mode
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    @property
    def engine(self) -> Any:
        return self._engine

    def _schema_is_ready(self) -> bool:
        """Indirection over ``self._schema_ready`` so the double-checked-lock
        re-read below isn't statically narrowed to a constant by mypy (the
        attribute genuinely can change while a concurrent caller awaits the
        lock — a method call, unlike a bare attribute expression, isn't
        narrowed across the ``async with`` the second check sits inside)."""
        return self._schema_ready

    async def _ensure_schema(self) -> None:
        """Create the broker tables if absent — idempotent, cheap after the
        first call (short-circuits on the in-process flag)."""
        if self._schema_is_ready():
            return
        async with self._schema_lock:
            if self._schema_is_ready():
                return
            metadata, _, _ = broker_schema()
            async with self._engine.begin() as conn:
                await conn.run_sync(metadata.create_all)
            self._schema_ready = True

    async def _write(self, operation: Callable[[Any], Awaitable[Any]]) -> Any:
        """Run ``operation(conn)`` inside one transaction, retrying a transient
        SQLite lock up to ``_SQLITE_BUSY_MAX_RETRIES`` times with backoff.

        Each attempt is a fresh transaction (``engine.begin()`` rolls back on
        the raised lock error), so a retry re-runs the whole operation from a
        clean state — safe for the SELECT-then-write claim path. Non-lock
        errors propagate immediately; ``CancelledError`` is never swallowed.
        On Postgres / MySQL ``_is_sqlite_locked`` never matches, so this is a
        plain single-attempt transaction there.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                async with self._engine.begin() as conn:
                    return await operation(conn)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= _SQLITE_BUSY_MAX_RETRIES or not _is_sqlite_locked(exc):
                    raise
                logger.warning(
                    "SQLite busy on write (attempt %d/%d) — retrying",
                    attempt,
                    _SQLITE_BUSY_MAX_RETRIES,
                )
                await asyncio.sleep(_backoff_delay(attempt))

    # ----- producer side -----------------------------------------------

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Fan out on write: one ``broker_message`` row per subscribed group.

        Looks up every ``consumer_group`` subscribed to ``target`` (populated
        by ``DatabaseConsumer.start()``) and inserts one row per group in a
        single transaction. Zero subscribers -> zero rows: no remote consumer
        has ever registered interest in this target yet.
        """
        await self._ensure_schema()
        from sqlalchemy import select

        _, subscription, message = broker_schema()
        event_type = (headers or {}).get("event_type", "")
        headers_blob = json.dumps(headers) if headers else None
        now = datetime.now(UTC)

        async def op(conn: Any) -> int:
            result = await conn.execute(
                select(subscription.c.consumer_group).where(subscription.c.target == target)
            )
            groups = [row[0] for row in result]
            if not groups:
                return 0
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

        count = await self._write(op)
        if count:
            logger.debug("publish target=%s fanned out to %d group(s)", target, count)
        else:
            logger.debug("publish target=%s has no subscribers — 0 rows written", target)

    async def close(self) -> None:
        """Dispose the engine (and its connection pool). Idempotent-friendly
        (close-after-close does not raise — ``AsyncEngine.dispose`` itself
        tolerates repeated calls)."""
        await self._engine.dispose()

    # ----- consumer side -------------------------------------------------

    async def subscribe(self, targets: list[str] | tuple[str, ...], group: str) -> None:
        """Upsert ``(target, group)`` subscription rows — idempotent AND
        concurrency-safe.

        Uses the dialect-native upsert (Postgres/SQLite ``ON CONFLICT DO
        UPDATE``, MySQL ``ON DUPLICATE KEY UPDATE``) so two workers of the same
        replicated module registering the same ``(target, consumer_group)`` at
        once can't collide on the PK. A check-then-insert races here: both
        workers see no row, both insert, and one dies with IntegrityError —
        exactly the first-deploy hazard for a replicated module. Refreshes
        ``updated_at`` on conflict so the row doubles as a liveness marker.
        """
        if not targets:
            return
        await self._ensure_schema()
        _, subscription, _ = broker_schema()
        now = datetime.now(UTC)
        rows = [{"target": t, "consumer_group": group, "updated_at": now} for t in targets]
        stmt = self._upsert_subscription(subscription, rows)

        async def op(conn: Any) -> None:
            await conn.execute(stmt)

        await self._write(op)

    def _upsert_subscription(self, subscription: Any, rows: list[dict[str, Any]]) -> Any:
        """Build the dialect-native subscription upsert statement.

        Postgres/SQLite: ``INSERT ... ON CONFLICT (target, consumer_group) DO
        UPDATE SET updated_at=excluded.updated_at``. MySQL: ``INSERT ... ON
        DUPLICATE KEY UPDATE``. Any other dialect (unreachable for the three
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
        if name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            mysql_stmt = mysql_insert(subscription).values(rows)
            return mysql_stmt.on_duplicate_key_update(updated_at=mysql_stmt.inserted.updated_at)
        from sqlalchemy import insert

        return insert(subscription).values(rows)

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
    ) -> list[dict[str, Any]]:
        """Claim up to ``batch_size`` due rows for ``group``.

        Picks up both freshly ``pending`` rows AND rows stuck in ``claimed``
        whose ``claimed_at`` is older than ``reclaim_stale_seconds`` — the
        latter are orphans left by a consumer that crashed between claim and
        ack, so reclaiming them is what preserves at-least-once delivery across
        a consumer crash (the DB analogue of the Redis adapter's XAUTOCLAIM
        reclaim). A reclaim re-stamps ``claimed_at``/``claimed_by``; it does NOT
        bump ``attempts`` (a crash is not a dispatch failure), matching the
        Redis reclaim path, so a poison row still dead-letters via the
        dispatch-failure cap rather than being reclaimed forever.

        Postgres/MySQL: ``FOR UPDATE SKIP LOCKED`` lets concurrent consumers
        partition the backlog instead of blocking or double-claiming. SQLite
        (no row locking) degrades to a plain claim inside one transaction —
        see ``_supports_skip_locked`` and the module docstring.
        """
        await self._ensure_schema()
        from sqlalchemy import and_, or_, select, update

        _, _, message = broker_schema()
        now = datetime.now(UTC)
        stale_cutoff = now - timedelta(seconds=reclaim_stale_seconds)

        async def op(conn: Any) -> list[dict[str, Any]]:
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
            if _supports_skip_locked(self._engine):
                stmt = stmt.with_for_update(skip_locked=True)
            result = await conn.execute(stmt)
            claimed_rows = [dict(row._mapping) for row in result]
            ids = [row["id"] for row in claimed_rows]
            if ids:
                await conn.execute(
                    update(message)
                    .where(message.c.id.in_(ids))
                    .values(status="claimed", claimed_at=now, claimed_by=consumer_name)
                )
            return claimed_rows

        rows: list[dict[str, Any]] = await self._write(op)
        return rows

    async def ack(self, row_id: str) -> None:
        """Complete a row: delete it (default) or mark it 'done' (mark mode,
        which keeps the row for the prune job — see ``prune``)."""
        from sqlalchemy import delete, update

        _, _, message = broker_schema()

        async def op(conn: Any) -> None:
            if self._completion_mode == "delete":
                await conn.execute(delete(message).where(message.c.id == row_id))
            else:
                await conn.execute(
                    update(message).where(message.c.id == row_id).values(status="done")
                )

        await self._write(op)

    async def fail(self, row_id: str, error: str, *, attempts: int, max_attempts: int) -> None:
        """Record a dispatch failure: attempts++ with backoff, staying
        'pending' until ``max_attempts`` is reached, then 'dead'."""
        from sqlalchemy import update

        _, _, message = broker_schema()
        new_attempts = attempts + 1
        if new_attempts >= max_attempts:
            status = "dead"
            available_at = datetime.now(UTC)
        else:
            status = "pending"
            available_at = datetime.now(UTC) + timedelta(seconds=_backoff_delay(new_attempts))

        async def op(conn: Any) -> None:
            await conn.execute(
                update(message)
                .where(message.c.id == row_id)
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

    async def dead_letter(self, row_id: str, error: str) -> None:
        """Mark a poison row 'dead' immediately (undeserializable payload or
        missing ``event_type`` — retrying can never succeed)."""
        from sqlalchemy import update

        _, _, message = broker_schema()

        async def op(conn: Any) -> None:
            await conn.execute(
                update(message)
                .where(message.c.id == row_id)
                .values(status="dead", last_error=error)
            )

        await self._write(op)

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

        Both are optional; with neither set this is a no-op returning 0. When
        both are set the age prune runs first and the count prune then applies
        to what remains — both in one transaction. **Only terminal rows are
        ever deleted** — 'pending'/'claimed' rows are undelivered work and are
        never touched, so prune can never cause message loss. Returns the total
        number of rows deleted (best-effort per ``_rowcount``).
        """
        if retention_age_seconds is None and retention_count is None:
            return 0
        await self._ensure_schema()
        from sqlalchemy import delete, func, select

        _, _, message = broker_schema()
        terminal = message.c.status.in_(_TERMINAL_STATUSES)

        async def op(conn: Any) -> int:
            deleted_local = 0
            if retention_age_seconds is not None:
                cutoff = datetime.now(UTC) - timedelta(seconds=retention_age_seconds)
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
                result = await conn.execute(delete(message).where(message.c.id.in_(doomed)))
                deleted_local += _rowcount(result)
            return deleted_local

        deleted: int = await self._write(op)
        if deleted:
            logger.debug("prune deleted %d terminal row(s)", deleted)
        return deleted


# ---------------------------------------------------------------------------
# Consumer implementation
# ---------------------------------------------------------------------------


class DatabaseConsumer:
    """Poll/claim/ack loop for the ``database`` broker scheme.

    Mirrors ``modulith._consumer.BrokerConsumer``'s resilience posture: a
    background ``asyncio.Task`` poll loop, capped exponential backoff on
    claim failures, poison messages dead-lettered immediately, dispatch
    failures retried with backoff up to an attempt cap, and a ``stop()``
    that cancels the loop and never raises.
    """

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
        max_attempts: int = _MAX_DELIVERY_ATTEMPTS,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
        prune_interval_s: float | None = None,
        retention_age_seconds: float | None = None,
        retention_count: int | None = None,
    ) -> None:
        self._broker = broker
        self._bus = bus
        self._serializer = serializer
        self._consumer_name = consumer_name
        self._group = group
        self._targets = list(targets)
        self._poll_interval_s = poll_interval_s
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._reclaim_stale_seconds = reclaim_stale_seconds
        self._prune_interval_s = prune_interval_s
        self._retention_age_seconds = retention_age_seconds
        self._retention_count = retention_count
        self._task: asyncio.Task[None] | None = None
        self._prune_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._consecutive_failures = 0

    async def start(self) -> None:
        """Upsert subscriptions, then launch the poll loop.

        No-op (no background task) when the worker consumes nothing — a leaf
        module with no @listener has no targets to claim for.
        """
        if not self._targets:
            logger.debug(
                "consumer %r has no subscribed targets — not starting", self._consumer_name
            )
            return
        await self._broker.subscribe(self._targets, self._group)
        self._task = asyncio.create_task(self._run())
        if self._prune_enabled():
            self._prune_task = asyncio.create_task(self._prune_loop())
        logger.info(
            "db consumer %r (group %r) subscribed to %d target(s)%s",
            self._consumer_name,
            self._group,
            len(self._targets),
            " (prune on)" if self._prune_enabled() else "",
        )

    def _prune_enabled(self) -> bool:
        """Prune runs when a retention knob is set and the interval is not
        explicitly disabled (``prune_interval_s == 0``)."""
        if self._prune_interval_s is not None and self._prune_interval_s <= 0:
            return False
        return self._retention_age_seconds is not None or self._retention_count is not None

    def _should_stop(self) -> bool:
        """Indirection over ``self._stopping`` so the in-loop re-check isn't
        statically narrowed to a constant by mypy — the flag genuinely can
        flip to True (via ``stop()`` from another task) while this task is
        between claims, and a method call, unlike the bare attribute
        expression checked at the top of ``_run``'s ``while`` loop, isn't
        narrowed by a later check in the same iteration."""
        return self._stopping

    async def stop(self) -> None:
        """Cancel the poll loop (and the prune loop, if running) and wait for
        both to unwind. Never raises."""
        self._stopping = True
        await self._cancel(self._task, "poll")
        self._task = None
        await self._cancel(self._prune_task, "prune")
        self._prune_task = None

    async def _cancel(self, task: asyncio.Task[None] | None, label: str) -> None:
        """Cancel one background task and swallow its unwind. Never raises."""
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception(
                "consumer %r %s task had already died with an unexpected error",
                self._consumer_name,
                label,
            )

    async def _run(self) -> None:
        while True:
            if self._stopping:
                return
            try:
                rows = await self._broker.claim_batch(
                    self._group,
                    batch_size=self._batch_size,
                    consumer_name=self._consumer_name,
                    reclaim_stale_seconds=self._reclaim_stale_seconds,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("claim failed for group %s", self._group)
                await self._backoff_after_failure()
                continue
            self._consecutive_failures = 0
            if not rows:
                await asyncio.sleep(self._poll_interval_s)
                continue
            for row in rows:
                if self._should_stop():
                    return
                try:
                    await self._dispatch_one(row)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Safety net mirroring BrokerConsumer._run: any escaped
                    # per-row exception must not kill this task permanently.
                    logger.exception(
                        "dispatch_one crashed for row %s — loop continues", row.get("id")
                    )

    async def _backoff_after_failure(self) -> None:
        self._consecutive_failures += 1
        await asyncio.sleep(_backoff_delay(self._consecutive_failures))

    async def _prune_loop(self) -> None:
        """Background retention sweep: every ``prune_interval_s`` (default
        ``_DEFAULT_PRUNE_INTERVAL_S`` when unset), delete terminal rows past the
        configured retention. Sleeps FIRST so many workers starting at once
        don't all prune simultaneously. A prune failure is logged and the loop
        continues — retention is best-effort maintenance, never fatal.

        Redundant-but-idempotent across replicas: every consuming worker runs
        this, and ``prune`` deletes globally, so extra runs are cheap no-ops
        rather than duplicated deletes.
        """
        interval = self._prune_interval_s or _DEFAULT_PRUNE_INTERVAL_S
        while True:
            await asyncio.sleep(interval)
            if self._should_stop():
                return
            try:
                await self._broker.prune(
                    retention_age_seconds=self._retention_age_seconds,
                    retention_count=self._retention_count,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("prune failed for group %s — loop continues", self._group)

    async def _dispatch_one(self, row: dict[str, Any]) -> None:
        """Deserialize one claimed row and dispatch it to local listeners.

        Acks on success. Missing ``event_type`` or an undeserializable
        payload is poison -> dead-lettered immediately. A dispatch failure
        increments the attempt count (staying 'pending' with backoff) until
        the cap is reached, then dead-letters.
        """
        row_id = cast(str, row["id"])
        event_type = row.get("event_type")
        payload = cast(bytes, row["payload"])

        if not event_type:
            logger.warning(
                "message %s on %s missing event_type — dead-lettering", row_id, row.get("target")
            )
            await self._broker.dead_letter(row_id, "missing event_type")
            return

        try:
            event = self._serializer.deserialize(payload, event_type)
        except Exception as exc:
            logger.exception("undeserializable message %s — dead-lettering", row_id)
            await self._broker.dead_letter(row_id, f"deserialize failed: {exc}")
            return

        try:
            await self._bus.publish(event)
        except Exception as exc:
            attempts = cast(int, row.get("attempts", 0))
            logger.warning("dispatch failed for %s (attempt %d) — %s", row_id, attempts + 1, exc)
            await self._broker.fail(
                row_id, str(exc), attempts=attempts, max_attempts=self._max_attempts
            )
            return

        try:
            await self._broker.ack(row_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ack failed for %s — message stays claimed", row_id)


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
    url = _broker_opt(opts, "url", "URL") or _broker_opt(opts, "dsn", "DSN")
    completion_mode = (
        _broker_opt(opts, "completion_mode", "COMPLETION_MODE") or _DEFAULT_COMPLETION_MODE
    )
    broker = DatabaseBroker(url=url, completion_mode=completion_mode, engine_options=opts)
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


def _opt_float(value: Any) -> float | None:
    """Coerce a broker-option value (typed ``Any`` from TOML/env) to a float,
    or None when absent."""
    return None if value is None else float(value)


def _opt_int(value: Any) -> int | None:
    """Coerce a broker-option value to an int, or None when absent."""
    return None if value is None else int(value)


def _make_db_consumer(spec: ConsumerSpec) -> Consumer:
    """Build a database consumer for one worker module from ``spec``.

    Wraps ``DatabaseConsumer`` around the ``DatabaseBroker`` already
    registered on the producer side (pulled from ``spec.broker_registry`` by
    scheme), whose consumer-side methods the loop drives. One engine serves
    both halves — no second engine is opened here.

    Consumer-loop cadence (``poll_interval_ms`` / ``batch_size``) and prune
    retention are read from ``[tool.modulith.broker_options]`` (env-overridable
    via ``MODULITH_BROKER_*``) so they are configurable per deployment.
    """
    from ..runtime import _runtime

    broker = cast(DatabaseBroker, spec.broker_registry.get(spec.scheme))
    cfg = _runtime.config
    opts = (cfg.broker_options if cfg is not None else None) or {}
    poll_interval_ms = _opt_float(_broker_opt(opts, "poll_interval_ms", "POLL_INTERVAL_MS"))
    batch_size = _opt_int(_broker_opt(opts, "batch_size", "BATCH_SIZE"))
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
        prune_interval_s=_opt_float(
            _broker_opt(opts, "prune_interval_seconds", "PRUNE_INTERVAL_SECONDS")
        ),
        retention_age_seconds=_opt_float(
            _broker_opt(opts, "retention_age_seconds", "RETENTION_AGE_SECONDS")
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
    "broker_schema",
    "modulith_register_brokers",
    "modulith_register_consumers",
]
