"""Publication, subscription, and retained-replay SQLite operations."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from ..config import ConfigurationError
from ._shm_schema import immediate_transaction
from ._shm_types import PublishResult

logger = logging.getLogger("modulith.adapters.shm")

_PUBLISH_PRUNE_LIMIT = 100

# idx_shm_publication_expiry backs the prune scan (see
# _prune_expired_empty_publications), but it still touches every expired
# orphan row up to its LIMIT on each run. Gating it to a cadence instead of
# running it on every publish call bounds how often that cost is paid; the
# caller (SqliteQueueStore) tracks the cadence and passes prune_due.
PRUNE_EVERY_N_PUBLISHES = 100

# Publishes stop this many pages below the page count max_store_bytes allows,
# so a consumer pass usually commits inside the limit. A consumer write that
# still hits the limit is retried past it (see SqliteQueueStore._consumer_write),
# so consumers always finish the backlog they can see; the reserve keeps the
# database file within max_store_bytes in the common case. Stores under 256
# pages reserve an eighth of their pages.
CONSUMER_RESERVE_PAGES = 32

# A subscribe replay stops this many pages below the publish budget, so a
# publish that fit before the replay (a small publication touches the
# shm_publication table and its three indexes) still fits after it.
REPLAY_PUBLISH_HEADROOM_PAGES = 8


# (deliveries replayed, [(target, replayed, skipped) for each replay cut short])
_Subscribed = tuple[int, list[tuple[str, int, int]]]


class _PublishReserveReached(Exception):
    """The publish would eat into the pages reserved for consumer writes."""


def spill(
    conn: sqlite3.Connection,
    messages: list[dict[str, Any]],
    retention_seconds: float,
    max_payload_bytes: int,
) -> None:
    """Persist legacy per-group rows using SQLite-allocated sequences."""
    if not messages:
        return
    prepared = [(message, bytes(message["payload"])) for message in messages]
    for _message, payload in prepared:
        _ensure_payload_size(payload, max_payload_bytes)

    with immediate_transaction(conn):
        for message, payload in prepared:
            now = float(message.get("created_at", time.time()))
            publication, _ = _insert_publication(
                conn,
                str(message.get("id") or uuid4()),
                str(message["target"]),
                payload,
                message.get("headers"),
                str(message.get("event_type") or message["target"]),
                now,
                now + retention_seconds,
            )
            _insert_delivery(
                conn,
                publication.publication_id,
                str(message["consumer_group"]),
                now,
            )


def publish(
    conn: sqlite3.Connection,
    target: str,
    payload: bytes,
    headers: dict[str, str] | None,
    publication_id: str | None,
    retention_seconds: float,
    max_payload_bytes: int,
    max_store_bytes: int,
    prune_due: bool = True,
) -> PublishResult:
    """Atomically persist one replayable publication and current deliveries."""
    _ensure_payload_size(payload, max_payload_bytes)
    now = time.time()
    try:
        with immediate_transaction(conn):
            if prune_due:
                _prune_expired_empty_publications(conn, now, _PUBLISH_PRUNE_LIMIT)
            publication, inserted = _insert_publication(
                conn,
                publication_id or str(uuid4()),
                target,
                payload,
                headers,
                (headers or {}).get("event_type", target),
                now,
                now + retention_seconds,
            )
            if not inserted:
                return publication
            groups = conn.execute(
                """
                SELECT consumer_group FROM shm_subscription
                WHERE target=? ORDER BY consumer_group
                """,
                (target,),
            )
            for row in groups:
                _insert_delivery(
                    conn,
                    publication.publication_id,
                    str(row["consumer_group"]),
                    now,
                )
            _ensure_consumer_reserve(conn, max_store_bytes)
    except (sqlite3.Error, _PublishReserveReached) as error:
        if isinstance(error, _PublishReserveReached) or _is_store_full(error):
            raise ConfigurationError(
                "SHM SQLite store is full while publishing. A publication stays in "
                "the store while any subscribed group has not consumed it (an "
                "undelivered backlog, including a retired group that never consumes "
                "again), for orphan_retention_seconds (currently "
                f"{retention_seconds:g}) after it is written even once every group "
                'has acked it, and, under completion_mode="mark" or once '
                "dead-lettered, until retention_age_seconds after completion. The "
                "store sustains about max_store_bytes / (bytes per publication x the "
                "longest of those retentions) publications per second. Let stopped "
                "consumers drain the backlog, remove a retired group (named in the "
                "modulith run startup warning) with modulith broker drop-group, or "
                f"raise max_store_bytes (currently {max_store_bytes}) and restart every "
                "process, since each opened store keeps the limit it opened with; if "
                "the disk itself is full, free disk space."
            ) from error
        raise
    return publication


def _ensure_payload_size(payload: bytes, max_payload_bytes: int) -> None:
    payload_size = len(payload)
    if payload_size > max_payload_bytes:
        raise ConfigurationError(
            f"SHM payload is {payload_size} bytes, exceeding "
            f"max_payload_bytes={max_payload_bytes}. Reduce the payload or increase the limit."
        )


def _ensure_consumer_reserve(conn: sqlite3.Connection, max_store_bytes: int) -> None:
    if _over_publish_budget(conn, max_store_bytes):
        raise _PublishReserveReached


def _over_publish_budget(conn: sqlite3.Connection, max_store_bytes: int) -> bool:
    return _used_pages(conn) > _publish_budget_pages(conn, max_store_bytes)


def _publish_budget_pages(conn: sqlite3.Connection, max_store_bytes: int) -> int:
    # Consumer writes can raise the connection's max_page_count past the
    # configured limit, so the publish budget comes from the setting itself.
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    max_pages = max(1, max_store_bytes // page_size)
    return max_pages - min(CONSUMER_RESERVE_PAGES, max_pages // 8)


def _used_pages(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA page_count").fetchone()[0]) - int(
        conn.execute("PRAGMA freelist_count").fetchone()[0]
    )


def _is_store_full(error: sqlite3.Error) -> bool:
    error_code = getattr(error, "sqlite_errorcode", None)
    if isinstance(error_code, int) and error_code & 0xFF == sqlite3.SQLITE_FULL:
        return True
    message = str(error).lower()
    return "database or disk is full" in message or "disk is full" in message


def subscribe(
    conn: sqlite3.Connection,
    targets: list[str],
    group: str,
    max_store_bytes: int,
    consumer_write: Callable[[Callable[[], _Subscribed]], _Subscribed],
    completion_mode: str = "delete",
) -> int:
    """Reconcile one group's subscriptions and replay newly added targets.

    The subscription is always recorded: it runs as a consumer write, so a
    store too full even for that grows past max_store_bytes rather than keeping
    the group's consumer from starting. The replay adds only the retained
    publications the group lacks and stops before it would take the store past
    its publish budget, so a publish that fit before it still fits. Under
    completion_mode="mark" it also leaves room for claiming and acking the rows
    it adds, which grows them in place.
    """
    try:
        inserted, cut_short = consumer_write(
            lambda: _subscribe(conn, targets, group, max_store_bytes, completion_mode)
        )
    except sqlite3.Error as error:
        if _is_store_full(error):
            raise ConfigurationError(
                f"SHM SQLite store could not record the subscription of group {group!r} "
                "or replay retained publications to it, so the consumer does not "
                "start: the disk is full, or SQLite would not raise max_page_count "
                "past max_store_bytes (see the error's note). Free disk space and "
                "restart the process."
            ) from error
        raise
    for target, replayed, skipped in cut_short:
        logger.warning(
            "SHM store reached its publish budget while replaying target %r to group "
            "%r: replayed %d and skipped %d of the retained publications the group "
            "lacked (the oldest were replayed first). The target now counts as "
            "subscribed, so the skipped publications reach this group only through "
            "another replay; its consumer receives every publication written from now "
            "on. To replay them without losing work, first let the group's workers "
            "drain its backlog on that target: modulith broker drop-group deletes every "
            "pending and claimed delivery the group holds on the target, the replayed "
            "ones included, and a replay restores only publications still within "
            "orphan_retention_seconds of being written. Once that backlog is empty, "
            "stop the group's workers, run modulith broker drop-group %s --target %s, "
            "raise max_store_bytes (currently %d), and restart every process before the "
            "skipped publications expire, orphan_retention_seconds after they were "
            "written.",
            target,
            group,
            replayed,
            skipped,
            group,
            target,
            max_store_bytes,
        )
    return inserted


def _subscribe(
    conn: sqlite3.Connection,
    targets: list[str],
    group: str,
    max_store_bytes: int,
    completion_mode: str,
) -> _Subscribed:
    inserted = 0
    cut_short: list[tuple[str, int, int]] = []
    now = time.time()
    page_limit = _publish_budget_pages(conn, max_store_bytes) - REPLAY_PUBLISH_HEADROOM_PAGES
    requested_targets = set(targets)
    with immediate_transaction(conn):
        current_targets = {
            str(row["target"])
            for row in conn.execute(
                """
                SELECT target FROM shm_subscription
                WHERE consumer_group=?
                """,
                (group,),
            )
        }
        conn.executemany(
            """
            DELETE FROM shm_subscription
            WHERE target=? AND consumer_group=?
            """,
            ((target, group) for target in sorted(current_targets - requested_targets)),
        )
        for target in sorted(requested_targets):
            conn.execute(
                """
                INSERT OR IGNORE INTO shm_subscription
                    (target, consumer_group) VALUES (?, ?)
                """,
                (target, group),
            )
        if completion_mode == "mark":
            # Claiming and mark-acking a replayed row rewrites it wider in place,
            # splitting the pages the replay packed full: draining a cut replay
            # grew the store by about half the pages the replay added. Charging
            # the replay as much again as it adds keeps that drain in budget.
            used = _used_pages(conn)
            if used < page_limit:
                page_limit = used + (page_limit - used) // 2
        for target in sorted(requested_targets - current_targets):
            replayed, skipped = _replay(conn, target, group, now, page_limit)
            inserted += replayed
            if skipped:
                cut_short.append((target, replayed, skipped))
            # Expiry never removes pending or claimed work because only
            # publications without delivery rows qualify.
            conn.execute(
                """
                DELETE FROM shm_publication
                WHERE target=? AND retained_until<=?
                  AND NOT EXISTS (
                    SELECT 1 FROM shm_delivery
                    WHERE publication_id=shm_publication.id
                  )
                """,
                (target, now),
            )
    return inserted, cut_short


def _replay(
    conn: sqlite3.Connection, target: str, group: str, now: float, page_limit: int
) -> tuple[int, int]:
    """Replay the retained publications the group lacks, oldest first, within page_limit.

    A publication the group holds a delivery or completion tombstone for is not
    missing, so it neither counts as skipped nor takes pages. Oldest first keeps
    each group's deliveries in publication-sequence order, which the claim
    queries in _shm_claims rely on. Returns (replayed, skipped).
    """
    missing = [
        str(row["id"])
        for row in conn.execute(
            """
            SELECT p.id FROM shm_publication AS p
            WHERE p.target=? AND p.retained_until>?
              AND NOT EXISTS (
                SELECT 1 FROM shm_delivery AS d
                WHERE d.publication_id=p.id AND d.consumer_group=?
              )
              AND NOT EXISTS (
                SELECT 1 FROM shm_completion_tombstone AS t
                WHERE t.publication_id=p.id AND t.consumer_group=?
              )
            ORDER BY p.sequence
            """,
            (target, now, group, group),
        )
    ]
    for index, publication_id in enumerate(missing):
        conn.execute("SAVEPOINT shm_replay")
        _insert_delivery(conn, publication_id, group, now)
        if _used_pages(conn) > page_limit:
            conn.execute("ROLLBACK TO shm_replay")
            conn.execute("RELEASE shm_replay")
            return index, len(missing) - index
        conn.execute("RELEASE shm_replay")
    return len(missing), 0


def group_backlog(conn: sqlite3.Connection) -> dict[str, int]:
    """Map every group to its pending and claimed delivery count.

    A group counts when it is subscribed or still holds pending or claimed
    deliveries, so deliveries left behind by an unsubscribed group show up.
    """
    rows = conn.execute(
        """
        SELECT s.consumer_group, (
            SELECT COUNT(*) FROM shm_delivery AS d
            WHERE d.consumer_group=s.consumer_group
              AND d.status IN ('pending', 'claimed')
        ) AS backlog
        FROM (
            SELECT consumer_group FROM shm_subscription
            UNION
            SELECT consumer_group FROM shm_delivery WHERE status IN ('pending', 'claimed')
        ) AS s
        ORDER BY s.consumer_group
        """
    )
    return {str(row["consumer_group"]): int(row["backlog"]) for row in rows}


def stale_targets(
    conn: sqlite3.Connection,
    group: str,
    targets: list[str],
) -> dict[str, int]:
    """Map each target ``group`` holds but ``targets`` omits to its backlog.

    A held target is one the group subscribes to or has pending or claimed
    deliveries for; the count is those undelivered deliveries.
    """
    consumed = set(targets)
    stale = {
        str(row["target"]): 0
        for row in conn.execute(
            "SELECT target FROM shm_subscription WHERE consumer_group=?",
            (group,),
        )
        if row["target"] not in consumed
    }
    rows = conn.execute(
        """
        SELECT p.target AS target, COUNT(*) AS backlog
        FROM shm_delivery AS d JOIN shm_publication AS p ON p.id=d.publication_id
        WHERE d.consumer_group=? AND d.status IN ('pending', 'claimed')
        GROUP BY p.target
        """,
        (group,),
    )
    for row in rows:
        if row["target"] not in consumed:
            stale[str(row["target"])] = int(row["backlog"])
    return dict(sorted(stale.items()))


def drop_group(
    conn: sqlite3.Connection,
    group: str,
    targets: list[str] | None = None,
) -> tuple[int, int]:
    """Delete one group's subscriptions and undelivered work.

    ``targets``, when given, limits the removal to those targets.
    Returns ``(subscriptions, deliveries)`` removed. Terminal rows stay for
    ``prune``; once the undelivered rows are gone, prune can reclaim the
    publications they pinned.
    """
    if targets is None:
        subscription_filter = delivery_filter = ""
        names: tuple[str, ...] = ()
    else:
        names = tuple(targets)
        marks = ",".join("?" * len(names))
        subscription_filter = f" AND target IN ({marks})"
        delivery_filter = (
            f" AND publication_id IN (SELECT id FROM shm_publication WHERE target IN ({marks}))"
        )
    with immediate_transaction(conn):
        subscriptions = conn.execute(
            f"DELETE FROM shm_subscription WHERE consumer_group=?{subscription_filter}",
            (group, *names),
        ).rowcount
        deliveries = conn.execute(
            f"""
            DELETE FROM shm_delivery
            WHERE consumer_group=? AND status IN ('pending', 'claimed'){delivery_filter}
            """,
            (group, *names),
        ).rowcount
    return subscriptions, deliveries


def get_subscriptions(
    conn: sqlite3.Connection,
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    rows = conn.execute(
        """
        SELECT target, consumer_group FROM shm_subscription
        ORDER BY target, consumer_group
        """
    )
    for row in rows:
        result.setdefault(str(row["target"]), []).append(str(row["consumer_group"]))
    return result


def _insert_publication(
    conn: sqlite3.Connection,
    publication_id: str,
    target: str,
    payload: bytes,
    headers: dict[str, str] | None,
    event_type: str,
    created_at: float,
    retained_until: float,
) -> tuple[PublishResult, bool]:
    encoded_headers = json.dumps(headers) if headers is not None else None
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO shm_publication (
            id, target, event_type, payload, headers,
            created_at, retained_until
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            publication_id,
            target,
            event_type,
            payload,
            encoded_headers,
            created_at,
            retained_until,
        ),
    )
    if cursor.rowcount:
        if cursor.lastrowid is None:
            raise RuntimeError("publication insert did not allocate a sequence")
        sequence = int(cursor.lastrowid)
    else:
        row = conn.execute(
            """
            SELECT sequence, target, event_type, payload, headers
            FROM shm_publication WHERE id=?
            """,
            (publication_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("publication insert did not produce a row")
        stored_headers = json.loads(str(row["headers"])) if row["headers"] is not None else None
        # Reusing an ID is idempotent only when every immutable field matches.
        if (
            str(row["target"]),
            str(row["event_type"]),
            bytes(row["payload"]),
            stored_headers,
        ) != (target, event_type, payload, headers):
            raise ValueError(
                f"publication id {publication_id!r} already exists with "
                "conflicting immutable fields"
            )
        sequence = int(row["sequence"])
    return PublishResult(publication_id, sequence), bool(cursor.rowcount)


def _insert_delivery(
    conn: sqlite3.Connection,
    publication_id: str,
    group: str,
    created_at: float,
) -> int:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO shm_delivery (
            publication_id, consumer_group, status, attempts,
            available_at, claim_generation, created_at
        )
        SELECT ?, ?, 'pending', 0, ?, 0, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM shm_completion_tombstone
            WHERE publication_id=? AND consumer_group=?
        )
        """,
        (
            publication_id,
            group,
            created_at,
            created_at,
            publication_id,
            group,
        ),
    )
    return cursor.rowcount


def _prune_expired_empty_publications(
    conn: sqlite3.Connection,
    now: float,
    limit: int,
) -> None:
    """Bound routine publish latency while removing expired orphan rows."""
    conn.execute(
        """
        DELETE FROM shm_publication
        WHERE id IN (
            SELECT p.id FROM shm_publication AS p
            WHERE p.retained_until<=?
              AND NOT EXISTS (
                SELECT 1 FROM shm_delivery AS d
                WHERE d.publication_id=p.id
              )
            ORDER BY p.retained_until, p.sequence
            LIMIT ?
        )
        """,
        (now, limit),
    )
