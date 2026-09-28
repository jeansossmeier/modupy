"""Publication, subscription, and retained-replay SQLite operations."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any
from uuid import uuid4

from ..config import ConfigurationError
from ._shm_schema import immediate_transaction
from ._shm_types import PublishResult

_PUBLISH_PRUNE_LIMIT = 100

# idx_shm_publication_expiry backs the prune scan (see
# _prune_expired_empty_publications), but it still touches every expired
# orphan row up to its LIMIT on each run. Gating it to a cadence instead of
# running it on every publish call bounds how often that cost is paid; the
# caller (SqliteQueueStore) tracks the cadence and passes prune_due.
PRUNE_EVERY_N_PUBLISHES = 100


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
    except sqlite3.Error as error:
        if _is_store_full(error):
            raise ConfigurationError(
                "SHM SQLite store is full while publishing. Every publication is "
                f"kept for orphan_retention_seconds (currently {retention_seconds:g}) "
                "even after every group has acked it, so the store sustains about "
                "max_store_bytes / (bytes per publication x orphan_retention_seconds) "
                "publications per second. Increase max_store_bytes (currently "
                f"{max_store_bytes}) or shorten orphan_retention_seconds; if the disk "
                "itself is full, free disk space."
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
) -> int:
    """Reconcile one group's subscriptions and replay newly added targets."""
    inserted = 0
    now = time.time()
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
        for target in sorted(requested_targets - current_targets):
            retained = conn.execute(
                """
                SELECT id FROM shm_publication
                WHERE target=? AND retained_until>?
                ORDER BY sequence
                """,
                (target, now),
            )
            for row in retained:
                inserted += _insert_delivery(
                    conn,
                    str(row["id"]),
                    group,
                    now,
                )
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
    return inserted


def group_backlog(conn: sqlite3.Connection) -> dict[str, int]:
    """Map every subscribed group to its pending and claimed delivery count."""
    rows = conn.execute(
        """
        SELECT s.consumer_group, (
            SELECT COUNT(*) FROM shm_delivery AS d
            WHERE d.consumer_group=s.consumer_group
              AND d.status IN ('pending', 'claimed')
        ) AS backlog
        FROM (SELECT DISTINCT consumer_group FROM shm_subscription) AS s
        ORDER BY s.consumer_group
        """
    )
    return {str(row["consumer_group"]): int(row["backlog"]) for row in rows}


def drop_group(conn: sqlite3.Connection, group: str) -> tuple[int, int]:
    """Delete one group's subscriptions and undelivered work.

    Returns ``(subscriptions, deliveries)`` removed. Terminal rows stay for
    ``prune``; once the undelivered rows are gone, prune can reclaim the
    publications they pinned.
    """
    with immediate_transaction(conn):
        subscriptions = conn.execute(
            "DELETE FROM shm_subscription WHERE consumer_group=?",
            (group,),
        ).rowcount
        deliveries = conn.execute(
            """
            DELETE FROM shm_delivery
            WHERE consumer_group=? AND status IN ('pending', 'claimed')
            """,
            (group,),
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
