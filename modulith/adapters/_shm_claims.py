"""Atomic claim and lease-renewal operations for the durable SHM queue."""

from __future__ import annotations

import heapq
import itertools
import json
import sqlite3
import time
from typing import Any

from ._shm_schema import immediate_transaction
from ._shm_types import ClaimToken, require_consumer_name

# Due pending work and reclaimable stale claims, oldest first within each
# status branch. ``d.id`` makes each branch's ordering total (deliveries for
# one group are always inserted in publication-sequence order: publish()
# fans a publication out to its groups right after allocating its sequence,
# and subscribe()'s replay walks retained publications ORDER BY sequence),
# so the same prefix of rows comes back for any LIMIT.
#
# The two statuses are queried as SEPARATE statements, not one OR'd query.
# An OR across two branches of the SAME index forces SQLite's "MULTI-INDEX
# OR" plan to fully materialize and merge-sort every matching row of BOTH
# branches before its combined LIMIT can apply -- measured flat 0.08-0.15ms
# regardless of backlog size (300..100k rows) once split, versus linear
# growth (~1ms/1000 rows) for the combined OR query. Splitting lets each
# branch's own LIMIT land directly on its own idx_shm_delivery_claim range
# scan, then the two already-sorted, already-bounded results are merged in
# Python -- O(limit), never O(backlog).
_PENDING_PREDICATE = "d.consumer_group=? AND d.available_at<=? AND d.status='pending'"
_STALE_CLAIMED_PREDICATE = (
    "d.consumer_group=? AND d.available_at<=? AND d.status='claimed' AND d.claimed_at<=?"
)
_JOIN = "FROM shm_delivery AS d JOIN shm_publication AS p ON p.id=d.publication_id"
_ORDER_LIMIT = "ORDER BY d.available_at, d.id LIMIT ?"

_SIZE_FIELDS = (
    "d.available_at, d.id AS delivery_id, "
    "LENGTH(p.payload) AS payload_bytes, "
    "LENGTH(CAST(p.headers AS BLOB)) AS headers_bytes"
)
_ROW_FIELDS = "d.id AS delivery_id, d.claim_generation, d.attempts, d.last_error, p.*"

_PENDING_SIZES_SQL = f"SELECT {_SIZE_FIELDS} {_JOIN} WHERE {_PENDING_PREDICATE} {_ORDER_LIMIT}"
_STALE_SIZES_SQL = f"SELECT {_SIZE_FIELDS} {_JOIN} WHERE {_STALE_CLAIMED_PREDICATE} {_ORDER_LIMIT}"
_PENDING_ROWS_SQL = f"SELECT {_ROW_FIELDS} {_JOIN} WHERE {_PENDING_PREDICATE} {_ORDER_LIMIT}"
_STALE_ROWS_SQL = f"SELECT {_ROW_FIELDS} {_JOIN} WHERE {_STALE_CLAIMED_PREDICATE} {_ORDER_LIMIT}"


def claim(
    conn: sqlite3.Connection,
    group: str,
    limit: int,
    consumer_name: str,
    reclaim_stale_seconds: float,
    max_claim_bytes: int,
) -> list[dict[str, Any]]:
    """Atomically claim due pending work and abandoned stale claims."""
    consumer_name = require_consumer_name(consumer_name)
    now = time.time()
    cutoff = now - reclaim_stale_seconds
    claimed: list[dict[str, Any]] = []
    with immediate_transaction(conn):
        candidates = _merged_size_candidates(conn, group, now, cutoff, limit)
        affordable = _affordable_candidates(candidates, max_claim_bytes)
        if not affordable:
            return claimed
        rows_by_id = _fetch_needed_rows(conn, group, now, cutoff, affordable)
        for candidate in affordable:
            row = rows_by_id[candidate[1]]
            generation = int(row["claim_generation"]) + 1
            conn.execute(
                """
                UPDATE shm_delivery
                SET status='claimed', claimed_at=?, claimed_by=?,
                    claim_generation=?
                WHERE id=?
                """,
                (now, consumer_name, generation, row["delivery_id"]),
            )
            claimed.append(_claimed_row(row, group, consumer_name, generation))
    return claimed


def _merged_size_candidates(
    conn: sqlite3.Connection,
    group: str,
    now: float,
    cutoff: float,
    limit: int,
) -> list[tuple[str, int, int]]:
    """Merge each branch's own bounded, pre-sorted candidates into one list.

    Each branch tuple is (branch, delivery_id, row_bytes). Both source
    queries are already ORDER BY (available_at, id) LIMIT ``limit``, so
    heapq.merge combines them in O(limit) without re-sorting either side.
    """

    def _entries(rows: sqlite3.Cursor, branch: str) -> Any:
        for row in rows:
            row_bytes = int(row["payload_bytes"]) + int(row["headers_bytes"] or 0)
            yield (row["available_at"], int(row["delivery_id"]), branch, row_bytes)

    pending = _entries(conn.execute(_PENDING_SIZES_SQL, (group, now, limit)), "pending")
    stale = _entries(
        conn.execute(_STALE_SIZES_SQL, (group, now, cutoff, limit)),
        "stale",
    )
    merged = heapq.merge(pending, stale, key=lambda entry: (entry[0], entry[1]))
    return [
        (branch, delivery_id, row_bytes)
        for _available_at, delivery_id, branch, row_bytes in itertools.islice(merged, limit)
    ]


def _affordable_candidates(
    candidates: list[tuple[str, int, int]],
    max_claim_bytes: int,
) -> list[tuple[str, int, int]]:
    """How many leading candidates fit the claim's byte budget."""
    affordable: list[tuple[str, int, int]] = []
    total = 0
    for candidate in candidates:
        row_bytes = candidate[2]
        # One row must always make progress, even when its headers put the
        # aggregate above the payload-derived claim budget.
        if affordable and total + row_bytes > max_claim_bytes:
            break
        affordable.append(candidate)
        total += row_bytes
    return affordable


def _fetch_needed_rows(
    conn: sqlite3.Connection,
    group: str,
    now: float,
    cutoff: float,
    affordable: list[tuple[str, int, int]],
) -> dict[int, sqlite3.Row]:
    """Read full payload rows only for the exact ids the budget affords."""
    needed_pending = sum(1 for candidate in affordable if candidate[0] == "pending")
    needed_stale = len(affordable) - needed_pending
    rows_by_id: dict[int, sqlite3.Row] = {}
    if needed_pending:
        for row in conn.execute(_PENDING_ROWS_SQL, (group, now, needed_pending)):
            rows_by_id[int(row["delivery_id"])] = row
    if needed_stale:
        for row in conn.execute(_STALE_ROWS_SQL, (group, now, cutoff, needed_stale)):
            rows_by_id[int(row["delivery_id"])] = row
    return rows_by_id


def renew_claims(
    conn: sqlite3.Connection,
    values: list[ClaimToken | str],
    consumer_name: str,
) -> int:
    """Renew only complete delivery-generation tokens owned by the caller."""
    consumer_name = require_consumer_name(consumer_name)
    tokens = [ClaimToken.decode(value) for value in values]
    if not tokens:
        return 0
    renewed = 0
    now = time.time()
    with immediate_transaction(conn):
        for token in tokens:
            cursor = conn.execute(
                """
                UPDATE shm_delivery SET claimed_at=?
                WHERE id=? AND status='claimed' AND claimed_by=?
                  AND claim_generation=?
                """,
                (
                    now,
                    token.delivery_id,
                    consumer_name,
                    token.generation,
                ),
            )
            renewed += cursor.rowcount
    return renewed


def owned_claim(
    conn: sqlite3.Connection,
    value: ClaimToken | str,
    consumer_name: str,
) -> tuple[ClaimToken, str] | None:
    """Resolve a strict token and verify its current owner."""
    consumer_name = require_consumer_name(consumer_name)
    token = ClaimToken.decode(value)
    row = conn.execute(
        """
        SELECT claimed_by FROM shm_delivery
        WHERE id=? AND status='claimed' AND claim_generation=?
        """,
        (token.delivery_id, token.generation),
    ).fetchone()
    if row is None or row["claimed_by"] is None:
        return None
    owner = str(row["claimed_by"])
    if owner != consumer_name:
        return None
    return token, owner


def owned_predicate() -> str:
    return "id=? AND status='claimed' AND claimed_by=? AND claim_generation=?"


def _claimed_row(
    row: sqlite3.Row,
    group: str,
    consumer_name: str,
    generation: int,
) -> dict[str, Any]:
    token = ClaimToken(int(row["delivery_id"]), generation)
    headers = row["headers"]
    return {
        "id": str(token),
        "claim_token": token,
        "message_id": str(row["id"]),
        "target": str(row["target"]),
        "consumer_group": group,
        "event_type": str(row["event_type"]),
        "payload": bytes(row["payload"]),
        "headers": json.loads(headers) if headers else None,
        "status": "claimed",
        "attempts": int(row["attempts"]),
        "sequence": int(row["sequence"]),
        "claimed_by": consumer_name,
        "claim_generation": generation,
        "last_error": row["last_error"],
    }
