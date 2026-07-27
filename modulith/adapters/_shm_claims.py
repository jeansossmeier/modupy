"""Atomic claim and lease-renewal operations for the durable SHM queue."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from ._shm_schema import immediate_transaction
from ._shm_types import ClaimToken, require_consumer_name

# Due pending work and reclaimable stale claims, oldest first. ``d.id`` makes
# the ordering total, so the same prefix of rows comes back for any LIMIT.
_CLAIM_CANDIDATES = """
    FROM shm_delivery AS d
    JOIN shm_publication AS p ON p.id=d.publication_id
    WHERE d.consumer_group=? AND d.available_at<=?
      AND (
        d.status='pending'
        OR (d.status='claimed' AND d.claimed_at<=?)
      )
    ORDER BY d.available_at, p.sequence, d.id
    LIMIT ?
    """

# ``p.sequence`` belongs to the joined table, so no index on shm_delivery can
# satisfy the ORDER BY and SQLite always sorts through a temp B-tree. Sizing
# the batch over LENGTH() keeps the payloads out of that sorter — SQLite reads
# a blob's length from its header without touching the overflow pages — so the
# rows the byte budget is about to reject are never materialized.
_CLAIM_SIZES_SQL = (
    "SELECT LENGTH(p.payload) AS payload_bytes, "
    "LENGTH(CAST(p.headers AS BLOB)) AS headers_bytes" + _CLAIM_CANDIDATES
)

_CLAIM_ROWS_SQL = (
    "SELECT d.id AS delivery_id, d.claim_generation, "
    "d.attempts, d.last_error, p.*" + _CLAIM_CANDIDATES
)


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
        parameters = (group, now, cutoff, limit)
        affordable = _affordable_rows(conn, parameters, max_claim_bytes)
        if not affordable:
            return claimed
        rows = conn.execute(_CLAIM_ROWS_SQL, (*parameters[:3], affordable))
        for row in rows:
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


def _affordable_rows(
    conn: sqlite3.Connection,
    parameters: tuple[str, float, float, int],
    max_claim_bytes: int,
) -> int:
    """How many leading candidate rows fit the claim's byte budget."""
    affordable = 0
    total = 0
    for row in conn.execute(_CLAIM_SIZES_SQL, parameters):
        row_bytes = row["payload_bytes"] + (row["headers_bytes"] or 0)
        # One row must always make progress, even when its headers put the
        # aggregate above the payload-derived claim budget.
        if affordable and total + row_bytes > max_claim_bytes:
            break
        affordable += 1
        total += row_bytes
    return affordable


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
