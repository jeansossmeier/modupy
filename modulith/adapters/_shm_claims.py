"""Atomic claim and lease-renewal operations for the durable SHM queue."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from ._shm_schema import immediate_transaction
from ._shm_types import ClaimToken, require_consumer_name


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
    claimed_bytes = 0
    with immediate_transaction(conn):
        rows = conn.execute(
            """
            SELECT d.id AS delivery_id, d.claim_generation,
                   d.attempts, d.last_error, p.*
            FROM shm_delivery AS d
            JOIN shm_publication AS p ON p.id=d.publication_id
            WHERE d.consumer_group=? AND d.available_at<=?
              AND (
                d.status='pending'
                OR (d.status='claimed' AND d.claimed_at<=?)
              )
            ORDER BY d.available_at, p.sequence, d.id
            LIMIT ?
            """,
            (group, now, cutoff, limit),
        )
        for row in rows:
            headers = row["headers"]
            row_bytes = len(row["payload"]) + (
                len(str(headers).encode("utf-8")) if headers is not None else 0
            )
            # One row must always make progress, even when its headers put the
            # aggregate above the payload-derived claim budget.
            if claimed and claimed_bytes + row_bytes > max_claim_bytes:
                break
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
            claimed_bytes += row_bytes
    return claimed


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
