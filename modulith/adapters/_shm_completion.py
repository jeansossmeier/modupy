"""Fenced completion, retry, dead-letter, and pruning operations."""

from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime, timedelta

from ._dead_letter import DeadLetter
from ._shm_claims import owned_claim, owned_predicate
from ._shm_schema import immediate_transaction
from ._shm_types import ClaimToken

_MAX_RETRY_EXPONENT = 1023


def ack(
    conn: sqlite3.Connection,
    value: ClaimToken | str,
    consumer_name: str,
    completion_mode: str,
) -> bool:
    """Delete or mark one delivery when owner and generation still match."""
    now = time.time()
    with immediate_transaction(conn):
        owned = owned_claim(conn, value, consumer_name)
        if owned is None:
            return False
        token, owner = owned
        if completion_mode == "delete":
            delivery = conn.execute(
                f"""
                SELECT publication_id, consumer_group
                FROM shm_delivery WHERE {owned_predicate()}
                """,
                (token.delivery_id, owner, token.generation),
            ).fetchone()
            if delivery is None:
                return False
            conn.execute(
                """
                INSERT OR IGNORE INTO shm_completion_tombstone (
                    publication_id, consumer_group, completed_at
                ) VALUES (?, ?, ?)
                """,
                (
                    delivery["publication_id"],
                    delivery["consumer_group"],
                    now,
                ),
            )
            cursor = conn.execute(
                f"DELETE FROM shm_delivery WHERE {owned_predicate()}",
                (token.delivery_id, owner, token.generation),
            )
            if cursor.rowcount:
                _delete_expired_empty_publication(
                    conn,
                    str(delivery["publication_id"]),
                    now,
                )
        else:
            cursor = conn.execute(
                f"""
                UPDATE shm_delivery
                SET status='done', completed_at=?,
                    claimed_at=NULL, claimed_by=NULL
                WHERE {owned_predicate()}
                """,
                (now, token.delivery_id, owner, token.generation),
            )
        return cursor.rowcount == 1


def fail(
    conn: sqlite3.Connection,
    value: ClaimToken | str,
    error: str,
    max_attempts: int,
    consumer_name: str,
    backoff_base: float,
    backoff_cap: float,
) -> bool:
    """Retry with backoff or terminally fail one currently owned claim."""
    now = time.time()
    with immediate_transaction(conn):
        owned = owned_claim(conn, value, consumer_name)
        if owned is None:
            return False
        token, owner = owned
        row = conn.execute(
            f"SELECT attempts FROM shm_delivery WHERE {owned_predicate()}",
            (token.delivery_id, owner, token.generation),
        ).fetchone()
        if row is None:
            return False
        attempts = int(row["attempts"]) + 1
        dead = attempts >= max_attempts
        retry_exponent = min(attempts - 1, _MAX_RETRY_EXPONENT)
        available_at = now if dead else now + min(backoff_base * (2.0**retry_exponent), backoff_cap)
        cursor = conn.execute(
            f"""
            UPDATE shm_delivery
            SET status=?, attempts=?, available_at=?, last_error=?,
                claimed_at=NULL, claimed_by=NULL, completed_at=?
            WHERE {owned_predicate()}
            """,
            (
                "dead" if dead else "pending",
                attempts,
                available_at,
                error,
                now if dead else None,
                token.delivery_id,
                owner,
                token.generation,
            ),
        )
        return cursor.rowcount == 1


def dead_letter(
    conn: sqlite3.Connection,
    value: ClaimToken | str,
    error: str,
    consumer_name: str,
) -> bool:
    """Dead-letter one delivery when owner and generation still match."""
    now = time.time()
    with immediate_transaction(conn):
        owned = owned_claim(conn, value, consumer_name)
        if owned is None:
            return False
        token, owner = owned
        cursor = conn.execute(
            f"""
            UPDATE shm_delivery
            SET status='dead', last_error=?, completed_at=?,
                claimed_at=NULL, claimed_by=NULL
            WHERE {owned_predicate()}
            """,
            (error, now, token.delivery_id, owner, token.generation),
        )
        return cursor.rowcount == 1


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)
_CREATED_MICROS = "CAST(ROUND(d.created_at * 1000000) AS INTEGER)"


def list_dead_letters(
    conn: sqlite3.Connection,
    after: tuple[datetime, str] | None,
    limit: int,
) -> list[DeadLetter]:
    """One page of dead deliveries ordered by creation time, then delivery id.

    Creation time is compared in whole microseconds, the precision a
    ``DeadLetter.created_at`` can carry, so a cursor built from a listed entry
    resumes exactly past it.
    """
    keyset = ""
    params: tuple[int, ...] = ()
    if after is not None:
        created_at, delivery_id = after
        micros = (created_at - _EPOCH) // _MICROSECOND
        keyset = f"AND ({_CREATED_MICROS} > ? OR ({_CREATED_MICROS} = ? AND d.id > ?))"
        params = (micros, micros, int(delivery_id))
    rows = conn.execute(
        f"""
        SELECT d.id, p.target, d.consumer_group, p.event_type, d.attempts,
               d.last_error, {_CREATED_MICROS} AS created_micros
        FROM shm_delivery AS d JOIN shm_publication AS p ON p.id=d.publication_id
        WHERE d.status='dead' {keyset}
        ORDER BY created_micros, d.id LIMIT ?
        """,
        (*params, limit),
    )
    return [
        DeadLetter(
            id=str(row["id"]),
            target=str(row["target"]),
            consumer_group=str(row["consumer_group"]),
            event_type=str(row["event_type"]),
            attempts=int(row["attempts"]),
            last_error=row["last_error"],
            created_at=_EPOCH + int(row["created_micros"]) * _MICROSECOND,
        )
        for row in rows
    ]


def retry_dead_letters(conn: sqlite3.Connection) -> int:
    """Make every dead delivery claimable again by its own group; return how many."""
    with immediate_transaction(conn):
        return conn.execute(
            """
            UPDATE shm_delivery
            SET status='pending', attempts=0, last_error=NULL, available_at=?,
                claimed_at=NULL, claimed_by=NULL, dispatch_started=0,
                completed_at=NULL
            WHERE status='dead'
            """,
            (time.time(),),
        ).rowcount


def prune(
    conn: sqlite3.Connection,
    retention_age_seconds: float,
    limit: int,
) -> int:
    """Delete bounded terminal work by completion time, never creation time."""
    now = time.time()
    cutoff = now - retention_age_seconds
    with immediate_transaction(conn):
        terminal_rows = list(
            conn.execute(
                """
                SELECT id, publication_id, consumer_group, completed_at
                FROM shm_delivery
                WHERE status IN ('done', 'dead')
                  AND completed_at IS NOT NULL AND completed_at<=?
                ORDER BY completed_at, id LIMIT ?
                """,
                (cutoff, limit),
            )
        )
        ids = [int(row["id"]) for row in terminal_rows]
        if ids:
            conn.executemany(
                """
                INSERT OR IGNORE INTO shm_completion_tombstone (
                    publication_id, consumer_group, completed_at
                ) VALUES (?, ?, ?)
                """,
                (
                    (
                        row["publication_id"],
                        row["consumer_group"],
                        row["completed_at"],
                    )
                    for row in terminal_rows
                ),
            )
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"DELETE FROM shm_delivery WHERE id IN ({placeholders})",
                ids,
            )
        remaining = limit - len(ids)
        empty_ids: list[str] = []
        if remaining:
            empty_ids = [
                str(row["id"])
                for row in conn.execute(
                    """
                    SELECT p.id FROM shm_publication AS p
                    WHERE p.retained_until<=?
                      AND NOT EXISTS (
                        SELECT 1 FROM shm_delivery AS d
                        WHERE d.publication_id=p.id
                      )
                    ORDER BY p.retained_until, p.sequence LIMIT ?
                    """,
                    (now, remaining),
                )
            ]
            if empty_ids:
                placeholders = ",".join("?" for _ in empty_ids)
                conn.execute(
                    f"DELETE FROM shm_publication WHERE id IN ({placeholders})",
                    empty_ids,
                )
    return len(ids) + len(empty_ids)


def _delete_expired_empty_publication(
    conn: sqlite3.Connection,
    publication_id: str,
    now: float,
) -> None:
    # A completed publication remains available to future groups until TTL.
    conn.execute(
        """
        DELETE FROM shm_publication
        WHERE id=? AND retained_until<=?
          AND NOT EXISTS (
            SELECT 1 FROM shm_delivery
            WHERE publication_id=shm_publication.id
          )
        """,
        (publication_id, now),
    )
