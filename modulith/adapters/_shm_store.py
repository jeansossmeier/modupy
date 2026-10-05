"""Thread-confined synchronous core for the durable SHM queue."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar

from ..config import DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_SHM_MAX_STORE_BYTES
from . import _shm_claims, _shm_completion, _shm_publications
from ._dead_letter import DeadLetter
from ._shm_schema import immediate_transaction, open_database
from ._shm_types import ClaimToken, PublishResult

__all__ = ["ClaimToken", "PublishResult", "SqliteQueueStore"]

_T = TypeVar("_T")

logger = logging.getLogger("modulith.adapters.shm")

# SQLite 3.31.1 and older parse PRAGMA max_page_count as a signed 32-bit int
# and read a larger value as a query that leaves the cap unchanged.
_UNCAPPED_PAGES = 2**31 - 1


class SqliteQueueStore:
    """Own one connection; every method must run on its executor thread."""

    def __init__(
        self,
        path: str,
        *,
        synchronous: str,
        completion_mode: str,
        orphan_retention_seconds: float,
        retry_backoff_base_seconds: float,
        retry_backoff_cap_seconds: float,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_store_bytes: int = DEFAULT_SHM_MAX_STORE_BYTES,
    ) -> None:
        self._conn = open_database(path, synchronous, max_store_bytes)
        self._completion_mode = completion_mode
        self._orphan_retention_seconds = orphan_retention_seconds
        self._retry_backoff_base_seconds = retry_backoff_base_seconds
        self._retry_backoff_cap_seconds = retry_backoff_cap_seconds
        self._max_payload_bytes = max_payload_bytes
        self._max_store_bytes = max_store_bytes
        self._publishes_since_prune = 0
        self._cap_lifted = False

    def _consumer_write(self, operation: Callable[[], _T]) -> _T:
        """Run a consumer write, past max_store_bytes if the limit refuses it.

        Claims, fails and mark-mode acks grow rows the store already holds, so a
        backlog that filled the store may need more pages than any fixed reserve
        leaves. A subscribe runs here so its subscription row is always
        recorded; its replay stops below the publish budget on its own. A
        heartbeat touch and a group drop run here too: either can need a page
        split when the store sits at its cap. Publishes stay refused while the
        store is over its publish budget, so the file grows past max_store_bytes
        only by work it already holds.

        The cap goes back once the write is done. A restore that fails is
        logged and never replaces the write's result; the next consumer write
        retries it before it runs.
        """
        if self._cap_lifted:
            self._restore_page_cap()
        try:
            return operation()
        except sqlite3.OperationalError as error:
            if not _shm_publications._is_store_full(error):
                raise
            capped_pages = self.read_pragma("max_page_count")
            # Set before the PRAGMA so a failure after it took effect leaves the restore pending.
            self._cap_lifted = True
            self._conn.execute(f"PRAGMA max_page_count={_UNCAPPED_PAGES}")
            if self.read_pragma("max_page_count") <= capped_pages:
                error.add_note(
                    f"SQLite {sqlite3.sqlite_version} kept PRAGMA max_page_count at "
                    f"{capped_pages} pages, so this consumer write cannot grow the "
                    "store past max_store_bytes."
                )
                raise
        try:
            return operation()
        finally:
            self._restore_page_cap()

    def _restore_page_cap(self) -> None:
        try:
            # SQLite keeps the cap at the current file size when that is larger.
            page_size = int(self._conn.execute("PRAGMA page_size").fetchone()[0])
            self._conn.execute(
                f"PRAGMA max_page_count={max(1, self._max_store_bytes // page_size)}"
            )
        except sqlite3.Error as error:
            logger.error(
                "SHM store could not restore PRAGMA max_page_count to max_store_bytes "
                "(%d bytes): %s. The consumer write that lifted it has committed and the "
                "next consumer write retries the restore; until it succeeds, SQLite "
                "itself no longer stops the file at max_store_bytes.",
                self._max_store_bytes,
                error,
            )
        else:
            self._cap_lifted = False

    def spill(self, messages: list[dict[str, Any]]) -> None:
        _shm_publications.spill(
            self._conn,
            messages,
            self._orphan_retention_seconds,
            self._max_payload_bytes,
        )

    def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None,
        publication_id: str | None,
    ) -> PublishResult:
        self._publishes_since_prune += 1
        prune_due = self._publishes_since_prune >= _shm_publications.PRUNE_EVERY_N_PUBLISHES
        if prune_due:
            self._publishes_since_prune = 0
        return _shm_publications.publish(
            self._conn,
            target,
            payload,
            headers,
            publication_id,
            self._orphan_retention_seconds,
            self._max_payload_bytes,
            self._max_store_bytes,
            prune_due,
        )

    def subscribe(self, targets: list[str], group: str) -> int:
        return _shm_publications.subscribe(
            self._conn,
            targets,
            group,
            self._max_store_bytes,
            self._consumer_write,
            self._completion_mode,
        )

    def get_subscriptions(self) -> dict[str, list[str]]:
        return _shm_publications.get_subscriptions(self._conn)

    def group_backlog(self, targets: list[str] | None = None) -> dict[str, int]:
        return _shm_publications.group_backlog(self._conn, targets)

    def active_groups(self, within_seconds: float) -> set[str]:
        """Groups a consumer refreshed, claimed or completed within ``within_seconds``."""
        cutoff = time.time() - within_seconds
        rows = self._conn.execute(
            """
            SELECT consumer_group FROM shm_subscription WHERE updated_at>=?
            UNION
            SELECT consumer_group FROM shm_delivery WHERE claimed_at>=? OR completed_at>=?
            UNION
            SELECT consumer_group FROM shm_completion_tombstone WHERE completed_at>=?
            """,
            (cutoff, cutoff, cutoff, cutoff),
        )
        return {str(row["consumer_group"]) for row in rows}

    def touch_subscriptions(self, targets: list[str], group: str) -> None:
        """Stamp this group's subscription rows as served by a running consumer."""
        marks = ",".join("?" * len(targets))

        def touch() -> None:
            with immediate_transaction(self._conn):
                self._conn.execute(
                    f"UPDATE shm_subscription SET updated_at=? "
                    f"WHERE consumer_group=? AND target IN ({marks})",
                    (time.time(), group, *targets),
                )

        self._consumer_write(touch)

    def sole_subscriber_targets(self, group: str) -> list[str]:
        """Targets ``group`` subscribes to that no other group subscribes to."""
        rows = self._conn.execute(
            """
            SELECT target FROM shm_subscription
            WHERE target IN (SELECT target FROM shm_subscription WHERE consumer_group=?)
            GROUP BY target HAVING COUNT(*)=1
            ORDER BY target
            """,
            (group,),
        )
        return [str(row["target"]) for row in rows]

    def stale_targets(self, group: str, targets: list[str]) -> dict[str, int]:
        return _shm_publications.stale_targets(self._conn, group, targets)

    def drop_group(self, group: str, targets: list[str] | None = None) -> tuple[int, int]:
        return self._consumer_write(
            lambda: _shm_publications.drop_group(self._conn, group, targets)
        )

    def claim(
        self,
        group: str,
        limit: int,
        consumer_name: str,
        reclaim_stale_seconds: float,
        max_attempts: int | None = None,
        targets: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        return self._consumer_write(
            lambda: _shm_claims.claim(
                self._conn,
                group,
                limit,
                consumer_name,
                reclaim_stale_seconds,
                self._max_payload_bytes,
                max_attempts,
                targets,
            )
        )

    def renew_claims(
        self,
        values: list[ClaimToken | str],
        consumer_name: str,
        start_dispatch: bool = False,
    ) -> int:
        return self._consumer_write(
            lambda: _shm_claims.renew_claims(
                self._conn,
                values,
                consumer_name,
                start_dispatch,
            )
        )

    def release_claims(self, values: list[ClaimToken | str], consumer_name: str) -> int:
        return self._consumer_write(
            lambda: _shm_claims.release_claims(self._conn, values, consumer_name)
        )

    def release_interrupted_claims(self, values: list[ClaimToken | str], consumer_name: str) -> int:
        return self._consumer_write(
            lambda: _shm_claims.release_claims(
                self._conn, values, consumer_name, include_started=True
            )
        )

    def ack(
        self,
        value: ClaimToken | str,
        consumer_name: str,
        completion_mode: str | None,
    ) -> bool:
        return self._consumer_write(
            lambda: _shm_completion.ack(
                self._conn,
                value,
                consumer_name,
                completion_mode or self._completion_mode,
            )
        )

    def fail(
        self,
        value: ClaimToken | str,
        error: str,
        max_attempts: int,
        consumer_name: str,
    ) -> bool:
        return self._consumer_write(
            lambda: _shm_completion.fail(
                self._conn,
                value,
                error,
                max_attempts,
                consumer_name,
                self._retry_backoff_base_seconds,
                self._retry_backoff_cap_seconds,
            )
        )

    def dead_letter(
        self,
        value: ClaimToken | str,
        error: str,
        consumer_name: str,
    ) -> bool:
        return self._consumer_write(
            lambda: _shm_completion.dead_letter(
                self._conn,
                value,
                error,
                consumer_name,
            )
        )

    def list_dead_letters(self, after: tuple[datetime, str] | None, limit: int) -> list[DeadLetter]:
        return _shm_completion.list_dead_letters(self._conn, after, limit)

    def retry_dead_letters(self) -> int:
        return self._consumer_write(lambda: _shm_completion.retry_dead_letters(self._conn))

    def prune(self, retention_age_seconds: float, limit: int) -> int:
        return self._consumer_write(
            lambda: _shm_completion.prune(
                self._conn,
                retention_age_seconds,
                limit,
            )
        )

    def read_pragma(self, name: str) -> int:
        if name not in {"synchronous", "page_size", "page_count", "max_page_count"}:
            raise ValueError(f"unsupported PRAGMA {name!r}")
        return int(self._conn.execute(f"PRAGMA {name}").fetchone()[0])

    def close(self) -> None:
        self._conn.close()
