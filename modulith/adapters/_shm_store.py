"""Thread-confined synchronous core for the durable SHM queue."""

from __future__ import annotations

from typing import Any

from ..config import DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_SHM_MAX_STORE_BYTES
from . import _shm_claims, _shm_completion, _shm_publications
from ._shm_schema import open_database
from ._shm_types import ClaimToken, PublishResult

__all__ = ["ClaimToken", "PublishResult", "SqliteQueueStore"]


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
        )

    def get_subscriptions(self) -> dict[str, list[str]]:
        return _shm_publications.get_subscriptions(self._conn)

    def claim(
        self,
        group: str,
        limit: int,
        consumer_name: str,
        reclaim_stale_seconds: float,
        max_attempts: int | None = None,
    ) -> list[dict[str, Any]]:
        return _shm_claims.claim(
            self._conn,
            group,
            limit,
            consumer_name,
            reclaim_stale_seconds,
            self._max_payload_bytes,
            max_attempts,
        )

    def renew_claims(
        self,
        values: list[ClaimToken | str],
        consumer_name: str,
    ) -> int:
        return _shm_claims.renew_claims(
            self._conn,
            values,
            consumer_name,
        )

    def ack(
        self,
        value: ClaimToken | str,
        consumer_name: str,
        completion_mode: str | None,
    ) -> bool:
        return _shm_completion.ack(
            self._conn,
            value,
            consumer_name,
            completion_mode or self._completion_mode,
        )

    def fail(
        self,
        value: ClaimToken | str,
        error: str,
        max_attempts: int,
        consumer_name: str,
    ) -> bool:
        return _shm_completion.fail(
            self._conn,
            value,
            error,
            max_attempts,
            consumer_name,
            self._retry_backoff_base_seconds,
            self._retry_backoff_cap_seconds,
        )

    def dead_letter(
        self,
        value: ClaimToken | str,
        error: str,
        consumer_name: str,
    ) -> bool:
        return _shm_completion.dead_letter(
            self._conn,
            value,
            error,
            consumer_name,
        )

    def prune(self, retention_age_seconds: float, limit: int) -> int:
        return _shm_completion.prune(
            self._conn,
            retention_age_seconds,
            limit,
        )

    def read_pragma(self, name: str) -> int:
        if name not in {"synchronous", "page_size", "page_count", "max_page_count"}:
            raise ValueError(f"unsupported PRAGMA {name!r}")
        return int(self._conn.execute(f"PRAGMA {name}").fetchone()[0])

    def close(self) -> None:
        self._conn.close()
