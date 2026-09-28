"""Async compatibility façade for the SQLite-authoritative SHM queue."""

from __future__ import annotations

import math
from functools import partial
from typing import Any, cast

from ..config import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_SHM_MAX_STORE_BYTES,
    _validate_shm_broker_options,
)
from ._shm_executor import SerialStoreExecutor
from ._shm_store import ClaimToken, PublishResult, SqliteQueueStore
from ._shm_types import require_consumer_name

_SYNCHRONOUS_MODES = frozenset({"NORMAL", "FULL"})
_COMPLETION_MODES = frozenset({"delete", "mark"})


class ShmColdStore(SerialStoreExecutor):
    """Durable stdlib-SQLite queue with legacy cold-store method names."""

    def __init__(
        self,
        db_path: str,
        *,
        synchronous: str = "NORMAL",
        completion_mode: str = "delete",
        orphan_retention_seconds: float = 86400.0,
        retry_backoff_base_seconds: float = 0.05,
        retry_backoff_cap_seconds: float = 5.0,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_store_bytes: int = DEFAULT_SHM_MAX_STORE_BYTES,
    ) -> None:
        synchronous = synchronous.upper()
        if synchronous not in _SYNCHRONOUS_MODES:
            raise ValueError(f"synchronous must be one of {sorted(_SYNCHRONOUS_MODES)}")
        if completion_mode not in _COMPLETION_MODES:
            raise ValueError(f"completion_mode must be one of {sorted(_COMPLETION_MODES)}")
        _positive(orphan_retention_seconds, "orphan_retention_seconds")
        _positive(retry_backoff_base_seconds, "retry_backoff_base_seconds")
        _positive(retry_backoff_cap_seconds, "retry_backoff_cap_seconds")
        _validate_shm_broker_options(
            {
                "max_payload_bytes": max_payload_bytes,
                "max_store_bytes": max_store_bytes,
            }
        )
        factory = partial(
            SqliteQueueStore,
            db_path,
            synchronous=synchronous,
            completion_mode=completion_mode,
            orphan_retention_seconds=orphan_retention_seconds,
            retry_backoff_base_seconds=retry_backoff_base_seconds,
            retry_backoff_cap_seconds=retry_backoff_cap_seconds,
            max_payload_bytes=max_payload_bytes,
            max_store_bytes=max_store_bytes,
        )
        super().__init__(factory)

    async def spill(self, messages: list[dict[str, Any]]) -> None:
        """Persist old per-group spill rows through the durable schema."""
        await self._call("spill", messages)

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        *,
        publication_id: str | None = None,
    ) -> PublishResult:
        """Persist one replayable publication with a SQLite sequence."""
        return cast(
            PublishResult,
            await self._call(
                "publish",
                target,
                payload,
                headers,
                publication_id,
            ),
        )

    async def subscribe(
        self,
        targets: list[str] | tuple[str, ...],
        group: str,
    ) -> int:
        return cast(int, await self._call("subscribe", list(targets), group))

    async def get_subscriptions(self) -> dict[str, list[str]]:
        return cast(
            dict[str, list[str]],
            await self._call("get_subscriptions"),
        )

    async def group_backlog(self) -> dict[str, int]:
        return cast(dict[str, int], await self._call("group_backlog"))

    async def drop_group(self, group: str) -> tuple[int, int]:
        return cast(tuple[int, int], await self._call("drop_group", group))

    async def claim(
        self,
        consumer_group: str,
        *,
        limit: int = 100,
        consumer_name: str = "",
        reclaim_stale_seconds: float = 60.0,
        max_attempts: int | None = None,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be an integer >= 1")
        _non_negative(reclaim_stale_seconds, "reclaim_stale_seconds")
        return cast(
            list[dict[str, Any]],
            await self._call(
                "claim",
                consumer_group,
                limit,
                consumer_name,
                reclaim_stale_seconds,
                max_attempts,
            ),
        )

    async def recover(
        self,
        consumer_group: str,
        *,
        limit: int = 100,
        consumer_name: str = "",
        reclaim_stale_seconds: float = 60.0,
    ) -> list[dict[str, Any]]:
        return await self.claim(
            consumer_group,
            limit=limit,
            consumer_name=consumer_name,
            reclaim_stale_seconds=reclaim_stale_seconds,
        )

    async def renew_claims(
        self,
        values: list[ClaimToken | str],
        consumer_name: str,
    ) -> int:
        consumer_name = require_consumer_name(consumer_name)
        return cast(
            int,
            await self._call("renew_claims", values, consumer_name),
        )

    async def ack(
        self,
        value: ClaimToken | str,
        *,
        consumer_name: str,
        completion_mode: str | None = None,
    ) -> bool:
        consumer_name = require_consumer_name(consumer_name)
        if completion_mode is not None and completion_mode not in _COMPLETION_MODES:
            raise ValueError(f"completion_mode must be one of {sorted(_COMPLETION_MODES)}")
        return cast(
            bool,
            await self._call("ack", value, consumer_name, completion_mode),
        )

    async def fail(
        self,
        value: ClaimToken | str,
        error: str,
        max_attempts: int,
        *,
        consumer_name: str,
    ) -> bool:
        consumer_name = require_consumer_name(consumer_name)
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be an integer >= 1")
        return cast(
            bool,
            await self._call("fail", value, error, max_attempts, consumer_name),
        )

    async def dead_letter(
        self,
        value: ClaimToken | str,
        error: str,
        *,
        consumer_name: str,
    ) -> bool:
        consumer_name = require_consumer_name(consumer_name)
        return cast(
            bool,
            await self._call("dead_letter", value, error, consumer_name),
        )

    async def prune(
        self,
        retention_age_seconds: float,
        *,
        limit: int = 1000,
    ) -> int:
        _non_negative(retention_age_seconds, "retention_age_seconds")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be an integer >= 1")
        return cast(
            int,
            await self._call("prune", retention_age_seconds, limit),
        )

    async def _read_pragma(self, name: str) -> int:
        return cast(int, await self._call("read_pragma", name))


def _positive(value: float, name: str) -> None:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number > 0")


def _non_negative(value: float, name: str) -> None:
    if isinstance(value, bool) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite number >= 0")
