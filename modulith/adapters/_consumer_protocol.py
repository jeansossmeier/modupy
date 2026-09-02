"""Internal contract shared by durable polling consumers."""

from __future__ import annotations

from typing import Any, Protocol


class PollingBroker(Protocol):
    """Store operations required by the common polling lifecycle.

    Row IDs are opaque strings. An adapter may encode ownership generations in
    them, so consumers must pass IDs through unchanged.
    """

    async def subscribe(self, targets: list[str], group: str) -> None: ...

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float,
        max_attempts: int | None = None,
    ) -> list[dict[str, Any]]: ...

    async def renew_claims(self, row_ids: list[str], *, consumer_name: str) -> int: ...

    async def ack(self, row_id: str, *, consumer_name: str) -> None: ...

    async def fail(
        self,
        row_id: str,
        error: str,
        *,
        consumer_name: str,
        max_attempts: int,
    ) -> None: ...

    async def dead_letter(
        self,
        row_id: str,
        error: str,
        *,
        consumer_name: str,
    ) -> None: ...

    async def prune(
        self,
        *,
        retention_age_seconds: float | None,
        retention_count: int | None,
    ) -> int: ...
