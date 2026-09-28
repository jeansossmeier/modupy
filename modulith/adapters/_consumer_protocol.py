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
        targets: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]: ...

    async def renew_claims(
        self,
        row_ids: list[str],
        *,
        consumer_name: str,
        start_dispatch: bool = False,
    ) -> int:
        """Extend owned claims; ``start_dispatch`` also marks dispatch started.

        A stale reclaim charges an attempt only to rows marked started, so
        rows claimed but never handed to a listener keep their retry budget.
        """
        ...

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
