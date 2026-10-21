"""Internal contract shared by durable polling consumers."""

from __future__ import annotations

import logging
from typing import Any, Protocol

# The last_error a claim records on a delivery it charged because the claim
# that last ran it lapsed mid-dispatch: the consumer died or wedged in the
# listener. The consumer runs a delivery claimed with it alone (see
# _delivery_dispatch._DispatchGate), so a repeat crash charges only that one.
RECLAIMED_MID_DISPATCH = "reclaimed without completing (consumer crashed or wedged mid-dispatch)"


def log_dead_letter(
    logger: logging.Logger,
    *,
    row_id: str,
    event_type: str,
    target: str,
    group: str,
    attempts: int,
    reason: str,
) -> None:
    """Report a dead-lettered delivery in the one wording every broker logs."""
    logger.error(
        "delivery %s of %s on %s for group %s dead-lettered after %d attempt(s): %s",
        row_id,
        event_type,
        target,
        group,
        attempts,
        reason,
    )


def log_charged_reclaim(
    logger: logging.Logger,
    *,
    group: str,
    row_id: str,
    event_type: str,
    target: str,
    attempts: int,
    retry_in_s: float | None,
) -> None:
    """Report a claim's charge for a delivery whose listener had started.

    ``retry_in_s`` is None when the charge dead-lettered the delivery.
    """
    if retry_in_s is None:
        log_dead_letter(
            logger,
            row_id=row_id,
            event_type=event_type,
            target=target,
            group=group,
            attempts=attempts,
            reason=RECLAIMED_MID_DISPATCH,
        )
        return
    logger.warning(
        "delivery %s of %s on %s for group %s %s; recorded as attempt %d and "
        "redelivered alone in %.3gs",
        row_id,
        event_type,
        target,
        group,
        RECLAIMED_MID_DISPATCH,
        attempts,
        retry_in_s,
    )


class PollingBroker(Protocol):
    """Store operations required by the common polling lifecycle.

    Row IDs are opaque strings. An adapter may encode ownership generations in
    them, so consumers must pass IDs through unchanged.
    """

    async def subscribe(self, targets: list[str], group: str) -> None: ...

    async def stale_targets(
        self, group: str, targets: list[str] | tuple[str, ...]
    ) -> dict[str, int]: ...

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float,
        max_attempts: int | None = None,
        targets: list[str] | tuple[str, ...] | None = None,
        exclude_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Claim due rows and stale claims, except the stale claims in ``exclude_ids``:
        rows whose listener the caller is still running past their renew deadline.
        """
        ...

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
    ) -> bool:
        """Retry or dead-letter a row this consumer still owns; False when it no longer does."""
        ...

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
