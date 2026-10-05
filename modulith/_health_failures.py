"""Broker-operation failures that hold a consumer's health at ``degraded``.

Both consumer families (the Redis ``BrokerConsumer`` and the polling consumer
behind the SHM and database brokers) share one rule, documented in DEPLOYMENT
"Health Checks and Monitoring":

- A failure clears when the same operation next succeeds for the same target.
- A completion-write failure (ack, fail, dead_letter, renew) also expires once
  the redelivery window has passed. By then the affected message has been
  reclaimed and retried by this or another replica; if the write still fails,
  the retry records a fresh failure, so a persistent failure stays visible.
- A failure whose write started before the last recovery for the same key is
  dropped when its caller passes ``started_at`` to ``record``: concurrent
  writes finish out of order, and a slow write that fails after a later one
  succeeded says nothing new about the broker.
- The Redis consumer additionally drops a completion-write failure as soon as
  its message is no longer pending in the consumer group.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from dataclasses import dataclass

from .protocols import ConsumerHealth

COMPLETION_OPERATIONS = frozenset({"ack", "fail", "dead_letter", "renew"})


@dataclass(frozen=True)
class _Failure:
    error: str
    recorded_at: float
    message_id: str | None


class HealthFailures:
    """Failures keyed by ``(operation, target)`` with completion-write expiry."""

    def __init__(self, completion_expiry_s: float) -> None:
        self._completion_expiry_s = completion_expiry_s
        self._failures: dict[tuple[str, str], _Failure] = {}
        self._recovered_at: dict[tuple[str, str], float] = {}

    def record(
        self,
        operation: str,
        target: str,
        exc: Exception,
        message_id: str | None = None,
        *,
        started_at: float | None = None,
    ) -> None:
        """Hold ``exc`` against ``(operation, target)`` until it recovers or expires.

        ``started_at`` is the ``time.monotonic()`` reading taken before the failed
        write began. A write that began before the key's last recovery is dropped:
        the recovery shows the operation working after that write started.
        """
        key = (operation, target)
        if started_at is not None and started_at < self._recovered_at.get(key, -math.inf):
            return
        self._failures[key] = _Failure(str(exc), time.monotonic(), message_id)

    def recover(self, operation: str, target: str) -> None:
        key = (operation, target)
        self._recovered_at[key] = time.monotonic()
        self._failures.pop(key, None)

    def clear(self) -> None:
        self._failures.clear()
        self._recovered_at.clear()

    def pending_messages(self) -> Iterator[tuple[str, str, str]]:
        """Yield ``(operation, target, message_id)`` for message-scoped failures."""
        for (operation, target), failure in list(self._failures.items()):
            if failure.message_id is not None:
                yield operation, target, failure.message_id

    def resolve_message(self, operation: str, target: str, message_id: str) -> None:
        """Drop the failure if it still belongs to ``message_id``."""
        failure = self._failures.get((operation, target))
        if failure is not None and failure.message_id == message_id:
            del self._failures[(operation, target)]

    def degraded(self) -> ConsumerHealth | None:
        now = time.monotonic()
        for key, failure in list(self._failures.items()):
            if (
                key[0] in COMPLETION_OPERATIONS
                and now - failure.recorded_at >= self._completion_expiry_s
            ):
                del self._failures[key]
        if not self._failures:
            return None
        details = list(self._failures.items())
        detail = (
            details[0][1].error
            if len(details) == 1
            else "; ".join(
                f"{operation} ({target}): {failure.error}"
                for (operation, target), failure in details
            )
        )
        return ConsumerHealth(ready=False, status="degraded", detail=detail)
