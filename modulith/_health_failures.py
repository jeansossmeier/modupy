"""Broker-operation failures that hold a consumer's health at ``degraded``.

Both consumer families (the Redis ``BrokerConsumer`` and the polling consumer
behind the SHM and database brokers) share one rule, documented in DEPLOYMENT
"Health Checks and Monitoring":

- A failure clears when the same operation next succeeds for the same target.
- A completion-write failure (ack, fail, dead_letter, renew) also expires once
  the redelivery window has passed. By then the affected message has been
  reclaimed and retried by this or another replica; if the write still fails,
  the retry records a fresh failure, so a persistent failure stays visible.
- The Redis consumer additionally drops a completion-write failure as soon as
  its message is no longer pending in the consumer group.
"""

from __future__ import annotations

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

    def record(
        self, operation: str, target: str, exc: Exception, message_id: str | None = None
    ) -> None:
        self._failures[(operation, target)] = _Failure(str(exc), time.monotonic(), message_id)

    def recover(self, operation: str, target: str) -> None:
        self._failures.pop((operation, target), None)

    def clear(self) -> None:
        self._failures.clear()

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
