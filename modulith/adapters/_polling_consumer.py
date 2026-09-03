"""Shared lifecycle for durable polling consumers."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any

from .._shutdown import DEFAULT_STOP_TIMEOUT_S, cancel_and_wait
from ..protocols import ConsumerHealth
from ._consumer_protocol import PollingBroker
from ._delivery_dispatch import DeliveryDispatch

_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 5.0
_BACKOFF_MAX_EXPONENT = 7
_IDLE_BACKOFF_CAP_S = 0.5
_DEFAULT_PRUNE_INTERVAL_S = 300.0
IdleWait = Callable[[float], Awaitable[None]]


class PollingConsumer(DeliveryDispatch):
    """Store-neutral poll, health, backoff, pruning, and shutdown lifecycle."""

    _stop_timeout_s: float = DEFAULT_STOP_TIMEOUT_S

    def __init__(
        self,
        *,
        broker: PollingBroker,
        bus: Any,
        serializer: Any,
        consumer_name: str,
        group: str,
        targets: list[str] | tuple[str, ...],
        poll_interval_s: float,
        batch_size: int,
        dispatch_concurrency: int,
        max_attempts: int,
        reclaim_stale_seconds: float,
        prune_interval_s: float | None,
        retention_age_seconds: float | None,
        retention_count: int | None,
        logger: logging.Logger,
        scheme: str,
        idle_backoff: bool,
        idle_wait: IdleWait | None = None,
        subscribe_when_empty: bool = False,
    ) -> None:
        self._broker = broker
        self._bus = bus
        self._serializer = serializer
        self._consumer_name = consumer_name
        self._group = group
        self._targets = list(targets)
        self._poll_interval_s = poll_interval_s
        self._batch_size = batch_size
        self._dispatch_concurrency = dispatch_concurrency
        self._max_attempts = max_attempts
        self._reclaim_stale_seconds = reclaim_stale_seconds
        self._prune_interval_s = prune_interval_s
        self._retention_age_seconds = retention_age_seconds
        self._retention_count = retention_count
        self._logger = logger
        self._scheme = scheme
        self._idle_backoff = idle_backoff
        self._idle_wait = idle_wait
        self._subscribe_when_empty = subscribe_when_empty
        self._task: asyncio.Task[None] | None = None
        self._prune_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._health = ConsumerHealth(ready=False, status="stopped")
        self._health_failures: dict[tuple[str, str], str] = {}
        self._consecutive_failures = 0

    async def start(self) -> None:
        """Subscribe once and launch the poll and optional prune loops."""
        if self._task is not None and not self._task.done():
            self._logger.debug(
                "%s consumer %r already started -- ignoring re-start",
                self._scheme,
                self._consumer_name,
            )
            return
        if self._task is not None:
            stale_task = self._task
            stale_prune_task = self._prune_task
            self._task = None
            self._prune_task = None
            # Consume a finished task's result before replacing it. Its done
            # callback already recorded and logged any poll-loop failure.
            if not stale_task.cancelled():
                stale_task.exception()
            await self._cancel(stale_prune_task, "prune")
        if not self._targets and not self._subscribe_when_empty:
            self._stopping = False
            self._health_failures.clear()
            self._health = ConsumerHealth(ready=True, status="ready")
            return
        self._stopping = False
        self._health_failures.clear()
        self._health = ConsumerHealth(ready=False, status="starting")
        try:
            await self._broker.subscribe(self._targets, self._group)
            if self._targets:
                self._task = asyncio.create_task(self._run())
                self._task.add_done_callback(self._on_task_done)
                if self._prune_enabled():
                    self._prune_task = asyncio.create_task(self._prune_loop())
        except Exception as exc:
            self._health = ConsumerHealth(ready=False, status="failed", detail=str(exc))
            raise
        if self._health.status == "starting":
            self._health = ConsumerHealth(ready=True, status="ready")
        self._logger.info(
            "%s consumer %r (group %r) subscribed to %d target(s)",
            self._scheme,
            self._consumer_name,
            self._group,
            len(self._targets),
        )

    def _prune_enabled(self) -> bool:
        if self._prune_interval_s is not None and self._prune_interval_s <= 0:
            return False
        return self._retention_age_seconds is not None or self._retention_count is not None

    def _should_stop(self) -> bool:
        return self._stopping

    async def stop(self) -> None:
        """Cancel all background work; safe before start and on repeated calls."""
        self._stopping = True
        try:
            await self._cancel(self._task, "poll")
        finally:
            self._task = None
            try:
                await self._cancel(self._prune_task, "prune")
            finally:
                self._prune_task = None
                self._health = ConsumerHealth(ready=False, status="stopped")

    def health(self) -> ConsumerHealth:
        """Return readiness plus independently tracked broker write failures."""
        if self._health.status != "ready":
            return self._health
        if self._targets and (self._task is None or self._task.done()):
            return ConsumerHealth(
                ready=False,
                status="failed",
                detail="poll loop is not running",
            )
        if self._health_failures:
            details = list(self._health_failures.items())
            detail = (
                details[0][1]
                if len(details) == 1
                else "; ".join(
                    f"{operation} ({target}): {error}" for (operation, target), error in details
                )
            )
            return ConsumerHealth(ready=False, status="degraded", detail=detail)
        return self._health

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping or task.cancelled():
            return
        error = task.exception()
        detail = str(error) if error is not None else "poll loop exited unexpectedly"
        self._health = ConsumerHealth(ready=False, status="failed", detail=detail)
        if error is None:
            self._logger.error("%s consumer poll task exited unexpectedly", self._scheme)
        else:
            self._logger.error(
                "%s consumer poll task exited with an unexpected error",
                self._scheme,
                exc_info=(type(error), error, error.__traceback__),
            )

    def _mark_broker_failure(self, operation: str, target: str, exc: Exception) -> None:
        self._health_failures[(operation, target)] = str(exc)

    def _mark_broker_recovered(self, operation: str, target: str) -> None:
        self._health_failures.pop((operation, target), None)

    async def _cancel(self, task: asyncio.Task[None] | None, label: str) -> None:
        if task is None:
            return
        await cancel_and_wait(
            task,
            timeout_s=self._stop_timeout_s,
            logger=self._logger,
            what=f"consumer {self._consumer_name!r} {label} task",
        )

    async def _run(self) -> None:
        idle_empty_streak = 0
        while not self._stopping:
            try:
                rows = await self._broker.claim_batch(
                    self._group,
                    batch_size=self._batch_size,
                    consumer_name=self._consumer_name,
                    reclaim_stale_seconds=self._reclaim_stale_seconds,
                    max_attempts=self._max_attempts,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mark_broker_failure("claim", self._group, exc)
                self._logger.exception("claim failed for group %s", self._group)
                await self._backoff_after_failure()
                continue
            self._consecutive_failures = 0
            self._mark_broker_recovered("claim", self._group)
            if rows:
                idle_empty_streak = 0
                await self._dispatch_batch(rows)
                continue
            idle_empty_streak += 1
            delay = self._poll_interval_s
            if self._idle_backoff:
                delay = min(
                    delay * (2.0 ** min(idle_empty_streak - 1, 5)),
                    max(self._poll_interval_s, _IDLE_BACKOFF_CAP_S),
                )
                delay += random.uniform(0.0, min(0.05, delay * 0.25))
            await self._wait_when_idle(delay)

    async def _wait_when_idle(self, safety_timeout: float) -> None:
        """Wait for an optional latency hint without extending the safety poll."""
        if self._idle_wait is None:
            await asyncio.sleep(safety_timeout)
            return
        await self._idle_wait(safety_timeout)

    async def _backoff_after_failure(self) -> None:
        self._consecutive_failures += 1
        exponent = min(self._consecutive_failures - 1, _BACKOFF_MAX_EXPONENT)
        delay = min(_BACKOFF_BASE_S * (2.0**exponent), _BACKOFF_CAP_S)
        await asyncio.sleep(delay)

    async def _prune_loop(self) -> None:
        interval = self._prune_interval_s or _DEFAULT_PRUNE_INTERVAL_S
        while True:
            await asyncio.sleep(interval * (0.75 + random.uniform(0.0, 0.5)))
            if self._should_stop():
                return
            try:
                await self._broker.prune(
                    retention_age_seconds=self._retention_age_seconds,
                    retention_count=self._retention_count,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception("prune failed for group %s -- loop continues", self._group)
