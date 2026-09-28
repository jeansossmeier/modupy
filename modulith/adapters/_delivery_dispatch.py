"""Concurrent delivery and fenced completion for polling consumers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

from ..runtime import _runtime
from ._consumer_protocol import PollingBroker

_MAX_LEASE_EXTENSION_FACTOR = 10.0
_MAX_MALFORMED_ROW_LOGS = 10


class DeliveryDispatch:
    """Mixin implementing delivery for a claimed broker batch."""

    _broker: PollingBroker
    _bus: Any
    _serializer: Any
    _consumer_name: str
    _group: str
    _dispatch_concurrency: int
    _max_attempts: int
    _reclaim_stale_seconds: float
    _logger: logging.Logger

    def _should_stop(self) -> bool:
        raise NotImplementedError

    async def _cancel(self, task: asyncio.Task[None] | None, label: str) -> None:
        raise NotImplementedError

    def _mark_broker_failure(self, operation: str, target: str, exc: Exception) -> None:
        raise NotImplementedError

    def _mark_broker_recovered(self, operation: str, target: str) -> None:
        raise NotImplementedError

    async def _dispatch_batch(self, rows: list[dict[str, Any]]) -> None:
        """Dispatch a batch concurrently while renewing unfinished claims."""
        valid_rows: list[dict[str, Any]] = []
        in_flight: set[str] = set()
        malformed_count = 0
        for row in rows:
            row_id = row.get("id")
            if not isinstance(row_id, str) or not row_id.strip():
                malformed_count += 1
                if malformed_count <= _MAX_MALFORMED_ROW_LOGS:
                    self._logger.warning("claimed row has no valid string id -- skipping")
                continue
            valid_rows.append(row)
            in_flight.add(row_id)

        if malformed_count > _MAX_MALFORMED_ROW_LOGS:
            self._logger.warning(
                "skipped %d additional claimed rows with no valid string id",
                malformed_count - _MAX_MALFORMED_ROW_LOGS,
            )
        if not valid_rows:
            return

        renewer = asyncio.create_task(self._renew_loop(in_flight))
        semaphore = asyncio.Semaphore(self._dispatch_concurrency)
        try:
            async with asyncio.TaskGroup() as tasks:
                for row in valid_rows:
                    tasks.create_task(self._dispatch_guarded(row, semaphore, in_flight))
        finally:
            await self._cancel(renewer, "claim-renewal")

    async def _dispatch_guarded(
        self,
        row: dict[str, Any],
        semaphore: asyncio.Semaphore,
        in_flight: set[str],
    ) -> None:
        """Dispatch one row only while its opaque fencing ID remains owned."""
        row_id = cast(str, row["id"])
        target = str(row.get("target") or "<unknown>")
        try:
            async with semaphore:
                if self._should_stop():
                    return
                try:
                    renewed = await self._broker.renew_claims(
                        [row_id],
                        consumer_name=self._consumer_name,
                        start_dispatch=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._mark_broker_failure("renew", target, exc)
                    raise
                self._mark_broker_recovered("renew", target)
                if renewed == 0:
                    self._logger.debug(
                        "row %s no longer owned by %s -- skipping dispatch",
                        row_id,
                        self._consumer_name,
                    )
                    return
                await self._dispatch_one(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            # One malformed row or transient completion write must not kill the
            # long-running poll loop.
            self._logger.exception("dispatch crashed for row %s -- loop continues", row_id)
        finally:
            in_flight.discard(row_id)

    async def _renew_loop(self, in_flight: set[str]) -> None:
        """Renew claims for a bounded period so wedged listeners can be reclaimed."""
        interval = self._reclaim_stale_seconds / 3.0
        deadline = (
            asyncio.get_running_loop().time()
            + self._reclaim_stale_seconds * _MAX_LEASE_EXTENSION_FACTOR
        )
        while True:
            await asyncio.sleep(interval)
            row_ids = list(in_flight)
            if not row_ids:
                return
            if asyncio.get_running_loop().time() >= deadline:
                self._logger.error(
                    "claim renewal for group %s exceeded %.0fs with %d row(s) in flight",
                    self._group,
                    self._reclaim_stale_seconds * _MAX_LEASE_EXTENSION_FACTOR,
                    len(row_ids),
                )
                return
            try:
                renewed = await self._broker.renew_claims(
                    row_ids,
                    consumer_name=self._consumer_name,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mark_broker_failure("renew", self._group, exc)
                self._logger.exception(
                    "claim renewal failed for group %s -- retrying next tick",
                    self._group,
                )
                continue
            self._mark_broker_recovered("renew", self._group)
            still_ours = sum(row_id in in_flight for row_id in row_ids)
            if renewed < still_ours:
                self._logger.warning(
                    "renewed %d/%d claims for group %s",
                    renewed,
                    still_ours,
                    self._group,
                )

    async def _dispatch_one(self, row: dict[str, Any]) -> None:
        """Deserialize, deliver, then complete one claimed row."""
        row_id = cast(str, row["id"])
        target = str(row.get("target") or "<unknown>")
        event_type = row.get("event_type")
        payload = cast(bytes, row["payload"])
        if not event_type:
            self._logger.warning("message %s missing event_type -- dead-lettering", row_id)
            await self._dead_letter(row_id, "missing event_type", target)
            return
        try:
            event = self._serializer.deserialize(payload, event_type)
        except Exception as exc:
            self._logger.exception("undeserializable message %s -- dead-lettering", row_id)
            await self._dead_letter(row_id, f"deserialize failed: {exc}", target)
            return
        try:
            # Runtime.dispatch_local, not bus.publish: a message consumed from
            # another process must fire the same per-listener lifecycle hooks
            # (modulith_on_listener_dispatch / _error / _complete) an in-memory
            # publish does, or every listener invocation in a worker process is
            # a telemetry blind spot and a plugin alerting on
            # modulith_on_listener_error never sees a cross-process failure. It
            # deliberately skips the *publish* hooks and broker routing: this
            # process did not publish the event, and re-routing it to the
            # target it was just consumed from is an infinite redelivery loop.
            await _runtime.dispatch_local(event, self._bus)
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if self._should_stop() or (task is not None and task.cancelling() > 0):
                raise
            self._logger.warning("listener cancelled delivery for %s", row_id)
            await self._fail(row_id, "listener cancelled", target)
            return
        except Exception as exc:
            attempts = cast(int, row.get("attempts", 0))
            self._logger.warning(
                "dispatch failed for %s (attempt %d) -- %s",
                row_id,
                attempts + 1,
                exc,
            )
            await self._fail(row_id, str(exc), target)
            return
        try:
            await self._broker.ack(row_id, consumer_name=self._consumer_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("ack", target, exc)
            self._logger.exception("ack failed for %s -- message stays claimed", row_id)
        else:
            self._mark_broker_recovered("ack", target)

    async def _fail(self, row_id: str, error: str, target: str) -> None:
        try:
            await self._broker.fail(
                row_id,
                error,
                consumer_name=self._consumer_name,
                max_attempts=self._max_attempts,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("fail", target, exc)
            raise
        self._mark_broker_recovered("fail", target)

    async def _dead_letter(self, row_id: str, reason: str, target: str) -> None:
        try:
            await self._broker.dead_letter(
                row_id,
                reason,
                consumer_name=self._consumer_name,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("dead_letter", target, exc)
            raise
        self._mark_broker_recovered("dead_letter", target)
