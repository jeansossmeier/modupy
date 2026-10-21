"""Concurrent delivery and fenced completion for polling consumers."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable

from .._consumer import is_foreign_event_type
from ..runtime import _PUBLISHER_MODULE_HEADER, _runtime
from ._consumer_protocol import RECLAIMED_MID_DISPATCH, PollingBroker, log_dead_letter

_MAX_LEASE_EXTENSION_FACTOR = 10.0
_MAX_MALFORMED_ROW_LOGS = 10
_MAX_STUCK_ROWS_NAMED = 5
# Same cap as the outbox's publication.last_error (modulith/builtin/outbox.py).
_MAX_STORED_ERROR_CHARS = 500


def _describe_rows(rows: list[dict[str, Any]]) -> str:
    named = ", ".join(
        f"{row.get('event_type') or '<unknown event>'} on {row.get('target') or '<unknown>'}"
        f" (row {row['id']})"
        for row in rows[:_MAX_STUCK_ROWS_NAMED]
    )
    extra = len(rows) - _MAX_STUCK_ROWS_NAMED
    return f"{named} and {extra} more" if extra > 0 else named


@dataclass(eq=False)
class _Admission:
    """One claimed row's way through the ``_DispatchGate``."""

    row: dict[str, Any]
    claimed_at: float
    started_at: float | None = None
    dispatch_started: bool = False

    @property
    def suspect(self) -> bool:
        return self.row.get("last_error") == RECLAIMED_MID_DISPATCH


class _DispatchGate:
    """Hands claimed rows to their listeners in claim order.

    At most ``limit`` listeners run at once. A suspect row, one a claim charged
    because the consumer that last ran it died or wedged mid-dispatch, starts
    only once no other listener is live, and nothing starts while a suspect is
    live: if it kills the process again, the next claim charges it alone, not
    the healthy rows that would have run beside it. A listener is live for
    ``stuck_after_s`` after it starts. Past that its claim is no longer
    renewed, so a claim charges it whatever runs beside it, and it no longer
    holds a suspect back; it keeps its slot until it returns.
    """

    def __init__(self, limit: int, stuck_after_s: float) -> None:
        self._limit = limit
        self._stuck_after_s = stuck_after_s
        self._queue: deque[_Admission] = deque()
        self.running: set[_Admission] = set()
        self.last_start = -math.inf
        self._waiters: set[asyncio.Future[None]] = set()

    def admit(self, admission: _Admission) -> None:
        self._queue.append(admission)

    def has_room(self) -> bool:
        """Whether a row claimed now would start at once."""
        return not self._queue and self._wait_s(suspect=False) == 0.0

    def room_wait_s(self) -> float:
        """How long until time alone makes room: a live suspect turning stuck."""
        return math.inf if self._queue else self._wait_s(suspect=False)

    async def enter(self, admission: _Admission) -> None:
        """Wait for ``admission``'s turn, then count its listener as running."""
        if admission not in self._queue:
            self._queue.append(admission)
        try:
            while True:
                head = self._queue[0] is admission
                wait_s = self._wait_s(admission.suspect) if head else math.inf
                if wait_s == 0.0:
                    break
                await self.changed(wait_s)
        except BaseException:
            self.leave(admission)
            raise
        self._queue.popleft()
        admission.started_at = self.last_start = time.monotonic()
        self.running.add(admission)
        self._notify()

    def leave(self, admission: _Admission) -> None:
        """Forget ``admission``, started or not."""
        self.running.discard(admission)
        if admission in self._queue:
            self._queue.remove(admission)
        self._notify()

    async def changed(self, timeout_s: float) -> None:
        """Return once a row starts or leaves, or after ``timeout_s``."""
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.add(waiter)
        try:
            await asyncio.wait({waiter}, timeout=None if math.isinf(timeout_s) else timeout_s)
        finally:
            self._waiters.discard(waiter)

    def _wait_s(self, suspect: bool) -> float:
        """0.0 when a row may start now, else how long until time alone can change that."""
        if len(self.running) >= self._limit:
            return math.inf
        now = time.monotonic()
        live_since: list[float] = []
        for admission in self.running:
            started_at = cast(float, admission.started_at)
            if (suspect or admission.suspect) and now - started_at < self._stuck_after_s:
                live_since.append(started_at)
        return min(live_since) + self._stuck_after_s - now if live_since else 0.0

    def _notify(self) -> None:
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)


@runtime_checkable
class _ClaimReleaser(Protocol):
    async def release_claims(self, row_ids: list[str], *, consumer_name: str) -> int: ...


@runtime_checkable
class _InterruptedClaimReleaser(Protocol):
    async def release_interrupted_claims(
        self, row_ids: list[str], *, consumer_name: str
    ) -> int: ...


def _row_headers(row: dict[str, Any]) -> dict[str, str]:
    """The claimed row's string headers; the database broker stores them as a JSON text blob."""
    headers = row.get("headers")
    if isinstance(headers, str):
        try:
            headers = json.loads(headers)
        except ValueError:
            return {}
    if not isinstance(headers, dict):
        return {}
    return {k: v for k, v in headers.items() if isinstance(k, str) and isinstance(v, str)}


class DeliveryDispatch:
    """Mixin implementing delivery for claimed broker batches."""

    _broker: PollingBroker
    _bus: Any
    _serializer: Any
    _consumer_name: str
    _group: str
    _dispatch_concurrency: int
    _max_attempts: int
    _reclaim_stale_seconds: float
    _logger: logging.Logger
    _gate: _DispatchGate
    _batches: set[asyncio.Task[None]]

    def _renew_deadline_s(self) -> float:
        return self._reclaim_stale_seconds * _MAX_LEASE_EXTENSION_FACTOR

    def _stuck_dispatch_detail(self) -> str | None:
        """Describe rows whose listener outlived the renew deadline, or None."""
        cutoff = time.monotonic() - self._renew_deadline_s()
        stuck = sorted(
            (a for a in self._gate.running if cast(float, a.started_at) <= cutoff),
            key=lambda a: cast(float, a.started_at),
        )
        if not stuck:
            return None
        return (
            f"listener has not returned after {self._renew_deadline_s():g}s for "
            f"{_describe_rows([a.row for a in stuck])}; its claim is no longer renewed "
            "and it holds a dispatch slot until it returns"
        )

    def _should_stop(self) -> bool:
        raise NotImplementedError

    async def _cancel(self, task: asyncio.Task[None] | None, label: str) -> None:
        raise NotImplementedError

    def _mark_broker_failure(
        self, operation: str, target: str, exc: Exception, started_at: float | None = None
    ) -> None:
        raise NotImplementedError

    def _mark_broker_recovered(self, operation: str, target: str) -> None:
        raise NotImplementedError

    async def _dispatch_batch(self, rows: list[dict[str, Any]]) -> None:
        """Dispatch a claimed batch and wait until every row of it is done."""
        batch = self._start_batch(rows)
        if batch is not None:
            await batch

    def _start_batch(self, rows: list[dict[str, Any]]) -> asyncio.Task[None] | None:
        """Queue a claimed batch on the dispatch gate and start delivering it.

        The rows are queued before this returns, so ``_gate.has_room()`` counts
        them before the poll loop claims again.
        """
        valid_rows: list[dict[str, Any]] = []
        malformed_count = 0
        for row in rows:
            row_id = row.get("id")
            if not isinstance(row_id, str) or not row_id.strip():
                malformed_count += 1
                if malformed_count <= _MAX_MALFORMED_ROW_LOGS:
                    self._logger.warning("claimed row has no valid string id -- skipping")
                continue
            valid_rows.append(row)

        if malformed_count > _MAX_MALFORMED_ROW_LOGS:
            self._logger.warning(
                "skipped %d additional claimed rows with no valid string id",
                malformed_count - _MAX_MALFORMED_ROW_LOGS,
            )
        if not valid_rows:
            return None
        claimed_at = time.monotonic()
        admissions = [_Admission(row, claimed_at) for row in valid_rows]
        for admission in admissions:
            self._gate.admit(admission)
        batch = asyncio.create_task(self._dispatch_admitted(admissions))
        self._batches.add(batch)
        batch.add_done_callback(self._batches.discard)
        return batch

    async def _dispatch_admitted(self, admissions: list[_Admission]) -> None:
        """Dispatch queued rows as the gate admits them while renewing their claims.

        Rows a stop kept from their listeners, by skipping them or by cancelling
        the batch while they still waited for a slot, are handed back on the way out.
        """
        in_flight = {cast(str, admission.row["id"]): admission for admission in admissions}
        renewer = asyncio.create_task(self._renew_loop(in_flight))
        try:
            async with asyncio.TaskGroup() as tasks:
                for admission in admissions:
                    tasks.create_task(self._dispatch_guarded(admission, in_flight))
        finally:
            # A dispatch cancelled before its first step never reached its own cleanup.
            for admission in admissions:
                self._gate.leave(admission)
            await self._cancel(renewer, "claim-renewal")
            unstarted = [
                cast(str, admission.row["id"])
                for admission in admissions
                if not admission.dispatch_started
            ]
            if unstarted:
                await self._release_unstarted(unstarted)

    async def _release_unstarted(self, row_ids: list[str]) -> None:
        """Hand rows a stop kept from their listeners back to the group at once,
        instead of leaving them to a stale reclaim ``_reclaim_stale_seconds`` later."""
        if not isinstance(self._broker, _ClaimReleaser):
            return
        try:
            await self._broker.release_claims(row_ids, consumer_name=self._consumer_name)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.warning(
                "could not release %d claimed rows the stop kept from their listeners; "
                "another consumer claims them after %gs",
                len(row_ids),
                self._reclaim_stale_seconds,
                exc_info=True,
            )

    async def _release_interrupted(self, row_id: str, ran_s: float) -> None:
        """Hand back, uncharged, a row whose listener a stop cancelled.

        A listener that ran past the renew deadline counts as hung: its row
        stays claimed, so the next reclaim charges the attempt and a listener
        that hangs on every delivery still ends dead-lettered. A broker
        without ``release_interrupted_claims`` keeps that charge for every
        cancelled row.
        """
        if ran_s >= self._renew_deadline_s():
            return
        if not isinstance(self._broker, _InterruptedClaimReleaser):
            return
        try:
            await self._broker.release_interrupted_claims(
                [row_id], consumer_name=self._consumer_name
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.warning(
                "could not release row %s whose listener the stop cancelled; "
                "another consumer claims it after %gs and charges an attempt",
                row_id,
                self._reclaim_stale_seconds,
                exc_info=True,
            )

    async def _dispatch_guarded(
        self,
        admission: _Admission,
        in_flight: dict[str, _Admission],
    ) -> None:
        """Dispatch one row in its turn, only while its opaque fencing ID remains owned."""
        row = admission.row
        row_id = cast(str, row["id"])
        target = str(row.get("target") or "<unknown>")
        try:
            await self._gate.enter(admission)
            if self._should_stop():
                return
            admission.dispatch_started = True
            started_at = time.monotonic()
            try:
                renewed = await self._broker.renew_claims(
                    [row_id],
                    consumer_name=self._consumer_name,
                    start_dispatch=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._mark_broker_failure("renew", target, exc, started_at)
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
            self._gate.leave(admission)
            in_flight.pop(row_id, None)

    async def _renew_loop(self, in_flight: dict[str, _Admission]) -> None:
        """Renew claims for a bounded time per row so a wedged listener's row can be reclaimed.

        A row is renewed until ``_renew_deadline_s()`` after its listener
        started. A row still waiting for a dispatch slot is renewed while the
        consumer keeps starting rows, until that long after the last start.
        """
        interval = self._reclaim_stale_seconds / 3.0
        deadline_s = self._renew_deadline_s()
        reported: set[str] = set()
        while True:
            await asyncio.sleep(interval)
            if not in_flight:
                return
            now = time.monotonic()
            row_ids: list[str] = []
            stuck: list[dict[str, Any]] = []
            for row_id, admission in list(in_flight.items()):
                started_at = admission.started_at
                anchor = (
                    max(admission.claimed_at, self._gate.last_start)
                    if started_at is None
                    else started_at
                )
                if now - anchor < deadline_s:
                    row_ids.append(row_id)
                elif started_at is not None and row_id not in reported:
                    reported.add(row_id)
                    stuck.append(admission.row)
            if stuck:
                self._logger.error(
                    "claim renewal for group %s stopped after %gs for %d row(s) whose "
                    "listener is still running: %s -- health is degraded, a reclaim "
                    "may deliver them again, and each holds a dispatch slot until its "
                    "listener returns",
                    self._group,
                    deadline_s,
                    len(stuck),
                    _describe_rows(stuck),
                )
            if not row_ids:
                continue
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
        if is_foreign_event_type(event_type, self._bus):
            self._logger.debug(
                "message %s has event type %s, which this module has no listener for -- completing",
                row_id,
                event_type,
            )
            await self._ack(row_id, target)
            return
        try:
            event = self._serializer.deserialize(payload, event_type)
        except Exception as exc:
            self._logger.exception("undeserializable message %s -- dead-lettering", row_id)
            await self._dead_letter(row_id, f"deserialize failed: {exc}", target)
            return
        dispatched_at = time.monotonic()
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
            headers = _row_headers(row)
            await _runtime.dispatch_local(
                event,
                self._bus,
                traceparent=headers.get("traceparent"),
                tracestate=headers.get("tracestate"),
                publisher_module=headers.get(_PUBLISHER_MODULE_HEADER),
            )
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if self._should_stop() or (task is not None and task.cancelling() > 0):
                await self._release_interrupted(row_id, time.monotonic() - dispatched_at)
                raise
            self._logger.warning("listener cancelled delivery for %s", row_id)
            if await self._fail(row_id, "listener cancelled", target):
                self._log_if_last_attempt(row, event_type, target, "listener cancelled")
            return
        except Exception as exc:
            attempts = cast(int, row.get("attempts", 0))
            self._logger.warning(
                "dispatch failed for %s (attempt %d) -- %s",
                row_id,
                attempts + 1,
                exc,
            )
            if await self._fail(row_id, str(exc), target):
                self._log_if_last_attempt(row, event_type, target, str(exc))
            return
        await self._ack(row_id, target)

    def _log_if_last_attempt(
        self, row: dict[str, Any], event_type: str, target: str, error: str
    ) -> None:
        """Log at ERROR when the failure just recorded used the delivery's last attempt.

        The broker's ``fail`` dead-letters a delivery once its attempts reach
        ``max_attempts``, counted from the same ``attempts`` the claim returned.
        """
        attempts = cast(int, row.get("attempts", 0)) + 1
        if attempts >= self._max_attempts:
            log_dead_letter(
                self._logger,
                row_id=row["id"],
                event_type=event_type,
                target=target,
                group=self._group,
                attempts=attempts,
                reason=error,
            )

    async def _ack(self, row_id: str, target: str) -> None:
        started_at = time.monotonic()
        try:
            await self._broker.ack(row_id, consumer_name=self._consumer_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("ack", target, exc, started_at)
            self._logger.exception("ack failed for %s -- message stays claimed", row_id)
        else:
            self._mark_broker_recovered("ack", target)

    async def _fail(self, row_id: str, error: str, target: str) -> bool:
        """Record a failed delivery; False when the claim was lost and nothing was written."""
        started_at = time.monotonic()
        try:
            recorded = await self._broker.fail(
                row_id,
                error[:_MAX_STORED_ERROR_CHARS],
                consumer_name=self._consumer_name,
                max_attempts=self._max_attempts,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("fail", target, exc, started_at)
            raise
        self._mark_broker_recovered("fail", target)
        return recorded

    async def _dead_letter(self, row_id: str, reason: str, target: str) -> None:
        started_at = time.monotonic()
        try:
            await self._broker.dead_letter(
                row_id,
                reason[:_MAX_STORED_ERROR_CHARS],
                consumer_name=self._consumer_name,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("dead_letter", target, exc, started_at)
            raise
        self._mark_broker_recovered("dead_letter", target)
