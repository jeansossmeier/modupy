"""Cross-process broker consumer loop (process-per-module topology).

The *producer* half lives in ``runtime._maybe_route_to_broker``: a cross-module
event is serialized and XADD'd to the broker under its fully-qualified name.
This module is the *consumer* half — without it, events were durably written to
the broker and **never delivered**, silently dropping every cross-process event
(the headline feature of ``topology='processes'``).

Each worker hosts one module. ``BrokerConsumer`` subscribes to the broker
streams for exactly the event types that module's local listeners consume
(``InMemoryEventBus.registered_event_types``), then for each stream:

  * ``ensure_group`` — create the per-module consumer group (idempotent) at
    startup; re-issued from the poll loop whenever the broker reports the
    group is gone (NOGROUP — e.g. Redis restarted without a snapshot),
  * ``reclaim`` — runs at startup AND periodically on every poll cycle:
    XAUTOCLAIM redelivers messages left pending (claimed but never ACK'd)
    longer than ``reclaim_min_idle_ms``, whether the claiming peer crashed or
    is merely slow. Reclaim is purely idle-time based — it has NO
    crash/liveness detection — so a healthy peer whose dispatch takes longer
    than ``reclaim_min_idle_ms`` has its in-flight message claimed away and
    double-dispatched. Keep ``reclaim_min_idle_ms`` (default 60s) above the
    worst-case listener latency; listeners must be idempotent regardless
    (at-least-once delivery).
  * a background loop ``read`` → deserialize (via the ``event_type`` header) →
    dispatch to local listeners → ``ack`` on success, ``dead_letter`` on poison
    or repeated failure. Broker failures retry with capped exponential backoff
    rather than busy-spinning.

Consumer groups are **per consuming module** (``modulith-<module>``) so that an
event consumed by several modules reaches all of them — a single shared group
would hand each message to only one module.

Dispatch goes through ``Runtime.dispatch_local`` (not ``runtime.publish``) so a
consumed event reaches local listeners — firing the same per-listener
lifecycle hooks an in-memory publish does, so worker processes are not a
telemetry blind spot — WITHOUT being re-routed back to the broker, which would
be an infinite loop.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from ._health_failures import HealthFailures
from ._shutdown import DEFAULT_STOP_TIMEOUT_S, cancel_and_wait
from .brokers import _split_broker_target
from .config import ConfigurationError
from .manifest import get_manifest
from .protocols import ConsumerHealth
from .runtime import _runtime

logger = logging.getLogger("modulith.consumer")

# A message that fails to dispatch this many times is dead-lettered rather than
# redelivered forever. Deserialize failures / malformed messages are poison and
# dead-lettered immediately (retrying can never succeed).
_MAX_DELIVERY_ATTEMPTS = 5

# Capped exponential backoff for consecutive broker read()/reclaim() failures.
# Without it, a downed Redis triggered an unbounded busy-retry loop (~281
# failures/sec measured) that — when the broker raised
# synchronously — never even yielded to the event loop, starving every other
# coroutine in the process. 0.05s, 0.1s, 0.2s, … capped at 5s.
_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 5.0
# 0.05 * 2**7 = 6.4s already exceeds the cap — bound the exponent so the
# power stays a small number no matter how long the outage lasts.
_BACKOFF_MAX_EXPONENT = 7
# Health turns degraded when a read round stays pending this many block
# intervals (plus one second of slack for round-trip time).
_READ_STALL_POLL_INTERVALS = 5


def _as_str(value: Any) -> str:
    """Redis returns bytes; tests may pass str. Normalize either way."""
    return value.decode() if isinstance(value, bytes) else str(value)


def _optional_str(value: Any) -> str | None:
    return None if value is None else _as_str(value)


class BrokerConsumer:
    """Drives one worker's subscribed broker streams into its local bus."""

    def __init__(
        self,
        *,
        broker: Any,
        bus: Any,
        serializer: Any,
        consumer_name: str,
        group: str,
        targets: list[str],
        poll_block_ms: int = 1000,
        reclaim_min_idle_ms: int = 60_000,
        max_delivery_attempts: int = _MAX_DELIVERY_ATTEMPTS,
    ) -> None:
        self._broker = broker
        self._bus = bus
        self._serializer = serializer
        self._consumer_name = consumer_name
        self._group = group
        self._targets = list(targets)
        # Real Redis treats XREADGROUP BLOCK 0 as "block forever awaiting new
        # entries" (the opposite of the immediate-return some test fakes
        # modeled) — a non-positive value would hang a worker
        # indefinitely, so it never reaches the broker.
        if poll_block_ms <= 0:
            logger.warning(
                "poll_block_ms=%d is unsafe (Redis BLOCK 0 blocks forever) — clamped to 1ms",
                poll_block_ms,
            )
            poll_block_ms = 1
        self._poll_block_ms = poll_block_ms
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._max_delivery_attempts = max_delivery_attempts
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._health = ConsumerHealth(ready=False, status="stopped")
        self._read_started_at: float | None = None
        self._read_stall_logged_for: float | None = None
        self._read_stall_s = _READ_STALL_POLL_INTERVALS * poll_block_ms / 1000 + 1.0
        # The redelivery window: past it, reclaim retries the message.
        self._health_failures = HealthFailures(completion_expiry_s=reclaim_min_idle_ms / 1000)
        # Consecutive broker read()/reclaim() failures — drives the capped
        # exponential backoff. Reset on any broker success.
        self._consecutive_failures = 0
        # Fallback for third-party brokers without durable delivery metadata.
        # Redis Streams provides the authoritative count in its PEL, which
        # survives restarts and XAUTOCLAIM ownership handoffs.
        self._attempts: dict[tuple[str, str], int] = {}

    async def start(self) -> None:
        """Create consumer groups, recover pending messages, start the loop.

        No-op (no background task) when the worker consumes nothing — a leaf
        module with no @listener has no streams to read.
        """
        if not self._targets:
            self._stopping = False
            self._health_failures.clear()
            self._health = ConsumerHealth(ready=True, status="ready")
            logger.debug(
                "consumer %r has no subscribed streams — not starting", self._consumer_name
            )
            return
        self._stopping = False
        self._health_failures.clear()
        self._health = ConsumerHealth(ready=False, status="starting")
        try:
            for target in self._targets:
                await self._broker.ensure_group(target, self._group)
                await self._reclaim(target)
            self._task = asyncio.create_task(self._run())
            self._task.add_done_callback(self._on_task_done)
        except Exception as exc:
            self._health = ConsumerHealth(ready=False, status="failed", detail=str(exc))
            raise
        if self._health.status == "starting":
            self._health = ConsumerHealth(ready=True, status="ready")
        logger.info(
            "consumer %r (group %r) subscribed to %d stream(s)",
            self._consumer_name,
            self._group,
            len(self._targets),
        )

    _stop_timeout_s: float = DEFAULT_STOP_TIMEOUT_S

    async def stop(self) -> None:
        """Cancel the loop and wait for it to unwind, never longer than twice
        ``_stop_timeout_s``.

        Never raises for the task's own outcome: a task that already died with
        a real exception is logged, and one that ignores cancellation is
        abandoned rather than wedging shutdown.
        """
        self._stopping = True
        if self._task is not None:
            await cancel_and_wait(
                self._task,
                timeout_s=self._stop_timeout_s,
                logger=logger,
                what=f"consumer {self._consumer_name!r} task",
            )
            self._task = None
        self._health = ConsumerHealth(ready=False, status="stopped")

    def health(self) -> ConsumerHealth:
        """Return an immutable snapshot of the consumer's readiness."""
        if self._health.status != "ready":
            return self._health
        if self._targets and (self._task is None or self._task.done()):
            return ConsumerHealth(
                ready=False,
                status="failed",
                detail="poll loop is not running",
            )
        started = self._read_started_at
        if started is not None and time.monotonic() - started >= self._read_stall_s:
            detail = f"no broker read completed in {self._read_stall_s:g}s"
            if self._read_stall_logged_for != started:
                self._read_stall_logged_for = started
                logger.warning(
                    "consumer %r: %s on %s -- the broker is not answering; "
                    "health is degraded until a read completes",
                    self._consumer_name,
                    detail,
                    self._targets,
                )
            return ConsumerHealth(ready=False, status="degraded", detail=detail)
        return self._health_failures.degraded() or self._health

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        """Record an unexpected poll-loop exit without changing shutdown."""
        if self._stopping or task.cancelled():
            return
        error = task.exception()
        detail = str(error) if error is not None else "poll loop exited unexpectedly"
        self._health = ConsumerHealth(ready=False, status="failed", detail=detail)
        if error is None:
            logger.error("consumer %r task exited unexpectedly", self._consumer_name)
        else:
            logger.error(
                "consumer %r task exited with an unexpected error",
                self._consumer_name,
                exc_info=(type(error), error, error.__traceback__),
            )

    def _mark_broker_failure(
        self, operation: str, target: str, exc: Exception, message_id: str | None = None
    ) -> None:
        self._health_failures.record(operation, target, exc, message_id)

    def _mark_broker_recovered(self, operation: str, target: str) -> None:
        self._health_failures.recover(operation, target)

    async def _drop_resolved_completion_failures(self) -> None:
        """Clear completion failures whose message left the group's pending list."""
        delivery_attempts = getattr(self._broker, "delivery_attempts", None)
        if not callable(delivery_attempts):
            return
        for operation, target, message_id in self._health_failures.pending_messages():
            try:
                attempts = await delivery_attempts(target, message_id, self._group)
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            if attempts is None:
                self._health_failures.resolve_message(operation, target, message_id)

    async def _run(self) -> None:
        """Read → dispatch each subscribed stream until stopped.

        ``while True`` rather than ``while not self._stopping`` because the flag
        is flipped concurrently by ``stop()`` from another task — the explicit
        in-loop checks below are the real exit (plus task cancellation at the
        next ``await``); a ``while not self._stopping`` condition would let a
        type-checker wrongly prove those checks unreachable.
        """
        while True:
            if self._stopping:
                return
            # No per-target _stopping check: stop() also cancels this task, so
            # the in-flight reads below raise CancelledError and unwind
            # immediately — the top-of-loop check handles the rest.
            await self._drop_resolved_completion_failures()
            for target in self._targets:
                await self._reclaim(target)
            # Read every subscribed stream CONCURRENTLY. Each read blocks
            # server-side for up to poll_block_ms, so awaiting them one after
            # another made an idle worker's delivery latency scale with its
            # stream count — an event landing just after its own stream was
            # polled waited (N-1) * poll_block_ms for the cycle to come back
            # around (~9s for a module listening to 10 event types on the 1s
            # default). Concurrently, idle latency is one poll_block_ms no
            # matter how many streams there are, at the cost of holding one
            # broker connection per stream for the duration of the block.
            self._read_started_at = time.monotonic()
            try:
                batches = await asyncio.gather(*(self._read(target) for target in self._targets))
            finally:
                self._read_started_at = None
            # Dispatch stays sequential: only the *waiting* is parallel, so a
            # slow listener on one stream still cannot interleave with another.
            for target, messages in zip(self._targets, batches, strict=True):
                if messages is None:
                    continue
                try:
                    await self._handle(target, messages)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Safety net: _dispatch_one guards its own
                    # broker calls, but ANY escaped per-batch exception would
                    # otherwise kill this task permanently and silently. The
                    # unacked remainder stays pending and is retried via reclaim.
                    logger.exception("message handling failed for %s — loop continues", target)

    async def _read(self, target: str) -> Any:
        """One blocking read, or None when the broker failed (already handled).

        A transient read error must not kill the poll loop, so it is logged,
        recorded against health, and followed by the capped backoff — the same
        recovery ``_reclaim`` uses.
        """
        try:
            messages = await self._broker.read(
                target,
                consumer=self._consumer_name,
                group=self._group,
                block_ms=self._poll_block_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("read", target, exc)
            logger.exception("broker read failed for %s", target)
            await self._recover_after_broker_failure(target, exc)
            return None
        self._consecutive_failures = 0
        self._mark_broker_recovered("read", target)
        return messages

    async def _reclaim(self, target: str) -> None:
        """Recover and re-dispatch pending messages idle past the threshold."""
        try:
            result = await self._broker.reclaim(
                target,
                consumer=self._consumer_name,
                group=self._group,
                min_idle_ms=self._reclaim_min_idle_ms,
            )
        except Exception as exc:
            self._mark_broker_failure("reclaim", target, exc)
            logger.exception("reclaim failed for %s", target)
            await self._recover_after_broker_failure(target, exc)
            return
        self._consecutive_failures = 0
        self._mark_broker_recovered("reclaim", target)
        # XAUTOCLAIM returns (cursor, [(id, fields), ...], deleted_ids).
        # ``deleted_ids`` are messages that were still pending (delivered but
        # never ACK'd) yet no longer exist in the stream — MAXLEN trimmed them
        # out from under the PEL. They are PERMANENTLY LOST (at-least-once is
        # violated for them), so surface the loss loudly instead of silently
        # discarding the tuple element.
        claimed = result[1] if result and len(result) > 1 else []
        deleted = result[2] if result and len(result) > 2 else []
        if deleted:
            lost_ids = [_as_str(d) for d in deleted]
            logger.error(
                "%d pending message(s) on %s were trimmed from the stream before "
                "reclaim and are permanently lost (MAXLEN trim of unacked entries; "
                "increase max_stream_len or reduce processing latency): %s",
                len(lost_ids),
                target,
                lost_ids,
            )
            for lost in lost_ids:
                self._attempts.pop((target, lost), None)
        await self._dispatch_claimed(target, claimed)

    async def _dispatch_claimed(self, target: str, claimed: Any) -> None:
        """Dispatch reclaimed entries, skipping nil rows defensively.

        XAUTOCLAIM on Redis < 7.0 returns nil for a pending entry that was
        deleted from the stream (7.0+ moves these to the ``deleted`` reply
        element instead). There is nothing to dispatch for a nil row — skip
        it so one nil entry cannot kill the consumer task and crash-loop
        worker startup.
        """
        nil_entries = 0
        for entry in claimed:
            if entry is None:
                nil_entries += 1
                continue
            message_id, fields = entry
            if fields is None:
                nil_entries += 1
                continue
            await self._dispatch_one(target, message_id, fields)
        if nil_entries:
            logger.info(
                "reclaim on %s returned %d nil entr(y/ies) (Redis < 7.0 "
                "reporting stream-deleted pending messages) — skipped",
                target,
                nil_entries,
            )

    async def _recover_after_broker_failure(self, target: str, exc: Exception) -> None:
        """Backoff + NOGROUP recovery after a failed broker ``read``/``reclaim``.

        * NOGROUP means the broker lost the stream/consumer-group state (e.g.
          Redis restarted without a snapshot). ``ensure_group`` was previously
          issued exactly once at ``start()``, so a recovered-but-empty Redis
          stalled consumption permanently and silently —
          re-issue it here so the next read()/reclaim() can succeed. A failure
          to re-create (broker still down) is logged and retried next cycle.
        * Sleep with capped exponential backoff so an outage degrades to
          periodic retries instead of a CPU-bound spin. The
          sleep also guarantees the loop yields control even when the broker
          raises synchronously, so this task can never starve the event loop.
        """
        self._consecutive_failures += 1
        if "NOGROUP" in str(exc):
            try:
                await self._broker.ensure_group(target, self._group)
                logger.warning(
                    "re-created consumer group %r on %s after NOGROUP "
                    "(broker lost stream/group state)",
                    self._group,
                    target,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("re-creating consumer group for %s failed — will retry", target)
        exponent = min(self._consecutive_failures - 1, _BACKOFF_MAX_EXPONENT)
        delay = min(_BACKOFF_BASE_S * (2**exponent), _BACKOFF_CAP_S)
        await asyncio.sleep(delay)

    async def _handle(self, target: str, messages: Any) -> None:
        """Process an XREADGROUP result: [(stream, [(id, fields), ...]), ...]."""
        if not messages:
            return
        for _stream, entries in messages:
            for message_id, fields in entries:
                if self._stopping:
                    return
                await self._dispatch_one(target, message_id, fields)

    async def _dispatch_one(self, target: str, message_id: Any, fields: dict[bytes, bytes]) -> None:
        """Deserialize one message and dispatch it to local listeners.

        ACKs on success. Malformed/undeserializable messages are poison →
        dead-lettered immediately. Dispatch failures are NOT ACK'd (so they stay
        pending for reclaim/redelivery) until they exceed the attempt cap, then
        dead-lettered.
        """
        mid = _as_str(message_id)
        key = (target, mid)
        data = fields.get(b"data")
        event_type = fields.get(b"h:event_type")
        if data is None or event_type is None:
            logger.warning(
                "message %s on %s missing data/event_type header — dead-lettering", mid, target
            )
            await self._dead_letter(target, mid, fields)
            return

        try:
            event = self._serializer.deserialize(data, _as_str(event_type))
        except Exception:
            logger.exception("undeserializable message %s on %s — dead-lettering", mid, target)
            await self._dead_letter(target, mid, fields)
            return

        try:
            await _runtime.dispatch_local(
                event,
                self._bus,
                traceparent=_optional_str(fields.get(b"h:traceparent")),
                tracestate=_optional_str(fields.get(b"h:tracestate")),
            )
        except Exception:
            attempts = await self._failed_delivery_attempts(target, mid, key)
            if attempts is None:
                return
            if attempts >= self._max_delivery_attempts:
                logger.exception(
                    "message %s on %s failed %d dispatch attempts — dead-lettering",
                    mid,
                    target,
                    attempts,
                )
                await self._dead_letter(target, mid, fields)
            else:
                # Leave unacked: it stays pending and is retried via reclaim.
                logger.warning(
                    "dispatch failed for %s on %s (attempt %d/%d) — will retry on redelivery",
                    mid,
                    target,
                    attempts,
                    self._max_delivery_attempts,
                )
            return

        try:
            await self._broker.ack(target, mid, self._group)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Broker-side blip must not kill the loop. The
            # un-ACK'd message stays pending → redelivered via reclaim; the
            # listener side must be idempotent anyway (at-least-once contract).
            self._mark_broker_failure("ack", target, exc, mid)
            logger.exception(
                "ack failed for %s on %s — message stays pending and will be redelivered",
                mid,
                target,
            )
        else:
            self._mark_broker_recovered("ack", target)
        self._attempts.pop(key, None)

    async def _failed_delivery_attempts(
        self, target: str, message_id: str, key: tuple[str, str]
    ) -> int | None:
        """Get the retry budget from durable metadata, or a local fallback."""
        delivery_attempts = getattr(self._broker, "delivery_attempts", None)
        if not callable(delivery_attempts):
            attempts = self._attempts.get(key, 0) + 1
            self._attempts[key] = attempts
            return attempts
        try:
            attempts = await delivery_attempts(target, message_id, self._group)
        except Exception:
            logger.exception(
                "could not read durable delivery attempts for %s on %s — leaving pending",
                message_id,
                target,
            )
            return None
        if attempts is None:
            logger.warning(
                "durable delivery metadata vanished for %s on %s — leaving pending",
                message_id,
                target,
            )
            return None
        return int(attempts)

    async def _dead_letter(self, target: str, mid: str, fields: dict[bytes, bytes]) -> None:
        """dead_letter via the broker, never letting a broker blip escape.

        A raise from broker.dead_letter() previously propagated out of the
        consumer task and killed the whole loop permanently.
        On failure the message stays pending (dead_letter ACKs only on
        success), so reclaim redelivers it and dead-lettering is retried; the
        attempt counter is only cleared on success so the retry dead-letters
        immediately rather than restarting the attempt cap.
        """
        try:
            await self._broker.dead_letter(target, mid, fields, self._group)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mark_broker_failure("dead_letter", target, exc, mid)
            logger.exception(
                "dead-letter failed for %s on %s — message stays pending for retry",
                mid,
                target,
            )
            return
        self._mark_broker_recovered("dead_letter", target)
        self._attempts.pop((target, mid), None)


def _broker_destination(target: object, broker_scheme: str) -> str:
    """Validate a full broker target and return its backend destination."""
    if type(target) is not str:
        raise ConfigurationError(
            f"invalid broker target {target!r}; expected non-empty 'scheme:destination'"
        )
    scheme, destination = _split_broker_target(target)
    if not scheme or not destination:
        raise ConfigurationError(
            f"invalid broker target {target!r}; expected non-empty 'scheme:destination'"
        )
    if scheme != broker_scheme:
        raise ConfigurationError(
            f"broker target {target!r} uses scheme {scheme!r}, "
            f"but the configured broker is {broker_scheme!r}"
        )
    return destination


def consumer_targets(bus: Any, cfg: Any, module_name: str) -> list[str]:
    """Resolve one worker's ordered backend subscription destinations."""
    event_types = _runtime.local_event_types(bus)
    full_targets = [
        getattr(
            event_type,
            "__modulith_broker_target__",
            f"{cfg.broker}:{event_type.__module__}.{event_type.__qualname__}",
        )
        for event_type in event_types
    ]

    if cfg.subscription_source == "manifest":
        package = f"{cfg.package}.{module_name}" if cfg.package else module_name
        manifest = get_manifest(package)
        declarations = manifest.broker_targets if manifest is not None else ()
    elif cfg.subscription_source == "config":
        declarations = cfg.subscriptions.get(module_name, ())
    elif cfg.subscription_source == "listener":
        declarations = tuple(
            target
            for event_type in event_types
            for handler in _runtime.local_listeners(bus.listeners_for(event_type))
            for target in getattr(handler, "__modulith_broker_targets__", ())
        )
    else:
        raise ConfigurationError(
            f"invalid subscription_source {cfg.subscription_source!r}; "
            "expected one of: manifest, config, listener"
        )

    destinations: list[str] = []
    seen: set[str] = set()
    for target in (*full_targets, *declarations):
        destination = _broker_destination(target, cfg.broker)
        if destination not in seen:
            seen.add(destination)
            destinations.append(destination)
    return destinations
