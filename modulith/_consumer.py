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

Dispatch is via the in-memory bus directly (not ``runtime.publish``) so a
consumed event is delivered to local listeners WITHOUT being re-routed back to
the broker — that would be an infinite loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("modulith.consumer")

# A message that fails to dispatch this many times is dead-lettered rather than
# redelivered forever. Deserialize failures / malformed messages are poison and
# dead-lettered immediately (retrying can never succeed).
_MAX_DELIVERY_ATTEMPTS = 5

# Capped exponential backoff for consecutive broker read()/reclaim() failures.
# Without it, a downed Redis triggered an unbounded busy-retry loop (~281
# failures/sec measured, audit A7-r1-23) that — when the broker raised
# synchronously — never even yielded to the event loop, starving every other
# coroutine in the process. 0.05s, 0.1s, 0.2s, … capped at 5s.
_BACKOFF_BASE_S = 0.05
_BACKOFF_CAP_S = 5.0
# 0.05 * 2**7 = 6.4s already exceeds the cap — bound the exponent so the
# power stays a small number no matter how long the outage lasts.
_BACKOFF_MAX_EXPONENT = 7


def _as_str(value: Any) -> str:
    """Redis returns bytes; tests may pass str. Normalize either way."""
    return value.decode() if isinstance(value, bytes) else str(value)


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
    ) -> None:
        self._broker = broker
        self._bus = bus
        self._serializer = serializer
        self._consumer_name = consumer_name
        self._group = group
        self._targets = list(targets)
        # Real Redis treats XREADGROUP BLOCK 0 as "block forever awaiting new
        # entries" (the opposite of the immediate-return some test fakes
        # modeled, audit S3-r3-163) — a non-positive value would hang a worker
        # indefinitely, so it never reaches the broker.
        if poll_block_ms <= 0:
            logger.warning(
                "poll_block_ms=%d is unsafe (Redis BLOCK 0 blocks forever) — clamped to 1ms",
                poll_block_ms,
            )
            poll_block_ms = 1
        self._poll_block_ms = poll_block_ms
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        # Consecutive broker read()/reclaim() failures — drives the capped
        # exponential backoff (audit A7-r1-23). Reset on any broker success.
        self._consecutive_failures = 0
        # (target, message id) -> failed dispatch attempts, so a transiently-
        # failing message dead-letters after _MAX_DELIVERY_ATTEMPTS rather than
        # looping. Keyed by target TOO because Redis stream ids are stream-local
        # — two different streams can carry the identical id, and a mid-only key
        # let them contaminate each other's counters (audit A7-r3-138).
        self._attempts: dict[tuple[str, str], int] = {}

    async def start(self) -> None:
        """Create consumer groups, recover pending messages, start the loop.

        No-op (no background task) when the worker consumes nothing — a leaf
        module with no @listener has no streams to read.
        """
        if not self._targets:
            logger.debug(
                "consumer %r has no subscribed streams — not starting", self._consumer_name
            )
            return
        for target in self._targets:
            await self._broker.ensure_group(target, self._group)
            await self._reclaim(target)
        self._task = asyncio.create_task(self._run())
        logger.info(
            "consumer %r (group %r) subscribed to %d stream(s)",
            self._consumer_name,
            self._group,
            len(self._targets),
        )

    async def stop(self) -> None:
        """Cancel the loop and wait for it to unwind.

        Never raises: a task that already died with a real exception (not
        CancelledError) would otherwise re-raise it here at shutdown time
        (audit A7-r1-22) — it is logged instead.
        """
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(
                    "consumer %r task had already died with an unexpected error",
                    self._consumer_name,
                )
            self._task = None

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
            for target in self._targets:
                # No per-target _stopping check: stop() also cancels this task,
                # so the in-flight read below raises CancelledError and unwinds
                # immediately — the top-of-loop check handles the rest.
                await self._reclaim(target)
                try:
                    messages = await self._broker.read(
                        target,
                        consumer=self._consumer_name,
                        group=self._group,
                        block_ms=self._poll_block_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # a transient broker read error must not kill the loop
                    logger.exception("broker read failed for %s", target)
                    await self._recover_after_broker_failure(target, exc)
                    continue
                self._consecutive_failures = 0
                try:
                    await self._handle(target, messages)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Safety net (audit A7-r1-22): _dispatch_one guards its own
                    # broker calls, but ANY escaped per-batch exception would
                    # otherwise kill this task permanently and silently. The
                    # unacked remainder stays pending and is retried via reclaim.
                    logger.exception("message handling failed for %s — loop continues", target)

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
            logger.exception("reclaim failed for %s", target)
            await self._recover_after_broker_failure(target, exc)
            return
        self._consecutive_failures = 0
        # XAUTOCLAIM returns (cursor, [(id, fields), ...], deleted_ids).
        # ``deleted_ids`` are messages that were still pending (delivered but
        # never ACK'd) yet no longer exist in the stream — MAXLEN trimmed them
        # out from under the PEL. They are PERMANENTLY LOST (at-least-once is
        # violated for them), so surface the loss loudly instead of silently
        # discarding the tuple element (audit A7-r1-21).
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
        for message_id, fields in claimed:
            await self._dispatch_one(target, message_id, fields)

    async def _recover_after_broker_failure(self, target: str, exc: Exception) -> None:
        """Backoff + NOGROUP recovery after a failed broker ``read``/``reclaim``.

        * NOGROUP means the broker lost the stream/consumer-group state (e.g.
          Redis restarted without a snapshot). ``ensure_group`` was previously
          issued exactly once at ``start()``, so a recovered-but-empty Redis
          stalled consumption permanently and silently (audit A7-r2-91) —
          re-issue it here so the next read()/reclaim() can succeed. A failure
          to re-create (broker still down) is logged and retried next cycle.
        * Sleep with capped exponential backoff so an outage degrades to
          periodic retries instead of a CPU-bound spin (audit A7-r1-23). The
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
            await self._bus.publish(event)
        except Exception:
            attempts = self._attempts.get(key, 0) + 1
            self._attempts[key] = attempts
            if attempts >= _MAX_DELIVERY_ATTEMPTS:
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
                    _MAX_DELIVERY_ATTEMPTS,
                )
            return

        try:
            await self._broker.ack(target, mid, self._group)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Broker-side blip must not kill the loop (audit A7-r1-22). The
            # un-ACK'd message stays pending → redelivered via reclaim; the
            # listener side must be idempotent anyway (at-least-once contract).
            logger.exception(
                "ack failed for %s on %s — message stays pending and will be redelivered",
                mid,
                target,
            )
        self._attempts.pop(key, None)

    async def _dead_letter(self, target: str, mid: str, fields: dict[bytes, bytes]) -> None:
        """dead_letter via the broker, never letting a broker blip escape.

        A raise from broker.dead_letter() previously propagated out of the
        consumer task and killed the whole loop permanently (audit A7-r1-22).
        On failure the message stays pending (dead_letter ACKs only on
        success), so reclaim redelivers it and dead-lettering is retried; the
        attempt counter is only cleared on success so the retry dead-letters
        immediately rather than restarting the attempt cap.
        """
        try:
            await self._broker.dead_letter(target, mid, fields, self._group)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "dead-letter failed for %s on %s — message stays pending for retry",
                mid,
                target,
            )
            return
        self._attempts.pop((target, mid), None)


def consumer_targets(bus: Any) -> list[str]:
    """Broker stream targets a worker must consume — the FQN of each event type
    that has a local listener. Matches the producer's destination (module +
    qualname), so producer and consumer key the same stream."""
    return [f"{et.__module__}.{et.__qualname__}" for et in bus.registered_event_types()]
