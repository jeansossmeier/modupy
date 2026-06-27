"""Cross-process broker consumer loop (process-per-module topology).

The *producer* half lives in ``runtime._maybe_route_to_broker``: a cross-module
event is serialized and XADD'd to the broker under its fully-qualified name.
This module is the *consumer* half — without it, events were durably written to
the broker and **never delivered**, silently dropping every cross-process event
(the headline feature of ``topology='processes'``).

Each worker hosts one module. ``BrokerConsumer`` subscribes to the broker
streams for exactly the event types that module's local listeners consume
(``InMemoryEventBus.registered_event_types``), then for each stream:

  * ``ensure_group`` — create the per-module consumer group (idempotent),
  * ``reclaim`` — recover messages a crashed peer claimed but never ACK'd
    (the at-least-once recovery path), then dispatch them,
  * a background loop ``read`` → deserialize (via the ``event_type`` header) →
    dispatch to local listeners → ``ack`` on success, ``dead_letter`` on poison
    or repeated failure.

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
        self._poll_block_ms = poll_block_ms
        self._reclaim_min_idle_ms = reclaim_min_idle_ms
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        # message id -> failed dispatch attempts, so a transiently-failing
        # message dead-letters after _MAX_DELIVERY_ATTEMPTS rather than looping.
        self._attempts: dict[str, int] = {}

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
        """Cancel the loop and wait for it to unwind."""
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
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
                except Exception:  # a transient broker read error must not kill the loop
                    logger.exception("broker read failed for %s", target)
                    continue
                await self._handle(target, messages)

    async def _reclaim(self, target: str) -> None:
        """Recover and re-dispatch pending messages idle past the threshold."""
        try:
            result = await self._broker.reclaim(
                target,
                consumer=self._consumer_name,
                group=self._group,
                min_idle_ms=self._reclaim_min_idle_ms,
            )
        except Exception:
            logger.exception("reclaim failed for %s", target)
            return
        # XAUTOCLAIM returns (cursor, [(id, fields), ...], deleted_ids).
        claimed = result[1] if result and len(result) > 1 else []
        for message_id, fields in claimed:
            await self._dispatch_one(target, message_id, fields)

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
        data = fields.get(b"data")
        event_type = fields.get(b"h:event_type")
        if data is None or event_type is None:
            logger.warning(
                "message %s on %s missing data/event_type header — dead-lettering", mid, target
            )
            await self._broker.dead_letter(target, mid, fields, self._group)
            self._attempts.pop(mid, None)
            return

        try:
            event = self._serializer.deserialize(data, _as_str(event_type))
        except Exception:
            logger.exception("undeserializable message %s on %s — dead-lettering", mid, target)
            await self._broker.dead_letter(target, mid, fields, self._group)
            self._attempts.pop(mid, None)
            return

        try:
            await self._bus.publish(event)
        except Exception:
            attempts = self._attempts.get(mid, 0) + 1
            self._attempts[mid] = attempts
            if attempts >= _MAX_DELIVERY_ATTEMPTS:
                logger.exception(
                    "message %s on %s failed %d dispatch attempts — dead-lettering",
                    mid,
                    target,
                    attempts,
                )
                await self._broker.dead_letter(target, mid, fields, self._group)
                self._attempts.pop(mid, None)
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

        await self._broker.ack(target, mid, self._group)
        self._attempts.pop(mid, None)


def consumer_targets(bus: Any) -> list[str]:
    """Broker stream targets a worker must consume — the FQN of each event type
    that has a local listener. Matches the producer's destination (module +
    qualname), so producer and consumer key the same stream."""
    return [f"{et.__module__}.{et.__qualname__}" for et in bus.registered_event_types()]
