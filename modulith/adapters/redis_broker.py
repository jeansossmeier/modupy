"""Redis Streams broker adapter.

Default broker for process-per-module topology. Redis Streams gives us
microsecond IPC latency, durable message delivery, consumer groups for
horizontal scaling, and is already in most teams' infrastructure.

Distributed via the `modulith[redis]` extra. Optional dependency:
redis>=5.0 (the official redis-py package, which has async support). The
import is lazy — applications that don't select this broker never pay the
cost, and the module imports fine without redis installed.

Configuration resolves env > [tool.modulith.broker] subtable > default:
  REDIS_URL / url                          connection URL (default redis://localhost:6379)
  MODULITH_STREAM_PREFIX / stream_prefix   stream namespace (default "modulith.events")
  MODULITH_CONSUMER_GROUP / consumer_group consumer group name (default "modulith")
  MODULITH_STREAM_MAXLEN / max_stream_len  bounded retention per stream (default 10000)

Selected with (broker *name* as a scalar, options in the subtable — TOML
forbids one key being both, so set the name via ``MODULITH_BROKER`` /
``configure(broker=...)`` if you use the subtable)::

    [tool.modulith]
    broker = "redis-streams"

    # …or, supplying connection options (set the name out-of-band):
    [tool.modulith.broker]
    url = "redis://cache:6379"
    consumer_group = "modulith-orders"

Why Redis Streams over pub/sub: streams persist until ACK'd (pub/sub drops
messages with no live consumer) and support consumer groups for load-sharing
across multiple workers of the same module.

Production hardening over the bare example (examples/redis_streams_broker.py):
  - Consumer group registration (XGROUP CREATE … MKSTREAM), idempotent.
  - Bounded retention via XADD MAXLEN ~ (approximate trim) so a stalled
    stream can't grow without limit. CAVEAT: trimming is by stream length
    alone — it is blind to consumer-group PEL state, so an undersized
    ``max_stream_len`` lets a publish burst push out entries that are still
    pending (delivered but never ACK'd) or not yet delivered at all. A
    trimmed pending entry is PERMANENTLY LOST (at-least-once is violated for
    it); the consumer detects such losses via XAUTOCLAIM's deleted-ids
    element and logs them at ERROR. Size ``max_stream_len`` well above the
    worst-case backlog: publish rate x (consumer downtime + processing
    latency + reclaim_min_idle_ms).
  - Pending-entry recovery via XAUTOCLAIM, run at startup and periodically
    from the consumer poll loop (reclaims messages a crashed or stalled
    worker never ACK'd) — subject to the retention caveat above.
  - Explicit dead-letter routing to a ``<stream>.dead`` stream for poison
    messages (itself MAXLEN-bounded — see ``dead_letter``).
  - Structured logging on every operation.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from modulith import BrokerRegistry, hookimpl

logger = logging.getLogger("modulith.adapters.redis")

_REDIS_SCHEME = "redis-streams"
_DEFAULT_URL = "redis://localhost:6379"
_DEFAULT_PREFIX = "modulith.events"
_DEFAULT_GROUP = "modulith"
_DEFAULT_MAXLEN = 10000


# ---------------------------------------------------------------------------
# Broker implementation
# ---------------------------------------------------------------------------


class RedisStreamsBroker:
    """Broker using Redis Streams (XADD / XREADGROUP / XAUTOCLAIM).

    Conforms structurally to ``modulith.Broker`` (publish + close); the extra
    consumer-side methods (``ensure_group``, ``read``, ``ack``, ``reclaim``,
    ``dead_letter``) are what the process-per-module worker drives. Duck-typed
    — no inheritance.

    A pre-built ``client`` may be injected (tests, or callers that manage their
    own connection pool); otherwise one is created lazily from ``url`` so redis
    stays a soft dependency.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        stream_prefix: str = _DEFAULT_PREFIX,
        consumer_group: str | None = None,
        max_stream_len: int = _DEFAULT_MAXLEN,
        dlq_max_stream_len: int | None = None,
        client: Any | None = None,
    ) -> None:
        if client is not None:
            self._client = client
        else:
            # Lazy import keeps redis a soft dependency.
            from redis.asyncio import Redis

            self._client = Redis.from_url(url or _DEFAULT_URL)
        self._stream_prefix = stream_prefix
        self._consumer_group = consumer_group or _DEFAULT_GROUP
        self._max_stream_len = max_stream_len
        # DLQ gets its own (by default larger) cap: poison messages are the
        # stream most likely to accumulate, but they're also the ones worth
        # retaining longest for inspection/replay. Still bounded so a poison
        # burst can't grow Redis memory without limit.
        self._dlq_max_stream_len = (
            dlq_max_stream_len if dlq_max_stream_len is not None else max_stream_len * 10
        )

    # ----- stream naming ----------------------------------------------------

    def _stream_name(self, target: str) -> str:
        """Full stream key for a target (registry strips the scheme, but be
        defensive about a leftover ``redis-streams:`` prefix)."""
        stream = target.removeprefix(f"{_REDIS_SCHEME}:")
        return f"{self._stream_prefix}.{stream}"

    # ----- producer side ----------------------------------------------------

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """XADD the payload to the target stream with bounded retention.

        Body goes under ``data``; each header is lifted to its own ``h:<key>``
        field so consumers read them without unpacking the payload. ``MAXLEN ~``
        caps the stream approximately (cheap radix-tree-aligned trim); the trim
        ignores consumer-group pending state — see the module docstring's
        retention caveat on sizing ``max_stream_len``.
        """
        stream = self._stream_name(target)
        fields: dict[bytes, bytes] = {b"data": payload}
        if headers:
            for key, value in headers.items():
                fields[f"h:{key}".encode()] = value.encode()
        await self._client.xadd(stream, fields, maxlen=self._max_stream_len, approximate=True)
        logger.debug("xadd stream=%s bytes=%d headers=%d", stream, len(payload), len(headers or {}))

    # ----- consumer side ----------------------------------------------------

    async def ensure_group(self, target: str, group: str | None = None) -> None:
        """Create the consumer group (and stream) if absent — idempotent.

        XGROUP CREATE raises ``BUSYGROUP`` when the group already exists; that
        is the success-on-restart case, so it's swallowed.
        """
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        try:
            await self._client.xgroup_create(stream, group_name, id="0", mkstream=True)
            logger.info("created consumer group %r on stream %s", group_name, stream)
        except Exception as exc:  # redis.exceptions.ResponseError on BUSYGROUP
            if "BUSYGROUP" not in str(exc):
                raise
            logger.debug("consumer group %r already exists on %s", group_name, stream)

    async def read(
        self,
        target: str,
        *,
        consumer: str,
        group: str | None = None,
        count: int = 10,
        block_ms: int = 1000,
    ) -> Any:
        """XREADGROUP new (never-delivered) messages for this consumer."""
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        return await self._client.xreadgroup(
            group_name, consumer, {stream: ">"}, count=count, block=block_ms
        )

    async def ack(self, target: str, message_id: str, group: str | None = None) -> None:
        """XACK a successfully-processed message so it stops being redelivered."""
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        await self._client.xack(stream, group_name, message_id)

    async def reclaim(
        self,
        target: str,
        *,
        consumer: str,
        min_idle_ms: int,
        group: str | None = None,
        count: int = 100,
    ) -> Any:
        """XAUTOCLAIM pending entries idle longer than ``min_idle_ms``.

        The at-least-once guarantee's recovery path. The consumer loop runs
        this at worker startup AND periodically on every poll cycle — not only
        after a crash. XAUTOCLAIM is purely idle-time based (it has no
        crash/liveness detection), so a message a live, healthy peer is still
        processing WILL be claimed away and re-dispatched once it has been
        pending longer than ``min_idle_ms``. Keep ``min_idle_ms`` above the
        worst-case handler latency, and keep handlers idempotent regardless
        (at-least-once delivery).
        """
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        return await self._client.xautoclaim(
            stream, group_name, consumer, min_idle_ms, start_id="0-0", count=count
        )

    async def dead_letter(
        self,
        target: str,
        message_id: str,
        fields: dict[bytes, bytes],
        group: str | None = None,
    ) -> None:
        """Route a poison message to ``<stream>.dead`` and ACK the original.

        Called by the worker once a message has exhausted its processing
        retries. ACKing the source stops the redelivery loop. The DLQ stream
        retains the payload for inspection/replay on a bounded, best-effort
        basis ONLY: it is itself capped (``dlq_max_stream_len``, MAXLEN ~
        approximate trim), so once enough dead-lettered volume accumulates the
        OLDEST entries are silently discarded. It is not a durable audit log —
        size ``dlq_max_stream_len`` to the forensic retention window you need.
        """
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        # Bound the DLQ too (MAXLEN ~) — the adapter advertises bounded
        # retention as a hardening property, and the .dead stream is the most
        # likely to fill with unacked poison messages.
        await self._client.xadd(
            f"{stream}.dead", fields, maxlen=self._dlq_max_stream_len, approximate=True
        )
        await self._client.xack(stream, group_name, message_id)
        logger.warning("dead-lettered message %s from stream %s", message_id, stream)

    async def close(self) -> None:
        """Disconnect from Redis. Idempotent-friendly (close-after-close ok)."""
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Plugin registration hook
# ---------------------------------------------------------------------------


@hookimpl
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    """Register the redis-streams scheme when the app selects it.

    Connection settings resolve env > ``[tool.modulith.broker]`` subtable >
    default — env vars stay the deploy-time override (12-factor), while the
    TOML subtable (``broker_options``) is the in-repo configuration the SPEC
    documents. No-op unless ``broker == "redis-streams"``.
    """
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _REDIS_SCHEME:
        return

    opts = cfg.broker_options or {}
    dlq_maxlen = opts.get("dlq_max_stream_len")
    broker = RedisStreamsBroker(
        url=os.environ.get("REDIS_URL") or opts.get("url") or _DEFAULT_URL,
        stream_prefix=(
            os.environ.get("MODULITH_STREAM_PREFIX") or opts.get("stream_prefix") or _DEFAULT_PREFIX
        ),
        consumer_group=os.environ.get("MODULITH_CONSUMER_GROUP") or opts.get("consumer_group"),
        max_stream_len=int(
            os.environ.get("MODULITH_STREAM_MAXLEN")
            or opts.get("max_stream_len")
            or _DEFAULT_MAXLEN
        ),
        dlq_max_stream_len=int(dlq_maxlen) if dlq_maxlen is not None else None,
    )
    registry.register(_REDIS_SCHEME, broker)
    logger.info("registered redis-streams broker (prefix=%s)", broker._stream_prefix)


__all__ = ["RedisStreamsBroker", "modulith_register_brokers"]
