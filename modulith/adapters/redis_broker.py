"""Redis Streams broker adapter.

Production-scale broker for distributed systems. Redis Streams gives us
microsecond IPC latency, durable message delivery, consumer groups for
horizontal scaling, and is already in most teams' infrastructure. Opt-in
via `modulith.configure(broker="redis")` for process-per-module and
process-per-monolith topologies.

Distributed via the `modupy[redis]` extra. Optional dependency:
redis>=5.0 (the official redis-py package, which has async support). The
import is lazy — applications that don't select this broker never pay the
cost, and the module imports fine without redis installed.

Configuration resolves env > [tool.modulith.broker] subtable > default:
  REDIS_URL / url                          connection URL (default redis://localhost:6379)
  MODULITH_STREAM_PREFIX / stream_prefix   stream namespace (default "modulith.events")
  MODULITH_CONSUMER_GROUP / consumer_group consumer group name (default "modulith")
  MODULITH_STREAM_MAXLEN / max_stream_len  bounded retention per stream (default 10000)
  MODULITH_BROKER_DLQ_MAX_STREAM_LEN /
  dlq_max_stream_len                       bounded retention for the dead-letter stream
                                            (default: max_stream_len * 10)
  MODULITH_BROKER_MAX_PAYLOAD_BYTES /
  max_payload_bytes                        producer-side payload size cap, rejected with
                                            ConfigurationError (default 16 MiB)
  poll_block_ms                            consumer XREADGROUP block timeout, ms (default 1000);
                                            also sets the client's socket_timeout to
                                            poll_block_ms/1000 + 5 s (with socket_keepalive)
                                            unless the URL's query string sets them
  reclaim_min_idle_ms                      consumer XAUTOCLAIM min-idle threshold, ms
                                            (default 60000)
  max_delivery_attempts                    consumer delivery attempts before dead-lettering
                                            (default 5)

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

from modulith import (
    BrokerRegistry,
    ConfigurationError,
    Consumer,
    ConsumerRegistry,
    ConsumerSpec,
    hookimpl,
)

from ..config import DEFAULT_MAX_PAYLOAD_BYTES, MAX_PAYLOAD_BYTES

logger = logging.getLogger("modulith.adapters.redis")

_REDIS_SCHEME = "redis-streams"
_DEFAULT_URL = "redis://localhost:6379"
_DEFAULT_PREFIX = "modulith.events"
_DEFAULT_GROUP = "modulith"
_DEFAULT_MAXLEN = 10000
_DLQ_DEDUP_TTL_SECONDS = 7 * 24 * 60 * 60
_SOCKET_TIMEOUT_MARGIN_S = 5.0


def _positive_int(value: object, name: str) -> int:
    """Accept a positive int, or a numeric string as env vars deliver it.

    Zero is rejected, not read as "no cap": ``XADD MAXLEN ~ 0`` trims every
    entry away in the same command that adds it.
    """
    number: object = value
    if isinstance(value, str):
        try:
            number = int(value)
        except ValueError:
            pass
    if type(number) is not int or number <= 0:
        raise ConfigurationError(f"{name} must be a positive integer, got {value!r}")
    return number


def _cursor_str(raw: Any) -> str:
    """Normalize an XAUTOCLAIM cursor (bytes from redis-py, str from fakes)."""
    return raw.decode() if isinstance(raw, bytes) else str(raw)


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
        max_stream_len: int | str = _DEFAULT_MAXLEN,
        dlq_max_stream_len: int | str | None = None,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        client: Any | None = None,
        poll_block_ms: int = 1000,
    ) -> None:
        if client is not None:
            self._client = client
        else:
            # Lazy import keeps redis a soft dependency.
            try:
                from redis.asyncio import Redis
            except ImportError as exc:
                # A bare ModuleNotFoundError('No module named redis') surfaces
                # from whichever call first touched the broker and names neither
                # the broker that needs it nor the extra that installs it.
                raise ConfigurationError(
                    "The 'redis-streams' broker requires redis-py (async). "
                    "Install the extra: pip install 'modupy[redis]'"
                ) from exc

            # XREADGROUP BLOCK is a server-side timeout: without a socket
            # timeout, a hung server or half-open connection stalls a read
            # forever. redis-py lets query options in the URL override these.
            self._client = Redis.from_url(
                url or _DEFAULT_URL,
                socket_timeout=max(poll_block_ms, 1) / 1000 + _SOCKET_TIMEOUT_MARGIN_S,
                socket_keepalive=True,
            )
        self._stream_prefix = stream_prefix
        self._consumer_group = consumer_group or _DEFAULT_GROUP
        self._max_stream_len = _positive_int(
            max_stream_len, "max_stream_len (MODULITH_STREAM_MAXLEN)"
        )
        # DLQ gets its own (by default larger) cap: poison messages are the
        # stream most likely to accumulate, but they're also the ones worth
        # retaining longest for inspection/replay. Still bounded so a poison
        # burst can't grow Redis memory without limit.
        self._dlq_max_stream_len = (
            _positive_int(
                dlq_max_stream_len, "dlq_max_stream_len (MODULITH_BROKER_DLQ_MAX_STREAM_LEN)"
            )
            if dlq_max_stream_len is not None
            else self._max_stream_len * 10
        )
        if type(max_payload_bytes) is not int or not 1 <= max_payload_bytes <= MAX_PAYLOAD_BYTES:
            raise ConfigurationError(
                f"max_payload_bytes must be an integer between 1 and "
                f"{MAX_PAYLOAD_BYTES}, got {max_payload_bytes!r}"
            )
        self._max_payload_bytes = max_payload_bytes

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
        if len(payload) > self._max_payload_bytes:
            raise ConfigurationError(
                f"redis-streams payload is {len(payload)} bytes, exceeding "
                f"max_payload_bytes={self._max_payload_bytes}. Reduce the payload "
                "or increase the limit."
            )
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

        XAUTOCLAIM caps each call at ``count`` and returns a continuation
        cursor; a single call therefore drains at most ``count`` entries.
        This method follows the cursor until it returns ``0-0`` — exactly one
        full PEL scan per reclaim, bounded (it never rescans, so it cannot
        spin) — and returns the aggregated
        ``(b"0-0", claimed, deleted)`` triple, so a >``count`` idle-pending
        backlog is recovered in one reclaim cycle instead of leaking across
        many poll intervals.
        """
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        cursor = "0-0"
        claimed: list[Any] = []
        deleted: list[Any] = []
        while True:
            result = await self._client.xautoclaim(
                stream, group_name, consumer, min_idle_ms, start_id=cursor, count=count
            )
            if result and len(result) > 1:
                claimed.extend(result[1])
            if result and len(result) > 2:
                deleted.extend(result[2])
            next_cursor = _cursor_str(result[0]) if result else "0-0"
            if next_cursor == "0-0":
                break  # full PEL scan completed
            if next_cursor == cursor:  # defensive: no progress → never spin
                logger.warning(
                    "xautoclaim cursor did not advance past %s on stream %s — "
                    "stopping this reclaim cycle",
                    next_cursor,
                    stream,
                )
                break
            cursor = next_cursor
        return (b"0-0", claimed, deleted)

    async def delivery_attempts(
        self, target: str, message_id: str, group: str | None = None
    ) -> int | None:
        """Read Redis's durable delivery count from the consumer-group PEL."""
        stream = self._stream_name(target)
        group_name = group or self._consumer_group
        entries = await self._client.xpending_range(
            stream, group_name, min=message_id, max=message_id, count=1
        )
        if not entries:
            return None
        entry = entries[0]
        if isinstance(entry, dict):
            entry_id = _cursor_str(entry["message_id"])
            attempts = entry["times_delivered"]
        else:
            entry_id, _consumer, _idle, attempts = entry
            entry_id = _cursor_str(entry_id)
        return int(attempts) if entry_id == message_id else None

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
        # The append and ACK must be atomic: a client timeout between XADD and
        # XACK used to produce a duplicate DLQ record on retry. The dedup key
        # uses the original stream/group/message identity and expires with the
        # bounded forensic retention window. It is written only AFTER the XADD
        # it guards succeeds — Lua's redis.call() aborts the script without
        # rolling back prior writes, so setting it any earlier would let a
        # failed XADD leave a dedup key with nothing behind it: the retry
        # would then see "already deduped", skip the XADD, and fall through to
        # the unconditional XACK below, silently dropping the payload.
        dedup_key = f"{stream}.dead.dedup.{group_name}.{message_id}"
        arguments: list[str | bytes | int] = [
            group_name,
            message_id,
            self._dlq_max_stream_len,
            "h:source_message_id",
            message_id,
            "h:source_group",
            group_name,
        ]
        for key, value in fields.items():
            arguments.extend((key, value))
        await self._client.eval(
            f"""
            local exists = redis.call('EXISTS', KEYS[3])
            if exists == 0 then
              local command = {{KEYS[2], 'MAXLEN', '~', ARGV[3], '*'}}
              for index = 4, #ARGV do table.insert(command, ARGV[index]) end
              redis.call('XADD', unpack(command))
              redis.call('SET', KEYS[3], ARGV[2], 'EX', {_DLQ_DEDUP_TTL_SECONDS})
            end
            return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            """,
            3,
            stream,
            f"{stream}.dead",
            dedup_key,
            *arguments,
        )
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
    from ..serializers import _resolve_max_payload_bytes

    cfg = _runtime.config
    if cfg is None or cfg.broker != _REDIS_SCHEME:
        return

    opts = cfg.broker_options or {}
    dlq_maxlen = os.environ.get("MODULITH_BROKER_DLQ_MAX_STREAM_LEN") or opts.get(
        "dlq_max_stream_len"
    )
    broker = RedisStreamsBroker(
        url=os.environ.get("REDIS_URL") or opts.get("url") or _DEFAULT_URL,
        stream_prefix=(
            os.environ.get("MODULITH_STREAM_PREFIX") or opts.get("stream_prefix") or _DEFAULT_PREFIX
        ),
        consumer_group=os.environ.get("MODULITH_CONSUMER_GROUP") or opts.get("consumer_group"),
        max_stream_len=(
            os.environ.get("MODULITH_STREAM_MAXLEN")
            or opts.get("max_stream_len")
            or _DEFAULT_MAXLEN
        ),
        dlq_max_stream_len=dlq_maxlen,
        max_payload_bytes=_resolve_max_payload_bytes(opts),
        poll_block_ms=opts.get("poll_block_ms", 1000),
    )
    registry.register(_REDIS_SCHEME, broker)
    logger.info("registered redis-streams broker (prefix=%s)", broker._stream_prefix)


# ---------------------------------------------------------------------------
# Consumer registration hook
# ---------------------------------------------------------------------------


def _make_redis_consumer(spec: ConsumerSpec) -> Consumer:
    """Build a Redis-Streams consumer for one worker module from ``spec``.

    Wraps the generic ``BrokerConsumer`` poll/claim/ack loop around the
    ``RedisStreamsBroker`` already registered on the producer side (pulled from
    ``spec.broker_registry`` by scheme), whose consumer-side methods
    (ensure_group / read / ack / reclaim / dead_letter) the loop drives. One
    Redis client serves both halves — no second connection is opened here.
    """
    from .._consumer import BrokerConsumer
    from ..runtime import _runtime

    broker = spec.broker_registry.get(spec.scheme)
    options = _runtime.config.broker_options if _runtime.config is not None else {}
    return BrokerConsumer(
        broker=broker,
        bus=spec.bus,
        serializer=spec.serializer,
        consumer_name=spec.consumer_name,
        group=spec.group,
        targets=list(spec.targets),
        poll_block_ms=options.get("poll_block_ms", 1000),
        reclaim_min_idle_ms=options.get("reclaim_min_idle_ms", 60_000),
        max_delivery_attempts=options.get("max_delivery_attempts", 5),
    )


@hookimpl
def modulith_register_consumers(registry: ConsumerRegistry) -> None:
    """Register the redis-streams consumer factory when the app selects it.

    The consumer-side mirror of ``modulith_register_brokers``: no-op unless
    ``broker == "redis-streams"``. The factory reuses the broker object that
    hook registered (fetched from the broker registry at build time), so no
    second Redis client is created.
    """
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _REDIS_SCHEME:
        return
    registry.register(_REDIS_SCHEME, _make_redis_consumer)


__all__ = [
    "RedisStreamsBroker",
    "modulith_register_brokers",
    "modulith_register_consumers",
]
