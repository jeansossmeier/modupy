"""Redis Streams broker adapter.

Default broker for process-per-module topology. Redis Streams gives us
microsecond IPC latency, durable message delivery, consumer groups for
horizontal scaling, and is already in most teams' infrastructure.

Implementation status: SKELETON. ~80 lines when complete.

Distributed via the `modulith[redis]` extra. Optional dependency:
redis>=5.0 (the official redis-py package, which has async support).

Usage in pyproject.toml:

    [tool.modulith]
    broker = "redis-streams"

    [tool.modulith.broker.options]
    url = "${REDIS_URL}"
    consumer_group = "modulith-${MODULITH_MODULE}"
    stream_prefix = "modulith.events"

Why Redis Streams over pub/sub:
  - Pub/sub drops messages if no consumer is connected when published.
    Streams persist messages until ACK'd.
  - Pub/sub has no consumer groups. Streams support them, which lets
    multiple workers of the same module share load.
  - The latency difference between streams and pub/sub is negligible
    (~ microseconds). Durability matters more.

Why not Redis lists (LPUSH/BRPOP):
  - No consumer groups
  - No automatic ACK — manual reliability handling
  - Streams are the modern primitive for this exact use case

There is a working example of this adapter in examples/redis_streams_broker.py
that can be referenced for the full implementation.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("modulith.adapters.redis")


# ---------------------------------------------------------------------------
# Broker implementation
# ---------------------------------------------------------------------------


class RedisStreamsBroker:
    """Broker using Redis Streams (XADD/XREADGROUP).

    Conforms structurally to modulith.Broker. Duck-typed; no inheritance.

    The full reference implementation is in examples/redis_streams_broker.py.
    The example shows publish() via XADD and a basic consumer pattern.
    Production use additionally needs:
      - Consumer group registration on startup (XGROUP CREATE)
      - Pending entries list polling (XPENDING) for crash recovery
      - Dead letter handling (after N retries, move to a DLQ stream)
      - Configurable serialization (the example uses bytes-only)
    """

    def __init__(
        self,
        url: str,
        *,
        stream_prefix: str = "modulith.events",
        consumer_group: str | None = None,
    ) -> None:
        """Build a broker pointed at a Redis instance.

        IMPLEMENTATION TODO:
        from redis.asyncio import Redis
        self._client = Redis.from_url(url)
        self._stream_prefix = stream_prefix
        self._consumer_group = consumer_group
        """
        raise NotImplementedError("Phase 2 — see TODO above")

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Add a message to the target stream.

        target format: "redis-streams:<stream_name>" or just stream name
        if dispatched via the registry.

        IMPLEMENTATION TODO:
        # Strip any "redis-streams:" prefix since the registry already
        # routed by scheme.
        stream = target.removeprefix("redis-streams:")
        full_stream = f"{self._stream_prefix}.{stream}"

        # Headers are extra fields in the XADD entry. Body is the payload.
        fields = {"_payload": payload}
        if headers:
            for key, value in headers.items():
                fields[f"_h_{key}"] = value

        await self._client.xadd(full_stream, fields)

        See examples/redis_streams_broker.py for working code.
        """
        raise NotImplementedError("Phase 2")

    async def close(self) -> None:
        """Disconnect from Redis."""
        raise NotImplementedError("Phase 2")


# ---------------------------------------------------------------------------
# Plugin registration hook
# ---------------------------------------------------------------------------

# Brokers register themselves via the modulith_register_brokers hook.
# When the user has [tool.modulith] broker = "redis-streams", the
# runtime imports this module and calls register() through the registry.

from modulith import BrokerRegistry, hookimpl


@hookimpl
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    """Register the redis-streams scheme with the broker registry.

    IMPLEMENTATION TODO:
    Read configuration:
      from modulith.config import get_configuration
      config = get_configuration()
      if config.broker != "redis-streams":
          return  # not us; bail

    Build broker:
      url = config.broker_options.get("url", "redis://localhost:6379")
      stream_prefix = config.broker_options.get("stream_prefix", "modulith.events")
      group = config.broker_options.get("consumer_group")
      broker = RedisStreamsBroker(url=url, stream_prefix=stream_prefix,
                                  consumer_group=group)

    Register:
      registry.register("redis-streams", broker)
    """
    raise NotImplementedError("Phase 2")


__all__ = ["RedisStreamsBroker", "modulith_register_brokers"]
