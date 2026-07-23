"""Example adapter: Redis Streams broker.

This file shows the complete authoring experience for a third-party
broker adapter. Published as ``modulith-redis-streams``, the package
declares its entry point in pyproject.toml:

    [project.entry-points."modulith"]
    redis_streams = "modulith_redis_streams.adapter"

After ``pip install modulith-redis-streams``, application code uses it
without any registration boilerplate:

    from dataclasses import dataclass
    from modulith import event, externalized

    @event
    @externalized("redis-streams-example:my-stream")
    @dataclass
    class OrderShipped:
        order_id: str

The total surface for a broker adapter is one class implementing the
Broker protocol plus one hookimpl function. No base class to inherit,
no manifest file, no framework knowledge beyond the protocol contract.

Scheme naming: each broker scheme may be registered exactly once per
BrokerRegistry — registering a scheme that is already taken raises
``DuplicateBrokerError`` at startup. Modulith ships a built-in adapter
under the ``redis-streams`` scheme (``modulith.adapters.redis_broker``),
so this example registers ``redis-streams-example`` instead. A
third-party adapter that intentionally wants to REPLACE a built-in
scheme must ship under the same scheme name and have the application
disable the built-in first (``create_plugin_manager(disable=
["modulith.adapters.redis_broker"])``); otherwise pick a unique scheme.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from modulith import BrokerRegistry, hookimpl

if TYPE_CHECKING:
    from redis.typing import EncodableT


class RedisStreamsBroker:
    """Minimal broker adapter for Redis Streams.

    Conforms structurally to ``modulith.Broker`` — no inheritance
    required, duck typing via Protocol. ``isinstance(b, Broker)`` works
    because Broker is ``runtime_checkable``.
    """

    def __init__(self, url: str) -> None:
        # Lazy import keeps redis a soft dependency. Users who don't
        # install this adapter never pay the import cost.
        import redis.asyncio as redis

        self._client = redis.from_url(url)  # type: ignore[no-untyped-call]  # redis-py 6.x from_url is unannotated

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        # Redis Streams entries are field maps. We pack the event body
        # under a single key and lift each header to its own h:* field
        # so consumers can read them without unpacking the payload.
        fields: dict[EncodableT, EncodableT] = {b"data": payload}
        if headers:
            for key, value in headers.items():
                fields[f"h:{key}".encode()] = value.encode()
        await self._client.xadd(target, fields)

    async def close(self) -> None:
        # Redis-py's async client uses aclose() in modern versions.
        # Older versions used close(); adapter authors should pin
        # their dependency version range.
        await self._client.aclose()


# The only modulith-specific code is this hook implementation. Pluggy
# discovers it via the pyproject entry point declaration and calls it
# once during application startup.
@hookimpl
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    # 'redis-streams-example', NOT 'redis-streams': the built-in adapter
    # already owns 'redis-streams', and a second register() for the same
    # scheme raises DuplicateBrokerError (see module docstring).
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    registry.register("redis-streams-example", RedisStreamsBroker(url))
