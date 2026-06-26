"""Tests for the production Redis Streams broker adapter.

Two layers:

  * **Unit** (default) — drive the broker against a hand-rolled async fake
    Redis client that records calls. ``fakeredis`` isn't a dependency, and a
    recording double is enough to pin the exact Redis commands the adapter
    issues (XADD with bounded MAXLEN, idempotent XGROUP CREATE, XREADGROUP,
    XACK, XAUTOCLAIM for crash recovery, dead-letter XADD) without a server.

  * **Integration** (``@pytest.mark.integration``) — a real publish→consume
    round-trip against a live Redis. Skipped unless ``MODULITH_TEST_REDIS_URL``
    points at a reachable server, so the suite stays green without Docker.

The fake matches redis-py's async surface for the methods the adapter uses.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from modulith.adapters.redis_broker import RedisStreamsBroker, modulith_register_brokers
from modulith.brokers import BrokerRegistry


class FakeRedis:
    """Records the Redis commands the broker issues; async-shaped."""

    def __init__(self) -> None:
        self.xadds: list[tuple[str, dict, int | None]] = []
        self.groups: list[tuple[str, str]] = []
        # (name, groupname, id) — the start id matters: '0' consumes from the
        # beginning of the stream, '$' only new messages. Recorded so a flip
        # that silently drops backlog is caught (#39).
        self.group_creates: list[tuple[str, str, str]] = []
        self.xreadgroup_calls: list[dict] = []
        self.xacks: list[tuple[str, str, tuple]] = []
        self.xautoclaim_calls: list[dict] = []
        self.closed = False
        self._existing_groups: set[tuple[str, str]] = set()

    async def xadd(
        self, name: str, fields: dict, *, maxlen: int | None = None, approximate: bool = True
    ) -> bytes:
        self.xadds.append((name, fields, maxlen))
        return b"1-0"

    async def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False
    ) -> bool:
        self.group_creates.append((name, groupname, id))
        key = (name, groupname)
        if key in self._existing_groups:
            # redis-py raises ResponseError("BUSYGROUP ...") for re-creation.
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        self._existing_groups.add(key)
        self.groups.append(key)
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict,
        count: int | None = None,
        block: int | None = None,
    ) -> list:
        self.xreadgroup_calls.append(
            {
                "group": groupname,
                "consumer": consumername,
                "streams": streams,
                "count": count,
                "block": block,
            }
        )
        return [("stream", [(b"1-0", {b"data": b"{}"})])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.xacks.append((name, groupname, ids))
        return len(ids)

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> Any:
        self.xautoclaim_calls.append(
            {
                "name": name,
                "group": groupname,
                "consumer": consumername,
                "min_idle_time": min_idle_time,
                "start_id": start_id,
                "count": count,
            }
        )
        return (b"0-0", [(b"1-0", {b"data": b"{}"})], [])

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def fake() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def broker(fake: FakeRedis) -> RedisStreamsBroker:
    return RedisStreamsBroker(
        client=fake, stream_prefix="modulith.events", consumer_group="g", max_stream_len=500
    )


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


async def test_publish_xadds_with_prefix_and_bounded_maxlen(broker, fake) -> None:
    await broker.publish("orders", b'{"id": 1}')

    assert len(fake.xadds) == 1
    name, fields, maxlen = fake.xadds[0]
    assert name == "modulith.events.orders"
    assert fields[b"data"] == b'{"id": 1}'
    assert maxlen == 500  # bounded retention (MAXLEN ~)


async def test_publish_strips_scheme_prefix_if_present(broker, fake) -> None:
    # The registry normally strips the scheme, but be defensive.
    await broker.publish("redis-streams:orders", b"{}")
    assert fake.xadds[0][0] == "modulith.events.orders"


async def test_publish_packs_headers_as_fields(broker, fake) -> None:
    await broker.publish("orders", b"{}", headers={"trace": "abc"})
    _name, fields, _maxlen = fake.xadds[0]
    assert fields[b"h:trace"] == b"abc"


# ---------------------------------------------------------------------------
# consumer group lifecycle + crash recovery
# ---------------------------------------------------------------------------


async def test_ensure_group_creates_with_mkstream(broker, fake) -> None:
    await broker.ensure_group("orders")
    assert ("modulith.events.orders", "g") in fake.groups
    # id='0' is load-bearing: the group must consume from the START of the
    # stream so messages published before a consumer subscribes are still
    # delivered. A regression to '$' (only-new) silently drops that backlog.
    assert ("modulith.events.orders", "g", "0") in fake.group_creates


async def test_ensure_group_is_idempotent(broker, fake) -> None:
    await broker.ensure_group("orders")
    # Second call hits BUSYGROUP — must be swallowed, not raised.
    await broker.ensure_group("orders")
    assert fake.groups.count(("modulith.events.orders", "g")) == 1


async def test_read_uses_xreadgroup_new_messages(broker, fake) -> None:
    await broker.read("orders", consumer="c1", count=5, block_ms=200)
    call = fake.xreadgroup_calls[0]
    assert call["group"] == "g"
    assert call["consumer"] == "c1"
    assert call["streams"] == {"modulith.events.orders": ">"}
    assert call["count"] == 5
    assert call["block"] == 200


async def test_ack_acknowledges_message(broker, fake) -> None:
    await broker.ack("orders", "1-0")
    name, group, ids = fake.xacks[0]
    assert name == "modulith.events.orders"
    assert group == "g"
    assert ids == ("1-0",)


async def test_reclaim_uses_xautoclaim_for_pending_recovery(broker, fake) -> None:
    await broker.reclaim("orders", consumer="c1", min_idle_ms=30000)
    call = fake.xautoclaim_calls[0]
    assert call["name"] == "modulith.events.orders"
    assert call["group"] == "g"
    assert call["consumer"] == "c1"
    assert call["min_idle_time"] == 30000


# ---------------------------------------------------------------------------
# dead-letter + close
# ---------------------------------------------------------------------------


async def test_dead_letter_xadds_to_dead_stream_and_acks(broker, fake) -> None:
    await broker.dead_letter("orders", "1-0", {b"data": b"{}"})
    # routed to the DLQ stream
    assert any(name == "modulith.events.orders.dead" for name, _f, _m in fake.xadds)
    # and acknowledged on the source stream so it stops being redelivered
    assert ("modulith.events.orders", "g", ("1-0",)) in fake.xacks


async def test_dead_letter_stream_is_bounded(broker, fake) -> None:
    """The DLQ XADD is capped too (#48) — a poison burst can't grow unbounded.

    Default DLQ cap is 10x the main stream cap (max_stream_len=500 here).
    """
    await broker.dead_letter("orders", "1-0", {b"data": b"{}"})
    dead = [(name, maxlen) for name, _f, maxlen in fake.xadds if name.endswith(".dead")]
    assert dead == [("modulith.events.orders.dead", 5000)]


async def test_dead_letter_cap_is_configurable() -> None:
    fake = FakeRedis()
    broker = RedisStreamsBroker(
        client=fake, stream_prefix="modulith.events", consumer_group="g", dlq_max_stream_len=42
    )
    await broker.dead_letter("orders", "1-0", {b"data": b"{}"})
    dead_maxlens = [maxlen for name, _f, maxlen in fake.xadds if name.endswith(".dead")]
    assert dead_maxlens == [42]


async def test_close_calls_aclose(broker, fake) -> None:
    await broker.close()
    assert fake.closed is True


# ---------------------------------------------------------------------------
# Stateful behavioral layer — a fake that models consumer-group semantics
# (append-only streams, per-group last-delivered cursor, PEL, XACK clearing,
# XAUTOCLAIM recovery). Unlike the recording FakeRedis above (which pins the
# exact commands/args), this exercises the adapter's consume→ack→reclaim
# *behavior*: a published message is consumable, ACK clears it from pending,
# and an un-ACK'd message is recoverable via reclaim — none of which the
# canned-return fake can assert (#40).
# ---------------------------------------------------------------------------


class StatefulFakeRedis:
    """In-memory Redis Streams + consumer groups, faithful enough to test
    delivery semantics without a server.

    Models: append-only streams with monotonic ``<seq>-0`` ids; per-(stream,
    group) last-delivered cursor and a pending-entries list (PEL); ``>`` reads
    deliver only past-the-cursor messages and add them to the PEL; XACK removes
    from the PEL; XAUTOCLAIM returns all currently-pending entries (it treats
    every pending message as idle past the threshold — sufficient to assert
    recoverability of an un-ACK'd message).
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[bytes, dict[bytes, bytes]]]] = {}
        # (stream, group) -> {"cursor": int, "pel": dict[bytes, str]}
        self.groups: dict[tuple[str, str], dict[str, Any]] = {}
        self._seq: dict[str, int] = {}
        self.closed = False

    async def xadd(
        self, name: str, fields: dict, *, maxlen: int | None = None, approximate: bool = True
    ) -> bytes:
        seq = self._seq.get(name, 0) + 1
        self._seq[name] = seq
        mid = f"{seq}-0".encode()
        self.streams.setdefault(name, []).append((mid, dict(fields)))
        if maxlen is not None and len(self.streams[name]) > maxlen:
            self.streams[name] = self.streams[name][-maxlen:]
        return mid

    async def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False
    ) -> bool:
        key = (name, groupname)
        if key in self.groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        # id='0' → cursor before the first message (deliver backlog);
        # id='$'  → cursor at the current tail (only new messages).
        cursor = 0 if id == "0" else self._seq.get(name, 0)
        self.groups[key] = {"cursor": cursor, "pel": {}}
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict,
        count: int | None = None,
        block: int | None = None,
    ) -> list:
        out = []
        for name, _sid in streams.items():  # adapter always passes ">"
            grp = self.groups[(name, groupname)]
            delivered = []
            for mid, fields in self.streams.get(name, []):
                seq = int(mid.split(b"-")[0])
                if seq > grp["cursor"]:
                    delivered.append((mid, fields))
                    grp["pel"][mid] = consumername
                    grp["cursor"] = seq
                    if count is not None and len(delivered) >= count:
                        break
            if delivered:
                out.append((name.encode(), delivered))
        return out

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        pel = self.groups[(name, groupname)]["pel"]
        acked = 0
        for i in ids:
            mid = i if isinstance(i, bytes) else i.encode()
            if pel.pop(mid, None) is not None:
                acked += 1
        return acked

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> Any:
        pel = self.groups[(name, groupname)]["pel"]
        by_id = dict(self.streams.get(name, []))
        claimed = []
        for mid in list(pel.keys()):
            pel[mid] = consumername  # reassign to the claiming consumer
            claimed.append((mid, by_id.get(mid, {})))
        return (b"0-0", claimed, [])

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def stateful_broker() -> RedisStreamsBroker:
    return RedisStreamsBroker(
        client=StatefulFakeRedis(), stream_prefix="modulith.events", consumer_group="g"
    )


async def test_publish_then_consume_round_trip(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b'{"id": 1}', headers={"event_type": "Order"})

    messages = await stateful_broker.read("orders", consumer="c1", count=10, block_ms=0)
    assert messages
    _stream, entries = messages[0]
    (_mid, fields) = entries[0]
    assert fields[b"data"] == b'{"id": 1}'
    assert fields[b"h:event_type"] == b"Order"
    # A second ">" read returns nothing new — the message was delivered once.
    assert await stateful_broker.read("orders", consumer="c1", count=10, block_ms=0) == []


async def test_unacked_message_is_recoverable_via_reclaim(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b"payload")
    [(_s, [(mid, _f)])] = await stateful_broker.read("orders", consumer="c1", block_ms=0)

    # Not ACK'd → still pending → another consumer can reclaim it (crash recovery).
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=0
    )
    assert [m for m, _ in claimed] == [mid]


async def test_ack_clears_message_from_pending(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b"payload")
    [(_s, [(mid, _f)])] = await stateful_broker.read("orders", consumer="c1", block_ms=0)

    await stateful_broker.ack("orders", mid.decode())

    # ACK'd → no longer pending → reclaim finds nothing to recover.
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=0
    )
    assert claimed == []


async def test_group_created_at_id_zero_delivers_pre_existing_backlog(stateful_broker) -> None:
    """Behavioral form of #39: a message published BEFORE the group exists is
    still delivered. The adapter's ensure_group(id='0') is what makes this work;
    a regression to '$' would leave the backlog undelivered and fail here."""
    await stateful_broker.publish("orders", b"published-before-subscribe")
    await stateful_broker.ensure_group("orders")  # adapter passes id='0'

    messages = await stateful_broker.read("orders", consumer="c1", count=10, block_ms=0)
    assert messages
    _stream, [(_mid, fields)] = messages[0]
    assert fields[b"data"] == b"published-before-subscribe"


# ---------------------------------------------------------------------------
# registration hook
# ---------------------------------------------------------------------------


def test_register_hook_noop_when_broker_not_redis(make_fake_app) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    configure(package="fakeapp", broker="memory")
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    assert "redis-streams" not in registry.schemes()


def test_register_hook_registers_when_configured(make_fake_app, monkeypatch) -> None:
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    configure(package="fakeapp", broker="redis-streams")
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    assert "redis-streams" in registry.schemes()


def test_register_hook_reads_broker_options_from_config(make_fake_app, monkeypatch) -> None:
    """[tool.modulith.broker] settings (broker_options) reach the adapter.

    Regression: TOML broker options were never parsed/consumed — only env
    vars were honored — so the SPEC's broker config silently did nothing.
    """
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    # No env overrides → broker_options is the sole source.
    for key in ("REDIS_URL", "MODULITH_STREAM_PREFIX", "MODULITH_CONSUMER_GROUP"):
        monkeypatch.delenv(key, raising=False)
    configure(
        package="fakeapp",
        broker="redis-streams",
        broker_options={"stream_prefix": "myapp.evts", "consumer_group": "grp-from-toml"},
    )
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    broker = registry.get("redis-streams")
    assert broker._stream_prefix == "myapp.evts"
    assert broker._consumer_group == "grp-from-toml"


def test_env_var_overrides_broker_options(make_fake_app, monkeypatch) -> None:
    """Env > TOML: deploy-time MODULITH_CONSUMER_GROUP wins over broker_options."""
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("MODULITH_CONSUMER_GROUP", "grp-from-env")
    configure(
        package="fakeapp",
        broker="redis-streams",
        broker_options={"consumer_group": "grp-from-toml"},
    )
    _runtime.ensure_bootstrapped()

    registry = BrokerRegistry()
    modulith_register_brokers(registry=registry)
    broker = registry.get("redis-streams")
    assert broker._consumer_group == "grp-from-env"


# ---------------------------------------------------------------------------
# Integration (real Redis) — gated on MODULITH_TEST_REDIS_URL
# ---------------------------------------------------------------------------

_REDIS_URL = os.environ.get("MODULITH_TEST_REDIS_URL")


@pytest.mark.integration
@pytest.mark.skipif(_REDIS_URL is None, reason="MODULITH_TEST_REDIS_URL not set")
async def test_integration_publish_and_consume_roundtrip() -> None:
    import redis.asyncio as redis

    client = redis.from_url(_REDIS_URL)
    try:
        await client.ping()
    except Exception:  # pragma: no cover - environment-dependent
        pytest.skip("Redis not reachable")

    broker = RedisStreamsBroker(
        url=_REDIS_URL, stream_prefix="modulith.test", consumer_group="itest"
    )
    stream = "roundtrip"
    full = "modulith.test.roundtrip"
    await client.delete(full)
    try:
        await broker.ensure_group(stream)
        await broker.publish(stream, b'{"hello": "world"}', headers={"k": "v"})

        messages = await broker.read(stream, consumer="c1", count=10, block_ms=1000)
        # XREADGROUP returns [(stream, [(id, {field: value}, ...)])]
        assert messages
        _stream_name, entries = messages[0]
        msg_id, fields = entries[0]
        assert fields[b"data"] == b'{"hello": "world"}'
        assert fields[b"h:k"] == b"v"
        await broker.ack(stream, msg_id.decode() if isinstance(msg_id, bytes) else msg_id)
    finally:
        await client.delete(full)
        await broker.close()
        await client.aclose()
