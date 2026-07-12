"""Tests for the production Redis Streams broker adapter.

Two layers:

  * **Unit** (default) — drive the broker against a hand-rolled async fake
    Redis client that records calls. ``fakeredis`` isn't a dependency, and a
    recording double is enough to pin the exact Redis commands the adapter
    issues (XADD with bounded MAXLEN, idempotent XGROUP CREATE, XREADGROUP,
    XACK, XAUTOCLAIM for crash recovery, dead-letter XADD) without a server.

  * **Integration** (``@pytest.mark.integration``) — a real publish→consume
    round-trip against a live Redis provisioned by the ``redis_url``/
    ``redis_client`` fixtures (a throwaway testcontainers Redis when Docker is
    available, or ``MODULITH_TEST_REDIS_URL`` when set), else skipped.

The fake matches redis-py's async surface for the methods the adapter uses.
"""

from __future__ import annotations

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
        # Canned XAUTOCLAIM reply — (cursor, claimed, deleted). Tests may
        # override it (e.g. to stage a non-empty deleted list, the
        # trimmed-while-pending loss channel real Redis reports; audit
        # S3-r3-160 — the old hardcoded return made that path unmodelable).
        self.xautoclaim_result: Any = (b"0-0", [(b"1-0", {b"data": b"{}"})], [])
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
        return self.xautoclaim_result

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

    Models (semantics probed against real Redis 7 / redis-py 6.4):
      * append-only streams with monotonic ``<seq>-0`` ids; ``maxlen`` trims
        the stream WITHOUT touching any group's PEL (real MAXLEN trimming is
        blind to pending state — audit A7-r1-24);
      * per-(stream, group) last-delivered cursor and a pending-entries dict
        (PEL) recording the owning consumer and a delivery timestamp on the
        fake's **virtual clock** (``now_ms`` / ``advance()`` — deterministic,
        no sleeps);
      * ``>`` reads deliver only past-the-cursor messages and add them to the
        PEL; XACK removes from the PEL;
      * XREADGROUP with no data returns ``[]`` for ``block=None`` and any
        ``block > 0`` (redis-py returns ``[]`` on a BLOCK timeout; the fake
        skips the actual wait). ``block == 0`` on an empty read raises — real
        Redis blocks FOREVER there (audit A7-r2-92), which a fake cannot
        model, so it fails loudly instead of returning the inverted ``[]``;
      * XAUTOCLAIM honors ``min_idle_time`` against the virtual clock (audit
        S3-r2-120), caps claims at ``count`` (Redis default 100) and returns
        an inclusive continuation cursor — ``0-0`` once the PEL scan
        completes (audit A7-r3-140); claiming reassigns the consumer and
        RESETS the idle clock; pending ids no longer present in the stream
        (trimmed/XDEL'd while pending) are reported via the third (deleted)
        tuple element and purged from the PEL (audits S3-r3-160, A7-r1-24).
    """

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[bytes, dict[bytes, bytes]]]] = {}
        # (stream, group) -> {"cursor": int, "pel": dict[bytes, dict]}
        # pel entry: mid -> {"consumer": str, "delivered_ms": int}
        self.groups: dict[tuple[str, str], dict[str, Any]] = {}
        self._seq: dict[str, int] = {}
        self.closed = False
        # Virtual clock (ms). Deliveries/claims are stamped with it; tests
        # advance it to make idle-time behavior deterministic.
        self.now_ms = 0

    def advance(self, ms: int) -> None:
        """Advance the virtual clock — 'time passes' for idle-time checks."""
        self.now_ms += ms

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
                    grp["pel"][mid] = {"consumer": consumername, "delivered_ms": self.now_ms}
                    grp["cursor"] = seq
                    if count is not None and len(delivered) >= count:
                        break
            if delivered:
                out.append((name.encode(), delivered))
        if not out and block == 0:
            raise NotImplementedError(
                "XREADGROUP BLOCK 0 with no data blocks FOREVER on real Redis "
                "(audit A7-r2-92) — the fake cannot model an infinite block; "
                "pass block > 0 (returns [] on timeout) or block=None."
            )
        # No data: redis-py returns [] both non-blocking and after a BLOCK
        # timeout (probed, Redis 7 / redis-py 6.4). The fake skips the wait.
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
        start_id: str | bytes = "0-0",
        count: int | None = None,
    ) -> Any:
        limit = 100 if count is None else count  # Redis COUNT default is 100
        pel = self.groups[(name, groupname)]["pel"]
        by_id = dict(self.streams.get(name, []))
        sid = start_id.decode() if isinstance(start_id, bytes) else start_id
        start_seq = int(sid.split("-")[0])
        claimed: list[tuple[bytes, dict[bytes, bytes]]] = []
        deleted: list[bytes] = []
        cursor = b"0-0"  # scan completed unless the count cap interrupts it
        for mid in sorted(pel.keys(), key=lambda m: int(m.split(b"-")[0])):
            if int(mid.split(b"-")[0]) < start_seq:
                continue  # start_id is INCLUSIVE (probed real-Redis behavior)
            if len(claimed) + len(deleted) >= limit:
                cursor = mid  # continuation cursor: next unscanned entry
                break
            if mid not in by_id:
                # Trimmed/XDEL'd while still pending: real Redis reports the id
                # via the third tuple element and purges it from the PEL as a
                # side effect (audits S3-r3-160, A7-r1-24) — it is NOT handed
                # back as a claimable entry with empty fields.
                del pel[mid]
                deleted.append(mid)
                continue
            entry = pel[mid]
            if self.now_ms - entry["delivered_ms"] < min_idle_time:
                continue  # not idle long enough — real XAUTOCLAIM skips it
            entry["consumer"] = consumername  # reassign to the claiming consumer
            entry["delivered_ms"] = self.now_ms  # claiming RESETS the idle clock
            claimed.append((mid, by_id[mid]))
        return (cursor, claimed, deleted)

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def stateful_fake() -> StatefulFakeRedis:
    return StatefulFakeRedis()


@pytest.fixture
def stateful_broker(stateful_fake: StatefulFakeRedis) -> RedisStreamsBroker:
    return RedisStreamsBroker(
        client=stateful_fake, stream_prefix="modulith.events", consumer_group="g"
    )


async def test_publish_then_consume_round_trip(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b'{"id": 1}', headers={"event_type": "Order"})

    messages = await stateful_broker.read("orders", consumer="c1", count=10, block_ms=1)
    assert messages
    _stream, entries = messages[0]
    (_mid, fields) = entries[0]
    assert fields[b"data"] == b'{"id": 1}'
    assert fields[b"h:event_type"] == b"Order"
    # A second ">" read returns nothing new — the message was delivered once.
    # block_ms must be POSITIVE: BLOCK 0 on an exhausted stream blocks forever
    # on real Redis (audit A7-r2-92); a positive BLOCK times out and returns [].
    assert await stateful_broker.read("orders", consumer="c1", count=10, block_ms=1) == []


async def test_unacked_message_is_recoverable_via_reclaim(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b"payload")
    [(_s, [(mid, _f)])] = await stateful_broker.read("orders", consumer="c1", block_ms=1)

    # Not ACK'd → still pending → another consumer can reclaim it (crash recovery).
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=0
    )
    assert [m for m, _ in claimed] == [mid]


async def test_ack_clears_message_from_pending(stateful_broker) -> None:
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b"payload")
    [(_s, [(mid, _f)])] = await stateful_broker.read("orders", consumer="c1", block_ms=1)

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

    messages = await stateful_broker.read("orders", consumer="c1", count=10, block_ms=1)
    assert messages
    _stream, [(_mid, fields)] = messages[0]
    assert fields[b"data"] == b"published-before-subscribe"


async def test_read_with_block_zero_on_empty_stream_is_rejected_by_fake(stateful_broker) -> None:
    """BLOCK 0 on an empty stream blocks FOREVER on real Redis (audit
    A7-r2-92) — the old fake returned [] immediately, the inverted contract.
    The fake cannot block forever, so it must fail loudly instead."""
    await stateful_broker.ensure_group("orders")
    with pytest.raises(NotImplementedError, match="BLOCK 0"):
        await stateful_broker.read("orders", consumer="c1", block_ms=0)


async def test_reclaim_honors_min_idle_time(stateful_fake, stateful_broker) -> None:
    """XAUTOCLAIM must NOT steal a freshly-delivered in-flight message (audit
    S3-r2-120): real Redis refuses to claim entries idle < min_idle_time; the
    old fake claimed everything unconditionally."""
    await stateful_broker.ensure_group("orders")
    await stateful_broker.publish("orders", b"payload")
    [(_s, [(mid, _f)])] = await stateful_broker.read("orders", consumer="c1", block_ms=1)

    # Freshly delivered (idle ~0) — the production 60s threshold must not
    # claim it away from the (possibly just slow) owning consumer.
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=60_000
    )
    assert claimed == []

    # Once it has been pending past the threshold, it IS reclaimable.
    stateful_fake.advance(60_000)
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=60_000
    )
    assert [m for m, _ in claimed] == [mid]

    # Claiming RESET the idle clock (probed real-Redis behavior): the entry is
    # immediately un-reclaimable again until it re-ages past the threshold.
    _cursor, claimed, _deleted = await stateful_broker.reclaim(
        "orders", consumer="c3", min_idle_ms=1
    )
    assert claimed == []


async def test_fake_xautoclaim_caps_at_count_and_pages_via_cursor(
    stateful_fake, stateful_broker
) -> None:
    """Real XAUTOCLAIM enforces COUNT and returns a continuation cursor (audit
    A7-r3-140, fake-fidelity half) — the old fake returned the entire PEL in
    one call. Probed at raw-client level; the adapter's reclaim() follows the
    cursor itself (test_reclaim_follows_cursor_to_drain_full_backlog)."""
    await stateful_broker.ensure_group("orders")
    for i in range(5):
        await stateful_broker.publish("orders", f"m{i}".encode())
    await stateful_broker.read("orders", consumer="c1", count=100, block_ms=1)  # 5 → PEL

    cursor1, claimed1, _ = await stateful_fake.xautoclaim(
        "modulith.events.orders", "g", "c2", 0, start_id="0-0", count=2
    )
    assert len(claimed1) == 2  # capped at count, NOT all 5
    assert cursor1 != b"0-0"  # continuation cursor — more pending remain

    cursor2, claimed2, _ = await stateful_fake.xautoclaim(
        "modulith.events.orders", "g", "c2", 0, start_id=cursor1, count=2
    )
    assert len(claimed2) == 2
    cursor3, claimed3, _ = await stateful_fake.xautoclaim(
        "modulith.events.orders", "g", "c2", 0, start_id=cursor2, count=2
    )
    assert len(claimed3) == 1
    assert cursor3 == b"0-0"  # PEL scan completed
    assert len({m for m, _ in claimed1 + claimed2 + claimed3}) == 5  # no overlap, full drain


async def test_reclaim_follows_cursor_to_drain_full_backlog(stateful_broker) -> None:
    """A7-r3-140 (production half, W2 RESIDUALS item 9): reclaim() used to
    issue a single XAUTOCLAIM(start_id='0-0', count=100) and never follow the
    continuation cursor, so a >COUNT idle-pending backlog drained only across
    successive poll cycles. reclaim() must page via the returned cursor until
    it comes back 0-0 — exactly one full PEL scan, bounded, no spin."""
    await stateful_broker.ensure_group("orders")
    for i in range(5):
        await stateful_broker.publish("orders", f"m{i}".encode())
    await stateful_broker.read("orders", consumer="c1", count=100, block_ms=1)  # 5 → PEL

    cursor, claimed, deleted = await stateful_broker.reclaim(
        "orders", consumer="c2", min_idle_ms=0, count=2
    )

    # ONE reclaim() call drains the whole idle-pending backlog (2+2+1 pages)…
    assert len({m for m, _ in claimed}) == 5  # no overlap, full drain
    assert deleted == []
    # …and reports the completed scan.
    assert cursor == b"0-0"


async def test_trimmed_pending_entry_is_reported_deleted_and_purged(stateful_fake) -> None:
    """MAXLEN-trim vs PEL (audits A7-r1-24 / S3-r3-160): an entry trimmed from
    the stream while still pending is reported via XAUTOCLAIM's third
    (deleted) element and purged from the PEL — NOT handed back as a claimable
    entry with empty fields (the old fake's silently-different failure mode).
    """
    broker = RedisStreamsBroker(
        client=stateful_fake,
        stream_prefix="modulith.events",
        consumer_group="g",
        max_stream_len=2,
    )
    await broker.ensure_group("orders")
    await broker.publish("orders", b"m1")
    [(_s, [(mid1, _f)])] = await broker.read("orders", consumer="c1", count=10, block_ms=1)

    # Publish burst trims m1 out of the stream while it is still pending.
    await broker.publish("orders", b"m2")
    await broker.publish("orders", b"m3")

    _cursor, claimed, deleted = await broker.reclaim("orders", consumer="c2", min_idle_ms=0)
    assert deleted == [mid1]  # reported as permanently lost
    assert claimed == []  # m2/m3 were never delivered → not in the PEL

    # Purged from the PEL server-side: a second reclaim reports nothing.
    _cursor, claimed2, deleted2 = await broker.reclaim("orders", consumer="c2", min_idle_ms=0)
    assert claimed2 == []
    assert deleted2 == []


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


def test_runtime_loads_redis_broker_builtin_when_configured(make_fake_app, monkeypatch) -> None:
    """Normal bootstrap must load the shipped Redis adapter.

    Unit tests that call ``modulith_register_brokers`` directly are not enough:
    process topology uses the runtime's plugin manager, so the adapter must be
    registered through the same built-in plugin path as the rest of modulith.
    """
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    configure(package="fakeapp", broker="redis-streams")

    _runtime.ensure_bootstrapped()

    assert _runtime.broker_registry is not None
    assert "redis-streams" in _runtime.broker_registry.schemes()


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
# Integration (real Redis) — see the redis_url/redis_client fixtures (conftest)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_integration_publish_and_consume_roundtrip(redis_url, redis_client) -> None:
    # redis_client flushes the DB before/after this test for cross-suite isolation.
    broker = RedisStreamsBroker(
        url=redis_url, stream_prefix="modulith.test", consumer_group="itest"
    )
    stream = "roundtrip"
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
        await broker.close()
