# Deployment Guide

This guide covers scaling modulith from a single-process monolith to a distributed topology of separate worker processes.

---

## Single-Process Monolith (Default)

The simplest deployment: all modules run in one process with an in-memory event bus.

```bash
pip install 'modupy[fastapi,cli]'
MODULITH_BROKER=memory uvicorn myapp.main:app --workers 1
```

**Characteristics:**
- No infrastructure required
- Events are in-memory; lost on crash
- Listeners are synchronous (or wrapped async in an event loop)
- Ideal for: development, testing, non-critical background work

**Trade-off:** No durability. A crash between publishing an event and dispatching it to listeners loses the event.

---

## Durable Single-Process (Outbox Pattern)

Add persistence without splitting processes: publish events atomically with your domain transaction.

In `myapp/main.py`, keep `outbox.configure()` at module import time (the CLI
outbox tooling below depends on that — see **Outbox operations**), and add a
lifespan that starts and stops the retry loop. Module-scope code runs before
the server's event loop exists, so `outbox.configure()` there cannot start the
retry loop: call `outbox.start()` in the lifespan's startup half, or rows a
crashed process left undelivered wait until this process's first
transactional publish. A bare module-scope `outbox.configure()` with no
matching `outbox.shutdown()` leaves the retry loop and the DB engine's
connection pool running until the process is killed instead of draining
gracefully. The outbox table must live in the same database as your business
data: the row commits atomically with your data only inside one transaction.

`main.py` does not run under `--topology processes`: its lifespan, middleware
and this outbox wiring are absent from every worker. Set `outbox_url` (below)
so each worker binds the store itself. A worker whose `outbox` is not
`"memory"` refuses to start with a `ConfigurationError` when neither
`outbox_url`, the module's import nor a `modulith_after_module_load` hook
bound a store. A deployment that exports `MODULITH_OUTBOX` without one of
those therefore stops booting its workers instead of silently publishing
without the outbox. Each worker starts the retry loop itself.

**Binding the store from configuration.** Instead of the module-scope
`outbox.configure()` below, set the outbox database's async SQLAlchemy URL:

```toml
[tool.modulith]
outbox = "postgres"
outbox_url = "postgresql+asyncpg://user:pass@localhost/mydb"  # or MODULITH_OUTBOX_URL
```

- modulith builds a `PostgresPublicationStore` on its own engine for that URL
  and binds it in every process: the single-process server, each
  process-topology worker, and the `modulith outbox ...` commands.
- A store the application binds with `outbox.configure()` before bootstrap
  wins, and no second store is built.
- The claim settings in `[tool.modulith.outbox_options]` (`claim_strategy`,
  `claim_lease_seconds`, `claim_batch_size`) apply to that store.
- The deserialization allowlist is the event types of the process's own
  listeners.
- The binding needs module discovery (`auto_discover`, the default) outside a
  worker, because the allowlist comes from the discovered listeners. With
  `auto_discover = false`, call `outbox.configure()` yourself.
- The outbox table must already exist (run the shipped Alembic migrations);
  the URL must name the database holding your business tables.
- The runtime's shutdown disposes the store and its engine. You still call
  `outbox.start()` in a single-process app's lifespan.

**Stored listener ids.** Each outbox row names its listener. A plain function
is stored as `module.function`. A callable instance or bound method
registered from an application module is stored as
`<module package>:<class module>.<ClassName>`, for example
`myapp.orders:myapp.shared.Notifier`, so one class used by two modules is
delivered per module. Rows written by an earlier release under the bare
`<class module>.<ClassName>` id no longer match a listener: drain them
(`modulith outbox status` shows none incomplete) before upgrading.
```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import configure
from modulith.builtin import outbox
from modulith.adapters.postgres_outbox import PostgresPublicationStore
from modulith.serializers import JsonEventSerializer
from sqlalchemy.ext.asyncio import create_async_engine

from myapp.contracts.events import OrderPlaced, StockReserved

async_engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
store = PostgresPublicationStore(engine=async_engine)
outbox.configure(
    store=store,
    # allowed_event_types is the deserialization allowlist — set it in
    # production wherever payloads can originate outside the trusted process
    # boundary (a shared outbox table, a broker). Without it, a forged
    # event_type could trigger an arbitrary-module import on deserialize, and
    # the first deserialize emits a RuntimeWarning saying so.
    serializer=JsonEventSerializer(allowed_event_types=[OrderPlaced, StockReserved]),
)
configure(outbox="postgres")


@asynccontextmanager
async def lifespan(app: FastAPI):
    outbox.start()  # crash-recovery sweep + retry loop on the server's loop
    yield
    # Teardown order matters: drain/unregister the store's after-commit hook
    # (store.dispose) BEFORE outbox.shutdown() stops the retry loop, and
    # dispose the engine LAST — both prior steps still need it to flush
    # in-flight dispatches and run the retry loop's final sweep.
    await store.dispose()
    await outbox.shutdown()
    await async_engine.dispose()


app = FastAPI(lifespan=lifespan)  # or your own ASGI app with an equivalent shutdown hook
```

Then run:
```bash
pip install 'modupy[fastapi,cli,postgres]'
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 1
```

**What changes:**
- Event publication is deferred until your domain transaction commits (the outbox pattern).
- The event row and your domain row are persisted in the same transaction.
- After commit, the outbox dispatches the event to listeners in a background loop.

**Characteristics:**
- Events persist in the database; recoverable after crash
- Listeners must be idempotent (at-least-once delivery)
- Single point of failure: the database
- Ideal for: critical transactional workflows where losing an event is unacceptable

**One event loop per engine.** Drive one `PostgresPublicationStore`/outbox
`AsyncEngine` from a single event loop. `await publish()` on the app loop and
`publish_sync()` (which runs on a daemon-thread loop — see below) share the
same engine across two loops. asyncpg and aiomysql connections only work on
the loop that opened them, so on Postgres and MySQL the first query a loop
runs on a pooled connection the other loop opened raises `RuntimeError: ...
attached to a different loop`, even when the pool has idle connections; an
asyncpg connection is then also unusable from its own loop (`InterfaceError:
cannot perform operation: another operation is in progress`). On SQLite,
connections work from any loop, but the pool's wait queue belongs to the first
loop that ever waited for a free connection. Another loop that later has to
wait raises `RuntimeError: <Queue> is bound to a different event loop`, and
waiting happens only once every pooled and overflow connection is checked
out, so the SQLite failure depends on load. modulith logs one warning the
first time a second loop uses the engine. Keep every publish for one outbox
engine on one loop; a larger pool only delays the SQLite failure and does not
help on Postgres or MySQL. Unlike the outbox store,
the database broker hands a call from another loop to the loop that owns its
engine; see §A. This also applies to the store bound from `outbox_url`.

**Listeners and durability:**
- The outbox persists only the **first hop** of events (e.g., `orders` → `inventory`).
- If `inventory` publishes a downstream event (e.g., `StockReserved` → `notifications`), that hop is **not durable by default**—it rides the in-memory bus.
- A listener takes exactly one argument — the event. No session is injected, and `publish()` takes no `session=` keyword. For a durable cascade, the listener opens its own session and binds it, so `publish()` finds it and enlists the outbox row in that transaction:
  ```python
  from modulith.adapters.postgres_outbox import bind_session, unbind_session

  @listener
  async def on_order_placed(event: OrderPlaced) -> None:
      async with async_session_maker() as session:
          token = bind_session(session)
          try:
              await session.execute(insert(Reservation).values(...))
              # Bound session ⇒ publish() persists StockReserved into this
              # transaction instead of dispatching it in-memory.
              await publish(StockReserved(...))
              await session.commit()
          finally:
              unbind_session(token)
  ```
  `examples/demo_app/shop/orders/api.py` uses the same `bind_session`/`unbind_session` pair, wrapped in a FastAPI dependency.

**Outbox operations:**

```bash
modulith outbox status              # pending events
modulith outbox retry <event-id>    # retry a failed event
modulith outbox purge               # remove delivered events
modulith outbox dead-letter         # inspect stuck events
```

These run in the CLI's **own** process and operate on the store that process
wires. They cannot reach into a separately-running server: `outbox.configure()`
binds the store in the calling process, and nothing is shared across process
boundaries. So they work only when your bootstrap module — the one the CLI
imports via `[tool.modulith]` — calls `outbox.configure()` at import time. An
app that wires the outbox inside a FastAPI lifespan instead (as
`examples/demo_app` does) gets "no store wired" from the CLI even while the
server is up; inspect that outbox through the running app.

**Per-module Postgres schema.** To keep a module's outbox and broker tables in a DB schema named after the module (see [per-module DB schema ownership](COOKBOOK.md) in the Cookbook), pass `schema_translate_map` to the engine before handing it to `PostgresPublicationStore` — the store takes the app's engine and saves through the app's bound session, so the map applies to every statement it issues, no store-level code change needed:

```python
async_engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/mydb"
).execution_options(schema_translate_map={None: "orders"})
store = PostgresPublicationStore(engine=async_engine)
```

For the database broker, set the schema via
`[tool.modulith.broker_options].schema` or `MODULITH_BROKER_SCHEMA` (Postgres
only; other dialects warn and ignore it). When neither is set, the broker
falls back to `MODULITH_DB_SCHEMA` — the same variable the migrations
read — so the runtime tracks whatever schema was migrated by default; an
explicit `broker_options.schema`/`MODULITH_BROKER_SCHEMA` still wins over
that fallback. Migrations use `MODULITH_DB_SCHEMA` or the packaged command's
global `-x` option before `upgrade`:
`alembic -c <packaged-alembic.ini> -x schema=orders upgrade head`.
Schema identifiers receive the same validation through every entry point.
Enabling a named migration schema does not move data and refuses to abandon
existing Modulith tables or Alembic history in `public`; see
[Migration Guide](../MIGRATION_GUIDE.md) Step 5.

---

## Process-Per-Module Topology

Split modules across separate worker processes for independent scaling, deployment, and lifecycle. Events flow through a broker (database, Redis, or other transports).

**Actuator note for every recipe in this section.** `modulith run` binds `--host 0.0.0.0`, and the default `actuator_mode="auto"` will not serve an unauthenticated `/_modulith/*` on a non-loopback host: with no token configured the actuator is left unmounted (a startup warning says so) and the health probes further down have nothing to call. Export a token if you want them:

```bash
export MODULITH_ACTUATOR_TOKEN="$(openssl rand -hex 32)"
```

See [Actuator Access](#actuator-access-_modulith).

**Forwarding headers.** The reverse proxy overwrites `X-Forwarded-For`,
`X-Forwarded-Proto`, `X-Forwarded-Host`, and `X-Forwarded-Port` from the
connection it accepted, and strips any client-supplied `Forwarded` or
`X-Real-IP` — a client cannot spoof its own IP, scheme, host, or port to a
worker. Behind a TLS-terminating ingress or load balancer, this means the
proxy itself sees the ingress as the client unless you configure trust: set
uvicorn's `FORWARDED_ALLOW_IPS` (env var, e.g. the ingress CIDR or `*` when
the proxy is reachable only through the ingress) on the proxy process so
`request.client`/scheme reflect the real client before this overwrite runs.

**Request targets.** The proxy forwards the path to the matched module's worker without decoding it, so an encoded `%2F` stays one segment. httpx percent-encodes a few characters on the way out (`"`, `<`, `>`, `` ` ``, `{`, `}`), which leaves the decoded path unchanged. The proxy answers `400` for a request-target, as the server's HTTP parser presented it, that does not start with `/` or that contains a `.` or `..` path segment, literal or percent-encoded, and contacts no worker for it. It connects to each worker's loopback port directly and ignores `HTTP_PROXY`, `ALL_PROXY` and related proxy variables in its environment.

**Query strings in logs.** The `modulith.proxy` logger never logs query strings. uvicorn's access log does: on the proxy and on every worker it records each request's full target, query string included, at `INFO`. `modulith run --log-level warning` switches it off on both, together with every other `INFO` line.

**Connection pool.** The proxy holds one upstream connection per in-flight request until the response has finished streaming, so long-polls, server-sent events and slow downloads each occupy one for their whole duration. `MODULITH_PROXY_MAX_CONNECTIONS` (default `1000`) bounds them. A request that finds every connection busy for 5 seconds gets `503` `{"detail": "proxy connection pool exhausted"}`; the worker is not marked down, and `/_modulith/health` and identity probes use a separate 100-connection pool, so a full request pool cannot fail readiness. The warning it logs names the pool that ran out, `request` or `health-probe`. Raise the bound for many concurrent long-lived requests, keeping the proxy's open-file limit (`ulimit -n`) above it.

**Set `state_dir` for the SHM broker in production.** Without it, the `shm`
store lives in a per-user directory named after a digest of the package's
resolved install path (symlinks followed). The same code deployed to another
path — a new `releases/<ts>` behind a `current` symlink, a new venv, another
checkout — therefore opens a new, empty store, and the old store's undelivered
and retained events are never delivered. Pin the location:

```toml
[tool.modulith.broker_options]
state_dir = "/var/lib/myapp/modulith"   # or MODULITH_BROKER_STATE_DIR
```

An absolute `sqlite_path` (or a filesystem `url`) also pins the store. At
startup, the `modulith run` process logs the SQLite store path and the state
directory at INFO, marked as explicit or as the default location. Worker
processes log the same line only when the application configures logging at
INFO. `modulith run --topology processes` also logs a warning while the store
sits in the default state directory.

**Sizing the default SHM store.** The local `shm` broker keeps a publication
while any of these holds:

- a subscribed group has not consumed it yet: an undelivered backlog, which
  includes a retired group that never consumes again;
- it is younger than `orphan_retention_seconds` (default 86400), even after
  every group has acked it, so late subscribers can replay it;
- a delivery of it is kept as a terminal row: every acked delivery under
  `completion_mode = "mark"`, and every dead letter in either mode, holds it
  until `retention_age_seconds` (default 259200, 3 days) after completion.

Its store (`max_store_bytes`, default 1 GiB) therefore caps the sustained
publish rate, not only the backlog:

```
sustainable publications/s ≈ max_store_bytes / (bytes per publication × longest retention above)
```

A 1 KiB payload with two subscribed groups uses about 1.8 KB of store, so the
defaults (delete mode, no dead letters) sustain roughly 7 publications/s; under
`completion_mode = "mark"` the 3-day terminal retention cuts that below
2.3 publications/s. Above that rate, every publish fails with "SHM SQLite store
is full". Size `max_store_bytes` (or `MODULITH_BROKER_MAX_STORE_BYTES`) for the
longest retention, or shorten `orphan_retention_seconds` before the store
fills: each publication keeps the orphan retention stamped when it was
written, so shortening it frees nothing in a store that is already full. A
backlog frees space only as consumers drain it, or when
`modulith broker drop-group` removes a retired group. A new `max_store_bytes`
or `retention_age_seconds` applies to a process only after it restarts.
`orphan_retention_seconds` is capped at 100 years (3153600000).
A group that subscribes after a publication replays it only within that window.
Publishes stop a 32-page consumer reserve (128 KiB at 4 KiB pages) below
`max_store_bytes`. Strictly, the numerator above is `max_store_bytes` minus that
reserve; at the 1 GiB default that is 0.01% and does not change the estimate.
Consumers drain a backlog while publishes are refused: a consumer write or
subscription record that `max_store_bytes` refuses is retried past it, so
`broker.db` can grow past the limit by the growth of rows it already holds. A
subscribe replay stops 8 pages below the publish budget instead (halfway there
under `completion_mode="mark"`), so a publish that fit before it still fits,
also after the group drains the replayed rows; one cut short logs a WARNING
with the replayed and skipped counts and a recovery that drains the group's
backlog before `drop-group --target`, and the skipped publications reach that
group only through another replay. Size
`max_store_bytes` for the retained backlog before adding a listener to a busy
store. This also drains a store that
filled before this release or that was opened with a lowered limit; publishes
resume once the drained store is back under the limit. Budget disk for
`broker.db-wal` on top: it is not counted, reaches about 4 MiB between
checkpoints, and keeps its largest size.

### A. SQLite Database Broker (Zero Infrastructure)

```bash
pip install 'modupy[fastapi,cli,database]' aiosqlite
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=sqlite+aiosqlite:////path/to/broker.db \
  modulith run myapp.main:app --topology processes
```

> **Single-host only.** All workers must access the same SQLite file, so this mode works only on a single machine (or a shared filesystem volume). For multi-host deployments, use Postgres or Redis instead.

Adopting the packaged Alembic migrations after the broker has already
self-bootstrapped its own tables is supported: `alembic upgrade head` stamps
cleanly over a database the broker created, since migrations `0002` and
`0004` inspect the schema first and skip any table/index that already
exists rather than failing on a duplicate.

**What changes:**
- Each module runs in its own worker process.
- A reverse proxy (`modulith/proxy.py`) routes HTTP requests to the correct worker.
- Events published by one module are persisted to the broker database and fanned out to subscribed workers.
- Listeners in other modules run in their own processes and are invoked via async message dispatch.

**Characteristics:**
- No Redis or other external infrastructure needed
- Single database (SQLite or Postgres) is the inter-process broker
- Each module is independently restartable
- Ideal for: single-host deployments where process isolation improves fault tolerance and independent restartability without requiring external infrastructure

**The database broker runs every call on the loop that owns its engine.** An
asyncpg or aiomysql connection only works on the event loop that opened it,
and the broker keeps one connection pool. The first event loop to use the
broker owns that pool. In a worker, that is the app loop, because the consumer
subscribes there at startup. A call from any other loop, such as
`publish_sync()`'s daemon-thread loop in a sync view, is submitted to the
owning loop and waits for it there. Pool sizing plays no part in this.

This has two consequences:

- The owning loop must stay running and unblocked. A marshalled call waits on
  it, so an owner blocked in synchronous code hangs every call from other
  loops until it unblocks. `publish_sync()` already refuses to run on a thread
  whose loop is running.
- When the owning loop has stopped or closed, the next calling loop takes
  ownership. A call that is submitted in the instant the owner stops can wait
  indefinitely, because no loop is left to run it.

modulith logs one warning the first time a call arrives from a second loop.

The database broker needs `FOR UPDATE SKIP LOCKED` to claim messages: MySQL
8.0.1 or newer, or MariaDB 10.6 or newer. A consumer connected to an older
server fails at startup with a `ConfigurationError` that names the server
version and the minimum. There is no unlocked fallback, because under InnoDB's
REPEATABLE READ two consumers would claim the same rows.

**Configuration:**

In `pyproject.toml`:
```toml
[tool.modulith]
broker = "database"
topology = "processes"

[tool.modulith.broker_options]
url = "sqlite+aiosqlite:////path/to/broker.db"  # or postgres://
```

Or via environment:
```bash
export MODULITH_BROKER=database
export MODULITH_BROKER_URL=sqlite+aiosqlite:////path/to/broker.db
modulith run myapp.main:app --topology processes
```

**Module assignment:**

By default, `modulith run --topology processes` detects subpackages and assigns one worker per submodule.

To customize worker counts per module, configure in `pyproject.toml`:
```toml
[tool.modulith.workers]
orders = 2         # 2 workers for orders module
inventory = 3      # 3 workers for inventory module
notifications = 1  # 1 worker for notifications (default)
```

Or override entirely via `--workers` JSON flag:
```bash
modulith run myapp.main:app --topology processes \
  --workers '{"orders": 2, "inventory": 3, "notifications": 1}'
```

### B. Redis Streams Broker

```bash
pip install 'modupy[fastapi,cli,redis]'
docker run -d -p 6379:6379 redis:latest

MODULITH_BROKER=redis-streams \
  REDIS_URL=redis://localhost:6379 \
  modulith run myapp.main:app --topology processes
```

**Characteristics:**
- Scales to high throughput
- Automatic consumer group management
- Built-in dead-letter handling
- Ideal for: high-volume deployments with Redis infrastructure already in place

**⚠️ Redis Durability Caveat:** XADD MAXLEN `~` (approximate trimming) is blind to consumer-group PEL state. An undersized `max_stream_len` can permanently drop unacked entries, violating at-least-once delivery. Size `max_stream_len` well above worst-case backlog: **publish_rate × (consumer_downtime + processing_latency + reclaim_min_idle_ms)**. Default SHM and database brokers are NOT affected.

**Tuning:**

The Redis adapter reads exactly four environment variables:

```bash
export REDIS_URL=redis://localhost:6379        # connection URL
export MODULITH_STREAM_PREFIX=myapp            # stream key prefix
export MODULITH_CONSUMER_GROUP=myapp-workers   # consumer group name
export MODULITH_STREAM_MAXLEN=100000           # XADD MAXLEN ~ cap (see caveat above)
```

The consumer-loop settings have no environment variable — set them in `pyproject.toml`:

```toml
[tool.modulith.broker_options]
poll_block_ms = 1000          # XREADGROUP block timeout
reclaim_min_idle_ms = 60000   # idle threshold before a pending entry is claimed
max_delivery_attempts = 5     # attempts before dead-lettering
```

The Redis client gets `socket_timeout = poll_block_ms / 1000 + 5` seconds (6 s by default) and `socket_keepalive = true`. `XREADGROUP BLOCK` is only a server-side timeout, so without these a hung server or a half-open connection would stall reads forever. Query options in `REDIS_URL` win over these defaults, for example `redis://cache:6379?socket_timeout=30&socket_keepalive=false`. Keep any `socket_timeout` you set above the block time, or every idle poll times out. With the timeout, a slow direct `publish()` raises `TimeoutError` instead of hanging.

Read batch size and listener concurrency are not tunable on the Redis path; scale out with more workers per module (`[tool.modulith.workers]`) instead.

### C. Postgres Broker (Advanced)

For deployments where Postgres is the primary data store and you want a single database:

```bash
pip install 'modupy[fastapi,cli,database]'
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=postgresql+asyncpg://user:pass@localhost/mydb \
  modulith run myapp.main:app --topology processes
```

Uses the `broker_message` and `broker_subscription` tables with `FOR UPDATE SKIP LOCKED` claims for lock-free fan-out. Supports multi-host deployments.

As with the SQLite broker above, running `alembic upgrade head` after the
broker has already self-bootstrapped is supported — migrations `0002` and
`0004` skip tables/indexes that already exist rather than failing.

**Tuning:**

```bash
export MODULITH_BROKER_BATCH_SIZE=50              # events per poll
export MODULITH_BROKER_DISPATCH_CONCURRENCY=5     # parallel listeners
export MODULITH_BROKER_POLL_INTERVAL_MS=1000      # how often to check for new events
```

---

## Docker Deployment

### Single-Process

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install -e '.[fastapi,cli,postgres]'
COPY . .
EXPOSE 8000
CMD ["uvicorn", "myapp.main:app", "--host", "0.0.0.0"]
```

### Process-Per-Module

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install -e '.[fastapi,cli,database]'
COPY . .
EXPOSE 8000-8100
ENV MODULITH_BROKER=database
ENV MODULITH_BROKER_URL=postgresql+asyncpg://...
# The supervisor binds 0.0.0.0, where actuator_mode="auto" only mounts
# /_modulith/* if a bearer token is configured. Pass MODULITH_ACTUATOR_TOKEN in
# at run time (never bake a secret into an image); omit it and the app still
# starts, just without the actuator.
CMD ["modulith", "run", "myapp.main:app", "--topology", "processes"]
```

With `docker-compose.yml`:

```yaml
version: "3.9"
services:
  app:
    build: .
    ports:
      - "8000-8100:8000-8100"
    environment:
      MODULITH_BROKER: database
      MODULITH_BROKER_URL: postgresql+asyncpg://user:pass@postgres/mydb
      # Mounts /_modulith/* on the proxy; drop this line to leave it unmounted.
      MODULITH_ACTUATOR_TOKEN: ${MODULITH_ACTUATOR_TOKEN:?set MODULITH_ACTUATOR_TOKEN}
    depends_on:
      - postgres
      - redis  # if using redis broker

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: modulith
      POSTGRES_PASSWORD: modulith
      POSTGRES_DB: mydb
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:  # optional
    image: redis:7

volumes:
  postgres_data:
```

---

## Kubernetes Deployment

### Single-Process Service

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: app
          image: myapp:1.0.0
          ports:
            - containerPort: 8000
          env:
            - name: MODULITH_OUTBOX
              value: "postgres"
            - name: MODULITH_DB_URL
              valueFrom:
                secretKeyRef:
                  name: db-creds
                  key: url
```

### Process-Per-Module: Generated Manifests

`modulith k8s-manifest` generates one Deployment + Service per module discovered under `--topology processes`, plus a single Ingress fanning out `/<module>` paths to each module's Service:

```bash
modulith k8s-manifest --output k8s/modulith.yaml --image myapp:1.0.0 --namespace prod
```

Options: `--output` (default `modulith-k8s.yaml`, `-` for stdout), `--image`
(default `<package>:latest`), `--namespace`, `--port` (default `8000`, valid
range 1–65535), and `--host` (Ingress host). Deployment and Service object
names are normalized to RFC-1123 labels (`fakeapp-order-items`); long names
keep a readable prefix plus a stable hash, and invalid or colliding names
fail generation. The Ingress path is **not** normalized — it is the raw
module name (`/order_items`), matching the worker's own mount point. A
module name that is not a dotted Python identifier is rejected before any
manifest is generated, since the worker imports it as a module and serves it
under `/<module_name>`.

Each Deployment's `replicas` comes from that module's
`[tool.modulith.workers]` count. Containers run
`python -m uvicorn modulith._worker:create_app --factory --host 0.0.0.0
--port <port>`. The manifest sets module, package, topology, broker, and
`MODULITH_CONTRACTS_MODULE` explicitly. It emits only database broker options
with a supported `MODULITH_BROKER_*` contract and Redis options with their
established `MODULITH_CONSUMER_GROUP`, `MODULITH_STREAM_PREFIX`, and
`MODULITH_STREAM_MAXLEN` names; unknown or credential-like options are omitted.
The broker URL is never embedded: `MODULITH_BROKER_URL` (and `REDIS_URL` for
Redis) reads key `url` from the generated `<package>-broker` Secret reference:

```bash
kubectl create secret generic myapp-broker \
  --from-literal=url=<broker connection URL> --namespace prod
```

The manifest comments include the selected namespace in both Secret-creation
commands. Each container also references an optional `<package>-env` Secret via
`envFrom` for additional variables such as `MODULITH_DB_URL`. Readiness uses
`httpGet /health`; liveness uses `tcpSocket` so a temporary broker outage does
not crash-loop a healthy pod. No `resources` or Secret objects are emitted.

`modulith k8s-manifest` refuses to generate manifests for a broker that cannot be shared across pods: `memory`, `shm`, or `database` pointed at a `sqlite://` URL. Configure `database` with a networked URL (`postgresql://`, `mysql://`) or `redis-streams` first.

The generator imports the configured application modules to derive workers.
Run it only against trusted source in the build environment.

---

## Scaling Strategies

### Horizontal Scaling (Add More Workers)

For **single-process**, add replicas with a load balancer:

```bash
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
```

For **process-per-module**, increase worker counts via `--workers` JSON flag or `pyproject.toml` configuration:

```bash
# Run more inventory workers:
MODULITH_BROKER=database modulith run myapp.main:app --topology processes \
  --workers '{"inventory": 4}'
```

Or in `pyproject.toml`:
```toml
[tool.modulith.workers]
inventory = 4  # more workers for high-traffic module
```

### Vertical Scaling (Increase Worker Resources)

Adjust worker memory and CPU in Kubernetes/Docker:

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

Or scale the broker database (connection pooling, read replicas, etc.).

### Module Isolation (Fault Domain Separation)

Bias capacity toward fault-prone or high-load modules by giving them more workers on the nodes that serve them.

**Every discovered module gets at least one worker.** `[tool.modulith.workers]` counts must be `>= 1`; `notifications = 0` is rejected at boot with `ConfigurationError: workers must map string module names to positive integer counts`, and so is a `0` passed through `--workers` JSON. `modulith run` has no per-deployment module opt-out — "this module does not run here" means a separate application package, not a worker count of zero.

**Approach 1: Separate instances with different worker configurations**

Configuration file 1 (`prod.toml` - for request-serving nodes):
```toml
[tool.modulith.workers]
orders = 4
inventory = 4
notifications = 1    # minimum; cannot be switched off per node
reporting = 1
```

Configuration file 2 (`batch.toml` - for batch nodes):
```toml
[tool.modulith.workers]
orders = 1           # minimum; cannot be switched off per node
inventory = 1
notifications = 2
reporting = 2
```

Then deploy each with appropriate configuration (via environment or config override).

**Approach 2: All modules in one deployment with specific worker counts**

```toml
[tool.modulith.workers]
orders = 4      # high-load module
inventory = 4
notifications = 1
reporting = 1
```

---

## Actuator Access (`/_modulith/*`)

In `--topology processes` the reverse proxy exposes three actuator routes — `/_modulith/topology`, `/_modulith/live`, and `/_modulith/health`. `modulith run` binds `--host 0.0.0.0` by default, so `actuator_mode` decides who may reach them:

| `actuator_mode` | Behaviour |
|---|---|
| `auto` (default) | Open only on a loopback, non-production bind. On any other bind — including the default `0.0.0.0` — a token is required: with one the routes are token-guarded, without one they are not mounted at all and startup logs a warning. |
| `token` | Token always mandatory, loopback included; the supervisor refuses to start without one. |
| `open` | No authentication. Anyone who can reach the port reads your topology and health. |
| `disabled` | Routes are not mounted at all — the right choice when you do not use `/_modulith/*`. Health probes then have nothing to call. |

Both settings come from the environment:

```bash
export MODULITH_ACTUATOR_MODE=auto                        # auto | token | open | disabled
export MODULITH_ACTUATOR_TOKEN="$(openssl rand -hex 32)"
```

When a token is configured, every actuator request must carry `Authorization: Bearer <token>` or it gets 401.

---

## API Documentation (`/<module>/docs`, `/<module>/openapi.json`)

**Per-module API docs are served on the public port.** In `--topology processes` each worker publishes its schema and doc UIs *inside* its own module prefix — the prefix the reverse proxy already forwards verbatim — so they are reachable from outside with no proxy-side configuration:

| URL on the public port | Serves |
|---|---|
| `/<module>/openapi.json` | that module's OpenAPI schema, paths already carrying the prefix (`/orders/place`) |
| `/<module>/docs` | that module's Swagger UI |
| `/<module>/redoc` | that module's ReDoc page |

```bash
curl http://localhost:8000/orders/openapi.json    # the orders worker's schema
open http://localhost:8000/orders/docs            # the orders worker's Swagger UI
open http://localhost:8000/inventory/docs         # a different worker, same public port
```

The same URLs answer on each worker's **internal** port (9001+ by default; `modulith run --worker-port-base N`, `[tool.modulith] worker_port_base = N` or `MODULITH_WORKER_PORT_BASE=N` moves the range, and each module takes one consecutive port per replica), which is where `kubectl port-forward` reaches them — note the module prefix is part of the path there too: `http://127.0.0.1:9001/orders/docs`, not `/docs`. Give each deployment sharing a host its own range: `modulith run` refuses a `--port` inside its worker range, and the proxy checks a worker's identity before its first request, again after the worker was marked down or foreign, and again after its worker process exits, whether or not the supervisor restarts it. A worker answering for another deployment is marked foreign and gets no requests; the proxy answers 503 when no worker of its own serves the module, and readiness reports the module as `foreign deployment`. Each identity probe is bounded (30 s in total and 64 KiB of body); past the deadline the request gets 504 and the worker is not marked down, so a slow but healthy worker is not turned away. Requests arriving while a worker's identity check is in flight wait on that one check instead of starting their own, so a stalled worker holds a single probe connection however many requests queue for it. The deployment token tells this deployment's workers from another deployment's after an accidental port collision. It is not a secret: any local process can read it from a worker's `/health`, so the check does not defend against a hostile process on the same host. The proxy stores no upstream cookie: a `Set-Cookie` reaches only the client that made the request, and each client's own `Cookie` header is forwarded unchanged.

Three consequences worth knowing:

- The app-root paths belong to no module, so `/docs`, `/openapi.json` and `/redoc` return `404 {"detail": "no worker route for '/docs'"}`. Point client generators at a module URL.
- Inside a module's prefix, the module's own routes win: a module named `docs` keeps every path under `/docs/*`, and a module defining its own `/docs` route keeps serving it. What loses the collision is the generated doc UI for that one module, never the application's route. The one exception is the worker's own `GET /health`: in a module named `health` it answers before the module's root `GET` route, and startup logs a warning that the module route is unreachable. The module's other routes, including `/health/`, still reach it.
- Each schema also lists the worker's own unprefixed `/health`. The proxy forwards `/health` only to a module named `health`, so strip it (or ignore the 404) in anything generated against the public port.

**A merged, cross-module schema remains out of scope for the supervisor**, and not for want of plumbing:

- **The pieces do not merge cleanly.** Each worker names its models under `components.schemas`, and two modules that both define an `Order` produce two different definitions of the same key. Merging silently picks one and mistypes the other module's API; renaming rewrites identifiers your generated clients already use.
- **Nothing owns the envelope.** `info.title`, `info.version` and the security schemes are per-worker values. A merged document has to invent one answer, so the version it reports matches no deployed module in particular.
- **It cannot be both fresh and cheap.** Fanning out to every worker per request puts an N-worker round trip on a public endpoint; caching serves a schema that silently lags a rolling deploy.
- **Rollouts have no good answer.** While a worker is respawning, its schema is unavailable — a per-module URL simply returns 502 for that one module, while a merged document must either omit a whole module's API without saying so or fail as a whole.

If you need one document, build it where those answers are yours to make:
`modulith openapi` imports every module, generates its document in isolation,
and prefixes each `components.schemas` key with `<module>_`. Exact duplicates
are deduplicated, but incompatible paths, components, top-level metadata,
schema-key collisions, and duplicate operation IDs fail generation rather than
discarding a definition. Install `modupy[fastapi]`; without FastAPI the command
exits with that actionable installation instruction. Options: `--output`
(default `openapi.json`), `--title` (default: the app package name), and
`--api-version` (default: `[project].version`, else `0.0.0`). Because generation
imports application modules, run it only against trusted source. Alternatively,
keep a checked-in schema generated from the single-process app.

Single-process topology is unaffected — modulith adds no HTTP routes there, so `/docs` is whatever your own FastAPI app configures.

---

## Health Checks and Monitoring

Both probes are served by the reverse proxy on the port `modulith run` binds (8000 by default), and both need the actuator mounted — set `MODULITH_ACTUATOR_TOKEN` (see [Actuator Access](#actuator-access-_modulith)). Plain `/health` belongs to each worker's own internal port (9001+ by default, set by `--worker-port-base` / `worker_port_base` / `MODULITH_WORKER_PORT_BASE`). The proxy answers it with 404, except when a module is named `health`: the proxy then forwards it to that module's worker, and the worker's own `/health` answers. There is no `/ready` route. Each `modulith run` hands its workers a random deployment token that their `/health` echoes as `"deployment"`.

### Liveness Probe (Is the Proxy Running?)

```bash
curl -H "Authorization: Bearer $MODULITH_ACTUATOR_TOKEN" http://localhost:8000/_modulith/live
# Returns 200 while the proxy is serving. Deliberately independent of backend
# health, so a degraded worker never gets the healthy proxy restarted.
```

### Readiness Probe (Are the Workers Up?)

```bash
curl -H "Authorization: Bearer $MODULITH_ACTUATOR_TOKEN" http://localhost:8000/_modulith/health
# Fans out to every worker's /health. 200 when all are ok, 503 otherwise.
```

Each module reports one of these states: `ok`, `unhealthy` (a replica
answered `/health` with a non-200 status), `foreign deployment` (the port
answered without this deployment's token: another deployment's worker, or an
unrelated process, holds it), `unreachable` (a replica is
mid-restart-backoff, or waiting, for up to 60 s, until a process the dead
worker started releases the worker's port), or `failed (given up)` (the crash-loop breaker has
given up on every replica). `failed (given up)` is only reported once every
replica of that module is unreachable — a module with even one healthy
replica reports `ok`.

In Kubernetes, probe headers are static strings — template the token in from the same secret the container reads, or set `MODULITH_ACTUATOR_MODE=open` if the port is only reachable inside the cluster and you accept unauthenticated topology/health:

```yaml
livenessProbe:
  httpGet:
    path: /_modulith/live
    port: 8000
    httpHeaders:
      - name: Authorization
        value: "Bearer <actuator token>"
readinessProbe:
  httpGet:
    path: /_modulith/health
    port: 8000
    httpHeaders:
      - name: Authorization
        value: "Bearer <actuator token>"
```

In single-process topology there is no proxy and no actuator: modulith adds no HTTP routes, so probe whatever endpoint your own app exposes.

**Per-worker-pod probes (generated manifests).** The manifests `modulith k8s-manifest` generates probe each worker pod directly rather than through the proxy: readiness is `httpGet /health` on the container port, and liveness is a `tcpSocket` check on the same port. `/health` returns 503 while that worker's broker consumer isn't ready, which readiness correctly treats as not-yet-serving; liveness intentionally does not use `httpGet`, since a worker whose broker connection is temporarily down would otherwise get killed and restarted for no reason.

**When a broker error degrades `/health`.** A failed broker call puts the consumer in `degraded` (503) until the same operation next succeeds for the same stream or target. Completion writes (ack, fail, dead-letter, claim renewal) can also clear in these ways:

- On every consumer, the failure expires after the redelivery window: `reclaim_min_idle_ms` for Redis, `reclaim_stale_seconds` for the database and SHM brokers. By then the affected message is eligible for reclaim, here or on a peer replica. A write that fails again on the retry records a fresh failure, so a persistent failure shows up again, but `/health` can report ready between the expiry and that retry.
- On Redis only, the failure clears as soon as its message is no longer pending in the consumer group, for example because a peer replica reclaimed and acknowledged it.

A failure on one target never clears because another target succeeded. The exceptions are the database and SHM consumers' claim and claim-renewal heartbeat, which are tracked per consumer group.

**When a stalled consumer degrades `/health`.** A consumer that stops making progress reports `degraded` (503) even when no broker call has failed:

- **A listener that never returns** (database and SHM brokers). The consumer claims nothing new until every row of its current batch has finished. Once a batch has run longer than `reclaim_stale_seconds * 10` (default 600 s), health reports `degraded` with the stuck event type, target and row. The consumer also logs an ERROR line starting `claim renewal for group ... exceeded` that names the same rows. The listener keeps running, because modulith never cancels user code. The consumer recovers only when the listener returns.
- **A Redis server that stops answering.** Health reports `degraded` ("no broker read completed in N s") once a read has waited `5 * poll_block_ms + 1 s`. The consumer logs one WARNING per stall. The Redis client's socket timeout (see [Redis Streams Broker](#b-redis-streams-broker)) then fails the hung read. The consumer logs `broker read failed`, backs off and retries, and health stays `degraded` until a read succeeds.

Restart a worker whose health stays `degraded` longer than you can tolerate; on a stuck listener a restart is the only remedy. The restarted consumer reclaims the stuck rows and charges each one a delivery attempt, so a listener that hangs on every delivery ends in the dead-letter state after `max_delivery_attempts`. The generated Kubernetes manifests do not do this for you: liveness is a `tcpSocket` check, so a degraded worker is only taken out of readiness. Add a liveness `httpGet /health` (with a generous `failureThreshold`) or an external watchdog if you want automatic restarts.

### Event Metrics

If OpenTelemetry is enabled (`modupy[otel]`), spans are emitted for:
- `modulith.publish` — an event was published
- `modulith.listen` — an event was dispatched to a listener
- `modulith.outbox.dispatch` — the outbox dispatched a batch

Export spans to Prometheus, Jaeger, or your observability stack.

---

## Operational Playbooks

### Adding a New Module

1. Create the subpackage: `myapp/my_module/__init__.py`
2. Add manifest: `myapp/my_module/_manifest.py`
3. Restart the supervisor (in single-process, restart the app; in topology mode, restart the appropriate worker)

### Retiring a Module

1. Remove listeners via `@listener` markers (existing events are no-longer-delivered)
2. Stop publishing events to that module
3. Run outstanding events via `modulith outbox retry` if needed
4. Delete the subpackage
5. Restart
6. Drop the retired module's broker consumer group (`modulith-<module>`; a
   renamed module leaves its old name's group behind the same way). Its
   consumer served the group until step 5, so for 24 hours after that the
   group still counts as live: `drop-group` refuses it unless you pass
   `--force`. Pass `--force` once you have checked that no service or host
   still runs the group, or wait 24 hours. The startup warning for a
   forgotten group likewise appears only when `modulith run` restarts at
   least 24 hours after the group's last consumer activity; the restart in
   step 5 does not report it.

Step 6 depends on the broker:

- **SHM and database brokers:** every publication fans out one delivery per
  subscribed group, and prune never removes undelivered work, so a group
  that never consumes again gets a queued row for every later publication
  to its targets. On SHM those rows also keep prune from reclaiming the
  publications; on the database broker they pile up in `broker_message`.
  `modulith run --topology processes` logs a warning at startup for each
  group that no current module derives and no consumer served in the last
  24 hours, naming its backlog. A running consumer re-stamps its
  subscriptions every hour even when idle, and every claim counts too, so
  a group that an extracted service or another host still consumes is not
  reported. Once the module is gone for good, run
  `modulith broker drop-group modulith-<module>` with the same broker
  configuration and environment as the service. It first prints the store
  it acts on (the SHM SQLite path, or the database URL with the password
  masked) and exits non-zero without creating anything when that store or
  its broker tables do not exist. It removes the group's subscriptions and
  deletes its pending and claimed messages (they are not delivered); the
  next prune reclaims the publications they held. It asks for confirmation
  unless `--yes` is given, exits non-zero when the store holds nothing for
  the group, and refuses a group that a current module derives or a
  consumer served in the last 24 hours unless `--force` is given. Before
  asking, it lists the targets the group is the only subscriber of and says
  what happens to later publishes to them, which step 2 stops first:
  - database broker, `error` policy (the default): each raises
    `NoSubscribersError`;
  - database broker, `wait` policy: each waits up to
    `no_subscriber_wait_timeout_seconds` (30 s by default), succeeds if a
    group subscribes meanwhile, and raises `NoSubscribersError` otherwise;
  - database broker, `store` policy: each is kept for
    `orphan_retention_seconds` (86400 s by default) and replayed to a group
    that subscribes before then (`ttl_all_groups`; only the first such group
    under `first_groups`), then pruned; under `expected_groups` it fans out
    at once to `expected_consumer_groups` and nothing is kept for a later
    subscriber;
  - SHM broker: each is kept for `orphan_retention_seconds` and replayed to
    a group that subscribes within it while the store has room below its
    publish budget.

  On the database broker with `no_subscriber_policy = "store"` and
  `orphan_replay_policy = "expected_groups"`, a group named in
  `expected_consumer_groups` gets a pending message for every later publish
  to those targets whether or not it subscribes, so dropping it has no
  lasting effect and the startup warning returns. `drop-group` says so;
  remove the group from `expected_consumer_groups` as well.
  Nothing is dropped automatically: a module that is only disabled for a
  deploy gets its backlog when it returns.
- **Redis Streams broker:** no storage cleanup is needed. Streams are
  trimmed by `MAXLEN` regardless of consumer groups, so a stale group holds
  no memory beyond its pending-entries list. To stop it from showing up
  with a growing lag in `XINFO GROUPS`, remove it from each stream it read
  with `XGROUP DESTROY <stream> modulith-<module>`.

### Stale Targets After a Listener Moves or an Upgrade

A module that still exists can stop consuming a target: its last listener
for an event moved to another module, or an upgrade changed which event
types a worker consumes. Since the release in which a worker consumes only
its own module's listeners, a worker no longer subscribes to event types
that only a sibling module it imports listens to.

On the SHM and database brokers, subscriptions are never removed
automatically, and neither are the pending or claimed messages queued for
them, so a rollback to a release that still consumes the target finds its
backlog intact. (SHM drops the group's subscription rows for targets it no
longer consumes when the worker starts, but keeps their queued deliveries.)
Until you clean up:

- every publish to the target still adds a message for the group;
- the group's consumer claims only the targets it currently consumes, so
  those messages stay pending, are never dead-lettered, and log no error;
- each time the worker's consumer starts, it logs one WARNING per stale
  target, naming the group, the target, its undelivered count, and the
  cleanup command. A module left with no targets at all still starts an
  empty consumer on these brokers, so it warns too.

Once no release that consumes the target will run again, remove it:

```bash
modulith broker drop-group modulith-<module> --target <target> [--target <other>] [--yes]
```

This removes only those subscriptions and deletes their pending and
claimed messages (they are not delivered). Later publishes to the target no
longer reach the group. It asks for confirmation unless `--yes` is given.
Unlike the whole-group form, it needs no `--force` for a current module's
group.

### Recovering from Broker Failure

**Database broker:**
- If the database is down, workers buffer events in memory and retry on reconnect
- Outbox events persist to the database; the background loop retries

**Redis broker:**
- Consumer groups are created by the first consumer; if all are down, events accumulate in Redis
- On restart, the supervisor reads pending entries and rebalances

### Graceful Shutdown

In Kubernetes:

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 5"]  # wait for in-flight requests
```

Consumer shutdown is bounded: a poll task whose cancellation is absorbed (a
driver that never finishes closing a cancelled connection) is cancelled again
after 10 s and, if it still ignores that, abandoned with an error log after
another 10 s, so `stop()` returns within 20 s in the worst case.

Modulith's supervisor handles SIGTERM and drains listeners before exit — on
POSIX. Windows has no signal delivery on `subprocess.Popen` (`terminate()`
is an immediate `TerminateProcess`, with no softer step for a worker's
lifespan to trap), and the `PDEATHSIG` orphan protection that stops a
hard-killed supervisor from leaving workers behind is Linux-only.

---

## Migration Path: Monolith → Processes → Microservices

1. **Start monolithic** — `MODULITH_BROKER=memory` (default)
   - Fast to iterate
   - All modules in one process

2. **Add durability** — `MODULITH_OUTBOX=postgres`
   - Same single-process deployment
   - Events now persist; listeners are at-least-once

3. **Split processes** — `MODULITH_BROKER=database --topology processes`
   - Modules now run in separate workers
   - Module code doesn't change; listeners stay `@listener` decorated
   - `main.py` does not run in workers: its lifespan, middleware, exception handlers and any `outbox.configure()` there are absent. Set `MODULITH_OUTBOX_URL` (or `[tool.modulith].outbox_url`) so every worker binds the outbox store itself, or bind it from each module's import or a `modulith_after_module_load` hook
   - A worker with `MODULITH_OUTBOX` set and no store bound refuses to start with a `ConfigurationError`, so a deployment that exported `MODULITH_OUTBOX` for step 2 must add the URL before this step
   - Each worker starts the outbox retry loop itself at startup

4. **Extract microservice** — `modulith extract <module>` scaffolds a standalone service
   - Copies the module, its contracts and every package-level helper module they import, transitively, into `--output` (default `<module>-service/`) and generates a `pyproject.toml`, `Dockerfile`, `README.md`, and `.env.example` to run it against `modulith._worker:create_app`. Contracts resolve as Python imports them: a `contracts.py` wins over a same-named directory without `__init__.py`, and a contracts directory without `__init__.py` has its files scanned for helper imports like any contracts package. A package ancestor with no `__init__.py` in the source (a PEP 420 namespace root such as `company/` for `package = "company.shop"`) gets none in the output either, so other portions of that namespace stay importable. The same holds for a helper's parent folder: one without `__init__.py` in the source (such as `shop/common/` holding `money.py`) stays a namespace folder in the output, so the service discovers no extra module; one with an `__init__.py` gets an empty initializer
   - Blocked (exit 1) by the module's own outbound boundary violations, tables it shares with another module, or a runtime import of another declared module from any copied file — `--force` overrides only these three and records what it overrode in the generated README; a non-empty `--output` directory is never overridable
   - Before publishing, imports the extracted module in a subprocess from the staged tree and exits 1 naming the failing import if that fails, or if the import loads first-party code from the source tree outside the extracted copy (reachable through `PYTHONPATH` or an editable install), so the service's third-party dependencies must be installed where you run `extract`. First-party code is anything under the directory that holds the app's top-level package. Modules under the interpreter's prefixes, standard library and site-packages directories are exempt, except that a directory containing that source tree exempts nothing: a virtualenv inside the project stays exempt, and a project inside a virtualenv is still checked (on Windows, `site.getsitepackages()` lists the virtualenv root itself). When the app resolves to an installed copy (a non-editable `pip install`), the directory holding it is site-packages, so only the app's own top-level package counts as first-party there and other installed distributions stay exempt. `--force` never overrides this import check, so a module-level import of another declared module fails even when forced; only a deferred one (inside a function) can be forced through
   - Other modules keep sending events via the broker; the extracted service subscribes and acts. The outbox is not auto-wired (the app's `main.py` is not copied), so code the worker imports must call `outbox.configure()` itself

This path is why modulith exists: **every module is a potential microservice, but you pay that cost only when it's profitable.**

---

## Troubleshooting

### Events Not Delivered

1. Check that the listener's module was actually imported: `modulith info` → the module must appear under `modules` marked `[manifest]`. (`info` prints modules, configuration, plugins, and registered broker schemes — there is no per-listener listing. A module that silently failed to import is the most common cause, and manifest verification is what turns a declared-but-unregistered listener into a boot failure.)
2. Verify the manifest declares the event: `_manifest.py` → check `consumes`
3. Inspect broker state:
   - Database: `SELECT * FROM broker_message WHERE status IN ('pending','claimed')` for work still in flight, and `WHERE status = 'dead'` for the dead-letter view. The column only ever holds `pending`, `claimed`, `done`, or `dead`.
   - Redis: `xinfo groups myapp-events`

### Worker Crash Loop

1. Check logs: `modulith run --topology processes 2>&1 | grep ERROR`
2. Verify broker connectivity: `modulith doctor`
3. Check disk space (SQLite needs it)

### High Latency

1. Database broker — tune batch size: `MODULITH_BROKER_BATCH_SIZE=200`
2. Database broker — increase concurrency: `MODULITH_BROKER_DISPATCH_CONCURRENCY=20`
3. Redis broker — neither knob exists; add workers instead (`[tool.modulith.workers]`)
4. Profile with `modupy[otel]` and check span duration

---

## Reference: Supervisor, Proxy, and Boundary Environment Variables

Broker and outbox settings are covered in the topology sections above
(`MODULITH_BROKER`, `MODULITH_BROKER_<KEY>`, `MODULITH_OUTBOX`,
`MODULITH_DB_URL`). These are the remaining process-level knobs:

| Variable | Default | Effect |
|---|---|---|
| `MODULITH_ACTUATOR_MODE` | `auto` | `auto` \| `token` \| `open` \| `disabled` — see [Actuator Access](#actuator-access-_modulith). |
| `MODULITH_ACTUATOR_TOKEN` | unset | Bearer token for `/_modulith/*`. Required to mount the actuator under `auto` on a non-loopback bind or in production. |
| `MODULITH_PRODUCTION` | unset (false) | `1`/`true`/`yes`, case-insensitive. Treats the deployment as production: the actuator's `auto` mode requires a token even on loopback, and the boundary gate below is never disarmed. |
| `MODULITH_PROXY_MAX_BODY_BYTES` | `10485760` (10 MiB) | Per-request body cap for the reverse proxy in `--topology processes`. The proxy buffers each request body in memory, which is why the cap exists; raise it for large uploads. Must be a positive integer — anything else fails startup with a `ConfigurationError`, rather than silently reverting to the default. |
| `MODULITH_PROXY_MAX_CONNECTIONS` | `1000` | Most upstream connections the reverse proxy in `--topology processes` holds open at once. Each in-flight request, including a long-poll or streaming response, uses one until it finishes; a request that finds none free for 5 seconds gets `503`. Must be a positive integer — anything else fails startup with a `ConfigurationError`. See [Connection pool](#process-per-module-topology). |
| `MODULITH_DEV_WARN_ONLY` | unset | Set to `1` by single-process `modulith dev` only. Under `strict_boundaries = true`, boundary violations then log a warning instead of aborting the boot, keeping interactive development usable. Process topology and `modulith run` ignore the marker. It is an environment variable rather than an in-process flag because it has to survive uvicorn's `--reload` fork. Do not set it in a deployment. |

---

## Reference: Broker Comparison

| Broker | Setup | Durability | Scale | Ideal For |
|---|---|---|---|---|
| `memory` | None | No | Single-process | Dev/test |
| `database` (SQLite) | Local file | Yes | Single-host process-per-module (shared filesystem required for multi-process) | Dev, single-host staging |
| `database` (Postgres) | Existing DB | Yes | Multi-host process-per-module | Production monolith and distributed |
| `redis-streams` | Docker/Cloud | Yes | High throughput, multi-host | High-load production |

---

## Next Steps

- For **detailed internal architecture**, see [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- For **API reference**, see [docs/API_REFERENCE.md](API_REFERENCE.md)
- For **working examples**, see [examples/demo_app](../examples/demo_app)
- For **testing**, see [Cookbook §9](COOKBOOK.md#9-test-an-event-flow-with-the-pytest-plugin)
