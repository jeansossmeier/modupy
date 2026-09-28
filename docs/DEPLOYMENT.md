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
outbox tooling below depends on that — see **Outbox operations**), but add a
lifespan whose only job is teardown: a bare module-scope `outbox.configure()`
with no matching `outbox.shutdown()` leaves the retry loop and the DB
engine's connection pool running until the process is killed, relying
entirely on the crash-recovery sweep on next start instead of a graceful
drain:
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
same engine across two loops; once the connection pool is exhausted,
SQLAlchemy raises `RuntimeError: <Queue> is bound to a different event
loop` (SQLite's forced `pool_size=1` hits this on the first concurrent
publish; Postgres/MySQL only under load). modulith logs one warning the
first time a second loop uses the engine. Keep publishes on one loop, or
size `pool_size`/`max_overflow` for the cross-loop concurrency. The database
broker does not share its pool across loops; see §A.

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

**Request targets.** The proxy forwards the client's path bytes unchanged (an encoded `%2F` stays one segment) to the matched module's own worker only. It answers `400` for a request-target that does not start with `/` or that contains a `.` or `..` path segment, literal or percent-encoded, and contacts no worker for it.

**Sizing the default SHM store.** The local `shm` broker keeps every
publication for `orphan_retention_seconds` (default 86400) even after every
group has acked it, so late subscribers can replay it. Its store (`max_store_bytes`,
default 1 GiB) therefore caps the sustained publish rate, not only the backlog:

```
sustainable publications/s ≈ max_store_bytes / (bytes per publication × orphan_retention_seconds)
```

A 1 KiB payload with two subscribed groups uses about 1.8 KB of store, so the
defaults sustain roughly 7 publications/s; above that, every publish fails with
"SHM SQLite store is full" after about a day. Raise `max_store_bytes` or
shorten `orphan_retention_seconds` in `[tool.modulith.broker_options]` (or
`MODULITH_BROKER_MAX_STORE_BYTES` / `MODULITH_BROKER_ORPHAN_RETENTION_SECONDS`).
A group that subscribes after a publication replays it only within that window.

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

The same URLs answer on each worker's **internal** port (9001+), which is where `kubectl port-forward` reaches them — note the module prefix is part of the path there too: `http://127.0.0.1:9001/orders/docs`, not `/docs`.

Three consequences worth knowing:

- The app-root paths belong to no module, so `/docs`, `/openapi.json` and `/redoc` return `404 {"detail": "no worker route for '/docs'"}`. Point client generators at a module URL.
- Inside a module's prefix, the module's own routes win: a module named `docs` keeps every path under `/docs/*`, and a module defining its own `/docs` route keeps serving it. What loses the collision is the generated doc UI for that one module, never the application's route.
- Each schema also lists the worker's own unprefixed `/health`, which the proxy does not forward. It answers on the internal port only, so strip it (or ignore the 404) in anything generated against the public port.

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

Both probes are served by the reverse proxy on the port `modulith run` binds (8000 by default), and both need the actuator mounted — set `MODULITH_ACTUATOR_TOKEN` (see [Actuator Access](#actuator-access-_modulith)). Plain `/health` exists only on each worker's own internal port (9001+) and 404s on the proxy; there is no `/ready` route.

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

Each module reports one of three states: `ok`, `unreachable` (a replica is
mid-restart-backoff), or `failed (given up)` (the crash-loop breaker has
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
   renamed module leaves its old name's group behind the same way)

Step 6 depends on the broker:

- **SHM and database brokers:** every publication fans out one delivery per
  subscribed group, and prune never removes undelivered work, so a group
  that never consumes again pins every later publication to its targets
  until the store fills. `modulith run --topology processes` logs a warning
  at startup for each subscribed group that no current module derives,
  naming its backlog. Once the module is gone for good, run
  `modulith broker drop-group modulith-<module>` from the project root. It
  removes the group's subscriptions and deletes its pending and claimed
  messages (they are not delivered); the next prune reclaims the
  publications they held. It asks for confirmation unless `--yes` is given,
  and refuses a group that a current module derives unless `--force` is
  given. Nothing is dropped automatically: a module that is only disabled
  for a deploy gets its backlog when it returns.
- **Redis Streams broker:** no storage cleanup is needed. Streams are
  trimmed by `MAXLEN` regardless of consumer groups, so a stale group holds
  no memory beyond its pending-entries list. To stop it from showing up
  with a growing lag in `XINFO GROUPS`, remove it from each stream it read
  with `XGROUP DESTROY <stream> modulith-<module>`.

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
   - Code doesn't change; listeners stay `@listener` decorated

4. **Extract microservice** — `modulith extract <module>` scaffolds a standalone service
   - Copies the module plus its contracts into `--output` (default `<module>-service/`) and generates a `pyproject.toml`, `Dockerfile`, `README.md`, and `.env.example` to run it against `modulith._worker:create_app`
   - Blocked (exit 1) by the module's own outbound boundary violations or tables it shares with another module — `--force` overrides either and records what it overrode in the generated README; a non-empty `--output` directory is never overridable
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
