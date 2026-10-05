# Deployment Guide

This guide covers scaling modupy from a single-process monolith to a distributed topology of separate worker processes. modupy installs the `modulith` package, so you `import modulith` and run `modulith`.

**Which setup do I need?**

| You need | Use | Details |
|---|---|---|
| One process, nothing to install; losing an in-flight event in a crash is acceptable | In-memory monolith (the default) | [Single-Process Monolith](#single-process-monolith-default) |
| One process whose events survive a crash | Durable outbox in your application's database | [Durable Single-Process](#durable-single-process-outbox-pattern) |
| Modules that scale or fail independently, on one host | `modulith run --topology processes`, with the default `shm` broker | [Process-Per-Module Topology](#process-per-module-topology), [Default Broker: `shm`](#default-broker-shm) |
| Modules on several hosts or Kubernetes pods | Process-per-module with a Postgres or MySQL `database` broker, or Redis Streams | [C. Postgres Broker](#c-postgres-broker-advanced), [B. Redis Streams Broker](#b-redis-streams-broker), [Broker Comparison](#reference-broker-comparison) |
| One module as its own service | `modulith extract` | [Migration Path](#migration-path-monolith--processes--microservices) |

**Contents**

- [Single-Process Monolith (Default)](#single-process-monolith-default)
- [Durable Single-Process (Outbox Pattern)](#durable-single-process-outbox-pattern), with [Reference: outbox details](#reference-outbox-details) and [Postgres and PgBouncer settings for `advisory_lock`](#postgres-and-pgbouncer-settings-for-advisory_lock)
- [Process-Per-Module Topology](#process-per-module-topology)
  - [Default Broker: `shm`](#default-broker-shm)
  - [A. SQLite Database Broker (Zero Infrastructure)](#a-sqlite-database-broker-zero-infrastructure)
  - [B. Redis Streams Broker](#b-redis-streams-broker)
  - [C. Postgres Broker (Advanced)](#c-postgres-broker-advanced)
  - [Sizing the Default SHM Store](#sizing-the-default-shm-store)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Scaling Strategies](#scaling-strategies)
- [Actuator Access (`/_modulith/*`)](#actuator-access-_modulith)
- [API Documentation (`/<module>/docs`, `/<module>/openapi.json`)](#api-documentation-moduledocs-moduleopenapijson)
- [Health Checks and Monitoring](#health-checks-and-monitoring)
- [Operational Playbooks](#operational-playbooks)
- [Migration Path: Monolith → Processes → Microservices](#migration-path-monolith--processes--microservices), with [What `modulith extract` copies and checks](#what-modulith-extract-copies-and-checks)
- [Troubleshooting](#troubleshooting)
- [Reference: Supervisor, Proxy, and Boundary Environment Variables](#reference-supervisor-proxy-and-boundary-environment-variables)
- [Reference: Broker Comparison](#reference-broker-comparison)
- [Next Steps](#next-steps)

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
- Listeners are `async def` or plain `def` (plain ones run in the event loop's thread pool), and all listeners of one event run concurrently
- Ideal for: development, testing, non-critical background work

**Trade-off:** No durability. A crash between publishing an event and dispatching it to listeners loses the event.

---

## Durable Single-Process (Outbox Pattern)

![One commit saves the order and one event_publications row per listener; after the commit each listener runs in the background, and a failing one is retried, then dead-lettered](images/outbox.svg)

Add persistence without splitting processes: publish events atomically with your domain transaction.

Three steps make it durable. Set `outbox_url` so modupy binds the store in
every process, the `modulith outbox` CLI included (see **Binding the store from
configuration** and **Outbox operations**). Create the outbox tables with
`modulith migrate`. Then wrap each business transaction in a bound session, as
`bind_session` below shows. `myapp/main.py` needs only a lifespan that starts
and stops the retry loop. Module-scope code runs before the server's event loop
exists, so a module-scope `outbox.configure()` cannot start the retry loop:
call `modulith.bootstrap()` and then `outbox.start()` in the lifespan's
startup half. Every sweep skips its rows until the runtime is bootstrapped,
and bootstrap otherwise first runs at the first `publish()`, so without both
calls rows a crashed process left undelivered can wait until this process's
first transactional publish. A bare module-scope `outbox.configure()` with no
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
outbox_url = "postgresql+asyncpg://app@localhost/mydb"  # set MODULITH_OUTBOX_URL instead if it needs a password
```

A committed `pyproject.toml` should carry no password. Put the full URL in
`MODULITH_OUTBOX_URL`, which overrides the file.

- modupy builds a `PostgresPublicationStore` on its own engine for that URL
  and binds it in every process: the single-process server, each
  process-topology worker, and the `modulith outbox ...` commands.
- A store the application binds with `outbox.configure()` before bootstrap
  wins, and no second store is built.
- The settings in `[tool.modulith.outbox_options]` apply to that store:
  `claim_strategy`, `claim_lease_seconds`, `claim_batch_size`,
  `dead_letter_after_attempts`, `retry_interval_seconds`,
  `retry_stale_seconds`, `max_retry_backoff_seconds` and `completion_mode`
  (`update`, `delete` or `archive`). For `claim_strategy = "advisory_lock"`, see
  [Postgres and PgBouncer settings for `advisory_lock`](#postgres-and-pgbouncer-settings-for-advisory_lock).
- `sqlite_wal = true` in the same table switches a SQLite `outbox_url` to WAL
  journal mode on every connection of that engine. It is off by default:
  modupy never changes a database file's journal mode unless asked. In the
  default rollback-journal mode a reader blocks a `COMMIT`, so an after-commit
  claim can wait out SQLite's 5 s busy timeout; WAL lets readers and the writer
  run together. WAL is persistent: once set it stays in the database file, and
  removing the key or setting `false` does not switch it back. It adds `-wal`
  and `-shm` files beside the database and cannot be used on network
  filesystems. The key applies to SQLite only and is ignored for other
  databases, so one pyproject can serve a SQLite development setup and a
  Postgres deployment. An application that builds its own engine for
  `outbox.configure()` gets the
  same effect with a `connect` listener running `PRAGMA journal_mode=WAL`
  (see the Cookbook), or by setting WAL once on the file with
  `sqlite3 app.db 'PRAGMA journal_mode=WAL'`.
- The deserialization allowlist is the event types of the process's own
  listeners.
- The binding needs module discovery (`auto_discover`, the default) outside a
  worker, because the allowlist comes from the discovered listeners. With
  `auto_discover = false`, call `outbox.configure()` yourself. The
  `modulith outbox` commands then bind no store, because they import none of
  your modules; run them with `MODULITH_AUTO_DISCOVER=true` when discovery can
  import the package.
- The outbox table must already exist; the URL must name the database holding
  your business tables. `modulith migrate` creates it. It migrates `outbox_url`
  by default, swapping the async driver for the sync one Alembic runs on
  (`+asyncpg` becomes `+psycopg`, `+aiosqlite` plain `sqlite`, `+aiomysql`
  `+pymysql`), and prints the target with the password masked. The same
  migration chain also creates the `broker_*` tables of the database broker,
  which stay unused unless you select that broker.

  ```bash
  modulith migrate                                    # to head, on outbox_url
  modulith migrate --url 'postgresql+psycopg://user:pass@localhost/mydb'
  ```

  The raw Alembic command is the alternative. `MODULITH_DB_URL` (a
  sync-driver URL) is the variable it reads:

  ```bash
  MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
    alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
    upgrade head
  ```

  The chain records its revision in its own table, `modulith_alembic_version`
  (in the migration schema when one is set), so it can share a database with
  your application's Alembic history in `alembic_version`. After upgrading
  modupy, run `modulith migrate` again before the new version starts: the
  store maps each of its columns on every save, so against a table that lacks
  one each publish fails, and with a bound session that takes the business
  transaction down with it.
- In a single-process app, call `modulith.bootstrap()` and then
  `outbox.start()` in the lifespan's startup half. Bootstrap binds the store,
  and it is lazy: without the explicit call it first runs at the first
  `publish()`. `outbox.start()` is a no-op while no store is bound, so a
  lifespan that calls only `start()` runs no crash sweep and no retry loop
  until something publishes.
- In a single-process app, call `outbox.shutdown()` in the lifespan's
  teardown. It stops the retry loop, then drains in-flight after-commit
  deliveries and closes the connection pool of the engine modulith created
  from `outbox_url`. The runtime's own shutdown does the same in
  process-topology workers. `outbox.shutdown()` never disposes a store you
  bound with `outbox.configure()`, or its engine: dispose those yourself, in
  the teardown order of the lifespan below. An after-commit delivery still
  running when the process exits without it is cancelled and delivered again
  later: under `"lease"` once its lease expires, under `"advisory_lock"` by a
  later sweep once its lock is released, at the latest when the dead
  process's lock connection closes, and under `"none"` by the next sweep.
  Run one lifespan per process. `outbox.shutdown()` is final: the store it
  disposes stays bound, and `modulith.bootstrap()` does nothing the second
  time, so it never rebinds one. A second lifespan in the same process (a
  second `asyncio.run()` over the same app, say) therefore reuses the disposed
  store. Its after-commit hook is gone, so every row waits for the retry
  loop's sweep, and nothing disposes the engine again at the second shutdown.
  Restart the process instead. Tests that need a second lifespan reset the
  runtime with `_reset_for_testing`, which the `modulith_app` fixture calls
  for every test.

**Wiring the store yourself.** Bind the store in code, with your own engine,
when `outbox_url` does not offer what you need: `connect_args` or a
`schema_translate_map` on the engine, a custom serializer, an application that
runs with `auto_discover = false`, or the graceful drain order below:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap, configure
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
    bootstrap()  # the sweep dispatches nothing until the runtime is bootstrapped
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

Then create the tables and run. With a store you wire yourself, `outbox_url` is
unset, so pass the database to `modulith migrate` as `--url`:

```bash
pip install 'modupy[fastapi,cli,postgres]'
modulith migrate --url 'postgresql+asyncpg://user:pass@localhost/mydb'
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 1
```

With `outbox_url` set (`MODULITH_OUTBOX_URL` works too), `modulith migrate` needs
no `--url`.

**What changes:**
- Event publication is deferred until your domain transaction commits (the outbox pattern).
- The event row and your domain row are persisted in the same transaction.
- After commit, the outbox dispatches the event to listeners in a background loop.

**Characteristics:**
- Events persist in the database; recoverable after crash
- Listeners must be idempotent (at-least-once delivery)
- Single point of failure: the database
- Ideal for: critical transactional workflows where losing an event is unacceptable

**Listeners and durability:**
- The outbox persists only the **first hop** of events (e.g., `orders` → `inventory`).
- If `inventory` publishes a downstream event (e.g., `StockReserved` → `notifications`), that hop is **not durable by default**—it rides the in-memory bus.
- A listener takes exactly one argument — the event. No session is injected, and `publish()` takes no `session=` keyword. For a durable cascade, the listener opens its own session and binds it, so `publish()` finds it and enlists the outbox row in that transaction:
  ```python
  from modulith.builtin.outbox import bind_session, unbind_session

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
  A route handler follows the same pattern: it calls a service function (or a
  `transaction()` helper you own) that binds, publishes, commits and unbinds
  before the route returns. Do not commit in a `yield` dependency's teardown:
  FastAPI runs it after the response is sent, so a commit that fails there
  still answers 200. `examples/demo_app` does this in
  `shop/orders/__init__.py::place_order` (called by the route in
  `shop/orders/api.py`) and in the `reserve_stock` listener of
  `shop/inventory/__init__.py`, which binds its own session for the durable
  cascade.
  `bind_session` and `unbind_session` live in `modulith.builtin.outbox`; the
  `modulith.adapters.postgres_outbox` import path still works as aliases.

**Outbox operations:**

```bash
modulith outbox status              # pending events
modulith outbox retry <event-id>    # retry a failed event
modulith outbox purge               # remove delivered events
modulith outbox failing             # events still being retried: attempts, last error, next retry
modulith outbox dead-letter         # inspect stuck events
```

`failing` lists undelivered rows that have failed at least once and are not yet
dead-lettered. Its next-retry time is the last attempt plus the retry backoff
(doubling per attempt, capped by `max_retry_backoff_seconds`); a time in the
past means the next sweep picks the row up. It prints `no failing publications`
when there are none, and exits 1 for a custom store without `find_failing`.

These run in the CLI's **own** process and operate on the store that process
binds. They cannot reach into a separately-running server: the store binds in
the calling process, and nothing is shared across process boundaries. With
`outbox_url` set (and `auto_discover` on, the default), bootstrap binds the
store from that URL in the CLI process too, so the commands work against the
same database the server writes to, as they do in the durable stages of
`examples/demo_app`. Without
`outbox_url` they work only when your bootstrap module — the one the CLI
imports via `[tool.modulith]` — calls `outbox.configure()` at import time. An
app that wires the outbox only inside a FastAPI lifespan gets "no outbox store"
from the CLI even while the server is up, and the message names the remedy;
inspect that outbox through the running app, or set `outbox_url`.

### Reference: outbox details

**Stored listener ids.** Each outbox row names its listener. A plain function
is stored as `module.function`. A callable instance or bound method
registered from an application module is stored as
`<module package>:<class module>.<ClassName>`, for example
`myapp.orders:myapp.shared.Notifier`, so one class used by two modules is
delivered per module.

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
out, so the SQLite failure depends on load. modupy logs one warning the
first time a second loop uses the engine. Keep every publish for one outbox
engine on one loop; a larger pool only delays the SQLite failure and does not
help on Postgres or MySQL. Unlike the outbox store,
the database broker hands a call from another loop to the loop that owns its
engine; see [A. SQLite Database Broker](#a-sqlite-database-broker-zero-infrastructure).
This also applies to the store bound from `outbox_url`.

**Replica clocks.** Replicas that share an outbox table must keep their clocks
within about a second of each other: run NTP or chrony on every host. Unlike
the database broker, the outbox does not use the database's clock. Each
replica stamps a claim's lease expiry (`claim_until`) and a failed row's
`last_attempt_at` from its own clock, and the other replicas compare those
stamps with theirs. With more skew, the replica whose clock runs ahead
re-dispatches early: it reads a peer's lease as expired, and a failed row's
retry backoff as elapsed, sooner by the skew. The first retry waits 1 s, so a
skew of 1 s makes it immediate. A clock that runs behind only delays recovery
and retries. See [ARCHITECTURE.md section 7.4](ARCHITECTURE.md#74-the-postgres-adapter).

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
that fallback. Migrations use `MODULITH_DB_SCHEMA` or `modulith migrate
--schema orders`. The raw alternative is the packaged command's global `-x`
option before `upgrade`:
`alembic -c <packaged-alembic.ini> -x schema=orders upgrade head`.
Schema identifiers receive the same validation through every entry point.
Enabling a named migration schema does not move data and refuses to abandon
existing modupy tables or Alembic history in `public`; see
[Migration Guide](../MIGRATION_GUIDE.md) Step 5.

### Postgres and PgBouncer settings for `advisory_lock`

`claim_strategy = "advisory_lock"` is Postgres only; the Cookbook's
[Coordinating concurrent sweepers](COOKBOOK.md#coordinating-concurrent-sweepers)
compares the three strategies. The strategy holds a session-level advisory lock
on a dedicated connection while a row is delivered, and every other sweep skips
a row whose lock is held. The lock lives as long as that Postgres session. When a
process dies on a live host, its kernel closes the socket and the lock is
released at once. After a host loss or a network partition nothing closes the
socket, and the row stays locked until Postgres drops the dead session through
TCP keepalive. With stock Linux defaults (7200 s idle, then 9 probes 75 s
apart) that takes about 2 h 11 min.

**Shorten the wait.** Lower the server's `tcp_keepalives_idle`,
`tcp_keepalives_interval` and `tcp_keepalives_count`; 60, 10 and 3 give about
90 s. Lock connections are opened with the outbox engine's connect arguments,
so a setting that reaches the engine reaches them. The engine modupy builds from
`outbox_url` takes no `connect_args`, so with a psycopg driver put the settings
in the URL's `options` query parameter:

```toml
[tool.modulith]
outbox_url = "postgresql+psycopg://app@db/mydb?options=-c%20tcp_keepalives_idle%3D60%20-c%20tcp_keepalives_interval%3D10%20-c%20tcp_keepalives_count%3D3"
```

Setting them on the database role (`ALTER ROLE app SET tcp_keepalives_idle = 60`
and likewise the other two) or in `postgresql.conf` works with any driver. An
`asyncpg` URL cannot carry them: the connection fails on the unknown query
parameter. For an engine you build yourself, pass them in `connect_args`; the
Cookbook shows the form for both drivers.

- **Unix-domain sockets ignore them.** Postgres ignores these settings on a
  connection over a Unix-domain socket and reads them as 0, so they matter only
  where the URL names a host and the connection is TCP.
- **psycopg's `options` replaces the URL's.** An `options` entry in
  `connect_args` replaces any `options` in the URL, such as a `search_path`, so
  put every `-c` setting in one `options` value.
- **A hung process keeps its session.** A process that hangs without exiting
  keeps its session, and so its locks, until it resumes or exits. Its host's
  kernel still answers the keepalive probes, so no keepalive setting shortens
  that. A fork-started child that outlives the process keeps the session too;
  the Cookbook covers that case.
- **Leave `idle_session_timeout` unset for the outbox's role.** A lock
  connection sits idle while its listener runs, so ending that session releases
  the lock mid-delivery and a peer's sweep can deliver the row again.
- **Behind PgBouncer,** use session pooling (`pool_mode = session`, the
  default) and never transaction or statement pooling: advisory locks need a
  server session that stays with one client connection. The server's keepalive
  then watches PgBouncer, not your process, so shorten the wait with PgBouncer's
  own settings. `tcp_keepalive` is on by default but uses the operating system's
  timings, so set `tcp_keepidle`, `tcp_keepintvl` and `tcp_keepcnt`.

---

## Process-Per-Module Topology

![modulith run starts a main process holding the proxy on port 8000 and the supervisor, plus one worker process per module, connected by the built-in SHM broker](images/processes.svg)

Split modules across separate worker processes for independent scaling, deployment, and lifecycle. Events flow through a broker: the built-in `shm` broker by default ([Default Broker: `shm`](#default-broker-shm)), a database (SQLite, Postgres or MySQL), Redis Streams, or other transports.

**Actuator note for every recipe in this section.** `modulith run` binds `--host 0.0.0.0` by default, and the default `actuator_mode="auto"` leaves `/_modulith/*` unmounted there unless a token is configured (a startup warning says so), so the health probes further down have nothing to call. Export a token if you want them; [Actuator Access](#actuator-access-_modulith) has the modes and the rules:

```bash
export MODULITH_ACTUATOR_TOKEN="$(openssl rand -hex 32)"
```

**Forwarding headers.** The reverse proxy overwrites `X-Forwarded-For`,
`X-Forwarded-Proto`, `X-Forwarded-Host`, and `X-Forwarded-Port` from the
connection it accepted, and strips any client-supplied `Forwarded` or
`X-Real-IP` — a client cannot spoof its own IP, scheme, host, or port to a
worker. Behind a TLS-terminating ingress or load balancer, this means the
proxy itself sees the ingress as the client unless you configure trust: set
uvicorn's `FORWARDED_ALLOW_IPS` (env var, e.g. the ingress CIDR or `*` when
the proxy is reachable only through the ingress) **on the proxy process only**
so `request.client`/scheme reflect the real client before this overwrite runs.
Workers run with the default `FORWARDED_ALLOW_IPS` and trust forwarded headers
only from the local proxy at 127.0.0.1.
`X-Forwarded-Port` is the port the proxy's socket accepted the connection on,
never the one a `Host` header names (the scheme's default, 80 or 443, when the
proxy listens on a Unix socket), and `X-Forwarded-Host` is the `Host` header as
received. Behind an ingress, `X-Forwarded-Port` is therefore the proxy's own
port, not the public one the client used; build public URLs from
`X-Forwarded-Proto` and `X-Forwarded-Host`. A `Host` header whose port is not a number from 0 to 65535 gets `400`
on every path, the actuator included, and no worker sees the request.

**Request targets.** The proxy forwards the path to the matched module's worker without decoding it, so an encoded `%2F` stays one segment. httpx percent-encodes a few characters on the way out (`"`, `<`, `>`, `` ` ``, `{`, `}`), which leaves the decoded path unchanged. The proxy answers `400` for a request-target, as the server's HTTP parser presented it, that does not start with `/` or that contains a `.` or `..` path segment, literal or percent-encoded, and contacts no worker for it. It connects to each worker's loopback port directly and ignores `HTTP_PROXY`, `ALL_PROXY` and related proxy variables in its environment.

**Query strings in logs.** The `modulith.proxy` logger never logs query strings, and neither do the other lines that name a request target. On the proxy and on every worker, each uvicorn access line (`INFO`) keeps the client, method, path, HTTP version and status and drops the request target from its first `?` on, so a token passed as a query parameter does not reach the logs. A WebSocket handshake line (`"WebSocket …" [accepted]`, `403` or a rejection status) loses its query string the same way. The proxy forwards through httpx, whose `HTTP Request: GET <url> "HTTP/1.1 200 OK"` line (`INFO`, one per forwarded request and per readiness probe) also loses its query string, in the process that runs the proxy. A worker the generated Kubernetes manifests or the Dockerfile from `modulith extract` start gets the same filters, because the worker app installs them itself. Without `--topology processes`, `modulith run` and `modulith dev` start plain uvicorn on your own application and leave its logs as they are. `modulith run --log-level warning` switches all of these lines off, together with every other `INFO` line.

**Connection pool.** The proxy holds one upstream connection per in-flight request until the response has finished streaming, so long-polls, server-sent events and slow downloads each occupy one for their whole duration. `MODULITH_PROXY_MAX_CONNECTIONS` (default `1000`) bounds them. A request that finds every connection busy for 5 seconds gets `503` `{"detail": "proxy connection pool exhausted"}`; the worker is not marked down, and `/_modulith/health` and identity probes use a separate 100-connection pool, so a full request pool cannot fail readiness. The warning it logs names the pool that ran out, `request` or `health-probe`. Raise the bound for many concurrent long-lived requests, keeping the proxy's open-file limit (`ulimit -n`) above it.

### Default Broker: `shm`

`--topology processes` with no broker configured uses `shm` (shared memory), so there is nothing to install or run. Events are stored in a SQLite file on the local machine. A memory-mapped ring only tells consumers that new work was committed, and they always read the work itself from SQLite, so a missed hint only delays them. It serves one host: use a database or Redis broker to span machines.

```bash
pip install 'modupy[fastapi,cli]'
MODULITH_BROKER_STATE_DIR=/var/lib/myapp/modulith \
  modulith run myapp.main:app --topology processes
```

modupy creates a missing state directory with mode 0700. One that already exists must already be 0700 (`chmod 700 /var/lib/myapp/modulith`): modupy never changes an existing directory's permissions and stops with a `ConfigurationError` instead.

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

### A. SQLite Database Broker (Zero Infrastructure)

```bash
pip install 'modupy[fastapi,cli,database]' aiosqlite
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=sqlite+aiosqlite:////path/to/broker.db \
  modulith run myapp.main:app --topology processes
```

> **Single-host only.** All workers must access the same SQLite file, so this mode works only on a single machine: the broker runs the file in WAL mode, which does not work over a network filesystem. For multi-host deployments, use Postgres or Redis instead.

Adopting the packaged Alembic migrations after the broker has already
self-bootstrapped its own tables is supported: `modulith migrate --url <broker
url>` (or the raw `alembic upgrade head`) stamps
cleanly over a database the broker created, since migrations `0002`, `0004`
and `0006` inspect the schema first and skip any table, index or column that
already exists rather than failing on a duplicate.

That inspection needs a connection, so offline output skips nothing. Only the
raw command has an offline mode (`alembic upgrade head --sql`); `modulith
migrate` always connects. The offline script creates every table and index of
`0002` and `0004` unconditionally, and `0006` adds `broker_message.dispatch_started`
with a plain `ALTER TABLE ... ADD COLUMN`. Apply that script only to a database
without those objects: a `broker_message` table the broker created already has
the `dispatch_started` column.

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

modupy logs one warning the first time a call arrives from a second loop.

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
url = "sqlite+aiosqlite:////path/to/broker.db"  # or postgresql+asyncpg://user:pass@host/db
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
export REDIS_PASSWORD="$(openssl rand -hex 16)"   # any password; Redis and modupy must use the same one
docker run -d -p 127.0.0.1:6379:6379 redis:7-alpine redis-server --requirepass "$REDIS_PASSWORD"

MODULITH_BROKER=redis-streams \
  REDIS_URL="redis://:$REDIS_PASSWORD@localhost:6379" \
  modulith run myapp.main:app --topology processes
```

The Redis image sets no password, so the command above requires one and publishes the port on the loopback interface only. Never publish an unauthenticated Redis port. To span several hosts, run a Redis you secure yourself and point `REDIS_URL` at it.

**Characteristics:**
- Scales to high throughput
- Automatic consumer group management
- Built-in dead-letter handling
- Ideal for: high-volume deployments with Redis infrastructure already in place

**⚠️ Redis Durability Caveat:** XADD MAXLEN `~` (approximate trimming) is blind to consumer-group PEL state. An undersized `max_stream_len` can permanently drop unacked entries, violating at-least-once delivery. Size `max_stream_len` well above worst-case backlog: **publish_rate × (consumer_downtime + processing_latency + reclaim_min_idle_ms)**. Default SHM and database brokers are NOT affected.

**Tuning:**

The Redis adapter reads these environment variables:

```bash
export REDIS_URL="redis://:<password>@localhost:6379"  # connection URL
export MODULITH_STREAM_PREFIX=myapp            # stream key prefix (default modulith.events)
export MODULITH_STREAM_MAXLEN=100000           # XADD MAXLEN ~ cap (see caveat above)
export MODULITH_BROKER_DLQ_MAX_STREAM_LEN=1000000  # dead-letter stream cap (default: 10x the stream cap)
export MODULITH_BROKER_MAX_PAYLOAD_BYTES=1048576   # producer-side payload cap (default 16 MiB)
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

As with the SQLite broker above, running `modulith migrate` (or the raw
`alembic upgrade head`) after the broker has already self-bootstrapped is
supported — migrations `0002`, `0004` and `0006` skip tables, indexes and
columns that already exist rather than failing. On a `postgresql+asyncpg://`
URL both commands connect through `psycopg`, which the `postgres` extra
installs (`pip install 'modupy[postgres]'`). Offline `--sql` output has no
connection to inspect and skips nothing, so apply it only to a database
without those objects, as described above.

**Redis persistence:** the events live in Redis streams, so they are only as durable as the Redis behind them. Redis ships with the append-only file off (`appendonly no`) and takes periodic snapshots, which can lose the latest writes in a crash. For events to survive a crash or restart, set `appendonly yes`, choose an `appendfsync` policy, and keep Redis's data directory on a persistent volume, or use a managed Redis with AOF enabled. `everysec` (Redis's default policy) can lose about the last second of writes in a crash, `always` syncs every write at a large cost in speed, and `no` leaves flushing to the operating system. The Docker command above does none of this, so it suits a first try only. See [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).

**Tuning:**

The values below are examples; the defaults are shown beside each variable.

```bash
export MODULITH_BROKER_BATCH_SIZE=200             # rows claimed per poll (default 100)
export MODULITH_BROKER_DISPATCH_CONCURRENCY=20    # rows dispatched in parallel per batch (default 10)
export MODULITH_BROKER_POLL_INTERVAL_MS=50        # idle poll cadence in ms (default 20); higher polls less often
```

### Sizing the Default SHM Store

The local `shm` broker keeps a publication while any of these holds:

- a subscribed group has not consumed it yet: an undelivered backlog, which
  includes a retired group that never consumes again;
- it is younger than `orphan_retention_seconds` (default 3600), even after
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
defaults (delete mode, no dead letters) sustain roughly 165 publications/s; under
`completion_mode = "mark"` the 3-day terminal retention cuts that below
2.3 publications/s. Above that rate, every publish fails with "SHM SQLite store
is full". Size `max_store_bytes` (or `MODULITH_BROKER_MAX_STORE_BYTES`) for the
longest retention, or shorten `orphan_retention_seconds` before the store
fills: each publication keeps the orphan retention stamped when it was
written, so shortening it frees nothing in a store that is already full.

A backlog frees space only as consumers drain it, or when
`modulith broker drop-group` removes a retired group. Until then a retired or
stopped group pins every publication to its targets. Only the supervisor of
`modulith run --topology processes` checks for a retired group at startup, and
its warning names the group only 24 hours after its last consumer activity; a
single-process or Kubernetes deployment gets none. Size for that backlog, or
drop the group with `--force` as soon as no host runs it.

A new `max_store_bytes` or `retention_age_seconds` applies to a process only
after it restarts. Every process that shares a store must use the same
`max_store_bytes`; restart them all after changing it. Each process enforces its
own limit on its own publishes, so a process with a larger limit fills the pages
that a process with a smaller limit reserves for its consumers, and the
smaller-limit process then has its publishes refused as store full.
`orphan_retention_seconds` is capped at 100 years (3153600000). A group that
subscribes after a publication replays it only within that window.

Publishes are refused once the pages in use (the page count minus the free
pages) pass `max_store_bytes` less a 32-page consumer reserve (128 KiB at 4 KiB
pages); a store of fewer than 256 pages (1 MiB at 4 KiB pages) reserves one
eighth of its pages, rounded down, instead. Strictly, the numerator above is
`max_store_bytes` minus that reserve; at the 1 GiB default that is 0.01% and
does not change the estimate. The empty schema takes 13 pages (52 KiB at 4 KiB
pages), so a store under 56 KiB refuses every publish, and a 64 KiB store holds
about 50 publications of a few bytes each for one subscribed group.

Consumers can always claim, ack, fail, dead-letter and prune the backlog they
see, and a group can always subscribe, while publishes are refused: a consumer
write or subscription record that `max_store_bytes` refuses is retried past it,
so the store file can grow past the limit by the growth of rows it already holds
(claims, error text, mark-mode completions, prune tombstones). A subscribe
replay itself never grows the store past its publish budget: it replays the
retained publications the group lacks, oldest first, and stops 8 pages below the
budget, so a small publish that fit before the replay still fits right after it.
Under `completion_mode="mark"` it uses at most half the room left, which covers
claiming and acking its own rows when one group drains them and its listeners
succeed. Draining can still take the store past the budget, as any consumer
write can: failed and dead-lettered rows keep their error text, and under
`completion_mode="mark"` claiming and acking grows every row, including rows
other groups replayed. Publishes are then refused until prune frees pages or
`max_store_bytes` is raised. Measured on a store filled to its publish budget
with small publications and then drained by one group, acking every delivery
left the file at 0.97x (delete mode) to 1.17x (mark mode) of `max_store_bytes`.
Dead-lettering every delivery with a 500-character error, the most the
dispatcher stores, left it at about 3x in either mode. Larger payloads lower
that ratio and more groups failing the same publications raise it. The file does
not shrink afterwards, so budget disk for that growth if a listener can fail
every delivery.

A replay cut short logs one WARNING naming the group and the target, with the
replayed and skipped counts. The skipped publications reach that group only
through another replay. To replay them without losing work, do this in order:

1. Let the group's workers drain its backlog on that target. The `drop-group` in
   step 3 deletes every pending and claimed delivery the group holds there,
   replayed ones included, and a replay restores only publications still within
   `orphan_retention_seconds` of being written.
2. Once that backlog is empty, stop the group's workers.
3. Run `modulith broker drop-group <group> --target <target>`.
4. Raise `max_store_bytes`.
5. Restart every process, before the skipped publications expire
   (`orphan_retention_seconds` after they were written).

Size `max_store_bytes` for the retained backlog before adding a listener to a
busy store. Consumers drain a store that was opened with a lowered limit the
same way; publishes resume once the drained store is back under the limit, and
an existing larger file keeps its size. Budget disk for the store's `-wal` file
on top: it is not counted, grows to about 4 MiB (SQLite's 1000-page
autocheckpoint) before checkpoints reuse it, grows further while a long read
blocks a checkpoint, grows by about 115 bytes for every publication a subscribe
replays (the replay is one transaction, so no checkpoint runs inside it: 5.4 MiB
for 50,000 replayed publications, 22.3 MiB for 200,000), and keeps its largest
size.

---

## Docker Deployment

Your application's `pyproject.toml` declares modupy and its extras as a
dependency (`dependencies = ["modupy[fastapi,cli,postgres]"]`, or
`modupy[fastapi,cli,database]` for the database broker). The extras belong to
modupy, not to your project, so `pip install '.[fastapi,cli,postgres]'` finds
none. Copy the source before installing, because `pip install .` builds
your package. The Dockerfile `modulith extract` generates follows the same
order. These images run as root, and so does the generated one.
[Running as a Non-Root User](#running-as-a-non-root-user) adds the lines that
drop privileges.

### Single-Process

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY myapp/ ./myapp/
RUN pip install --no-cache-dir .
EXPOSE 8000
ENV MODULITH_OUTBOX=postgres
# Pass MODULITH_OUTBOX_URL (the business database's async SQLAlchemy URL) in at
# run time and never bake it into the image; run `modulith migrate` once per
# release with the same variable set.
CMD ["uvicorn", "myapp.main:app", "--host", "0.0.0.0"]
```

### Process-Per-Module

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY myapp/ ./myapp/
RUN pip install --no-cache-dir .
EXPOSE 8000
ENV MODULITH_BROKER=database
# Pass MODULITH_BROKER_URL (the broker database's async SQLAlchemy URL) in at
# run time and never bake it into the image.
# The supervisor binds 0.0.0.0, where actuator_mode="auto" only mounts
# /_modulith/* if a bearer token is configured. Pass MODULITH_ACTUATOR_TOKEN in
# at run time (never bake a secret into an image); omit it and the app still
# starts, just without the actuator.
CMD ["modulith", "run", "myapp.main:app", "--topology", "processes"]
```

With `docker-compose.yml`:

```yaml
services:
  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      MODULITH_BROKER: database
      # Throwaway demo credentials: they must match the postgres service below.
      MODULITH_BROKER_URL: postgresql+asyncpg://modulith:modulith@postgres/mydb
      # Mounts /_modulith/* on the proxy; drop this line to leave it unmounted.
      MODULITH_ACTUATOR_TOKEN: ${MODULITH_ACTUATOR_TOKEN:?set MODULITH_ACTUATOR_TOKEN}
    depends_on:
      - postgres
      - redis  # if using redis broker

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: modulith
      POSTGRES_PASSWORD: modulith
      POSTGRES_DB: mydb
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:  # optional; not durable as written, see "Redis persistence" above
    image: redis:7-alpine

volumes:
  postgres_data:
```

The Postgres credentials are throwaway demo values, and this file publishes no Postgres port. Anywhere else, use a secret of your own.

### Running as a Non-Root User

To run as a non-root user, add these lines to any of the three Dockerfiles
after `RUN pip install`. `COPY` leaves its files owned by root whatever `USER`
says, so the `chown` follows it and gives the application directory to the new
user:

```dockerfile
RUN useradd --create-home --uid 10001 app \
 && chown -R app:app /app
USER app
```

**SHM broker state directory.** The default `shm` broker keeps its SQLite store
in a state directory, and the new user must own that directory. modupy creates
a missing one with mode 0700 and refuses an existing one with wider
permissions, so only its owner can use it. Neither Dockerfile above runs the
broker: the first has none and the second selects the database broker. A
container running `--topology processes` with no `MODULITH_BROKER` does.

Unless `MODULITH_BROKER_STATE_DIR` (or `state_dir`) names it, the directory is
`$XDG_STATE_HOME/modulith/<package>-<digest>`, or
`~/.local/state/modulith/<package>-<digest>` when `XDG_STATE_HOME` is unset. A
user whose home directory is missing or unwritable therefore fails at startup
with a `ConfigurationError` ("could not create private broker state
directory"); `--create-home` above prevents that. To keep the store on a
volume, pin the directory and create it in the image, owned by the new user:

```dockerfile
RUN useradd --create-home --uid 10001 app \
 && mkdir -p /var/lib/myapp/modulith \
 && chown -R app:app /app /var/lib/myapp/modulith \
 && chmod 700 /var/lib/myapp/modulith
USER app
ENV MODULITH_BROKER_STATE_DIR=/var/lib/myapp/modulith
```

Whatever you mount at that path must arrive owned by uid 10001 with mode 0700.
A new named volume inherits both from the image's directory. A path the image
lacks arrives owned by root with mode 0755, which modupy refuses. A bind mount
keeps the host directory's owner and mode, and a volume that an earlier root
container filled keeps its root ownership until you `chown -R` it to 10001.
[Default Broker: `shm`](#default-broker-shm) has the rest of the state directory
rules.

---

## Kubernetes Deployment

### Single-Process Service

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
  labels:
    app: myapp
spec:
  replicas: 2
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
        - name: app
          image: myapp:1.0.0
          ports:
            - containerPort: 8000
          env:
            - name: MODULITH_OUTBOX
              value: "postgres"
            - name: MODULITH_OUTBOX_URL   # async SQLAlchemy URL of the business database
              valueFrom:
                secretKeyRef:
                  name: db-creds
                  key: url
---
apiVersion: v1
kind: Service
metadata:
  name: myapp
spec:
  selector:
    app: myapp
  ports:
    - port: 8000
      targetPort: 8000
```

The Deployment's `selector` must match its pod template's labels, and the
Service selects the same pods, so the two replicas answer on one cluster
address, port 8000. Put an Ingress in front of the Service for traffic from
outside the cluster.

`MODULITH_OUTBOX_URL` is the URL the running app binds its outbox store to;
`MODULITH_DB_URL` is only the sync-driver URL the Alembic migrations read. Run
`modulith migrate` as a release step (a Job or an init container) with
`MODULITH_OUTBOX_URL` set.

### Process-Per-Module: Generated Manifests

`modulith k8s-manifest` generates one Deployment + Service per module discovered under `--topology processes`, plus a single Ingress fanning out `/<module>` paths to each module's Service:

```bash
mkdir -p k8s   # k8s-manifest does not create the output directory
modulith k8s-manifest --output k8s/modulith.yaml --image myapp:1.0.0 --namespace prod
```

Each pod runs one worker, so the Ingress and the Services do the routing that the proxy does under `modulith run`.

```mermaid
flowchart TD
    client["Client"] --> ing["Ingress<br>myapp-ingress"]
    ing -->|"/orders"| so["Service<br>myapp-orders"]
    ing -->|"/inventory"| si["Service<br>myapp-inventory"]
    subgraph deps["Deployment per module"]
        dor["myapp-orders"]
        din["myapp-inventory"]
    end
    so --> dor
    si --> din
    sb["Secret myapp-broker<br>created by you"] -.->|"MODULITH_BROKER_URL"| deps
    se["Secret myapp-env<br>created by you"] -.->|"envFrom, optional"| deps
    deps --> broker[("Shared broker<br>Redis / database / other")]
```

Options: `--output` (default `modulith-k8s.yaml`, `-` for stdout), `--image`
(default `<package>:latest`), `--namespace`, `--port` (default `8000`, valid
range 1–65535), and `--host` (Ingress host). Deployment and Service object
names are normalized to RFC-1123 labels (`myapp-order-items`); long names
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
established `MODULITH_STREAM_PREFIX` and `MODULITH_STREAM_MAXLEN` names.
Unknown or credential-like options are omitted.
The broker URL is never embedded: `MODULITH_BROKER_URL` (and `REDIS_URL` for
Redis) reads key `url` from the generated `<package>-broker` Secret reference:

```bash
kubectl create secret generic myapp-broker \
  --from-literal=url=<broker connection URL> --namespace prod
```

The manifest comments include the selected namespace in both Secret-creation
commands. Each container also references a `<package>-env` Secret via `envFrom`
for additional variables. The reference is `optional: true`, so applying the
manifest never requires the Secret, but whenever `outbox` is not `memory` it
must carry `MODULITH_OUTBOX_URL`, the business database's async SQLAlchemy URL
(unless the image's `[tool.modulith]` already sets `outbox_url`). A pod with a
durable outbox and no store refuses to start:

```bash
kubectl create secret generic myapp-env \
  --from-literal=MODULITH_OUTBOX_URL=<async SQLAlchemy URL> --namespace prod
```

`MODULITH_DB_URL` is not a runtime variable; it feeds the migrations only.
Readiness uses
`httpGet /health`; liveness uses `tcpSocket` so a temporary broker outage does
not crash-loop a healthy pod. No `resources` or Secret objects are emitted.

`modulith k8s-manifest` refuses to generate manifests for a broker that cannot be shared across pods: `memory`, `shm`, or `database` pointed at a `sqlite://` URL. Configure `database` with a networked URL (`postgresql://`, `mysql://`) or `redis-streams` first.

The generator imports the configured application modules to derive workers.
Run it only against trusted source in the build environment.

---

## Scaling Strategies

### Horizontal Scaling (Add More Workers)

For **single-process**, add replicas with a load balancer. Three copies on one host need three ports; on separate hosts or containers each copy can keep the default port 8000:

```bash
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 --port 8001 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 --port 8002 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 --port 8003 &
```

For **process-per-module**, increase worker counts via `--workers` JSON flag or `pyproject.toml` configuration. The two `--workers` differ: `uvicorn --workers 4` above is a process count, while `modulith run --workers` takes a JSON object of per-module counts:

```bash
# Run more inventory workers (the JSON replaces the pyproject table, so every other module gets one worker):
MODULITH_BROKER=database modulith run myapp.main:app --topology processes \
  --workers '{"inventory": 4}'
```

Or in `pyproject.toml`:
```toml
[tool.modulith.workers]
inventory = 4  # more workers for high-traffic module
```

The workers of one module share a consumer group (`modulith-<module>`), so each event reaches one of them, and every other module that consumes it gets its own copy. Here `notifications` also listens to `OrderPlaced`:

```mermaid
flowchart LR
    pub["orders publishes<br>OrderPlaced"] --> broker[("Broker<br>SHM / Redis / database / other")]
    broker -->|"one worker takes it"| inv
    broker -->|"its own copy"| ntf
    subgraph inv["modulith-inventory"]
        i1["inventory worker"]
        i2["inventory worker"]
    end
    subgraph ntf["modulith-notifications"]
        n1["notifications worker"]
    end
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

**Every discovered module gets at least one worker.** `[tool.modulith.workers]` counts must be `>= 1`; `notifications = 0` is rejected at boot with `ConfigurationError: workers must map string module names to positive integer counts`, and so is a `0` passed through `--workers` JSON. `modulith run` has no per-deployment module opt-out — "this module does not run here" means a separate application package, not a worker count of zero. To run one module by itself, see **Starting one module on its own** below.

**Approach 1: Separate deployments with different worker counts**

modupy reads `[tool.modulith.workers]` only from the nearest `pyproject.toml`, and that table has no environment variable. To give two deployments of the same code different counts, pass `--workers` to each. The JSON replaces the table entirely, so every module it does not name runs one worker, the minimum (unless the JSON has a `default` key):

```bash
# request-serving nodes
modulith run myapp.main:app --topology processes \
  --workers '{"orders": 4, "inventory": 4}'

# batch nodes
modulith run myapp.main:app --topology processes \
  --workers '{"notifications": 2, "reporting": 2}'
```

**Approach 2: All modules in one deployment with specific worker counts**

```toml
[tool.modulith.workers]
orders = 4      # high-load module
inventory = 4
notifications = 1
reporting = 1
```

Both approaches start every module on every deployment and differ only in worker counts.

**Starting one module on its own.** Two things start a single module without its siblings. `modulith dev myapp.main:app --isolate orders` is for development: the other modules are not started and their routes 404 through the proxy. The worker factory is what the generated Kubernetes manifests and extracted services run: `MODULITH_MODULE=orders MODULITH_APP_PACKAGE=myapp uvicorn modulith._worker:create_app --factory` serves that module's `router` under `/orders`; give it the same broker settings as the rest of the deployment.

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

The `/_modulith/topology` endpoint returns the list of modules and their backend replicas:

```json
{
  "routes": [
    {
      "prefix": "/orders",
      "backend": "http://127.0.0.1:9001",
      "replicas": ["http://127.0.0.1:9001", "http://127.0.0.1:9002"]
    }
  ]
}
```

- `prefix`: URL prefix of the module (e.g., `/orders`)
- `backend`: the first replica's URL (kept for backwards compatibility with single-replica modules)
- `replicas`: list of all replica URLs in spawn order; for a single-replica module, this list contains one URL

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

The same URLs answer on each worker's **internal** port (9001+ by default; `--worker-port-base N` on `modulith run` or `modulith dev`, `[tool.modulith] worker_port_base = N` or `MODULITH_WORKER_PORT_BASE=N` moves the range, and each module takes one consecutive port per replica), which is where `kubectl port-forward` reaches them — note the module prefix is part of the path there too: `http://127.0.0.1:9001/orders/docs`, not `/docs`. Give each deployment sharing a host its own range: `modulith run` refuses a `--port` inside its worker range, and the proxy checks a worker's identity before its first request, again after the worker was marked down or foreign, and again after its worker process exits, whether or not the supervisor restarts it. A worker answering for another deployment is marked foreign and gets no requests; the proxy answers 503 when no worker of its own serves the module, and readiness reports the module as `foreign deployment`. Each identity probe is bounded (30 s in total and 64 KiB of body); past the deadline the request gets 504 and the worker is not marked down, so a slow but healthy worker is not turned away. Requests arriving while a worker's identity check is in flight wait on that one check instead of starting their own, so a stalled worker holds a single probe connection however many requests queue for it. The deployment token is a secret that no worker response contains. Each identity probe sends a fresh random nonce in an `x-modulith-nonce` header, and the worker answers `/health` with a `"proof"`: an HMAC-SHA256, keyed by the token, over the nonce, the module name and the port the probe arrived on (read from the connection, never from a header). The proxy accepts only the proof it computes itself for that worker's module and port, so a listener that echoes a token, relays the probe to another live worker, or replays an earlier answer is marked foreign. A process that cannot read the workers' environment cannot answer the challenge, which tells this deployment's workers from another deployment's and from a process that took over a dead worker's port. A verified port stays trusted until the worker exits, is marked down, or fails a later probe; the check does not authenticate each connection, and a process running as the same user as the workers can read the token from their environment. The proxy stores no upstream cookie: a `Set-Cookie` reaches only the client that made the request, and each client's own `Cookie` header is forwarded unchanged.

Three consequences worth knowing:

- The app-root paths belong to no module, so `/docs`, `/openapi.json` and `/redoc` return `404 {"detail": "no worker route for '/docs'"}`. Point client generators at a module URL.
- Inside a module's prefix, the module's own routes win: a module named `docs` keeps every path under `/docs/*`, and a module defining its own `/docs` route keeps serving it. What loses the collision is the generated doc UI for that one module, never the application's route. The one exception is the worker's own `GET /health`: in a module named `health` it answers before the module's root `GET` route, and startup logs a warning that the module route is unreachable. The module's other routes, including `/health/`, still reach it. In a module named `health`, the worker's `GET /health` also hides the index page of a `frontend()` mount at the module root (`router.frontend("/", ...)`), and startup logs no warning for that: `/health` answers the health JSON and `/health/` redirects to it, while the mount's other paths, such as `/health/assets/app.js`, still reach it.
- Each schema also lists the worker's own unprefixed `/health`. The proxy forwards `/health` only to a module named `health`, so strip it (or ignore the 404) in anything generated against the public port.

**The supervisor serves no merged, cross-module schema.** Each worker publishes only its own module's schema, and a merged one would have to pick a single answer where two modules define the same model name differently and for the `info` block and security schemes, which belong to each worker.

If you need one document, build it yourself:
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

Single-process topology is unaffected — modupy adds no HTTP routes there, so `/docs` is whatever your own FastAPI app configures.

---

## Health Checks and Monitoring

Both probes are served by the reverse proxy on the port `modulith run` binds (8000 by default), and both need the actuator mounted — set `MODULITH_ACTUATOR_TOKEN` (see [Actuator Access](#actuator-access-_modulith)). Plain `/health` belongs to each worker's own internal port (9001+ by default, set by `--worker-port-base` / `worker_port_base` / `MODULITH_WORKER_PORT_BASE`). The proxy answers it with 404, except when a module is named `health`: the proxy then forwards it to that module's worker, and the worker's own `/health` answers. There is no `/ready` route. Each `modulith run` hands its workers a random deployment token and never returns it: a worker's `/health` answers the proxy's `x-modulith-nonce` challenge with a `"proof"` field, an HMAC over the nonce, the module and the port (a worker started without a token still serves `/health`, without a proof).

Liveness answers from the proxy alone, while readiness asks every replica of every module.

```mermaid
sequenceDiagram
    participant K as Probe
    participant P as Proxy :8000
    participant I as inventory :9001
    participant A as orders :9002
    participant B as orders :9003
    K->>P: GET /_modulith/live
    P-->>K: 200, no worker called
    K->>P: GET /_modulith/health
    par
        P->>I: GET /health
    and
        P->>A: GET /health
    and
        P->>B: GET /health
    end
    I-->>P: 503
    A--xP: no answer within 2 s
    B-->>P: 200
    P-->>K: 503, inventory unhealthy, orders ok
```

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
given up on at least one replica). `failed (given up)` is only reported once every
replica of that module is unreachable — a module with even one healthy
replica reports `ok`.

In Kubernetes, probe headers are static strings, and a probe header cannot reference a Secret: a token templated in from the same secret the container reads becomes a literal in the Deployment spec, readable by anyone who can read Deployments. The probes in generated manifests need no token (see **Per-worker-pod probes** below). To probe the proxy, template the token in anyway, or set `MODULITH_ACTUATOR_MODE=open` if the port is only reachable inside the cluster and you accept unauthenticated topology/health:

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

In single-process topology there is no proxy and no actuator: modupy adds no HTTP routes, so probe whatever endpoint your own app exposes.

**Per-worker-pod probes (generated manifests).** The manifests `modulith k8s-manifest` generates probe each worker pod directly rather than through the proxy: readiness is `httpGet /health` on the container port, and liveness is a `tcpSocket` check on the same port. `/health` returns 503 while that worker's broker consumer isn't ready, which readiness correctly treats as not-yet-serving; liveness intentionally does not use `httpGet`, since a worker whose broker connection is temporarily down would otherwise get killed and restarted for no reason.

**When a broker error degrades `/health`.** A failed broker call puts the consumer in `degraded` (503) until the same operation next succeeds for the same stream or target. Completion writes (ack, fail, dead-letter, claim renewal) can also clear in these ways:

- On every consumer, the failure expires after the redelivery window: `reclaim_min_idle_ms` for Redis, `reclaim_stale_seconds` for the database and SHM brokers. By then the affected message is eligible for reclaim, here or on a peer replica. A write that fails again on the retry records a fresh failure, so a persistent failure shows up again, but `/health` can report ready between the expiry and that retry.
- On Redis only, the failure clears as soon as its message is no longer pending in the consumer group, for example because a peer replica reclaimed and acknowledged it.

A failure on one target never clears because another target succeeded. The exceptions are the database and SHM consumers' claim and claim-renewal heartbeat, which are tracked per consumer group.

**When a stalled consumer degrades `/health`.** A consumer that stops making progress reports `degraded` (503) even when no broker call has failed:

- **A listener that never returns** (database and SHM brokers). The consumer claims nothing new until every row of its current batch has finished. Once a batch has run longer than `reclaim_stale_seconds * 10` (default 600 s), health reports `degraded` with the stuck event type, target and row. The consumer also logs an ERROR line starting `claim renewal for group ... exceeded` that names the same rows. While the worker runs, modupy never cancels a listener, so this one keeps running; a stop of the worker cancels it after a 1 s grace, unless it is a plain `def` listener (see [Graceful Shutdown](#graceful-shutdown)). The consumer recovers only when the listener returns.
- **A Redis server that stops answering.** Health reports `degraded` ("no broker read completed in N s") once a read has waited `5 * poll_block_ms + 1 s`. The consumer logs one WARNING per stall. The Redis client's socket timeout (see [Redis Streams Broker](#b-redis-streams-broker)) then fails the hung read. The consumer logs `broker read failed`, backs off and retries, and health stays `degraded` until a read succeeds.

Restart a worker whose health stays `degraded` longer than you can tolerate; on a stuck listener a restart is the only remedy. The restarted consumer reclaims the stuck rows and charges each one a delivery attempt, so a listener that hangs on every delivery ends in the dead-letter state after `max_delivery_attempts`. The generated Kubernetes manifests do not do this for you: liveness is a `tcpSocket` check, so a degraded worker is only taken out of readiness. Add a liveness `httpGet /health` (with a generous `failureThreshold`) or an external watchdog if you want automatic restarts.

### Tracing

modupy emits spans and no metrics. If OpenTelemetry is enabled (`modupy[otel]`), spans are emitted for:
- `modulith.event.publish` — an event was published
- `modulith.event.dispatch` — an event was dispatched to a listener

The `listener.name` attribute of a dispatch span is the same for a listener
whether the event arrived in memory, through the outbox or through a broker:
`module.qualname` (for example `myapp.inventory.reserve_stock`). A bound method
or callable instance gets an `owner:` prefix naming the dotted package of the
application module that registered it
(`myapp.inventory:myapp.inventory.Reserver.reserve`). It is the id the outbox
stores for the listener.

The `observability` setting (`[tool.modulith] observability`, the
`MODULITH_OBSERVABILITY` environment variable, or `configure(observability=...)`)
controls tracing. Unset, tracing turns on when OpenTelemetry is installed and is
a silent no-op otherwise. `false` creates no spans. `true` makes bootstrap raise
`ConfigurationError` when OpenTelemetry is missing, so a deployment that relies
on traces fails at startup; install it with `pip install 'modupy[otel]'`.

There is no outbox span. On the durable path the publish span brackets writing
the outbox row. Each outbox row stores that span's W3C trace context, so the
dispatch span of an outbox-delivered event, whether delivered after commit or on
a retry, is a child of the publish span that created it, in the same trace. A
row with no stored trace context (tracing was off when it was written) gets a
dispatch span with no parent.

Broker messages carry the same context in `traceparent` and `tracestate` headers
(`tracestate` only when non-empty), on the outbox route and on the inline route
alike. A `BrokerConsumer` or shm/database consumer hands them to its dispatch
hooks, so a consumer's dispatch spans join the publisher's trace as children of
the publish span. A third-party broker adapter that drops message headers loses
that link: its consumers' dispatch spans have no parent. A message without the
headers behaves the same way.

modupy configures no exporter. Set up an OpenTelemetry tracer provider in your application and send the spans to a tracing backend, such as Jaeger, Tempo or any OTLP collector.

---

## Operational Playbooks

### Adding a New Module

1. Create the subpackage: `myapp/my_module/__init__.py`
2. Optionally add a manifest: `myapp/my_module/_manifest.py`
3. Restart: in single-process, restart the app; in topology mode, restart `modulith run`, because it derives its workers and proxy routes once at startup, so a module added later has no worker to restart (under Kubernetes, regenerate the manifests with `modulith k8s-manifest` and roll them out)

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
   still runs the group, or wait 24 hours. On the SHM broker, do not wait:
   the group pins every publication to its targets until it is dropped, and
   a store sized for the one-hour default retention can fill within those 24
   hours. The startup warning for a
   forgotten group likewise appears only when `modulith run --topology
   processes` restarts at least 24 hours after the group's last consumer
   activity; the restart in step 5 does not report it.

On the SHM and database brokers, the retired group moves through these states.

```mermaid
stateDiagram-v2
    [*] --> Served
    Served --> RecentlyServed: module deleted, workers restarted
    RecentlyServed --> Idle: 24 h without consumer activity
    RecentlyServed --> Dropped: drop-group with --force
    Idle --> Dropped: drop-group
    Dropped --> [*]
    RecentlyServed : Recently served
    note right of RecentlyServed
        drop-group refuses without --force
    end note
    note right of Idle
        modulith run --topology processes warns at startup
    end note
```

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
  reported.

  Only the process-topology supervisor runs that check. A single-process
  deployment and Kubernetes (the single-process Deployment, or the manifests
  from `modulith k8s-manifest`, whose pods each run one worker with no
  supervisor) get no startup warning for a retired group, and no command
  lists the groups a store holds. There you must know the group yourself:
  `modulith-<module>` for the module you deleted or renamed.

  Once the module is gone for good, run
  `modulith broker drop-group modulith-<module>` with the same broker
  configuration and environment as the service. It first prints the store
  it acts on (the SHM SQLite path, or the database URL with the password
  masked) and exits non-zero without creating anything when that store or
  its broker tables do not exist. It removes the group's subscriptions and
  deletes its pending and claimed messages (they are not delivered); the
  next prune reclaims the publications they held.

  It asks for confirmation unless `--yes` is given, exits non-zero when the
  store holds nothing for the group, and refuses a group that a current module
  derives or a consumer served in the last 24 hours unless `--force` is given.
  Before asking, it lists the targets the group is the only subscriber of and
  says what happens to later publishes to them, which step 2 stops first:
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
  - SHM broker: each is kept for `orphan_retention_seconds` (3600 s by
    default) and replayed to a group that subscribes within it while the
    store has room below its publish budget.

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
types a worker consumes. A worker consumes only its own module's listeners,
so it does not subscribe to event types that only a sibling module it imports
listens to.

On the SHM and database brokers, the pending or claimed messages queued for
the target are never removed automatically, so a rollback to a release that
still consumes the target finds them intact. The database broker also keeps
the group's subscription, so every publish to the target still adds a
message for the group. The SHM broker drops the group's subscription rows for
targets it no longer consumes when the worker starts: later publishes add
nothing for the group, and a rollback that subscribes again replays only the
publications still inside `orphan_retention_seconds` (one hour by default),
so older ones written in between never reach the group. Until you clean up:

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
longer reach the group, unless `expected_consumer_groups` still lists the
group for that target (database broker, `store` policy with
`orphan_replay_policy = "expected_groups"`): the group then keeps receiving a
pending message for every publish, and the command warns about it. Remove the
group from `expected_consumer_groups` too. The command asks for confirmation
unless `--yes` is given. Unlike the whole-group form, it needs no `--force`
for a current module's group.

### Recovering from Broker Failure

**Publishing (every broker):**
- A direct `publish()` raises the broker's error to its caller. modupy keeps no in-memory buffer of events it could not send
- Inside a bound outbox session, the broker send is saved as an outbox row in your transaction and made after the commit. While the broker is down the row stays in the outbox and the retry loop sends it again, until `dead_letter_after_attempts` (10 by default) failed attempts dead-letter it. `modulith outbox dead-letter --retry-all` resubmits dead-lettered rows once the broker is back

**Dead-lettered deliveries (database, shm and redis-streams brokers):**

```bash
modulith broker dead-letter               # list them, with attempts and last error (--list is the default)
modulith broker dead-letter --retry-all   # resubmit every one, with attempts reset
```

A delivery is dead-lettered after `max_delivery_attempts` failed dispatches or
reclaims, or at once when its payload cannot be read. Each belongs to one
consumer group, so `--retry-all` hands it back only to that group; a group that
already completed the same message does not receive it again. `--list` and
`--retry-all` are mutually exclusive. The command runs against the broker store
the service is configured with, and exits 1 with `no database broker tables` (or
`no shm broker store`) if that store has none. On the shm broker the resubmitted
delivery is claimed from the SQLite store, so a running consumer receives it on
its next poll. A broker without dead-letter support, such as a custom one, exits
1 saying so. For the outbox, use `modulith outbox dead-letter`.

On the Redis broker the command reads the `<stream_prefix>.<target>.dead`
streams. Redis stores no error text for a dead letter, so `last_error` is
`None`. `attempts` is how many times the consumer group delivered the message
before it was dead-lettered. The creation time is the entry id's timestamp. `--retry-all` re-adds each entry to
its target's stream and deletes it from the dead stream. Redis cannot address
one consumer group, so a re-added message reaches every group on that stream:
a target is resubmitted only when its stream has **one** consumer group. For a
target with several groups nothing is resubmitted; the command resubmits the
other targets, then names the target and its groups in an error and exits 1.

**Consuming (every broker):**
- A failed broker call is logged and retried under capped exponential backoff, 0.05 s doubling up to 5 s. `/health` reports `degraded` until that call succeeds again
- A message that a stopped or crashed consumer left claimed or pending is delivered again once it has sat idle for `reclaim_stale_seconds` (database and SHM) or `reclaim_min_idle_ms` (Redis). Consumers reclaim it themselves: the supervisor takes no part, and nothing rebalances
- A database or SHM consumer that stops gracefully hands back the messages it claimed but never started delivering, uncharged, so a peer takes them on its next poll. If a listener is still running when the stop's 1 s grace ends, the stop cancels it (except a plain `def` listener; see [Graceful Shutdown](#graceful-shutdown)), and the messages of that batch wait for the reclaim above. The database consumer hands back the message whose listener the stop cancelled at once as well, without charging a delivery attempt, so restarts of a healthy worker never dead-letter it. That holds while the listener has run for less than `reclaim_stale_seconds * 10` (600 s by default), the point where `/health` reports it stuck. A listener past that keeps its message claimed, and the reclaim charges the message one attempt, so a listener that hangs on every delivery still ends dead-lettered after `max_delivery_attempts`

**Redis broker:**
- A consumer creates its group when it starts, from the beginning of the stream, and creates it again if Redis reports `NOGROUP`. Entries published while every consumer is down wait in the stream, until the `MODULITH_STREAM_MAXLEN` cap (10000 by default) trims the oldest, unacknowledged ones included (see the Redis durability caveat above)

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
another 10 s, so a Redis consumer's `stop()` returns within 20 s in the worst
case. The database and SHM consumers first give a claim or dispatch already
under way up to 1 s to finish, then stop the poll task and then a prune task,
each under that bound, so theirs can take up to 41 s. A consumer sleeping
between polls skips that second. A database consumer whose stop cancels a
running listener releases that message uncharged when the listener has run for
less than `reclaim_stale_seconds * 10`, and leaves it to the reclaim, which
charges an attempt, when it ran longer.

On POSIX, `modulith run` shuts down in this order:

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant S as Supervisor and proxy
    participant W as Worker
    O->>S: SIGTERM
    Note over S: proxy stops accepting,<br>in-flight requests finish
    S->>W: SIGTERM to every worker
    Note over W: in-flight requests finish, then<br>1 broker consumer stops<br>2 outbox retry loop stops<br>3 outbox deliveries finish<br>4 store and engine disposed<br>5 brokers closed
    alt worker exits within 30 s
        W-->>S: exit
    else still alive after 30 s
        S->>W: SIGKILL
    end
    S-->>O: exit
```

The supervisor sends SIGTERM to every worker at once, and SIGKILL to any still running 30 s later.
Within that window, in-flight requests and after-commit outbox deliveries run to completion, but a listener that a broker consumer is running when its worker stops is cancelled, and its message is delivered again later. A plain `def` listener is the exception: it runs on an executor thread, so the stop cancels only the wait for it, the thread keeps running until the listener returns, and the worker does not exit before then unless SIGKILL ends it. Its message is delivered again later as well.
A pod from the generated Kubernetes manifests runs a worker alone, so it gets SIGTERM directly and skips the supervisor's steps.

A database or SHM consumer's worst-case stop (41 s) is longer than that 30 s window, so SIGKILL can cut a slow stop short. The messages the consumer still held then come back through the reclaim described under [Recovering from Broker Failure](#recovering-from-broker-failure): after `reclaim_stale_seconds` on the database and SHM brokers, after `reclaim_min_idle_ms` on Redis.

In Kubernetes the pod's `terminationGracePeriodSeconds` takes the place of the supervisor's 30 s window. The generated manifests leave it at Kubernetes's default of 30 s, and that window also covers a `preStop` hook such as the one above. Raise it above the `preStop` sleep plus the longest your in-flight requests, after-commit deliveries and consumer stop (up to 20 s on Redis, 41 s on the database broker) need, if you want stops to finish instead of relying on that reclaim.

Windows has no signal delivery on `subprocess.Popen` (`terminate()`
is an immediate `TerminateProcess`, with no softer step for a worker's
lifespan to trap), and the `PDEATHSIG` orphan protection that stops a
hard-killed supervisor from leaving workers behind is Linux-only.

---

## Migration Path: Monolith → Processes → Microservices

![The same three modules in three shapes: one process on day one, one process per module when a feature gets busy, and payments split off into its own service](images/growth.svg)

1. **Start monolithic** — `MODULITH_BROKER=memory` (default)
   - Fast to iterate
   - All modules in one process

2. **Add durability** — `MODULITH_OUTBOX=postgres` plus `MODULITH_OUTBOX_URL` (or `[tool.modulith].outbox_url`)
   - `MODULITH_OUTBOX=postgres` alone persists nothing: the store binds only from `outbox_url` (or an explicit `outbox.configure()`), and only a session bound around each transaction (`bind_session`) lets `publish()` enlist the outbox row
   - Run `modulith migrate` to create the outbox tables, and add `bootstrap()`, `outbox.start()` and `outbox.shutdown()` to the lifespan
   - Same single-process deployment
   - Events now persist; listeners are at-least-once

3. **Split processes** — `MODULITH_BROKER=database --topology processes`
   - Modules now run in separate workers
   - Module code doesn't change; listeners stay `@listener` decorated
   - `main.py` does not run in workers, so set `MODULITH_OUTBOX_URL` (or `[tool.modulith].outbox_url`) to let every worker bind the outbox store itself (see [Durable Single-Process](#durable-single-process-outbox-pattern))
   - A worker with `MODULITH_OUTBOX` set and no store bound refuses to start with a `ConfigurationError`, so a deployment that exported `MODULITH_OUTBOX` for step 2 must add the URL before this step
   - Each worker starts the outbox retry loop itself at startup

4. **Extract microservice** — `modulith extract <module>` scaffolds a standalone service
   - Copies the module, its contracts and every package-level helper module they import, transitively, into `--output` (default `<module>-service/`) and generates a `pyproject.toml`, `Dockerfile`, `README.md`, and `.env.example` to run it against `modulith._worker:create_app`
   - Blocked (exit 1) by the module's own outbound boundary violations, tables it shares with another module, or a runtime import of another declared module from any copied file — `--force` overrides only these three and records what it overrode in the generated README; a non-empty `--output` directory is never overridable
   - Before publishing, imports the extracted module in a subprocess from the staged tree and exits 1 naming the failing import if that fails, or if the import loads first-party code from the source tree outside the extracted copy (reachable through `PYTHONPATH` or an editable install), so the service's third-party dependencies must be installed where you run `extract`
   - Other modules keep sending events via the broker; the extracted service subscribes and acts. As in a worker, the service binds its outbox store from `MODULITH_OUTBOX_URL` (`[tool.modulith].outbox_url`) and refuses to start without a store when `MODULITH_OUTBOX` is not `memory`, because the app's `main.py` is not copied (see [Durable Single-Process](#durable-single-process-outbox-pattern)). The generated `.env.example` and README list `MODULITH_OUTBOX` and `MODULITH_OUTBOX_URL`
   - How contracts, helpers, symlinks, namespace folders and first-party code are handled: [What `modulith extract` copies and checks](#what-modulith-extract-copies-and-checks)

This path is why modupy exists: **every module is a potential microservice, but you pay that cost only when it's profitable.**

### What `modulith extract` copies and checks

- **Contracts.** Contracts resolve as Python imports them: a `contracts.py` wins over a same-named directory without `__init__.py`. Only the file is copied, so data files in that directory (such as `contracts/order.avsc`) stay out of the service. A contracts directory without `__init__.py` and with no `contracts.py` beside it is copied whole, and its files are scanned for helper imports like any contracts package.
- **Helpers.** A helper package is copied whole, not just the modules imported from it: every file under its directory goes into the service, imported or not (bytecode caches aside). A helper module file is copied alone, like `contracts.py`: a same-named directory without `__init__.py` beside `emails.py` stays out.
- **Symlinks.** `extract` exits 1 with `source symlink <path> is not supported` on any symlink in the module, the contracts or a helper package. A helper package is checked whole, as it is copied whole, so a link nothing imports blocks extraction too. `--force` does not override this refusal. Replace the link with a real file, or move it out of the package.
- **Namespace folders.** A package ancestor with no `__init__.py` in the source (a PEP 420 namespace root such as `company/` for `package = "company.shop"`) gets none in the output either, so other portions of that namespace stay importable. The same holds for a helper's parent folder: one without `__init__.py` in the source (such as `shop/common/` holding `money.py`) stays a namespace folder in the output, so the service discovers no extra module; one with an `__init__.py` gets an empty initializer.
- **First-party code in the import check.** First-party code is anything under the directory that holds the app's top-level package. Modules under the interpreter's prefixes, standard library and site-packages directories are exempt, except that a directory containing that source tree exempts nothing: a virtualenv inside the project stays exempt, and a project inside a virtualenv is still checked (on Windows, `site.getsitepackages()` lists the virtualenv root itself).
- **Installed copies.** When the app resolves to an installed copy in the interpreter's site-packages or user site (a plain or `--user` `pip install`), only the app's own top-level package counts as first-party there, and other installed distributions stay exempt. A directory the app was installed into with `pip install --target` or `--prefix` and reached through `PYTHONPATH`, a `.pth` file or any other `sys.path` entry is not one of the interpreter's library directories, so everything in it counts as first-party: `extract` reports the app's dependencies installed there and exits 1.
- **`--force`.** `--force` never overrides the import check, so a module-level import of another declared module fails even when forced; only a deferred one (inside a function) can be forced through.

---

## Troubleshooting

### Events Not Delivered

1. Check that the listener's module was discovered: `modulith info` lists it under `modules`, and the INFO-level startup log names it in the `discovered N module(s)` line. A module without a `_manifest.py` shows `[no manifest]`, which is normal: manifests are optional. (`info` prints modules, configuration, plugins, and registered broker schemes — there is no per-listener listing. A module whose import fails stops the boot with a `ConfigurationError` that names it, and for a module that has a manifest, a declared listener that never registered is a boot failure too.)
2. If the module has a manifest, verify that it declares the event: `_manifest.py` → check `consumes`
3. Inspect broker state:
   - Database: `SELECT * FROM broker_message WHERE status IN ('pending','claimed')` for work still in flight, and `WHERE status = 'dead'` for the dead-letter view. The column only ever holds `pending`, `claimed`, `done`, or `dead`.
   - Redis: `xinfo groups modulith.events.<target>`, where `<target>` is the event's destination, by default its module and class name (for example `myapp.contracts.events.OrderPlaced`), and `modulith.events` is the default stream prefix (`MODULITH_STREAM_PREFIX` replaces it). List the streams with `SCAN 0 MATCH 'modulith.events.*'`, repeating with the returned cursor until it is `0`.

### Worker Crash Loop

The supervisor respawns a crashed worker after a doubling delay, and stops respawning it after more than five crashes in a row.

```mermaid
stateDiagram-v2
    [*] --> Running: spawn
    Running --> Waiting: crash 1 to 5
    Waiting --> Running: respawn
    Running --> GivenUp: crash 6 in a row
    Waiting : Waiting to respawn
    GivenUp : Given up
    note right of Waiting
        waits 1, 2, 4, 8, 16 s;
        then up to 60 s while
        the port is still held
    end note
    note left of Running
        a run of 60 s or more resets
        the delay and the count
    end note
    note right of GivenUp
        no respawn until the
        supervisor restarts
    end note
```

1. Read the supervisor's output (the terminal running `modulith run`, or the container logs): it prefixes every worker line with the worker's name, such as `[orders]`
2. Test the broker itself: connect to its URL with `psql` (without the `+asyncpg` driver suffix) or `redis-cli`, or read a worker's `/health`, which answers 503 while its broker consumer is not ready. `modulith doctor` does not do this: it reports on configuration, module boundaries and the outbox, and its Redis check skips the live query without failing when no server answers
3. Check disk space (SQLite needs it)

### High Latency

1. Database broker — tune batch size: `MODULITH_BROKER_BATCH_SIZE=200` (default 100)
2. Database broker — increase concurrency: `MODULITH_BROKER_DISPATCH_CONCURRENCY=20` (default 10)
3. Redis broker — neither knob exists; add workers instead (`[tool.modulith.workers]`)
4. Profile with `modupy[otel]` and check span duration

---

## Reference: Supervisor, Proxy, and Boundary Environment Variables

Broker and outbox settings are covered in the topology sections above
(`MODULITH_BROKER`, `MODULITH_BROKER_<KEY>`, `MODULITH_OUTBOX`,
`MODULITH_OUTBOX_URL`). `MODULITH_DB_URL` is not a runtime variable: it is the
sync-driver URL the raw Alembic command reads, and `modulith migrate` reads
`outbox_url` instead. These are the remaining process-level knobs:

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
| `shm` (the default for `--topology processes`) | None (a SQLite file in `state_dir`) | Yes | Single-host process-per-module | Single host, nothing extra to run |
| `database` (SQLite) | Local file | Yes | Single-host process-per-module | Dev, single-host staging |
| `database` (Postgres) | Existing DB | Yes | Multi-host process-per-module | Production monolith and distributed |
| `database` (MySQL or MariaDB) | Existing DB (MySQL 8.0.1+ or MariaDB 10.6+) | Yes | Multi-host process-per-module | Production on MySQL or MariaDB |
| `redis-streams` | Docker/Cloud | Yes, with AOF¹ | High throughput, multi-host | High-load production |

¹ Redis streams are only as durable as Redis's own persistence: set `appendonly yes` with an `appendfsync` policy and keep Redis's data directory on a persistent volume, or use a managed Redis with AOF enabled. See [B. Redis Streams Broker](#b-redis-streams-broker).

---

## Next Steps

- For **detailed internal architecture**, see [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- For **API reference**, see [docs/API_REFERENCE.md](API_REFERENCE.md)
- For **working examples**, see [examples/demo_app](../examples/demo_app): three modules whose stages (in-memory, durable outbox, process-per-module, Postgres, Redis) are switched by configuration, with the outbox bound from `outbox_url` and service functions that bind, publish, commit and unbind. [examples/README.md](../examples/README.md) lists the smaller and larger projects
- For **testing**, see [Cookbook §9](COOKBOOK.md#9-test-an-event-flow-with-the-pytest-plugin)
