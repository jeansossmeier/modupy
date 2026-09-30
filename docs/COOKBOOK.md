# modulith Cookbook

Task-oriented recipes for common jobs. Each one uses only the documented public
API (see [API_REFERENCE.md](API_REFERENCE.md)) plus the documented wiring
surface (see [STABILITY.md](STABILITY.md)); for the design behind them see
[ARCHITECTURE.md](ARCHITECTURE.md) and [SPEC.md](../SPEC.md). The runnable
end-to-end version of recipes 1–3, 5, 6 and 8 lives in
[`examples/demo_app`](../examples/demo_app). Recipe 4 (`publish_sync`) is not
in it: the demo publishes from async code only.

**Contents**

1. [Define an event and a listener](#1-define-an-event-and-a-listener)
2. [Share event types through a contracts module](#2-share-event-types-through-a-contracts-module)
3. [Publish from async code](#3-publish-from-async-code)
4. [Publish from sync code (views, scripts)](#4-publish-from-sync-code-views-scripts)
5. [Declare a manifest and let bootstrap verify it](#5-declare-a-manifest-and-let-bootstrap-verify-it)
6. [Enable the durable Postgres outbox](#6-enable-the-durable-postgres-outbox)
    - [Coordinating concurrent sweepers](#coordinating-concurrent-sweepers)
    - [Putting the outbox in a per-module schema](#putting-the-outbox-in-a-per-module-schema)
7. [Choose an outbox completion mode](#7-choose-an-outbox-completion-mode)
8. [Go process-per-module and externalize an event](#8-go-process-per-module-and-externalize-an-event)
    - [Durable local SHM default](#durable-local-shm-default)
    - [Use a shared database broker](#use-a-shared-database-broker)
    - [No-subscriber and orphan-replay policies](#no-subscriber-and-orphan-replay-policies)
    - [Declaring broker destinations](#declaring-broker-destinations)
9. [Test an event flow with the pytest plugin](#9-test-an-event-flow-with-the-pytest-plugin)
10. [Enforce boundaries in CI](#10-enforce-boundaries-in-ci)
11. [Extend modulith with a plugin](#11-extend-modulith-with-a-plugin)

---

## 1. Define an event and a listener

**Goal:** react to something that happened in another module, without either
module importing the other.

An event is a plain class marked with `@event` — a frozen dataclass is the
recommended shape because its value semantics (equality, hashing) make it safe
to share and give the outbox reliable round-trip fidelity. A listener is a
function whose first parameter is annotated with the event type it consumes.

```python
from dataclasses import dataclass
from modulith import event, listener

@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    customer_id: str
    total: float

@listener
async def reserve_stock(event: OrderPlaced) -> None:
    await stock_service.reserve(event.order_id)
```

The event type is inferred from the annotation, so `@listener` needs no
arguments. Listeners may be `async def` (preferred) or plain `def` (recipe 4).
Multiple listeners can subscribe to the same event; they run concurrently and
one failing does not block the others. Once they have all finished, `publish()`
re-raises the first failure, in registration order, to the publisher. That
holds for the in-memory path; under the durable outbox (recipe 6) listeners run
after commit, so a failure is retried and never reaches the publisher.

Dispatch is by exact type: a listener receives only events whose class is the
annotated one. A listener annotated with a base class does not receive its
subclasses' events, so give each event type its own listener.

> **Registration timing.** Discovery imports each module *package*, so keep
> `@listener` functions reachable from the module's `__init__.py` (e.g.
> `from . import handlers`). A listener that never imports never registers.

---

## 2. Share event types through a contracts module

**Goal:** let a publisher and a consumer agree on an event's shape without
depending on each other's code.

Put shared event definitions in a dedicated `contracts` subpackage. Both the
publishing and consuming modules import the event from there — they depend on a
schema, not on each other. This is the pattern that keeps module coupling low.

```
myapp/
├── contracts/             # shared vocabulary
│   ├── __init__.py
│   └── events.py          # OrderPlaced, StockReserved
├── orders/                # publishes OrderPlaced
├── inventory/             # listens for OrderPlaced, publishes StockReserved
└── notifications/         # listens for StockReserved
```

Every module directory needs an `__init__.py`, `contracts/` included —
discovery walks subpackages, so a directory without one is a namespace package
and is skipped silently.

```python
# myapp/contracts/events.py
from dataclasses import dataclass
from modulith import event

@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    customer_id: str
    total: float

@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
```

The boundary verifier (recipe 10) treats the contracts module as a *sink* —
every module may import from it, and it imports from no module. The default
contracts module name is `contracts`; override it with
`[tool.modulith].contracts_module` if your app names it differently.

---

## 3. Publish from async code

**Goal:** emit an event from an `async` service function.

```python
# myapp/orders/__init__.py
from uuid import uuid4
from modulith import publish
from myapp.contracts.events import OrderPlaced

async def place_order(customer_id: str, total: float) -> str:
    order_id = uuid4().hex[:8]
    await persist_order(order_id, customer_id, total)
    await publish(OrderPlaced(order_id=order_id, customer_id=customer_id, total=total))
    return order_id
```

`place_order` knows nothing about who reacts to `OrderPlaced`. In single-process
mode `publish()` dispatches in-memory; with the outbox enabled and a transaction
bound (recipe 6) the same call persists the event durably and dispatches after
commit. **Your code does not change between those modes** — only configuration
does.

---

## 4. Publish from sync code (views, scripts)

**Goal:** publish from a FastAPI sync view, a management script, or any code
that can't `await`.

Use `publish_sync()` and a plain `def` listener. `publish_sync()` detects its
context and does the right thing; a `def` listener is wrapped to run in the
event loop's executor so it never blocks the loop.

```python
from modulith import publish_sync, listener
from myapp.contracts.events import OrderPlaced

def place_order_sync(customer_id: str, total: float) -> str:
    order_id = save_order(customer_id, total)   # sync DB code
    publish_sync(OrderPlaced(order_id=order_id, customer_id=customer_id, total=total))
    return order_id

@listener
def send_receipt(event: OrderPlaced) -> None:   # sync listener, runs in a thread
    mailer.send(event.customer_id, event.order_id)
```

Three sharp edges (the first two from `wrap_sync_listener`'s contract):

- Only **synchronous** SQLAlchemy sessions work inside a sync listener's
  executor thread. An `AsyncSession` needs SQLAlchemy's greenlet bridge — use an
  `async` listener for `AsyncSession` work.
- Multiple sync listeners for one event run **concurrently on separate
  threads**, each with a copy of the caller's context. Share only thread-safe
  resources; give each listener its own session/lock.
- `publish_sync()` runs on its own persistent daemon-thread loop.
  - **Outbox:** if the outbox is also driven with `await publish()` on the
    app's own loop, the same `AsyncEngine` is shared across two loops. On
    Postgres and MySQL the first query one loop runs on a pooled connection the
    other loop opened raises `RuntimeError: ... attached to a different loop`;
    no pool exhaustion is needed. On SQLite, connections work from any loop,
    but once every pooled connection is checked out, a loop that waits for one
    after another loop already has raises `RuntimeError: <Queue> is bound to a
    different event loop`. The outbox store, unlike the database broker, does
    not hand calls to its engine's loop, and a larger pool only delays the
    SQLite failure. Keep publishes for one outbox engine on one loop.
  - **Database broker:** the broker submits the call to the loop that first
    used it, which in a worker is the app loop, and runs it there. That loop
    must stay running and unblocked, or the `publish_sync()` call waits for it
    (DEPLOYMENT.md §A).

`publish_sync()` takes a `timeout` (default 30s) and raises `PublishSyncTimeout`
if a listener deadlocks. Do **not** call it from inside async code on the loop's
own thread — it raises `RuntimeError` telling you to `await publish()` instead.

---

## 5. Declare a manifest and let bootstrap verify it

**Goal:** turn silent drift (a module that failed to import, a renamed event)
into a loud startup failure.

Add a `_manifest.py` at module scope declaring the module's contract. modulith
reads it at startup and checks it against reality.

```python
# myapp/orders/_manifest.py
from modulith import declare_module

declare_module(
    publishes=["OrderPlaced"],
    declared_dependencies=["contracts"],
)
```

At bootstrap (when `verify_manifests` is on, the default) modulith verifies two
cheap things: every declared `listeners` entry actually registered against the
bus, and every `publishes` name is defined in the package namespace. A mismatch
aborts boot with a `file:line` instead of silently dropping events. Declaring
`listeners` is the highest-value check — it catches a module that failed to
import:

```python
# myapp/inventory/_manifest.py
from modulith import declare_module
from . import handlers            # ensures the listener is imported + registered

declare_module(
    consumes=["OrderPlaced"],
    publishes=["StockReserved"],
    listeners=[handlers.reserve_stock],
    owns_tables=["inventory_stock_levels"],
    declared_dependencies=["contracts"],
)
```

`consumes`, `owns_tables`, and `declared_dependencies` are consumed by the docs
generator, the audit tool, and the AST verifier (recipe 10) — not cross-checked
at bootstrap. Note the distinction on `declared_dependencies`: **omitting it**
(the default `None`) leaves the verifier's dependency rule off; an **explicit
empty list** means "depends on nothing" (deny-all, contracts excepted).

**Per-module DB schema ownership.** Every table a module defines belongs in
its `owns_tables` list. The table's name then carries the module as either a
prefix (`inventory_stock_levels`) or a DB schema (`inventory.stock_levels`) —
`owns_tables` holds the name the module passes to `Table("...")` or sets as
`__tablename__`: `inventory_stock_levels` under the prefix convention above,
the bare `stock_levels` under a DB schema, which is where the table lives, not
part of its name. If a module
declares a non-empty `owns_tables`, the verifier's `data-ownership` rule warns
on any table the module defines (`Table("x")` or `__tablename__`) but doesn't
list — the manifest is meant to stay a complete inventory of the module's
data. `modulith doctor`'s process-split readiness check separately reports,
per module, how many defined tables aren't prefixed with the module's name —
informational, not a failure.

---

## 6. Enable the durable Postgres outbox

**Goal:** stop losing events on crash — deliver at-least-once, atomically with
the business transaction.

Install the extras (`postgres` for the store and the migrations, `cli` for the
`modulith` command) and point modulith at the database that holds your business
data:

```bash
pip install 'modupy[postgres,cli]'
```

```toml
# pyproject.toml
[tool.modulith]
outbox = "postgres"
outbox_url = "postgresql+asyncpg://user:pass@localhost/mydb"  # or MODULITH_OUTBOX_URL
```

Bootstrap builds a `PostgresPublicationStore` on its own engine for that URL
and binds it in every process, so the `modulith outbox` commands see the same
store. Apply the packaged migrations with `modulith migrate`. It migrates
`outbox_url` by default, swapping the async driver for the sync one Alembic
runs on (`+asyncpg` becomes `+psycopg`, `+aiosqlite` plain `sqlite`,
`+aiomysql` `+pymysql`), and prints the target with the password masked:

```bash
modulith migrate                          # to head, on outbox_url
modulith migrate --url 'postgresql+psycopg://user:pass@localhost/mydb'
modulith migrate --schema orders          # Postgres only; see the per-module schema below
```

The chain also creates the `broker_*` tables of the database broker (recipe 8)
in that database. They stay unused unless you select that broker.

Alembic's own command works as the alternative. modulith ships its alembic
config inside the installed package, and `MODULITH_DB_URL` (a sync-driver URL)
is the variable it reads:

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  upgrade head
```

The store is only half of the wiring. Bind a SQLAlchemy session around each
business transaction so the `publish()` calls inside it are captured by the
outbox. The service function binds, publishes, commits and unbinds before the
route returns:

```python
# myapp/orders/__init__.py
from modulith import publish
from modulith.builtin.outbox import bind_session, unbind_session
from myapp.contracts.events import OrderPlaced
from myapp.db import async_session_maker, Order   # your engine, sessionmaker and model

async def place_order(order_id: str, customer_id: str, total: float) -> None:
    async with async_session_maker() as session:
        token = bind_session(session)
        try:
            session.add(Order(id=order_id, customer_id=customer_id, total=total))
            await publish(OrderPlaced(order_id=order_id, customer_id=customer_id, total=total))
            await session.commit()   # the order row and the event commit together
        finally:
            unbind_session(token)
```

Put the commit in the service function, or in a `transaction()` helper you own
that wraps this bind-publish-commit-unbind sequence. Do not put it in a `yield`
dependency's teardown: FastAPI runs that teardown after the response is sent,
so a commit that fails there still answers 200.

Now a `publish()` inside a bound transaction is persisted atomically with your
data: a rollback discards the event (no ghosts), a commit guarantees delivery
(no losses), and listeners are retried at-least-once after commit. Because
delivery is at-least-once, **listeners must be idempotent**.

A publish joins whatever transaction the bound session has open, and only a
later `commit()` delivers it. A transaction that ends uncommitted (a rollback,
or the session closing) discards its publications, and the adapter logs a
WARNING naming their event types. A publish made after `unbind_session` is not
transactional: it dispatches directly, with no outbox row, exactly like a
publish outside any bound scope.

A task started with `asyncio.create_task` inside the bound scope inherits the
binding only until `unbind_session`. Its publishes before then join the
session's open transaction, under the same rule: they are delivered only if a
later commit covers them. Its publishes after then are not transactional. For
a durable publish from such a task, open and bind a session in the task itself.
Inspect the queue with `modulith outbox status`; a persistently-failing
publication is dead-lettered after 10 attempts.

`bind_session` and `unbind_session` live in `modulith.builtin.outbox`. The
older `modulith.adapters.postgres_outbox` import path still works, as aliases
of the same functions.

Bootstrap runs lazily, at the first `publish()`, and every sweep skips its rows
until the runtime is bootstrapped. Call `modulith.bootstrap()` and then
`outbox.start()` in your ASGI lifespan's startup half, so rows a crashed
process left undelivered are swept at startup instead of waiting for the first
transactional publish. In a single-process app, call `outbox.shutdown()` in the
lifespan's teardown to stop the retry loop; the store bound from `outbox_url`
is disposed only by process-topology workers. The outbox table must live in the
database that holds your business data, or the row and your data cannot commit
in one transaction. Under `--topology processes`, `main.py` (its lifespan and
middleware) does not run in workers; with `outbox_url` set, each worker binds
the store itself. See
[DEPLOYMENT.md's Durable Single-Process recipe](DEPLOYMENT.md#durable-single-process-outbox-pattern)
for the full pattern.

**Wiring the store yourself.** Bind the store in code, with your own engine,
when you need what `outbox_url` does not offer: `connect_args` or a
`schema_translate_map` on the engine, a custom serializer, an application that
runs with `auto_discover = false`, or the graceful shutdown order below.
`outbox_url` binds only while `auto_discover` is on, because the
deserialization allowlist comes from the discovered listeners. A store you bind
with `outbox.configure()` before bootstrap wins, and no second store is built.
The tuning keywords (`claim_strategy`, `completion_mode`, ...) are the same
ones `[tool.modulith.outbox_options]` forwards; see recipe 7.

```python
# myapp/main.py
from modulith.adapters.postgres_outbox import PostgresPublicationStore
from modulith.builtin import outbox
from modulith.serializers import JsonEventSerializer
from myapp.contracts.events import OrderPlaced, StockReserved

store = PostgresPublicationStore(engine=async_engine)
outbox.configure(
    store=store,
    # allowed_event_types is the deserialization allowlist — set it in
    # production wherever payloads can originate outside the trusted process
    # boundary (a shared outbox table, a broker). Without it, a forged
    # event_type could trigger an arbitrary-module import on deserialize.
    serializer=JsonEventSerializer(allowed_event_types=[OrderPlaced, StockReserved]),
    completion_mode="update",
)
```

A module-scope `outbox.configure()` runs before the server's event loop exists,
so it cannot start the retry loop: call `modulith.bootstrap()` and then
`outbox.start()` in the lifespan's startup half, as above. Under
`--topology processes`, `main.py` does not run in workers, so bind the store
from the module's import or a `modulith_after_module_load` hook instead.

On shutdown, drain the retry loop instead of letting the process die mid-flight
— call `store.dispose()`, then `outbox.shutdown()`, then dispose the engine, in
that order, from an ASGI lifespan or equivalent shutdown hook.

### Coordinating concurrent sweepers

Every process that wires the outbox runs its own retry loop against the same
table. `claim_strategy` decides how those sweepers stay off each other's rows:

| `claim_strategy` | Behaviour |
|---|---|
| `"lease"` (default) | claim a batch in one committed transaction, renew the lease while dispatching, fence the completion write on the claim token |
| `"advisory_lock"` | hold a Postgres advisory lock per row for the dispatch. Rejected at `configure()` on a non-Postgres store. Lock connections come from a second pool sized like the engine's. With a `QueuePool` (the async engine default), a process can hold up to 2×(`pool_size` + `max_overflow`) Postgres connections during a burst and keeps up to `pool_size` idle lock connections afterwards; budget `max_connections` for that. `NullPool` and `max_overflow=-1` are unbounded: one lock connection per in-flight row. An after-commit dispatch that waits past `pool_timeout` for a lock connection logs a WARNING and leaves its row to the sweep, which delivers one row at a time, so a burst larger than the lock pool can serve within `pool_timeout` drains slowly |
| `"none"` | no coordination — two sweepers may dispatch the same row. Warns at `configure()` |

```python
outbox.configure(
    store=store,
    serializer=serializer,
    claim_strategy="lease",     # default
    claim_lease_seconds=60.0,   # must exceed your slowest listener
    claim_batch_size=100,       # rows claimed per sweep
)
```

A lease shorter than a listener's runtime expires mid-dispatch and lets a peer
legitimately reclaim the row — a duplicate delivery, not a bug. Raise
`claim_lease_seconds` rather than lowering it to chase latency. The lease is
also the crash-recovery bound: rows a crashed process was delivering are
recovered once their lease expires, normally within `claim_lease_seconds`
plus `retry_interval_seconds` of the crash. Recovery takes longer when
`retry_stale_seconds` exceeds the lease, while a slow sweep is still running,
or while the runtime is not bootstrapped. A crash or a graceful stop in the
middle of a sweep leaves the row being delivered, and every row of its
claimed batch not yet reached, leased until the lease expires. Rows it had
already delivered, failed or released are not leased.

Under `"advisory_lock"` a crashed process's rows are recovered at once when
the process dies on a live host, unless a descendant forked from it is still
running. A fork-started `multiprocessing` or `ProcessPoolExecutor` child keeps
a copy of the lock connection's socket, so the locks stay held until that
child exits. Start such children with the `spawn` or `forkserver` method. After a host loss or a network partition
they stay locked until Postgres drops the dead session through TCP
keepalive, about 2 h 11 min with stock Linux defaults. Lower the server's
keepalive settings to shorten that; lock connections use the engine's
`connect_args`:

```python
engine = create_async_engine(
    "postgresql+asyncpg://user:pass@db/app",
    connect_args={"server_settings": {
        "tcp_keepalives_idle": "60",
        "tcp_keepalives_interval": "10",
        "tcp_keepalives_count": "3",
    }},  # psycopg: {"options": "-c tcp_keepalives_idle=60 -c ..."}
)
```

An engine built from `outbox_url` takes no `connect_args`. With
`postgresql+psycopg`, put the keepalives in the URL's `options` query
parameter:
`?options=-c%20tcp_keepalives_idle%3D60%20-c%20tcp_keepalives_interval%3D10%20-c%20tcp_keepalives_count%3D3`.
With asyncpg, set them in `postgresql.conf` or with `ALTER ROLE ... SET`. Leave
`idle_session_timeout` unset for the outbox's role, because a lock
connection sits idle while its listener runs and ending that session frees
the row for a second delivery. Behind PgBouncer, advisory locks need session
pooling; transaction pooling breaks them.

The default strategy needs the lease columns, which arrive in migration
`0003_outbox_claim_leases`: migrate to `head`, not to `0001_initial`. In your
own `outbox.configure()` call, `claim_strategy`, `claim_lease_seconds` and
`claim_batch_size` are keyword arguments. When the runtime binds the store from
`outbox_url`, it applies the same keys from `[tool.modulith.outbox_options]`
— see the note under recipe 7.

### Putting the outbox in a per-module schema

To keep a module's outbox tables in a Postgres schema named after the module
(see recipe 5's per-module DB schema ownership convention), set a
`schema_translate_map` on the engine before passing it to
`PostgresPublicationStore` — no store-level configuration exists because the
store just uses whatever engine it's given:

```python
async_engine = create_async_engine(db_url).execution_options(
    schema_translate_map={None: "orders"}
)
store = PostgresPublicationStore(engine=async_engine)
```

Run the migration against the same schema with `modulith migrate --schema
<name>`, or with Alembic's `-x schema=<name>` or `MODULITH_DB_SCHEMA`:

```bash
modulith migrate --schema orders
```

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  -x schema=orders upgrade head
```

This is Postgres-only; `alembic`'s `--sql` offline mode combined with a schema
exits with an error, since there's no live connection to carry the
translation map.

The database broker (recipe 8) takes the equivalent knob on its own engine:
`[tool.modulith.broker_options].schema`, or the environment variable
`MODULITH_BROKER_SCHEMA` (which wins, like every other `broker_options` key).
It must be a portable unquoted SQL identifier. Validation is identical through
configuration, environment variables, Alembic `-x`, and direct broker
construction. Like the outbox knob above, it applies only on Postgres; other
dialects log a warning and ignore it.

Enabling a named migration schema never moves existing data. If the target has
no Alembic history while `public` contains Modulith tables or history, the
migration refuses to create a second history. Back up the database, explicitly
move and verify the tables, then rerun the command.

---

## 7. Choose an outbox completion mode

**Goal:** decide what happens to a publication row once its listener succeeds.

Pass `completion_mode` to `outbox.configure()` (recipe 6):

| Mode | Effect on success | Use when |
|---|---|---|
| `"update"` (default) | sets `completed_at` in place | you want an audit trail of delivered events |
| `"delete"` | hard-deletes the row | you want the smallest possible outbox table |
| `"archive"` | moves it to `event_publications_archive` | audit trail without bloating the hot table |

```python
outbox.configure(store=store, serializer=serializer, completion_mode="archive")
```

With `"archive"`, trim old archive rows past their retention with
`modulith outbox purge`. When the runtime binds the store from `outbox_url`,
set the mode in pyproject instead: `[tool.modulith.outbox_options]` validates
`completion_mode` and forwards it to `outbox.configure()`.

```toml
[tool.modulith.outbox_options]
completion_mode = "archive"
```

The same table carries the other outbox tuning, and only when the runtime binds
the store from `outbox_url`. It validates these eight keys and forwards them as
the matching `outbox.configure()` keyword arguments:

| Key | Value |
|---|---|
| `claim_strategy` | `"lease"`, `"advisory_lock"` or `"none"` (recipe 6) |
| `claim_lease_seconds` | positive finite number |
| `claim_batch_size` | positive integer |
| `dead_letter_after_attempts` | positive integer |
| `retry_interval_seconds` | positive finite number |
| `retry_stale_seconds` | positive finite number |
| `max_retry_backoff_seconds` | positive finite number |
| `completion_mode` | `"update"`, `"delete"` or `"archive"` |

A key outside these eight is accepted and ignored. A store you bind yourself
with `outbox.configure()` takes these settings as keyword arguments instead.

---

## 8. Go process-per-module and externalize an event

**Goal:** run one module in its own process (its own CPU/memory budget) while
keeping the same module code.

Switch topology to process-per-module. With no broker or URL configured,
modulith selects the stdlib-only, durable local `shm` broker:

```toml
# pyproject.toml
[tool.modulith]
topology = "processes"

[tool.modulith.workers]
default = 1
reports = 4            # the reports module gets 4 worker processes
```

Cross-module events now have to leave the process, and they need no marker to
do it. An event with no listener in the publishing process routes to the broker
under the default target `{broker}:{event-fqn}`: the `OrderPlaced` that `orders`
publishes and only `inventory` consumes reaches the `inventory` worker as is.
`@externalized` covers the two cases that rule misses. Marked events go to the
broker **in addition to** any local listeners (fan-out across processes), and
`target=` pins the destination:

```python
from dataclasses import dataclass
from modulith import event, externalized

@externalized                                  # local listeners AND remote workers; target {broker}:{event-fqn}
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str

@externalized(target="shm:orders.placed")             # explicitly pins local SHM
@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
```

Run it under the supervisor + reverse proxy. Both extras are required here even
if single-process mode never needed them: `cli` provides the `modulith` command,
and `fastapi` provides the FastAPI + uvicorn that the reverse proxy and every
worker subprocess are built from:

```bash
pip install 'modupy[fastapi,cli]'
modulith run myapp.main:app --topology=processes
```

The supervisor spawns one uvicorn subprocess per module (restarting crashes with
backoff), and the reverse proxy routes each request to the right worker by URL
prefix. In single-process topology `@externalized` is an inert marker, so you can
add it before you need it and it costs nothing until then.

**Expose each module's HTTP routes as `router` on the module package.** A worker
mounts the `router` attribute of the module package it hosts — `myapp.orders` —
under `/<module>`, and the proxy forwards `/orders/...` to that worker. A router
defined one level down (`myapp/orders/api.py`) is invisible to the worker unless
the package re-exports it: the worker still starts, `/health` still reports
`ready`, and every route 404s. Re-export it:

```python
# myapp/orders/__init__.py
from myapp.orders.api import router as router   # the alias marks a deliberate re-export
```

Put that import at the *bottom* of `__init__.py` if `api.py` imports back from
the package (e.g. the `place_order` of recipe 3) — the name it needs must exist
before the import runs. Declare routes relative to the router root
(`@router.post("")`) so one mount convention serves both topologies: with
`app.include_router(router, prefix="/orders")` in `myapp/main.py` and with the
worker's `/orders` mount, `POST /orders` is the same URL either way. A module
with no `router` is a listener-only worker — it consumes events and serves only
`/health`.

The proxy's `/_modulith/*` actuator routes (topology, liveness, health) are a
separate matter from your module routes: `modulith run` binds
`0.0.0.0`, and the default `actuator_mode="auto"` refuses to serve them
unauthenticated off loopback — with no `MODULITH_ACTUATOR_TOKEN` set they are
left unmounted and startup logs a warning. Export a token if you want them; see
[DEPLOYMENT.md §Actuator Access](DEPLOYMENT.md#actuator-access-_modulith).

For Redis Streams, install `modupy[redis]` and set
`broker = "redis-streams"` explicitly.

### Durable local SHM default

The `shm` broker is local-host only. SQLite is authoritative for publications,
subscriptions, claims, retries, and acknowledgements. The mmap ring stores only
advisory committed-sequence hints; consumers safely poll SQLite when a hint is
missing, corrupt, stale, or wrapped. A successful `publish()` has already
committed to SQLite.

By default its absolute, package-namespaced files live in the platform's private
per-user state directory, under a name that digests the package's resolved
install path. Redeploying the same code to another path (a new release
directory behind a `current` symlink, a new venv) therefore switches to a new,
empty store and strands the old one's backlog, so production deploys must set
`state_dir` (or `MODULITH_BROKER_STATE_DIR`) or an absolute `sqlite_path`. The
`modulith run` process logs the SQLite store path at INFO and whether it is the
default; workers log it only when the application configures INFO logging. Configure paths canonically when needed:

```toml
[tool.modulith.broker_options]
state_dir = "/private/app-state"
sqlite_path = "broker.db"       # relative to state_dir
hint_path = "broker.hints"      # advisory notifier, never payload storage
sqlite_synchronous = "NORMAL"   # set "FULL" for power-loss durability
max_payload_bytes = 16777216    # default 16 MiB; maximum 1 GiB
max_store_bytes = 1073741824    # default 1 GiB; maximum 1 TiB
```

`NORMAL` preserves committed work across application, worker, supervisor, and
process restarts on the same disk. Only `FULL` promises the last commits across
OS failure or power loss. Delivery is at-least-once: a crash after a listener
returns but before its ack commits can cause a duplicate, so listeners must be
idempotent. Every publication, including one every group has already acked, is
retained for `orphan_retention_seconds` (default 3600, one hour) and replayed
once to every group that subscribes before expiry, as far as the store has room
below its publish budget (see below).

Payloads over `max_payload_bytes` are rejected before a transaction starts.
`max_store_bytes` bounds what publishes may add to the database file
(`broker.db`), not the size of the file itself. A full store rejects new
publishes until retained publications expire, consumers drain their backlog,
`modulith broker drop-group` removes a retired group, or every process restarts
with a raised limit. Publishes are refused a small reserve early, once
`page_count - freelist_count` would pass the configured page count minus 32
pages (one eighth of the pages below 256 pages). Consumers can always claim,
ack, fail, dead-letter and prune the backlog they see, and a group can always
subscribe: a consumer write or subscription record the limit refuses is
retried past it, so the database file can grow past `max_store_bytes` by the
growth of rows it already holds (claims, error text, mark-mode completions,
prune tombstones) while publishes stay refused. A subscribe replay itself never
grows the store past its publish budget: it replays the retained publications
the group lacks, oldest first, and stops 8 pages below the budget, so a small
publish that fit before the replay still fits right after it. Under
`completion_mode="mark"` it uses at most half the room left, which covers
claiming and acking its own rows when one group drains them and its listeners
succeed. Draining can still take the store past the budget, as any consumer
write can: failed and dead-lettered rows keep their error text, and under
`completion_mode="mark"` claiming and acking grows every row, including rows
other groups replayed. Publishes are then refused until prune frees pages or
`max_store_bytes` is raised. A replay cut short logs one WARNING with the group, the target and the replayed and skipped
counts. The skipped publications reach that group only through another replay.
To replay them without losing work, first let the group's workers drain its
backlog on that target: `modulith broker drop-group <group> --target <target>`
deletes every pending and claimed delivery the group holds there, replayed ones
included, and a replay restores only publications still retained. Then stop
the group's workers, run that drop-group, raise `max_store_bytes` and restart
every process before the skipped publications expire. This
also drains a store that filled before this release or whose `max_store_bytes`
was lowered below its size; an existing larger file keeps its size. The
`broker.db-wal` file is not counted: it grows to about 4 MiB (SQLite's
1000-page autocheckpoint) before checkpoints reuse it, further while a long
read blocks a checkpoint, and it keeps its largest size, so budget disk for
it on top of `max_store_bytes`. The empty schema takes 13 pages, so a store
under 56 KiB refuses every publish, and a 64 KiB store holds about 50 small
publications. A publication stays while any group has not
consumed it, for `orphan_retention_seconds` after it is written, and, under
`completion_mode = "mark"` or once dead-lettered, until `retention_age_seconds`
(default 3 days) after completion. The store sustains about
`max_store_bytes / (bytes per publication × the longest of those retentions)`
publications per second: roughly 165/s for 1 KiB payloads and two groups with
the defaults in delete mode, and under 2.3/s in mark mode. Raise
`max_store_bytes` or shorten `orphan_retention_seconds` (at most 100 years)
before the store fills; publications already stored keep the retention they
were written with, and late subscribers replay only within the shorter window.
Override these
with `MODULITH_BROKER_MAX_PAYLOAD_BYTES`, `MODULITH_BROKER_MAX_STORE_BYTES`, and
`MODULITH_BROKER_ORPHAN_RETENTION_SECONDS`.
`shm_slot_size` is deprecated and ignored because hint slots are fixed-size.

Explicit `broker = "shm"` rejects DSNs and SQLAlchemy/network URLs. If the
broker name is omitted but `broker_options.url`/`dsn` (or the equivalent
environment variable) exists, modulith infers the `database` adapter instead.

### Use a shared database broker

For cross-host delivery, the same process-per-module topology can use a
relational database. Bare `@externalized` events need only configuration
changes; intentionally scheme-pinned targets must be updated.

```bash
pip install 'modupy[database]'
```

An explicit database broker may still use an embedded SQLite file for a small
single-host deployment:

```toml
# pyproject.toml
[tool.modulith]
topology = "processes"
broker = "database"

[tool.modulith.broker_options]
url = "sqlite+aiosqlite:///./modulith-broker.db"
```

For production, point the same `url` at Postgres or MySQL — the dialect is
inferred from the URL, and Postgres/MySQL get real `FOR UPDATE SKIP LOCKED`
competing-consumer claims (SQLite is best-effort multi-process, not
high-throughput):

```toml
[tool.modulith.broker_options]
url = "postgresql+asyncpg://user:pass@db/app"   # or mysql+aiomysql://...
pool_size = 10                                   # server connection pool
poll_interval_ms = 250                           # consumer poll cadence
reclaim_stale_seconds = 60                        # reclaim a crashed consumer's claim after 60s
max_delivery_attempts = 5                         # dead-letter after 5 failed dispatches
retention_age_seconds = 604800                   # prune terminal rows after 7 days
```

Timing (claim visibility, the reclaim window, retry backoff, prune age) is
gated on the **database server clock**, so producers and competing consumers on
different hosts stay consistent without a synchronized wall clock.

With `broker = "database"`, a bare `@externalized` event's default target is
`database:{event-fqn}`; pin one explicitly with
`@externalized(target="database:orders.placed")` exactly as with Redis. Every
`broker_options` key is env-overridable via `MODULITH_BROKER_<KEY>` (e.g.
`MODULITH_BROKER_URL`, `MODULITH_BROKER_POLL_INTERVAL_MS`), and the env var
wins over the pyproject value. That is also how a worker process gets its
connection URL: `modulith run --topology processes` resolves
`broker_options` once in the supervisor and forwards each key into every
worker's environment under that name, because a worker re-bootstraps from
scratch and cannot re-derive the table itself.

### No-subscriber and orphan-replay policies

When a message is published to a database-broker target that has no consumer
group yet, `no_subscriber_policy` decides what happens:

| Policy | Behavior |
|--------|----------|
| `error` (default) | Raise `NoSubscribersError` immediately; readiness stays degraded until subscriptions exist |
| `wait` | Poll for subscribers until `no_subscriber_wait_timeout_seconds` |
| `store` | Persist a retained source message and replay per `orphan_replay_policy` |

`orphan_replay_policy` (store mode only): `ttl_all_groups` (default — every
group that registers before expiry gets a copy), `first_groups` (fan out to
the first registration set then delete), or `expected_groups` (pre-create
delivery rows for configured groups). A group listed in
`expected_consumer_groups` keeps receiving rows even after `modulith broker
drop-group` removes its subscription; remove it from that setting when you
retire its module.

### Declaring broker destinations

`subscription_source` (default `manifest`) controls where dynamic broker
targets are declared: `declare_module(broker_targets=...)`,
`[tool.modulith.subscriptions]`, or `@listener(broker_targets=...)`. Static
`@externalized(target=...)` destinations are always inferred.

The broker creates its `broker_message` / `broker_subscription` tables
automatically on first use; to manage the schema explicitly instead, they ship
in the packaged alembic migration — see
[MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md), Step 5. Delivery is at-least-once
with the same crash-recovery and dead-lettering as the Redis broker; see
[ARCHITECTURE.md](ARCHITECTURE.md) §8.4 for the design.

---

## 9. Test an event flow with the pytest plugin

**Goal:** assert that publishing one event causes the expected downstream event,
without `sleep`s or real infrastructure.

The `modulith` pytest plugin ships fixtures that reset the runtime per test
and capture what was published. It registers through the `pytest11` entry
point and loads in any pytest run where `modupy` is installed — the
`modupy[test]` extra only adds the libraries the fixtures need, it does not
gate registration. Disable it in an unrelated suite with `pytest -p
no:modulith`.

Capture and assert directly with the `modulith_app` fixture:

```python
from myapp.contracts.events import OrderPlaced, StockReserved
from myapp.orders import place_order

async def test_order_reserves_stock(modulith_app):
    await place_order(customer_id="c-1", total=19.99)

    reserved = modulith_app.published_events_of_type(StockReserved)
    assert len(reserved) == 1
```

Or use the fluent `scenario` fixture for trigger-then-expect flows:

```python
from myapp.contracts.events import OrderPlaced, StockReserved

def test_order_flow(scenario):
    (
        scenario
        .publish(OrderPlaced(order_id="o-1", customer_id="c-1", total=9.99))
        .expect_event(StockReserved)
        .matching(lambda e: e.order_id == "o-1")
        .within(seconds=2)
    )
```

`.within()` is the terminal step: it fires the trigger, then polls the captured
events for a match, raising `AssertionError` on a miss (never a bare
`TimeoutError`). For tests that need full process isolation (import-time state,
module reloading), mark them `@pytest.mark.modulith_isolated` to run in a
subprocess.

---

## 10. Enforce boundaries in CI

**Goal:** stop new cross-module boundary violations from merging, without having
to fix every existing one first.

Run the AST boundary verifier in CI. Bare `verify` fails on ERROR-severity
violations only; opt in to WARNINGs when you want the stricter gate:

```bash
modulith verify                       # exit 1 on ERROR-severity violations
modulith verify --fail-on-warnings    # also fail on WARNING-severity findings
```

Adopting on a messy existing codebase? Record a ratcheting baseline that
grandfathers today's violations, then fail only on **new** ones. `--mode=ratchet`
does not create the baseline: `--update-baseline` writes it, to
`.modulith-baseline.json` unless you pass `--baseline`, and you commit that file.
Until it exists, ratchet mode grandfathers nothing and behaves like a strict run:

```bash
modulith verify --update-baseline     # record today's violations; exits 0
git add .modulith-baseline.json && git commit -m "Record the boundary baseline"
modulith verify --mode=ratchet        # fail only on violations not in the baseline
```

The baseline is count-aware: it records existing violations by a stable hash, so
you can enforce "no new violations" while paying down the old ones over time.
Rerun `--update-baseline` after paying some down, to lower what is
grandfathered. Add
the check to CI (exit code `0` = clean, `1` = violations or a bad flag *value*,
`2` = an internal error or a CLI usage error such as an unknown option — click's
convention). `--fail-on-warnings` makes the gate cover every new violation, not
just the ERROR-severity ones:

```yaml
# .github/workflows/ci.yml
- run: pip install 'modupy[cli]'
- run: modulith verify --mode=ratchet --fail-on-warnings
```

`modulith doctor` complements this with operational + architectural health
checks (outbox health, boundary health, split-readiness) for a running app.
Table-only cross-module coupling is a split-readiness warning even without
imports or event interactions; `actuator_mode="token"` without
`MODULITH_ACTUATOR_TOKEN` is an error because process topology will not start.

---

## 11. Extend modulith with a plugin

**Goal:** add your own verification rule, broker, or documentation output.

modulith's own behavior is built from plugins, and yours load exactly the same
way — via the `modulith` entry-point group. No application code changes; install
the package and the plugin's hooks run.

**A custom verification rule** (aggregate hook — your rule's violations combine
with the built-in ones):

```python
# modulith_naming_rules/plugin.py
from modulith import ModuleInfo, Violation, ViolationSeverity, hookimpl

@hookimpl
def modulith_verify_module(
    module: ModuleInfo,
    all_modules: list[ModuleInfo],
) -> list[Violation]:
    """Flag domain events not named in past tense (OrderCreated, not CreateOrder)."""
    violations: list[Violation] = []
    # ... inspect module.package, append Violation(...) per offender ...
    return violations
```

```toml
# the plugin package's pyproject.toml
[project.entry-points."modulith"]
naming_rules = "modulith_naming_rules.plugin"
```

**A custom broker** (implement the `Broker` protocol by duck typing and register
it against a URI scheme):

```python
from modulith import Broker, hookimpl

class MyBroker:                       # no need to subclass Broker
    async def publish(self, target: str, payload: bytes,
                      headers: dict[str, str] | None = None) -> None:
        ...
    async def close(self) -> None:
        ...

@hookimpl
def modulith_register_brokers(registry) -> None:
    registry.register("my-scheme", MyBroker())
```

Events targeting `my-scheme:destination` (via `@externalized`) now route to your
broker. Full worked examples ship in
[`examples/naming_convention_verifier.py`](../examples/naming_convention_verifier.py)
and [`examples/redis_streams_broker.py`](../examples/redis_streams_broker.py).
The complete extension contract — all 13 hookspecs and 5 protocols — is in
[ARCHITECTURE.md §5](ARCHITECTURE.md#5-the-plugin-contract) and
[SPEC.md Part IV](../SPEC.md).
