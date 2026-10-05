# modupy Cookbook

Task-oriented recipes for common jobs, each using only API documented in
[API_REFERENCE.md](API_REFERENCE.md) and [STABILITY.md](STABILITY.md). Start
with recipes 1–3 and 9.

The runnable end-to-end version of recipes 1–3, 5, 6 and 8 lives in
[`examples/demo_app`](../examples/demo_app). Recipe 4 (`publish_sync`) is not
in it: the demo publishes from async code only.

modupy installs the `modulith` package, so you `import modulith` and run
`modulith`.

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
11. [Extend modupy with a plugin](#11-extend-modupy-with-a-plugin)

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
async def reserve_stock(evt: OrderPlaced) -> None:
    await stock_service.reserve(evt.order_id)
```

The event type is inferred from the annotation, so `@listener` needs no
arguments. Listeners may be `async def` (preferred) or plain `def` (recipe 4).
Multiple listeners can subscribe to the same event; they run concurrently and
one failing does not block the others. Once they have all finished, `publish()`
re-raises the first failure, in registration order, to the publisher. That
holds for the in-memory path; under the durable outbox (recipe 6) listeners run
after commit, so a failure is retried and never reaches the publisher.

On the in-memory path, one failing listener plays out like this:

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as publish()
    participant LA as Listener A
    participant LB as Listener B
    P->>B: await publish(OrderPlaced)
    Note over B: Finds the listeners by exact event type
    par concurrently
        B->>LA: OrderPlaced
    and
        B->>LB: OrderPlaced
    end
    LA--xB: raises
    LB-->>B: returns
    Note over B: Waits until every listener is done
    B--xP: re-raises the first failure in registration order
```

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
commit. **Your events, listeners and `publish()` calls do not change between
those modes.** Configuration does, and the outbox adds a few lines of session
wiring around the transaction (recipe 6).

Under the memory outbox (`outbox = "memory"`, the default) `publish()` runs the
listeners inline. It returns only after they have finished, while your own
transaction is still open. On a SQLite file that matters once you have flushed
a write: the flush takes the file's write lock, and your transaction keeps it
until it ends. A listener that writes in its own transaction then waits on that
lock until SQLite's busy timeout runs out (5 s with the default
`sqlite+aiosqlite` driver), fails with `database is locked`, and `publish()`
re-raises that failure to you.

So publish before you flush. `place_order` in the
[demo app](../examples/demo_app/shop/orders/__init__.py) publishes `OrderPlaced`
first, then adds the order row, and commits once.

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
def send_receipt(evt: OrderPlaced) -> None:   # sync listener, runs in a thread
    mailer.send(evt.customer_id, evt.order_id)
```

`publish_sync()` hands the dispatch to a loop on its own thread and waits for it:

```mermaid
sequenceDiagram
    participant C as Caller thread
    participant L as Daemon-thread loop
    participant E as Executor threads
    C->>L: submit the publish, then wait
    par async listeners
        L->>L: run on the loop
    and def listeners
        L->>E: run in the executor
    end
    alt done within the timeout
        L-->>C: return, or raise the first failure
    else timeout, 30 s by default
        L--xC: PublishSyncTimeout, dispatch cancelled
    end
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

The first `publish_sync()` also bootstraps modupy, lazily, as the first
`await publish()` does. It runs the bootstrap on the daemon-thread loop's thread
and inside that call's `timeout`, so a startup slower than the timeout raises
`PublishSyncTimeout`, and a failing one raises its error from that first
publish. With the durable outbox (recipe 6) bound from `outbox_url`, the same
bootstrap starts the retry loop and its crash-recovery sweep on that loop, so
rows a crashed run left undelivered wait for that first call.

Call `modulith.bootstrap()` before the first `publish_sync()` to run startup,
and surface its errors, at a point you choose and outside the timeout. It does
not start the retry loop: sync code has no running loop to start it on, so the
loop starts at the first publish made inside a bound session.

---

## 5. Declare a manifest and let bootstrap verify it

**Goal:** turn silent drift (a module that failed to import, a renamed event)
into a loud startup failure.

Add a `_manifest.py` at module scope declaring the module's contract. modupy
reads it at startup and checks it against reality.

```python
# myapp/orders/_manifest.py
from modulith import declare_module

declare_module(
    publishes=["OrderPlaced"],
    declared_dependencies=["contracts"],
)
```

At bootstrap (when `verify_manifests` is on, the default) modupy verifies two
cheap things: every declared `listeners` entry actually registered against the
bus, and every `publishes` name is defined in the package namespace, so a
module's `__init__.py` must import each event it publishes. A mismatch aborts
boot, naming the file (and the line, for a listener), instead of silently
dropping events. Declaring `listeners` is the highest-value check — it catches a
module that failed to import:

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

`StockReserved` lives in `contracts`, so `inventory/__init__.py` re-exports it
for the `publishes` check to find:

```python
# myapp/inventory/__init__.py
from . import handlers
from myapp.contracts.events import StockReserved as StockReserved
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

![One commit saves the order and one event_publications row per listener; after the commit each listener runs in the background, and a failing one is retried, then dead-lettered](images/outbox.svg)

**Goal:** stop losing events on crash — deliver at-least-once, atomically with
the business transaction.

Install the extras (`postgres` for the store and the migrations, `cli` for the
`modulith` command) and point modupy at the database that holds your business
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

Alembic's own command works as the alternative. modupy ships its alembic
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
data: a rollback discards the event (no ghosts), and a commit stores it, to be
retried until it is delivered or set aside as a dead letter. Because delivery is
at-least-once, **listeners must be idempotent**.

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
publication is dead-lettered after 10 attempts. `modulith outbox failing` lists
the ones still being retried, with their attempts, last error and next retry
time (it needs a store with `find_failing`, which the built-in SQL store has).

`bind_session` and `unbind_session` live in `modulith.builtin.outbox`. The
older `modulith.adapters.postgres_outbox` import path still works, as aliases
of the same functions.

Bootstrap runs lazily, at the first `publish()`, and every sweep skips its rows
until the runtime is bootstrapped. Call `modulith.bootstrap()` and then
`outbox.start()` in your ASGI lifespan's startup half, so rows a crashed
process left undelivered are swept at startup instead of waiting for the first
transactional publish. In a single-process app, call `outbox.shutdown()` in the
lifespan's teardown: it stops the retry loop and closes the engine modulith
created from `outbox_url`, but never a store or engine you configured
yourself. Run one lifespan per process: `outbox.shutdown()` is final, so a
second lifespan in the same process reuses the store it disposed, whose
after-commit hook is gone and whose rows then wait for the retry loop's sweep.
Restart the process instead; tests reset the runtime with
`_reset_for_testing`, which the `modulith_app` fixture calls for every test.
The outbox table must live in the
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
| `"advisory_lock"` | hold a Postgres advisory lock per row for the dispatch. Rejected at `configure()` on a non-Postgres store. Lock connections come from a second pool sized like the engine's. With a `QueuePool` (the async engine default), a process can hold up to 2×(`pool_size` + `max_overflow`) Postgres connections during a burst and keeps up to `pool_size` idle lock connections afterwards; budget `max_connections` for that. `NullPool`, `max_overflow=-1` and `pool_size=0` are unbounded: one lock connection per in-flight row. An after-commit dispatch that waits past `pool_timeout` for a lock connection logs a WARNING and leaves its row to the sweep, which delivers one row at a time, so a burst larger than the lock pool can serve within `pool_timeout` drains slowly |
| `"none"` | no coordination — two sweepers may dispatch the same row. Warns at `configure()` |

```python
outbox.configure(
    store=store,
    serializer=serializer,
    claim_strategy="lease",     # default
    claim_lease_seconds=60.0,   # must exceed the longest loop stall
    claim_batch_size=100,       # rows claimed per sweep
)
```

Two sweepers under the default `"lease"` strategy:

```mermaid
sequenceDiagram
    participant A as Sweeper A
    participant T as Outbox table
    participant B as Sweeper B
    A->>T: claim a batch: lease and token
    B->>T: claim a batch
    T-->>B: skips rows under A's live lease
    loop while a listener runs
        A->>T: renew every third of the lease
    end
    alt A finishes in time
        A->>T: complete, with its token
        T-->>A: row done
    else A stalls and the lease expires
        B->>T: claim a batch
        T-->>B: the row, with a new token
        A->>T: complete, with the old token
        T-->>A: refused as stale
    end
```

The lease renews every third of its length while a listener runs, so a slow
listener keeps its row. A lease expires mid-dispatch only when renewal cannot
run: a listener that blocks the event loop for longer than the lease, or
renewals that keep failing for longer than it. A peer can then legitimately
reclaim the row — a duplicate delivery, not a bug. Raise
`claim_lease_seconds` rather than lowering it to chase latency, but not above
86400 seconds (one day): `outbox.configure()` and `outbox_options` reject more.
The lease is also the crash-recovery bound: rows a crashed process was delivering are
recovered once their lease expires, normally within `claim_lease_seconds`
plus `retry_interval_seconds` of the crash. Recovery takes longer when
`retry_stale_seconds` exceeds the lease, while a slow sweep is still running,
or while the runtime is not bootstrapped. A crash in the middle of a sweep
leaves the row being delivered, and every row of its claimed batch not yet
reached, leased until the lease expires. Rows it had already delivered,
failed or released are not leased. A graceful stop (`outbox.shutdown()`)
lets the delivery in flight finish and releases the rest of the claimed
batch at once, uncharged. Only a stop that outlasts the 10 s shutdown
grace period cancels the sweep: the row being delivered is then released
at once, and only the rows not yet reached stay leased until the lease
expires.

Under `"lease"` a fork-started child can also keep a crashed process's row
locks. A claim or completion transaction holds row locks until it commits. If
the process dies inside one while a forked `multiprocessing` or
`ProcessPoolExecutor` child still holds a copy of that connection's socket,
the session stays idle in its open transaction, and every sweep skips those
rows until the child exits or Postgres ends the session. Set
`idle_in_transaction_session_timeout` on the outbox's role so that Postgres
ends it (`ALTER ROLE app SET idle_in_transaction_session_timeout = '60s'`).
Pick a value above the longest time any transaction on that role legitimately
sits idle between two statements, a listener's included, because the setting
ends those sessions too. Starting such children with the `spawn` or
`forkserver` method avoids the case.

Under `"advisory_lock"` a crashed process's rows are recovered at once when
the process dies on a live host, unless a descendant forked from it is still
running. A fork-started `multiprocessing` or `ProcessPoolExecutor` child keeps
a copy of the lock connection's socket, so the locks stay held for as long as
that child lives. An orphaned `ProcessPoolExecutor` worker does not exit on
its own, so it must be killed to free the locks. Start such children with the
`spawn` or `forkserver` method. After a host loss or a network partition
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

Put per-connection settings in `connect_args` as above, or register a pool
listener (`event.listen(engine.sync_engine.pool, "connect", ...)`) before the
first advisory delivery. Lock connections come from a second pool that copies
the engine pool's listeners when that delivery builds it, so a pool listener
registered later never runs for them, and a listener registered on the engine
for statement events such as `before_cursor_execute` never does.

An engine built from `outbox_url` takes no `connect_args`. With
`postgresql+psycopg`, put the keepalives in the URL's `options` query
parameter:
`?options=-c%20tcp_keepalives_idle%3D60%20-c%20tcp_keepalives_interval%3D10%20-c%20tcp_keepalives_count%3D3`.
With asyncpg, set them in `postgresql.conf` or with `ALTER ROLE ... SET`. Set
`idle_session_timeout = 0` for the outbox's role
(`ALTER ROLE app SET idle_session_timeout = 0`), because a lock
connection sits idle while its listener runs and ending that session frees
the row for a second delivery. A role that leaves it unset inherits the
database's or the server's value, so run `SHOW idle_session_timeout` in a
session that logs in as the role the lock sessions use. Behind PgBouncer,
advisory locks need session pooling; transaction pooling breaks them. Do not
rely on the per-connection `options` or `server_settings` there: PgBouncer
raises an error for a startup parameter it does not track, or ignores it when
`ignore_startup_parameters` lists it. DEPLOYMENT.md's
[Postgres and PgBouncer settings for `advisory_lock`](DEPLOYMENT.md#postgres-and-pgbouncer-settings-for-advisory_lock)
covers PgBouncer's `client_idle_timeout` and `server_reset_query`.

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
no Alembic history while `public` contains modupy tables or history, the
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
| `claim_lease_seconds` | positive finite number, at most 86400 (one day) |
| `claim_batch_size` | positive integer |
| `dead_letter_after_attempts` | positive integer |
| `retry_interval_seconds` | positive finite number |
| `retry_stale_seconds` | positive finite number |
| `max_retry_backoff_seconds` | positive finite number |
| `completion_mode` | `"update"`, `"delete"` or `"archive"` |

`sqlite_wal` is the one key that is not a `configure()` setting: it applies to
the engine the runtime builds from a SQLite `outbox_url`. `true` runs
`PRAGMA journal_mode=WAL` on every connection; it is off by default, and modupy
never changes a database file's journal mode unless asked. WAL persists in the
database file, adds `-wal` and `-shm` files and cannot be used on network
filesystems. It applies to SQLite only and is ignored for other databases, so
one pyproject can serve a SQLite development setup and a Postgres deployment.
An engine you build yourself for `outbox.configure()` gets the same effect with
a `connect` listener, or by setting WAL once on the file:

```python
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///app.db")


@event.listens_for(engine.sync_engine, "connect")
def enable_wal(dbapi_connection, _record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()
```

A key outside these eight and `sqlite_wal` is accepted and ignored. A store you
bind yourself with `outbox.configure()` takes the eight settings as keyword
arguments instead.

---

## 8. Go process-per-module and externalize an event

![modulith run starts a main process holding the proxy on port 8000 and the supervisor, plus one worker process per module, connected by the built-in SHM broker](images/processes.svg)

**Goal:** run one module in its own process (its own CPU/memory budget) while
keeping the same module code.

Switch topology to process-per-module. With no broker or URL configured,
modupy selects the stdlib-only, durable local `shm` broker:

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

@externalized(target="shm:stock.reserved")            # explicitly pins local SHM
@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
```

In a worker, each `publish()` takes one of these routes
([ARCHITECTURE.md](ARCHITECTURE.md) §8.1 has the full decision):

```mermaid
flowchart LR
    P["publish(event)"] --> L{"Listener in the<br>publishing process?"}
    L -->|"yes"| R["Run the local<br>listeners"]
    L -->|"no"| B["Send to the broker"]
    R --> X{"Marked @externalized<br>or plugin-routed?"}
    X -->|"yes"| B
    X -->|"no"| D["Stays in<br>this process"]
    B --> W["Listeners in<br>other workers"]
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

One publication, from commit to ack:

```mermaid
sequenceDiagram
    participant P as Publisher worker
    participant S as SQLite store
    participant H as mmap ring
    participant C as Consumer worker
    P->>S: commit the publication
    P->>H: write a sequence hint
    C->>H: idle, so read hints
    H-->>C: a newer sequence
    C->>S: claim a batch
    C->>C: run the listeners
    C->>S: ack the delivery
    Note over H,C: Lost hints cost latency, never a message
```

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
max_payload_bytes = 16777216    # default 16 MiB; maximum 1,000,000,000 bytes
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
with a raised limit. Override `max_payload_bytes`, `max_store_bytes` and
`orphan_retention_seconds` with `MODULITH_BROKER_MAX_PAYLOAD_BYTES`,
`MODULITH_BROKER_MAX_STORE_BYTES` and
`MODULITH_BROKER_ORPHAN_RETENTION_SECONDS`.
DEPLOYMENT.md's [Sizing the Default SHM Store](DEPLOYMENT.md#sizing-the-default-shm-store)
covers the page budget, the replay runbook and how fast a store fills.

Explicit `broker = "shm"` rejects DSNs and SQLAlchemy/network URLs. If the
broker name is omitted but `broker_options.url`/`dsn` (or the equivalent
environment variable) exists, modupy infers the `database` adapter instead.

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

A delivery that fails `max_delivery_attempts` times, or that a consumer
crashes on that often, stays in `broker_message` as a dead letter until prune
removes it. `modulith broker dead-letter` lists them with their last error;
`modulith broker dead-letter --retry-all` makes every one claimable again with
its attempts reset. Each dead letter belongs to one consumer group, so only
that group's consumer receives it again and a group that already handled the
message does not. The database, shm and redis-streams brokers support this
command. On redis-streams a dead letter has no recorded error and `--retry-all` resubmits
a target only when its stream has exactly one consumer group, because a
re-added message reaches every group on the stream.

With `broker = "database"`, a bare `@externalized` event's default target is
`database:{event-fqn}`; pin one explicitly with
`@externalized(target="database:orders.placed")`. Every
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
| `error` (default) | Raise `NoSubscribersError` immediately; no row is written |
| `wait` | Poll for subscribers until `no_subscriber_wait_timeout_seconds` |
| `store` | Persist a retained source message and replay per `orphan_replay_policy` |

`NoSubscribersError` lives in `modulith.adapters.db_broker`, an adapter module
that [STABILITY.md](STABILITY.md#experimental-surface-broker-adapter-internals)
lists as experimental, so the import path may move.

`orphan_replay_policy` (store mode only): `ttl_all_groups` (default — every
group that registers before expiry gets a copy), `first_groups` (fan out to
the first registration set then delete), or `expected_groups` (pre-create
delivery rows for configured groups). A group listed in
`expected_consumer_groups` keeps receiving rows even after `modulith broker
drop-group` removes its subscription; remove it from that setting when you
retire its module.

The replay policies differ in when each group gets its delivery row:

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Database broker
    participant G as Consumer group
    Note over P,G: store mode, no group has subscribed yet
    P->>B: publish
    alt ttl_all_groups
        B->>B: retain the message until it expires
        G->>B: any group subscribes in time
        B-->>G: replay a copy to that group
    else first_groups
        B->>B: retain the message
        G->>B: the first groups subscribe
        B-->>G: replay to them, then delete it
    else expected_groups
        B->>B: queue a row per configured group
        Note over B: nothing is retained
        G->>B: a group subscribes later
        Note over B,G: nothing is replayed
    end
```

### Declaring broker destinations

`subscription_source` (default `manifest`) controls where dynamic broker
targets are declared: `declare_module(broker_targets=...)`,
`[tool.modulith.subscriptions]`, or `@listener(broker_targets=...)`. Static
`@externalized(target=...)` destinations are always inferred.

The broker creates its `broker_message` / `broker_subscription` tables
automatically on first use; to manage the schema explicitly instead, they ship
in the packaged alembic migration — see
[MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md), Step 5. Delivery is at-least-once,
with the crash recovery and dead-lettering described above; see
[ARCHITECTURE.md](ARCHITECTURE.md) §8.4 for the design.

---

## 9. Test an event flow with the pytest plugin

**Goal:** assert that publishing one event causes the expected downstream event,
without `sleep`s or real infrastructure.

The `modulith` pytest plugin ships fixtures that reset the runtime per test
and capture what was published. It registers through the `pytest11` entry
point and loads in any pytest run where `modupy` is installed — the
`modupy[test]` extra adds pytest and pytest-asyncio, and does not gate
registration. Disable it in an unrelated suite with `pytest -p
no:modulith`.

`pip install 'modupy[test]'` brings `pytest` and `pytest-asyncio`. Two settings
finish the setup: `pythonpath = ["."]` lets your tests import your package, and
`asyncio_mode = "auto"` lets pytest-asyncio run the `async def` test below
without a marker:

```toml
# pyproject.toml
[tool.pytest.ini_options]
pythonpath = ["."]
asyncio_mode = "auto"
```

Capture and assert directly with the `modulith_app` fixture:

```python
async def test_order_reserves_stock(modulith_app):
    from myapp.contracts.events import StockReserved
    from myapp.orders import place_order

    await place_order(customer_id="c-1", total=19.99)

    reserved = modulith_app.published_events_of_type(StockReserved)
    assert len(reserved) == 1
```

Or use the fluent `scenario` fixture for trigger-then-expect flows:

```python
def test_order_flow(scenario):
    from myapp.contracts.events import OrderPlaced, StockReserved

    (
        scenario
        .publish(OrderPlaced(order_id="o-1", customer_id="c-1", total=9.99))
        .expect_event(StockReserved)
        .matching(lambda e: e.order_id == "o-1")
        .within(seconds=2)
    )
```

The imports sit inside the tests because each test gets a fresh copy of your
modules. A module-scope import of a module with `@listener` functions registers
them before the fixture resets the runtime, so the test runs without them; if
its manifest declares them, boot fails with "not registered against the event
bus — module may have failed to import", although the import succeeded.

`.within()` is the terminal step: it fires the trigger, then polls the captured
events for a match, raising `AssertionError` on a miss (never a bare
`TimeoutError`). For tests that need full process isolation (import-time state,
module reloading), mark them `@pytest.mark.modulith_isolated` to run in a
subprocess.

---

## 10. Enforce boundaries in CI

![payments may import the public API of orders and the events in contracts, but modulith verify refuses an import of a private name such as _orders](images/boundaries.svg)

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
# .github/workflows/ci.yml, in the job that installs your app's dependencies
- run: pip install 'modupy[cli]'
- run: modulith verify --mode=ratchet --fail-on-warnings
```

`verify` imports every module package, so a job that lacks your app's own
dependencies exits 1 with a module import failure.

`modulith doctor` complements this with operational + architectural health
checks (outbox health, boundary health, split-readiness) for a running app.
Table-only cross-module coupling is a split-readiness warning even without
imports or event interactions; `actuator_mode="token"` without
`MODULITH_ACTUATOR_TOKEN` is an error because process topology will not start.

---

## 11. Extend modupy with a plugin

**Goal:** add your own verification rule, broker, or documentation output.

modupy's own behavior is built from plugins, and yours implement the same
hooks. The built-in plugins ship in the package; a third-party plugin loads
through the `modulith` entry-point group. No application code changes; install
the package and the plugin's hooks run.

**A custom verification rule** (aggregate hook — your rule's violations combine
with the built-in ones). The skeleton below leaves the body out; the complete
rule is
[`examples/naming_convention_verifier.py`](../examples/naming_convention_verifier.py):

```python
# modulith_naming_rules/plugin.py
from modulith import ModuleInfo, Violation, hookimpl

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

With `topology = "processes"`, events targeting `my-scheme:destination` (via
`@externalized`) now route to your broker. In the default single-process
topology `@externalized` is an inert marker and nothing is sent. Registering a
`Broker` covers only the sending side: for the workers to receive these events,
set `broker = "my-scheme"` and register a consumer factory for the same scheme
through `modulith_register_consumers`; it turns a `ConsumerSpec` into a
`Consumer`. A worked example of the sending side ships in
[`examples/redis_streams_broker.py`](../examples/redis_streams_broker.py).
The complete extension contract — all 13 hookspecs and 5 protocols — is in
[ARCHITECTURE.md §5](ARCHITECTURE.md#5-the-plugin-contract) and
[SPEC.md Part IV](../SPEC.md).
