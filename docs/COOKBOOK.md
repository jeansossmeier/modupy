# modulith Cookbook

Task-oriented recipes for common jobs. Each one uses only the documented public
API (see [API_REFERENCE.md](API_REFERENCE.md)); for the design behind them see
[ARCHITECTURE.md](ARCHITECTURE.md) and [SPEC.md](../SPEC.md). The runnable
end-to-end version of recipes 1–5 lives in
[`examples/demo_app`](../examples/demo_app).

**Contents**

1. [Define an event and a listener](#1-define-an-event-and-a-listener)
2. [Share event types through a contracts module](#2-share-event-types-through-a-contracts-module)
3. [Publish from async code](#3-publish-from-async-code)
4. [Publish from sync code (views, scripts)](#4-publish-from-sync-code-views-scripts)
5. [Declare a manifest and let bootstrap verify it](#5-declare-a-manifest-and-let-bootstrap-verify-it)
6. [Enable the durable Postgres outbox](#6-enable-the-durable-postgres-outbox)
7. [Choose an outbox completion mode](#7-choose-an-outbox-completion-mode)
8. [Go process-per-module and externalize an event](#8-go-process-per-module-and-externalize-an-event)
    - [Without Redis: a database as the broker](#without-redis-a-database-as-the-broker)
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
one failing does not block the others.

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
├── contracts/
│   └── events.py          # OrderPlaced, StockReserved  (shared vocabulary)
├── orders/                # publishes OrderPlaced
├── inventory/             # listens for OrderPlaced, publishes StockReserved
└── notifications/         # listens for StockReserved
```

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

Two sharp edges (both from `wrap_sync_listener`'s contract):

- Only **synchronous** SQLAlchemy sessions work inside a sync listener's
  executor thread. An `AsyncSession` needs SQLAlchemy's greenlet bridge — use an
  `async` listener for `AsyncSession` work.
- Multiple sync listeners for one event run **concurrently on separate
  threads**, each with a copy of the caller's context. Share only thread-safe
  resources; give each listener its own session/lock.

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
    owns_tables=["stock_levels"],
    declared_dependencies=["contracts"],
)
```

`consumes`, `owns_tables`, and `declared_dependencies` are consumed by the docs
generator, the audit tool, and the AST verifier (recipe 10) — not cross-checked
at bootstrap. Note the distinction on `declared_dependencies`: **omitting it**
(the default `None`) leaves the verifier's dependency rule off; an **explicit
empty list** means "depends on nothing" (deny-all, contracts excepted).

---

## 6. Enable the durable Postgres outbox

**Goal:** stop losing events on crash — deliver at-least-once, atomically with
the business transaction.

Install the extra and select the adapter:

```bash
pip install 'modulith[postgres]'
```

```toml
# pyproject.toml
[tool.modulith]
outbox = "postgres"
```

Run the packaged Alembic migration (modulith ships its alembic config inside the
installed package; alembic runs on a **sync** driver even if your app uses
asyncpg):

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  upgrade head
```

Wire your SQLAlchemy session to modulith at startup, and bind the session per
request so `publish()` calls inside it are captured by the outbox:

```python
# myapp/main.py
from modulith.adapters.postgres_outbox import PostgresPublicationStore, bind_session
from modulith.builtin import outbox
from modulith.builtin.outbox import _current_session
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

async def get_db():
    async with async_session_maker() as session:
        token = bind_session(session)
        try:
            yield session
        finally:
            _current_session.reset(token)
```

Now a `publish()` inside a bound transaction is persisted atomically with your
data: a rollback discards the event (no ghosts), a commit guarantees delivery
(no losses), and listeners are retried at-least-once after commit. Because
delivery is at-least-once, **listeners must be idempotent**. Inspect the queue
with `modulith outbox status`; a persistently-failing publication is
dead-lettered after 10 attempts.

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
`modulith outbox purge`. The `[tool.modulith.outbox_options]` subtable is
reserved for future use — the runtime does not read its keys yet, so set the
mode here in the wiring code, not in pyproject.

---

## 8. Go process-per-module and externalize an event

**Goal:** run one module in its own process (its own CPU/memory budget) while
keeping the same module code.

Switch topology and configure a real cross-process broker (the in-memory broker
can't cross processes, so `topology="processes"` with the default broker is a
loud startup error):

```bash
pip install 'modulith[redis]'
```

```toml
# pyproject.toml
[tool.modulith]
topology = "processes"
broker = "redis-streams"

[tool.modulith.workers]
default = 1
reports = 4            # the reports module gets 4 worker processes
```

Cross-module events now have to leave the process. Mark the events that remote
workers must consume with `@externalized` — this routes the event to the broker
**in addition to** any local listeners (fan-out across processes):

```python
from dataclasses import dataclass
from modulith import event, externalized

@externalized                                  # default target: {broker}:{event-fqn}
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str

@externalized(target="redis-streams:orders.placed")   # or pin the destination
@event
@dataclass(frozen=True)
class StockReserved:
    order_id: str
```

Run it under the supervisor + reverse proxy:

```bash
modulith run myapp.main:app --topology=processes
```

The supervisor spawns one uvicorn subprocess per module (restarting crashes with
backoff), and the reverse proxy routes each request to the right worker by URL
prefix. In single-process topology `@externalized` is an inert marker, so you can
add it before you need multi-process and it costs nothing until then.

### Without Redis: a database as the broker

The setup above uses Redis, but the same process-per-module topology runs over a
relational database instead — and with SQLite, no server at all. Only the
configuration changes; the module code (the `@externalized` events above) is
identical, because the broker is chosen at bootstrap.

```bash
pip install 'modulith[database]'
```

The zero-infrastructure bootstrap is an embedded SQLite *file* shared by the
worker processes — nothing to run, ideal for getting started or a small
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
retention_age_seconds = 604800                   # prune terminal rows after 7 days
```

With `broker = "database"`, a bare `@externalized` event's default target is
`database:{event-fqn}`; pin one explicitly with
`@externalized(target="database:orders.placed")` exactly as with Redis. Every
`broker_options` key is env-overridable via `MODULITH_BROKER_<KEY>` (e.g.
`MODULITH_BROKER_URL`) — which is how each worker process receives its
connection URL.

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

The `modulith` pytest plugin (installed with `modulith[test]`) ships fixtures
that reset the runtime per test and capture what was published.

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

Run the AST boundary verifier in CI. On a clean codebase, fail on any violation:

```bash
modulith verify                       # exit 1 on any violation
modulith verify --fail-on-warnings    # also fail on WARNING-severity findings
```

Adopting on a messy existing codebase? Generate a ratcheting baseline that
grandfathers today's violations, then fail only on **new** ones:

```bash
modulith verify --mode=ratchet
```

The baseline is count-aware: it records existing violations by a stable hash, so
you can enforce "no new violations" while paying down the old ones over time. Add
the check to CI (exit code `0` = clean, `1` = violations/user error, `2` =
internal error):

```yaml
# .github/workflows/ci.yml
- run: pip install 'modulith[cli]'
- run: modulith verify --mode=ratchet
```

`modulith doctor` complements this with operational + architectural health
checks (outbox health, boundary health, split-readiness) for a running app.

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
The complete extension contract — all 12 hookspecs and 4 protocols — is in
[ARCHITECTURE.md §5](ARCHITECTURE.md#5-the-plugin-contract) and
[SPEC.md Part IV](../SPEC.md).
