# modulith demo — the shop

A minimal but complete modular monolith built with modulith. Three modules
collaborate purely through events:

```
POST /orders
   │
   ▼
┌──────────┐  OrderPlaced   ┌────────────┐  StockReserved  ┌────────────────┐
│  orders  │ ─────────────▶ │ inventory  │ ──────────────▶ │ notifications  │
└──────────┘                └────────────┘                 └────────────────┘
```

No module imports another module's code. They share the event vocabulary in
`shop/contracts/events.py` and communicate through the in-memory event bus.
Adding, removing, or splitting a module out to its own process changes none of
the others.

## Layout

```
shop/
├── contracts/
│   └── events.py            # OrderPlaced, StockReserved  (shared vocabulary)
├── orders/
│   ├── __init__.py          # place_order() → publishes OrderPlaced; re-exports `router`
│   ├── _manifest.py         # declared contract (verified at startup)
│   └── api.py               # FastAPI router: POST /orders
├── inventory/
│   ├── __init__.py          # @listener reserve_stock → publishes StockReserved; `router`
│   └── _manifest.py
├── notifications/
│   ├── __init__.py          # @listener notify_customer; `router`
│   └── _manifest.py
└── main.py                  # FastAPI app (includes all three module routers)
```

Every module package exposes its HTTP routes as a `router` attribute — the
orders module re-exports the one defined in `orders/api.py`. That attribute is
what the process-per-module worker mounts under `/<module>` (modes D and E
below), so a router left reachable only as `orders.api.router` would give
healthy workers and a 404 on every route. `shop/main.py` mounts the same three
routers under the same prefixes, which is why `POST /orders` and
`GET /inventory/reserved` are the same URLs in every mode.

## Run it

From this directory (`examples/demo_app`), pick a deployment mode below.

### A. Zero-config (default: in-memory, single-process)

The simplest path — no database, no Docker, all events live in RAM:

```bash
pip install 'modupy[fastapi,cli]'
uvicorn shop.main:app --reload
#   …or with the CLI (adds the modulith banner):
# modulith dev shop.main:app

# In another terminal:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id":"c-1","total":19.99}'
# → {"order_id": "…"}  — the event chain fans out across modules in-memory.
```

### B. Durable transactional outbox on SQLite

Persists orders and events atomically in a local SQLite file (zero infrastructure):

```bash
pip install 'modupy[fastapi,cli,postgres]' aiosqlite
MODULITH_OUTBOX=postgres MODULITH_DB_URL=sqlite+aiosqlite:///./demo.db \
  uvicorn shop.main:app

# In another terminal:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id":"c-1","total":19.99}'
```

**What changes:** The order row AND the `OrderPlaced` event are persisted in the
same transaction. After commit, the event is dispatched to listeners. Listeners
must be idempotent (at-least-once delivery).

**Only hop 1 is durable.** The outbox persists `OrderPlaced` (orders →
inventory) atomically with the order row, but the after-commit dispatch that
delivers it is deliberately session-less (`postgres_outbox.py`'s
`_dispatch_after_commit`), so `inventory`'s own `StockReserved` publish
(inventory → notifications) rides the in-memory bus, not the outbox — a crash
between the two hops loses the second one. A production listener that wants a
durable cascade must bind its own session and publish inside it (see
`shop.orders.api.get_session` for the pattern). Because outbox delivery is
at-least-once, `inventory.reserve_stock` and `notifications.notify_customer`
each guard against a redelivered event with an `order_id` membership check
before acting.

### C. Durable outbox on Postgres

Same as mode B, but on a persistent Postgres database:

```bash
docker compose up -d postgres
MODULITH_OUTBOX=postgres \
  MODULITH_DB_URL=postgresql+asyncpg://modulith:modulith@localhost:5432/modulith \
  uvicorn shop.main:app

# Test it:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id":"c-1","total":19.99}'
```

**`down` vs `stop`:** the `postgres` service's data lives in the
`modulith-postgres-data` named volume in `docker-compose.yml`, so both
`docker compose stop` and `docker compose down` preserve it across restarts —
only `docker compose down -v` (or an explicit `docker volume rm`) deletes it.

**Startup warning in modes D and E.** `modulith run` binds `0.0.0.0`, and the
default `actuator_mode="auto"` will not serve an unauthenticated `/_modulith/*`
on a non-loopback bind. With no `MODULITH_ACTUATOR_TOKEN` exported the
supervisor logs a warning and leaves the actuator unmounted — the demo's own
routes below are unaffected, so you can ignore it. Export a token
(`export MODULITH_ACTUATOR_TOKEN="$(openssl rand -hex 32)"`) if you want the
topology/health endpoints; see
[docs/DEPLOYMENT.md §Actuator Access](../../docs/DEPLOYMENT.md#actuator-access-_modulith).

### D. Process-per-module topology over SQLite database broker

Distributes modules across separate workers using SQLite as the inter-process
message bus (zero infrastructure):

```bash
pip install 'modupy[fastapi,cli,database]' aiosqlite
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=sqlite+aiosqlite:///$(pwd)/demo-broker.db \
  modulith run shop.main:app --topology processes

# In another terminal, test it:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id":"c-1","total":19.99}'

# Inspect routed events:
curl localhost:8000/inventory/reserved
curl localhost:8000/notifications/sent
```

**What changes:** Each module runs in its own worker process behind a reverse
proxy. The `@listener` decorators on `@externalized` events are no-ops in
single-process mode but enable cross-process delivery here.

**Env var vs `--topology` flag:** commands D and E pass `--topology processes`
rather than also setting `MODULITH_TOPOLOGY=processes` — the process
subcommands always pass `topology="processes"` as an explicit configuration
override (`modulith/cli.py`, `_configure_process_runtime`), which wins over
any `MODULITH_TOPOLOGY` env var under the documented pyproject-then-env-
then-explicit-override precedence, so setting the env var here would be
redundant.

### E. Process topology over Redis Streams

Distributes modules across workers using Redis (requires Redis running):

```bash
pip install 'modupy[fastapi,cli,redis]'
docker compose up -d redis
MODULITH_BROKER=redis-streams \
  REDIS_URL=redis://:modulith@localhost:6379 \
  modulith run shop.main:app --topology processes

# Test it:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id":"c-1","total":19.99}'
curl localhost:8000/inventory/reserved
curl localhost:8000/notifications/sent
```

### F. CLI operations (modes A–E compatible)

The `modulith` CLI auto-detects the `shop` package via `[tool.modulith]` in
`pyproject.toml`, and puts the directory holding that `pyproject.toml` on
`sys.path` before running the command (`modulith/cli.py`,
`_add_project_root_to_syspath`). So `shop` is importable from this directory
even though it lives here rather than in site-packages — no `PYTHONPATH=.`
prefix is needed on any `modulith` command in this file. (`modulith dev` and
single-process `modulith run` hand off to `uvicorn`, which adds the current
directory itself, exactly as mode A does.)

```bash
modulith info      # detected package, modules, manifests, plugins
modulith verify    # boundary checks — this demo passes clean
modulith docs      # Mermaid architecture + event-flow diagrams + module canvases → writes into docs/modulith/ (gitignored)
modulith doctor    # health check on wired drivers and stores
```

(`modulith audit` is deliberately absent: it is the migration-readiness scanner
for codebases that have *not* adopted modulith yet. Run here, it audits the
`shop` package and reports `readiness score: 33/100` and
`3 cross-module import pattern(s), 0 shared table(s)`. All three patterns are
`shop/main.py` importing each module's router, plus the orders models' `Base`
for table creation — composition-root wiring the scanner cannot tell from
coupling. The modules themselves talk
only through `contracts` and events. It also writes a `MIGRATION.md` into the
directory it runs from. `verify` is the boundary check for a modulith-native
codebase.)

**Important caveat:** The `modulith outbox status|retry <id>|purge|dead-letter`
subcommands operate on a **wired outbox store**. This demo wires the store
inside the FastAPI app's lifespan (in `shop/main.py`), not at bare CLI
bootstrap. So `modulith outbox status` reports "no store wired," and `modulith
doctor` notes that the outbox is configured but no store is active. This is by
design: stores are initialized by the application at startup, not
auto-discovered.

Running the app in another terminal does not change that — `outbox.configure()`
binds the store in the *calling process*, and the CLI is a different process
with nothing shared between them. The subcommands are usable only from a
process that wires the store itself, i.e. an app whose bootstrap module
(imported by the CLI via `[tool.modulith]`) calls `outbox.configure()` at import
time. This demo wires it in the lifespan instead, so its outbox is inspectable
through the running app, not through the CLI.

### G. Testing your modules

The demo includes tests using the `modupy[test]` extra and pytest plugin:

```bash
pip install 'modupy[fastapi,cli,test]' pytest pytest-asyncio
pytest tests/
```

Example test from `test_shop_flow.py`:

```python
def test_order_placed_triggers_stock_reserved_via_scenario(scenario: Scenario) -> None:
    """``scenario.publish(...).expect_event(...).within(...)`` across modules."""
    result = (
        scenario.publish(OrderPlaced(order_id="s-1", customer_id="cust-1", total=9.99))
        .expect_event(StockReserved)
        .matching(lambda e: e.order_id == "s-1")
        .within(seconds=2)
    )
    assert isinstance(result, StockReserved)
```

The `scenario` and `modulith_app` fixtures are provided by the `modupy[test]`
extra and auto-loaded via the pytest11 plugin entry point.

### H. OpenTelemetry (optional, any mode)

Enable distributed tracing (console exporter):

```bash
pip install 'modupy[otel]'
MODULITH_DEMO_OTEL=1 uvicorn shop.main:app
```

Spans print to the console. Without a configured OpenTelemetry exporter, spans
are no-ops (no overhead).

### I. Custom serializer for outbox storage (optional, modes B or C)

Demonstrates pluggable outbox storage serializers (independent of the broker
wire format, which is fixed JSON in v1):

```bash
MODULITH_OUTBOX=postgres MODULITH_DEMO_SERIALIZER=custom \
  MODULITH_DB_URL=sqlite+aiosqlite:///./demo.db \
  uvicorn shop.main:app
```

The `VersionedJsonSerializer` in `shop/serialization.py` wraps the default
JSON serializer in a versioned envelope for storage only — the broker still
uses the fixed JSON wire format internally.

## What it demonstrates

- **Auto-discovery** — modules are subpackages of `shop`; listeners register
  with no wiring code.
- **Event-driven boundaries** — cross-module communication is `publish()` +
  `@listener`, never a direct import.
- **The contracts module** — shared event types live in `shop/contracts`, so
  producers and consumers depend on a schema, not on each other.
- **Manifests** — each module declares what it publishes/consumes; modulith
  verifies that against reality at startup (a listener that fails to register
  aborts the boot instead of silently dropping events).
- **Durable outbox** — transactional persistence of order + event (mode B, C).
- **Process-per-module topologies** — each module runs as its own worker,
  communicating via database or Redis (modes D, E).
- **Pluggable serializers** — custom storage serializers for the outbox (mode I).
- **OpenTelemetry integration** — distributed tracing across modules (mode H).
- **Testing with scenarios** — high-level assertions on event chains (mode G).
