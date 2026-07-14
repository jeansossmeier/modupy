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
│   ├── __init__.py          # place_order() → publishes OrderPlaced
│   ├── _manifest.py         # declared contract (verified at startup)
│   └── api.py               # FastAPI router: POST /orders
├── inventory/
│   ├── __init__.py          # @listener reserve_stock → publishes StockReserved
│   └── _manifest.py
├── notifications/
│   ├── __init__.py          # @listener notify_customer
│   └── _manifest.py
└── main.py                  # FastAPI app (includes the orders router)
```

## Run it

From this directory (`examples/demo_app`), pick a deployment mode below.

### A. Zero-config (default: in-memory, single-process)

The simplest path — no database, no Docker, all events live in RAM:

```bash
pip install 'modulith[fastapi,cli]'
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
pip install 'modulith[fastapi,cli,postgres]' aiosqlite
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

### D. Process-per-module topology over SQLite database broker

Distributes modules across separate workers using SQLite as the inter-process
message bus (zero infrastructure):

```bash
pip install 'modulith[fastapi,cli,database]' aiosqlite
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=sqlite+aiosqlite:////$(pwd)/demo-broker.db \
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
docker compose up -d redis
MODULITH_BROKER=redis-streams \
  REDIS_URL=redis://localhost:6379 \
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
`pyproject.toml`:

```bash
modulith info        # detected package, modules, manifests, plugins
modulith verify      # boundary checks — this demo passes clean
modulith docs        # Mermaid architecture + event-flow diagrams + module canvases
modulith doctor      # health check on wired drivers and stores
modulith audit       # code-level boundary compliance (no cross-module imports)
```

**Important caveat:** The `modulith outbox status|retry <id>|purge|dead-letter`
subcommands operate on a **wired outbox store**. This demo wires the store
inside the FastAPI app's lifespan (in `shop/main.py`), not at bare CLI
bootstrap. So if you run `modulith outbox status` standalone while the app is
not running, it will report "no store wired," and `modulith doctor` will note
that the outbox is configured but no store is active. This is by design: stores
are initialized by the application at startup, not auto-discovered. To inspect
the outbox, run the app in mode B or C first, then use the subcommands in
another terminal.

### G. Testing your modules

The demo includes tests using the `modulith[test]` extra and pytest plugin:

```bash
pip install 'modulith[fastapi,cli,test]' pytest pytest-asyncio
pytest examples/demo_app/tests/
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

The `scenario` and `modulith_app` fixtures are provided by the `modulith[test]`
extra and auto-loaded via the pytest11 plugin entry point.

### H. OpenTelemetry (optional, any mode)

Enable distributed tracing (console exporter):

```bash
pip install 'modulith[otel]'
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
