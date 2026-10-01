# Quickstart

The smallest complete modupy project: three modules and a contracts package,
no database, no broker, nothing to start first. It is the code from the root
[README](../../README.md#the-30-second-pitch), file for file. Orders publish
`OrderCreated`; `payments` charges it and publishes `PaymentReceived`;
`orders` marks the order fulfilled; `inventory` reserves stock. No module
imports another module, only the shared events.

![orders publishes OrderCreated, which payments and inventory receive; payments publishes PaymentReceived, which orders receives; both events live in contracts](../../docs/images/event-flow.svg)

- [`myapp/contracts/events.py`](myapp/contracts/events.py): the two events
- [`myapp/orders/`](myapp/orders): creates orders and reads back fulfilment
- [`myapp/payments/`](myapp/payments): listener only, so it has no `router`
- [`myapp/inventory/`](myapp/inventory): reserves stock and reads a reservation
- [`myapp/main.py`](myapp/main.py): the FastAPI app
- [`tests/`](tests): the same flows under `pytest`

Every command below runs from this directory, exactly as written. CI executes
them, so the output shown is the output you get. The server logs also carry
process ids and timestamps, which are left out.

## Install and look around

```bash
pip install 'modupy[fastapi,cli]'
```

The project is not installed: `uvicorn` and `modulith` both put the current
directory on the import path. `pyproject.toml` names the package (`myapp`), and
that is all the CLI needs to find the modules.

```bash
$ modulith info
modulith
  package: myapp

  modules (4):
    - contracts  (myapp.contracts)  [no manifest]
    - inventory  (myapp.inventory)  [no manifest]
    - orders  (myapp.orders)  [no manifest]
    - payments  (myapp.payments)  [no manifest]

  configuration:
    outbox:        memory
    broker:        memory
    topology:      single
    observability: None
    production:    False
$ modulith verify
✓ no boundary violations
```

## Run it in one process

```bash
$ uvicorn myapp.main:app
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:modulith:detected application package 'myapp'
INFO:modulith:discovered 4 module(s): contracts, inventory, orders, payments
INFO:modulith:outbox=memory, broker=memory, topology=single
INFO:modulith:outbox disabled — set [tool.modulith].outbox = 'postgres' for durable event delivery
INFO:modulith:ready
```

Bootstrap is lazy, so the modulith lines appear on the first `publish()`, the
`curl` below, not at process start. In another terminal:

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' -d '{"customer_id": "alice"}'
{"order_id":"ord-1"}
```

The order fans out through events, not calls: `payments` charged it and
published `PaymentReceived`, which `orders` handled, while `inventory`
reserved the stock.

```bash
$ curl -s localhost:8000/orders/ord-1/fulfilment
{"order_id":"ord-1","fulfilled":true}
$ curl -s localhost:8000/inventory/ord-1
{"order_id":"ord-1","reserved":true}
```

For development, let uvicorn restart on every edit:

```bash
uvicorn myapp.main:app --reload
```

## Run the same code, one process per module

Stop the server first. The application code does not change:

```bash
$ modulith run myapp.main:app --topology=processes
modulith → process-per-module: 3 worker(s) [inventory:9001, orders:9002, payments:9003], reverse proxy on http://0.0.0.0:8000
```

![modulith run starts a main process holding the proxy on port 8000 and the supervisor, plus one worker process per module, connected by the built-in SHM broker](../../docs/images/processes.svg)

Each module now runs in its own process behind one public port, and events
cross the process boundary through the default broker. Startup logs several
warnings about that default broker; they are for production deployments and
do not affect this walkthrough.

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' -d '{"customer_id": "alice"}'
{"order_id":"ord-1"}
```

Each event now crosses a process boundary, so give the order a moment before
reading it back; until `payments` has charged it, the fulfilment route
answers 404.

```bash
$ curl -s localhost:8000/orders/ord-1/fulfilment
{"order_id":"ord-1","fulfilled":true}
$ curl -s localhost:8000/inventory/ord-1
{"order_id":"ord-1","reserved":true}
```

`payments` has no `router`, so it serves no HTTP routes and still consumes
`OrderCreated`; its worker logs a warning saying so.

## Run its tests

The tests drive the same flows in one process with modupy's test fixtures,
without starting a server:

```bash
pip install pytest httpx2
pytest
```
