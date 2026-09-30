# The shop

The mid-size example: one shop, three modules, four ways to run it. The code
never changes between stages; environment variables change where events live
and how many processes serve them.

`orders` accepts an order and publishes `OrderPlaced`. `inventory` reserves the
stock and publishes `StockReserved`. `notifications` records the notice. No
module imports another module, only the shared events.

- [`shop/contracts/events.py`](shop/contracts/events.py): the two events
- [`shop/orders/`](shop/orders): `place_order`, and the `POST /orders` and `GET /orders/{id}` routes
- [`shop/inventory/`](shop/inventory): the `reserve_stock` listener and its reservation route
- [`shop/notifications/`](shop/notifications): the `notify_customer` listener and its route
- [`shop/database.py`](shop/database.py): one SQLite (or Postgres) database for business rows and outbox rows
- [`shop/main.py`](shop/main.py): the FastAPI app and its lifespan
- [`tests/`](tests): the flows under `pytest`

Every command below runs from this directory, exactly as written. CI executes
them, so the output shown is the output you get. Server logs also carry
process ids and access lines, which are left out. Every stage places an order
with an id of its own (`o-1`, `o-2`, ...) because `shop.db` keeps the earlier
stages' orders, and a repeated id is answered with 409.

## Look before you run

```bash
pip install 'modupy[fastapi,cli,postgres]' aiosqlite
```

The project is not installed: `uvicorn` and `modulith` both put the current
directory on the import path, and `pyproject.toml` names the package (`shop`).
The `postgres` extra brings Alembic and the drivers that `modulith migrate` and
stage 4a need; `aiosqlite` serves the SQLite stages.

```bash
$ modulith info
modulith
  package: shop

  modules (4):
    - contracts  (shop.contracts)  [no manifest]
    - inventory  (shop.inventory)  [manifest]
    - notifications  (shop.notifications)  [manifest]
    - orders  (shop.orders)  [manifest]

  configuration:
    outbox:        memory
    broker:        memory
    topology:      single
    observability: None
    production:    False
$ modulith verify
✓ no boundary violations
$ modulith docs
generated 6 file(s) in docs/modulith:
  architecture.mmd
  modules/contracts.md
  modules/inventory.md
  modules/notifications.md
  modules/orders.md
  events.mmd
```

## Run its tests

```bash
pip install pytest pytest-asyncio
pytest
```

Every test points `MODULITH_OUTBOX_URL` at a database in its own temporary
directory before it imports `shop`, so the stages below start from an empty
`shop.db`.

## Stage 1: in-memory events

The default configuration needs no infrastructure: events live in memory and
each listener runs inline, inside the publisher's transaction. Create the
tables, then start the app:

```bash
python -m shop.schema
```

```bash
$ uvicorn shop.main:app
INFO:     Waiting for application startup.
INFO:modulith:detected application package 'shop'
INFO:modulith:discovered 4 module(s): contracts, inventory, notifications, orders
INFO:modulith:outbox=memory, broker=memory, topology=single
INFO:modulith:outbox disabled — set [tool.modulith].outbox = 'postgres' for durable event delivery
INFO:modulith:ready
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

In another terminal, place an order and read back what the three modules made
of it:

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' \
      -d '{"order_id": "o-1", "customer_id": "alice", "total": 19.99}'
{"order_id":"o-1"}
$ curl -s localhost:8000/orders/o-1
{"order_id":"o-1","customer_id":"alice","total":19.99}
$ curl -s localhost:8000/inventory/reservations/o-1
{"order_id":"o-1","reserved":true}
$ curl -s localhost:8000/notifications/o-1
{"order_id":"o-1","notified":true}
```

`place_order` publishes `OrderPlaced` first and only then adds the order row,
and it never flushes in between. Under the memory outbox `reserve_stock` runs
inline while the order's transaction is still open; a flushed order row would
hold SQLite's write lock and block the listener's own session.

Each service function and listener binds a session, publishes, commits and
unbinds before it returns. The route therefore answers only after the commit:
a failed commit is a 500, never a false 200. And each listener checks for its
row by primary key first, so a redelivered event changes nothing.

## Stage 2: a durable outbox on SQLite

Now the order row and its events commit together. The outbox lives in the same
`shop.db` as the business tables; `modulith migrate` adds the outbox tables.

```bash
export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db
```

The outbox setting is named `postgres` because that is the SQL outbox adapter;
the URL decides which database it talks to.

```bash
$ python -m shop.schema
$ modulith migrate
migrated sqlite:///shop.db to head
```

Stop the stage 1 server with Ctrl-C if it still runs, then start this one:

```bash
$ uvicorn shop.main:app
INFO:modulith:detected application package 'shop'
INFO:modulith:discovered 4 module(s): contracts, inventory, notifications, orders
INFO:modulith:outbox=postgres, broker=memory, topology=single
INFO:modulith:ready
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' \
      -d '{"order_id": "o-2", "customer_id": "alice", "total": 19.99}'
{"order_id":"o-2"}
```

Delivery now happens after the commit, so the notification appears a moment
after the order does. The status command reads the outbox from the same file
the app writes:

```bash
$ curl -s localhost:8000/notifications/o-2
{"order_id":"o-2","notified":true}
$ modulith outbox status
incomplete:    0
completed:     2
dead-lettered: 0
```

## Stage 2 afterwards: what the outbox kept

Stop the server with Ctrl-C. The two publications, `OrderPlaced` and
`StockReserved`, are rows in `shop.db`, so they are still there with no process
running:

```bash
$ modulith outbox status
incomplete:    0
completed:     2
dead-lettered: 0
$ modulith doctor
modulith doctor

✓ boundary health — 0 violation(s)
✓ outbox health — 0 incomplete, 2 completed, 0 dead-lettered
✓ listener registration — 2 declared listener(s), all registered
overall: ok
```

## Stage 3: the same code, one process per module

Nothing in `shop/` changes. The `inventory = 2` line in `pyproject.toml` gives
that module two workers, so it takes ports 9001 and 9002. `--host 127.0.0.1`
keeps every port on loopback, which is also what lets the operational
endpoints answer without a token. Startup logs several warnings about the
default broker; they are for production deployments and do not affect this
walkthrough. Four processes now write the one `shop.db`; SQLite serializes
them, and each write waits its turn.

```bash
export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db
```

```bash
$ modulith run shop.main:app --topology processes --host 127.0.0.1
modulith → process-per-module: 3 worker(s) [inventory:9001, notifications:9003, orders:9004], reverse proxy on http://127.0.0.1:8000
```

```bash
$ curl -s localhost:8000/_modulith/health
{"status":"ok","backends":{"/inventory":"ok","/notifications":"ok","/orders":"ok"}}
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' \
      -d '{"order_id": "o-3", "customer_id": "alice", "total": 19.99}'
{"order_id":"o-3"}
```

Each event now crosses a process boundary, so give the order a moment before
reading it back:

```bash
$ curl -s localhost:8000/orders/o-3
{"order_id":"o-3","customer_id":"alice","total":19.99}
$ curl -s localhost:8000/inventory/reservations/o-3
{"order_id":"o-3","reserved":true}
$ curl -s localhost:8000/notifications/o-3
{"order_id":"o-3","notified":true}
$ modulith outbox status
incomplete:    0
completed:     4
dead-lettered: 0
```

The counts include stage 2's two publications, because both stages share
`shop.db`.

## Stage 3 afterwards: the outbox after the drain

Stop the supervisor with Ctrl-C. It signals every worker and waits for them to
finish what they were delivering, so nothing is left half done:

```bash
$ modulith outbox status
incomplete:    0
completed:     4
dead-lettered: 0
```

## Stage 4a: the outbox on Postgres

Stage 4 needs Docker. `docker-compose.yml` publishes Postgres on port 55433 of
the loopback interface, so a Postgres of your own on 5432 does not collide.

```bash
docker compose up -d --wait postgres
```

Only the URL changes; the code is the same:

```bash
export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=postgresql+asyncpg://modulith:modulith@localhost:55433/modulith
```

```bash
$ python -m shop.schema
$ modulith migrate
migrated postgresql+psycopg://modulith:***@localhost:55433/modulith to head
$ uvicorn shop.main:app
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' \
      -d '{"order_id": "o-4", "customer_id": "alice", "total": 19.99}'
{"order_id":"o-4"}
$ curl -s localhost:8000/notifications/o-4
{"order_id":"o-4","notified":true}
$ modulith outbox status
incomplete:    0
completed:     2
dead-lettered: 0
```

## Stage 4b: processes over Redis Streams

Stop the server with Ctrl-C. Events between the processes can travel over
Redis Streams instead of the default broker; the outbox goes back to SQLite.

```bash
pip install 'modupy[redis]'
docker compose up -d --wait redis
```

```bash
export MODULITH_OUTBOX=postgres MODULITH_OUTBOX_URL=sqlite+aiosqlite:///shop.db MODULITH_BROKER=redis-streams REDIS_URL=redis://:modulith@localhost:56379
```

```bash
$ modulith run shop.main:app --topology processes --host 127.0.0.1
modulith → process-per-module: 3 worker(s) [inventory:9001, notifications:9003, orders:9004], reverse proxy on http://127.0.0.1:8000
```

```bash
$ curl -sX POST localhost:8000/orders \
      -H 'content-type: application/json' \
      -d '{"order_id": "o-5", "customer_id": "alice", "total": 19.99}'
{"order_id":"o-5"}
$ curl -s localhost:8000/orders/o-5
{"order_id":"o-5","customer_id":"alice","total":19.99}
$ curl -s localhost:8000/inventory/reservations/o-5
{"order_id":"o-5","reserved":true}
$ curl -s localhost:8000/notifications/o-5
{"order_id":"o-5","notified":true}
```

## Clean up

Stop the supervisor with Ctrl-C, then remove the containers and their volumes:

```bash
docker compose down -v
```
