# The marketplace

The large example: seven modules that separate teams could own, a checkout
saga with compensation, and a Postgres that every process shares. It shows
what only a big codebase needs: rules the whole team checks automatically,
operators who recover failed work by command, and one module lifted out into a
service of its own. It does not repeat the smaller examples' lessons, so read [`quickstart`](../quickstart) and
[`demo_app`](../demo_app) first.

`orders` accepts an order and asks for stock. `inventory` reserves it, or
releases it again when the payment is declined. `payments` charges a card
through a fake gateway. `shipping` books a carrier. `notifications` tells the
customer, and `reporting` keeps the read models. `catalog` prices what can be
ordered.

- [`marketplace/contracts/`](marketplace/contracts): the eleven events the modules share
- [`marketplace/catalog/`](marketplace/catalog): `price_of(sku)`, and a command that lists a product
- [`marketplace/inventory/`](marketplace/inventory): reserves stock, and releases it as compensation
- [`marketplace/orders/`](marketplace/orders): the order and the saga that confirms or cancels it
- [`marketplace/payments/`](marketplace/payments): the fake card gateway, kept in `_internal/`
- [`marketplace/shipping/`](marketplace/shipping): carrier booking, and the shipping zones an operator edits
- [`marketplace/notifications/`](marketplace/notifications): the customer's notices, and the module you extract
- [`marketplace/reporting/`](marketplace/reporting): read models, served by two workers
- [`marketplace/db.py`](marketplace/db.py): the one database for business rows and outbox rows
- [`marketplace/main.py`](marketplace/main.py): the single-process app
- [`marketplace_platform/`](marketplace_platform): the team's own boundary rule and tracing, shipped as a plugin
- [`tests/`](tests): the architecture checks and every saga path under `pytest`

Every command below runs from this directory, exactly as written. CI executes
them, so the output shown is the output you get. Log lines that carry paths,
process ids or timestamps are left out.

## Check the architecture

```bash
pip install -e ".[test]"
```

This is the one example you install. Its team rule lives in
`marketplace_platform`, which the project registers under the `modulith` entry
point, and an entry point exists only for an installed project. `-e` keeps the
code editable. `pip` needs the network here, to build the project and to fetch
what its extras name.

```bash
pytest
```

The suite needs no Docker. The saga tests run against a SQLite file that
`modulith migrate` prepares, and the architecture tests run the commands below.

None of the commands below opens a database connection: `marketplace.db` reads
`MODULITH_OUTBOX_URL` only when a request or a listener first needs the engine.
The project's `database` broker is another matter. With no
`MODULITH_BROKER_URL`, each command falls back to an embedded SQLite file
under `$XDG_STATE_HOME/modulith/` and prints a notice naming it.

```bash
$ modulith verify
✓ no boundary violations
```

`verify` applies modulith's own boundary rules, and one of the team's:
`marketplace_platform` refuses any module that owns a table not named
`<module>_...`, so the owner of every table is obvious from its name. The same
rule runs each time the app starts, because `pyproject.toml` sets
`strict_boundaries = true`. A violation stops the process instead of reaching
production.

```bash
$ modulith docs --output-dir build/docs
generated 10 file(s) in build/docs:
  architecture.mmd
  modules/catalog.md
  modules/contracts.md
  modules/inventory.md
  modules/notifications.md
  modules/orders.md
  modules/payments.md
  modules/reporting.md
  modules/shipping.md
  events.mmd
```

The generated pages come from the code: the module graph, each module's
manifest, and who publishes or consumes each event. Nobody keeps them by hand.

```bash
$ modulith openapi --output build/openapi.json
wrote OpenAPI for 5 module(s) to build/openapi.json
skipped (no router): catalog, payments
```

One document covers the modules that expose HTTP routes. It prefixes each
module's schemas with the module name, so schemas from different modules
cannot collide. `catalog` and `payments` have no `router`, so they are
skipped.

```bash
$ modulith k8s-manifest --image marketplace:1.0.0 --output build/k8s.yaml
wrote 7 module manifest(s) to build/k8s.yaml
```

The manifest holds one Deployment per module, and two replicas of `reporting`,
as `[tool.modulith.workers]` says. It carries no connection strings. Each pod
reads the broker URL from a `marketplace-broker` Secret and the rest of its
environment from a `marketplace-env` Secret, and you create both.

## Run the platform, then extract a service

This section needs Docker. Postgres holds everything: the business tables, the
outbox and the database broker that carries events between processes. One
database adds no second stateful dependency, and it lets an event commit in the
same transaction as the row that caused it. Moving the broker to Redis later is
a configuration change.

### 1. Start and prepare the database

```bash
docker compose up -d --wait postgres
```

The URLs and the token come from the environment, never from
`pyproject.toml`. The token is a fixed local value; production injects a secret.

```bash
export MODULITH_OUTBOX_URL=postgresql+asyncpg://marketplace:marketplace@localhost:55432/marketplace \
       MODULITH_BROKER_URL=postgresql+asyncpg://marketplace:marketplace@localhost:55432/marketplace \
       MODULITH_ACTUATOR_TOKEN=local-demo-token
```

`modulith migrate` creates the outbox and broker tables. It cannot create the
business tables, so `marketplace.schema` does, and seeds the shipping zones for
`US` and `DE`. `marketplace.catalog` lists one product through the same
outbox: `inventory` learns its stock from the `ProductListed` event, and the
command returns once that event is delivered.

```bash
$ modulith migrate
migrated postgresql+psycopg://marketplace:***@localhost:55432/marketplace to head
$ python -m marketplace.schema
$ python -m marketplace.catalog SKU-MUG "Stoneware mug" 1200 10
$ modulith doctor
modulith doctor
✓ boundary health — 0 violation(s)
✓ process-split readiness — 100% of cross-module interactions via events — microservice-ready
✓ schema drift — recorded 12 event schema(s)
✓ outbox health — 0 incomplete, 1 completed, 0 dead-lettered
✓ listener registration — 17 declared listener(s), all registered
✓ shm notifier — no shm broker registered
✓ actuator token — single-process topology, no actuator proxy
✓ single-host broker — single-process topology
✓ redis retention — broker is not redis-streams
overall: ok
```

### 2. Serve the platform

```bash
$ modulith run marketplace.main:app --topology processes
INFO:  discovered 8 module(s): catalog, contracts, inventory, notifications, orders, payments, reporting, shipping
INFO:  outbox=postgres, broker=database, topology=processes
modulith → process-per-module: 7 worker(s) [catalog:9001, inventory:9002, notifications:9003, orders:9004, payments:9005, reporting:9006, shipping:9008], reverse proxy on http://0.0.0.0:8000
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

Every module runs in its own process behind a reverse proxy on port 8000, and
`[tool.modulith.workers]` gives `reporting` a second process (`9007`, which the
banner leaves out). The application code is the code you tested: `main.py` never
runs in a worker, so the plugin and the configured outbox wire each worker
themselves.

Because `actuator_mode = "token"`, the proxy serves `/_modulith/health` only to
a request that carries the token:

```bash
$ curl -s localhost:8000/_modulith/health -H "authorization: Bearer $MODULITH_ACTUATOR_TOKEN"
{"status":"ok","backends":{"/catalog":"ok","/inventory":"ok","/notifications":"ok","/orders":"ok","/payments":"ok","/reporting":"ok","/shipping":"ok"}}
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/_modulith/health
401
```

### 3. Run the order stories

Orders are client-numbered, so every body below is the same each time. An order
is a saga that takes a few seconds: each module reacts to the previous module's
event, and nothing coordinates them. The steps that read a finished order are
re-run by CI until they match, and you re-run them by hand the same way. The
first read of each story asks for its last effect, so the reads after it can
rely on everything before.

**A paid order.** `orders` publishes `OrderPlaced`. `inventory` reserves the
stock, `orders` asks for payment, `payments` captures it, and `orders` confirms.
`shipping` books a carrier and `notifications` writes the shipped notice.

```bash
$ curl -sX POST localhost:8000/orders -H 'content-type: application/json' -d '{"order_id": "o-100", "customer_id": "alice", "sku": "SKU-MUG", "quantity": 2, "card_token": "tok_visa", "country": "US"}'
{"order_id":"o-100","status":"placed"}
$ curl -s localhost:8000/notifications/o-100
[{"kind":"received","message":"order received"},{"kind":"confirmed","message":"order confirmed"},{"kind":"shipped","message":"shipped with UPS, tracking TRK-o-100"}]
$ curl -s localhost:8000/orders/o-100
{"order_id":"o-100","status":"confirmed","reason":null}
$ curl -s localhost:8000/shipping/o-100
{"order_id":"o-100","status":"booked","carrier":"UPS","tracking_number":"TRK-o-100"}
$ curl -s localhost:8000/inventory/SKU-MUG
{"sku":"SKU-MUG","on_hand":8}
```

**A declined card.** `tok_declined` is the token the fake gateway refuses. A
declined payment is a business outcome, so it is an event: `inventory` puts the
stock back and only then announces `StockReleased`, and `orders` cancels. A
cancelled order therefore always means the stock is available again.

```bash
$ curl -sX POST localhost:8000/orders -H 'content-type: application/json' -d '{"order_id": "o-200", "customer_id": "bob", "sku": "SKU-MUG", "quantity": 3, "card_token": "tok_declined", "country": "DE"}'
{"order_id":"o-200","status":"placed"}
$ curl -s localhost:8000/notifications/o-200
[{"kind":"received","message":"order received"},{"kind":"cancelled","message":"order cancelled: payment declined"}]
$ curl -s localhost:8000/orders/o-200
{"order_id":"o-200","status":"cancelled","reason":"payment declined"}
$ curl -s localhost:8000/inventory/SKU-MUG
{"sku":"SKU-MUG","on_hand":8}
$ curl -s localhost:8000/shipping/o-200
{"detail":"no shipment for order 'o-200'"}
```

**A technical failure.** Only `US` and `DE` have a shipping zone, so the carrier
booking for an order to `NZ` raises. Booking runs behind a `ShipmentRequested`
event that only `shipping` knows, so the failure lands in the outbox, where an
operator can manage it, and the customer's order stays confirmed. The outbox
retries the delivery, then dead-letters it after 3 attempts. The retry and lease
values in `[tool.modulith.outbox_options]` are tuned for this demo; production
keeps the defaults.

```bash
$ curl -sX POST localhost:8000/orders -H 'content-type: application/json' -d '{"order_id": "o-300", "customer_id": "carol", "sku": "SKU-MUG", "quantity": 1, "card_token": "tok_visa", "country": "NZ"}'
{"order_id":"o-300","status":"placed"}
$ modulith outbox dead-letter --list
1 dead-lettered publication(s):
$ curl -s localhost:8000/shipping/o-300
{"order_id":"o-300","status":"requested","carrier":null,"tracking_number":null}
```

The listing also prints the publication's id, its listener and its last error,
and the id is different every time, so it is left out. The operator fixes the
cause, then resubmits:

```bash
$ curl -sX PUT localhost:8000/shipping/zones/NZ -H 'content-type: application/json' -d '{"carrier": "NZPost"}'
{"country":"NZ","carrier":"NZPost"}
$ modulith outbox dead-letter --retry-all
resubmitted 1 dead-lettered publication(s)
$ curl -s localhost:8000/notifications/o-300
[{"kind":"received","message":"order received"},{"kind":"confirmed","message":"order confirmed"},{"kind":"shipped","message":"shipped with NZPost, tracking TRK-o-300"}]
```

The command runs the booking listener inside the CLI, so it books the shipment
at once. The `ShipmentBooked` event it publishes waits for a running worker to
sweep it: the CLI's claim on that event lasts `claim_lease_seconds`, and the
notice appears only after it expires and a sweep runs, seconds later.

### 4. Check the totals

`reporting` keeps one row per order, and `GET /reporting/summary` adds them up.
`confirmed` counts every order that was ever confirmed, so shipped orders stay
in it: o-100 and o-300. `revenue_cents` sums the totals of those same orders,
`2 x 1200 + 1 x 1200`. The declined o-200 is counted only under `cancelled`.
`modulith outbox status` shows that nothing is left to deliver and nothing is
dead-lettered.

```bash
$ curl -s localhost:8000/reporting/summary
{"confirmed":2,"cancelled":1,"shipped":2,"revenue_cents":3600}
$ modulith outbox status
incomplete:    0
dead-lettered: 0
```

### 5. Clean up

Stop the platform with Ctrl-C, then remove Postgres and its volume:

```bash
docker compose down -v
```
