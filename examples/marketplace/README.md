# The marketplace

The large example: seven modules that separate teams could own, a checkout saga with compensation, and a Postgres that every process shares. It
shows what only a big codebase needs: rules the whole team checks
automatically, operators who recover failed work by command, and one module
lifted out into a service of its own. It does not repeat the smaller
examples' lessons, so read [`quickstart`](../quickstart) and
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
