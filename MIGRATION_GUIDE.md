# Migration Guide

> How to adopt modupy (which installs the `modulith` package) on an
> existing FastAPI application. The path matters because brownfield is
> where most adoption happens; greenfield is rare.

This guide assumes you have a FastAPI app of moderate size (50k-200k
lines), structured as folders without enforced boundaries, with at
least some database access via SQLAlchemy. Django projects and sync-only
codebases that won't go async are outside the audience modupy is built
for (SPEC.md, "Who This Is Not For"); [When modupy is the wrong
choice](#when-modupy-is-the-wrong-choice) lists the other reasons to skip it.

The migration has seven steps. Steps 1-3 are mandatory for any
modupy adoption. Steps 4-5 are optional but recommended — they are
where the payoff is (events, then the transactional outbox). Steps 6-7
are optional and gated on real need.

---

## Step 1 — Install and audit

```bash
uv add 'modupy[cli]'         # or: pip install 'modupy[cli]'
modulith audit                 # writes MIGRATION.md (--output to change, --force to replace one)
```

(The `cli` extra installs the `modulith` command's dependencies; the bare
`modulith` package is import-only. Don't shell-redirect stdout onto
`MIGRATION.md` — the command already writes the full report there and
echoes a short summary to stdout, so a redirect onto the same file
corrupts the report it just wrote.)

`modulith audit` reads your codebase non-destructively and produces a
Markdown report. It never replaces a file that already exists: when
`MIGRATION.md` (or the `--output` file) is there, it exits with code 1
naming the file and leaves it unchanged, and `--force` replaces it. Run
from the project root without a path, it audits your
application package: `src/` holding one package, otherwise the one top-level
package (`app/`), and that package's subpackages are the module candidates.
Loose-script directories without an `__init__.py` (`tools/`, `bin/`) beside
the package don't count, and tests, docs, scripts, examples, migrations,
virtualenvs, hidden, build and unreadable directories are ignored when it
decides. It prints the directory it chose. Pass a directory
(`modulith audit app`) to audit exactly that directory instead: its
subdirectories become the module candidates, and a file path is rejected
with exit code 1. The report names files relative to the audited directory,
so it can be committed and diffed. The report holds:
- A proposed module structure based on your folder layout
- A list of cross-module imports that would become violations
- A list of database tables that multiple parts of the code touch (these
  are your future ownership decisions)
- A "modupy-readiness score" (0-100) based on how much of your
  cross-module communication already goes through indirection. With only
  one module candidate there are no boundaries to measure, so the audit
  warns and reports the score as not applicable. A loose-script directory
  doesn't count as a candidate, and the score is also withheld when every
  import names a package below the audited directory that isn't a
  candidate, such as `modulith audit .` above `src/shopkit`.

Read the report with the team. Argue about the proposed module
boundaries. The audit is a starting point, not a verdict.

**Time-box:** the team should agree on a module structure within 2
hours. If you can't, the modulith pattern probably isn't the right
abstraction for your codebase, or the team isn't aligned on the
domain — neither of which modupy fixes.

## Step 2 — Restructure files

Move files into the proposed structure. **Don't change any logic yet.**

Before:
```
app/
├── views/
│   ├── orders.py
│   ├── inventory.py
│   └── payments.py
├── models/
│   ├── order.py
│   ├── inventory_item.py
│   └── payment.py
└── services/
    ├── order_service.py
    └── ...
```

After:
```
app/
├── contracts/                  # new — for shared event types (created in step 4)
│   ├── __init__.py
│   └── events.py
├── orders/
│   ├── __init__.py
│   ├── api.py                  # was views/orders.py
│   ├── _internal/
│   │   ├── models.py           # was models/order.py
│   │   └── service.py          # was services/order_service.py
│   └── handlers.py             # new — for @listener functions (step 4)
├── inventory/
│   └── ... (same pattern)
├── payments/
│   └── ... (same pattern)
└── main.py
```

Imports change shape but the logic is identical. Run your existing test
suite to verify nothing broke. **At this point modupy is doing nothing
yet — you've just reorganized files.**

## Step 3 — Generate baseline and add CI verification

```bash
modulith verify --mode=ratchet --update-baseline
```

This generates `.modulith-baseline.json` containing every existing
boundary violation. The verifier passes them as grandfathered. Add to
your CI:

```yaml
# .github/workflows/ci.yml
- run: modulith verify --mode=ratchet
```

Now boundaries are enforced **going forward**: any new error-level violation
fails the build (add `--fail-on-warnings` to fail on warnings too). Existing
violations are tracked but don't block you.

Violations enter the baseline once and leave it as you fix them:

```mermaid
flowchart LR
    A["modulith verify<br>--mode=ratchet<br>--update-baseline"] --> B[".modulith-baseline.json<br>every existing violation"]
    B --> C{"CI on every PR:<br>modulith verify<br>--mode=ratchet"}
    C -->|"a new error"| D["Build fails"]
    C -->|"only baselined ones"| E["Build passes"]
    F["A PR fixes<br>a violation"] --> G["modulith verify<br>--update-baseline<br>rewrites a smaller file"]
    G --> B
```

This is where modupy starts being useful. The verifier prevents the
common pattern where someone "just imports something quickly" across
module boundaries and the codebase erodes over time. Existing problems
stay; new ones don't.

**Tighten over weeks.** Each PR that fixes a baseline violation
removes it via `--update-baseline` regenerating the file. The diff in
git review shows what got fixed.

## Step 4 — Migrate to events (incremental, optional but recommended)

Pick the first cross-module call that bothers you. Replace it with an
event.

Before, in `orders/_internal/service.py`:
```python
from app.inventory._internal.service import reserve_stock  # cross-module call

async def create_order(customer_id):
    order_id = await persist(...)
    await reserve_stock(order_id)  # tight coupling
    return order_id
```

After:
```python
# app/contracts/events.py
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str
    customer_id: str
```

Keep an empty `app/contracts/__init__.py` beside it, as the Step 2 tree shows.
A folder without an `__init__.py` is not a module, so `modulith verify` would
silently skip the contracts rules.

```python
# app/orders/_internal/service.py
from app.contracts.events import OrderCreated
from modulith import publish

async def create_order(customer_id):
    order_id = await persist(...)
    await publish(OrderCreated(order_id=order_id, customer_id=customer_id))
    return order_id
```

```python
# app/inventory/handlers.py
from app.contracts.events import OrderCreated
from app.inventory._internal.service import reserve_stock
from modulith import listener

@listener
async def on_order_created(event: OrderCreated) -> None:
    await reserve_stock(event.order_id)
```

```python
# app/inventory/__init__.py
from app.inventory import handlers  # noqa: F401 — registers the @listeners
```

That last import matters: module discovery imports each module *package*
(its `__init__.py`), not every submodule — a `@listener` in `handlers.py`
only registers if the package imports it (or a `_manifest.py` declares it).

Solid arrows are imports, dotted arrows are the event at runtime:

```mermaid
flowchart TB
    subgraph before["Before"]
        direction LR
        o1["orders"] -->|"imports reserve_stock<br>a private import"| i1["inventory._internal"]
    end
    subgraph after["After"]
        direction LR
        o2["orders"] -->|"imports OrderCreated"| c["contracts"]
        i2["inventory"] -->|"imports OrderCreated"| c
        o2 -.->|"publishes"| m(["modupy"])
        m -.->|"delivers OrderCreated"| i2
    end
    before ~~~ after
```

Same logic. Different coupling. The orders module no longer knows that
inventory exists; it just announces what happened. **Do this one
cross-module call at a time** — each PR is small and reversible.

After a few migrations:
```bash
modulith doctor
```
shows your "process-split readiness" score climbing. At 80%+ you could
start evaluating a process split, but the percentage is not a release gate:
table-only cross-module coupling produces a warning even when there are no
import or event interactions, and any shared-table dependency still blocks
safe extraction.

## Step 5 — Enable the transactional outbox (durable, at-least-once delivery)

![One commit saves the order and one event_publications row per listener; after the commit each listener runs in the background, and a failing one is retried, then dead-lettered](docs/images/outbox.svg)

When you have real users in production and event loss matters, five things
turn the outbox on:

1. Install the Postgres extra.
2. Set `outbox = "postgres"` and `outbox_url` (or `MODULITH_OUTBOX_URL`) under
   `[tool.modulith]`.
3. Run `modulith migrate`.
4. Publish inside a session bound with `bind_session`/`unbind_session` from
   `modulith.builtin.outbox`.
5. Call `outbox.start()` and `await outbox.shutdown()` in the app's lifespan,
   after `modulith.bootstrap()`.

The rest of this step shows each one in order. Tuning keys, the raw Alembic
command, Postgres schemas and binding your own store follow under
[More outbox options](#more-outbox-options).

```bash
uv add 'modupy[postgres]'
```

```toml
[tool.modulith]
outbox = "postgres"
outbox_url = "postgresql+asyncpg://user:pass@localhost/mydb"  # your business database, or MODULITH_OUTBOX_URL
```

`outbox_url` is the async SQLAlchemy URL of the database your business data
lives in. With it set, modupy binds the outbox store for you.

Run the packaged schema migration. `modulith migrate` applies it to
`outbox_url`, swapping the async driver for the sync one Alembic runs on, and
prints the target with the password masked:

```bash
modulith migrate
```

Pass `--url <sqlalchemy url>` to migrate another database, and a revision
(`modulith migrate <revision>`) to stop short of `head`. The chain creates the
outbox tables and also the `broker_*` tables of the database broker. The chain
records its revision in its own `modulith_alembic_version` table, so it can
share a database with your application's Alembic history in `alembic_version`.

Bind your SQLAlchemy session to modupy around each transaction. The service
function binds, publishes, commits and unbinds before the route returns:

```python
# app/orders/service.py
from modulith import publish
from modulith.builtin.outbox import bind_session, unbind_session

from app.contracts.events import OrderCreated  # your event types


async def place_order(order_id: str) -> None:
    async with async_session_maker() as session:
        token = bind_session(session)
        try:
            session.add(Order(id=order_id))
            await publish(OrderCreated(order_id=order_id))
            await session.commit()
        finally:
            # bind_session returned this token; reset it when the transaction ends.
            unbind_session(token)
```

Commit inside the function, not in the teardown of a `yield` dependency:
FastAPI runs that teardown after the response is sent, so a commit that fails
there still answers 200. Wrap the same four steps in a `transaction()` helper
if many routes need them.

Call `modulith.bootstrap()` and then `outbox.start()` in your ASGI lifespan's
startup half, and `await outbox.shutdown()` in its shutdown half: module-scope
code runs before the server's event loop exists, so `configure()` there cannot
start the retry loop that redelivers rows a crashed process left behind, and
that loop skips every row until bootstrap has run. Keep the outbox table in the
same database as your business data, or the row and your data cannot commit in
one transaction. Under `--topology processes`, `main.py` does not run in
workers. Set `[tool.modulith].outbox_url` (env `MODULITH_OUTBOX_URL`) and
modupy binds the store in every process-topology worker and, while
`auto_discover` is on (the default), in the single-process server and the
`modulith outbox` CLI; without discovery, call `outbox.configure()` yourself. A
worker with a durable `outbox` and no store refuses to start.

Now `publish()` calls inside a transaction are atomically persisted.
A crash after the commit does not lose the event. Rolled-back transactions
don't leak ghost events. Listeners are called at-least-once after commit.

**This is the feature that justifies modupy over "FastAPI plus
folders."** Without it, you have a structural pattern. With it, you
have durable, at-least-once delivery.

### More outbox options

The tuning knobs live in `[tool.modulith.outbox_options]`, and the runtime
validates and forwards eight keys to `outbox.configure()` when it binds the
store from `outbox_url`: `claim_strategy`, `claim_lease_seconds`,
`claim_batch_size`, `dead_letter_after_attempts`, `retry_interval_seconds`,
`retry_stale_seconds`, `max_retry_backoff_seconds` and `completion_mode`
(`"update"` keeps history visible; `"delete"` and `"archive"` are the
alternatives). `sqlite_wal = true` switches a SQLite `outbox_url` database to
WAL journal mode, so readers no longer block a commit. It is off by default, is
ignored for other databases, persists in the database file, and cannot be used
on network filesystems. Any other key in the table is accepted and ignored. An
application that binds its own store, as the manual wiring below does, passes
these settings to `outbox.configure()` as keyword arguments.

The raw Alembic command remains the alternative to `modulith migrate`. modupy
ships its alembic config *inside* the installed package (your project needs no
alembic.ini), so point alembic's `-c` at it and supply the database URL via the
`MODULITH_DB_URL` env var (alembic runs on a **sync** driver, e.g.
`postgresql+psycopg://`, even if your app connects with asyncpg):

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  upgrade head
```

(`-x url=...` works instead of the env var; a bare `alembic upgrade head`
fails with "No 'script_location' key found" because there is no
alembic.ini in your project root.)

To put the outbox tables in a Postgres schema named after a module instead of
`public`, pass `modulith migrate --schema <name>`, or add `-x schema=<name>`
(or set `MODULITH_DB_SCHEMA`) to the raw command — Postgres only;
other dialects log a warning and ignore it. This is separate from, but usually
paired with, the database broker's own `broker_options.schema` /
`MODULITH_BROKER_SCHEMA` knob.

With the raw command, Alembic's `-x` is a global option and must precede the
command:

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  -x schema=orders upgrade head
```

Schema names must be portable unquoted SQL identifiers. The same validation
applies to configuration, environment variables, Alembic `-x`, and direct
database-broker construction. Enabling a named schema does not move data: if
`public` already contains modupy tables or Alembic history and the target has
no history, migration stops until you back up, explicitly move and verify the
tables, then rerun it.

To bind your own store or serializer instead of `outbox_url`, call
`outbox.configure()` yourself, at startup:

```python
# app/main.py
from modulith.adapters.postgres_outbox import PostgresPublicationStore
from modulith.builtin import outbox
from modulith.serializers import JsonEventSerializer

from app.contracts.events import OrderCreated  # your event types

# allowed_event_types is JsonEventSerializer's deserialization
# allowlist — recommended in production wherever payloads can originate
# outside the trusted process boundary (a shared outbox table, a broker):
# deserialize() imports the module named in the record's event_type, so
# without an allowlist a forged record can trigger arbitrary-module import.
store = PostgresPublicationStore(engine=async_engine)
outbox.configure(
    store=store,
    serializer=JsonEventSerializer(allowed_event_types=[OrderCreated]),
    completion_mode="update",  # "update" (default) | "delete" | "archive"
)
```

The session binding above is the same either way.

---

## Step 6 (optional) — Process-per-module (when one module needs more CPU)

![modulith run starts a main process holding the proxy on port 8000 and the supervisor, plus one worker process per module, connected by the built-in SHM broker](docs/images/processes.svg)

When a single module starts saturating your one process — typically
reports/analytics modules that do heavy CPU work in Python — promote
it to its own process:

```toml
[tool.modulith]
topology = "processes"

[tool.modulith.workers]
default = 1
reports = 4                     # this module gets 4 worker processes
```

```bash
modulith run app.main:app --topology=processes
```

With no broker or URL/DSN configured, process topology selects the stdlib-only
`shm` broker. If a URL/DSN is present, it infers `database`. An explicit
in-memory broker is a loud `ConfigurationError`, and explicit SHM rejects
SQLAlchemy/network URLs.

The supervisor spawns workers. The reverse proxy routes requests by
URL prefix. Cross-module events flow through the configured broker: an event
whose consumer lives in another worker routes to the broker automatically. The
one exception is fan-out: an event consumed *both* by a local listener *and* by
a remote worker must be marked `@externalized` (from `modulith`), or
`@externalized(target="scheme:destination")` to pin a destination:

```python
from modulith import event, externalized

@externalized          # local listeners still run; remote workers also consume
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
```

Your routes need a check too, because a worker serves one module package and
never your `main.py`:

- A module with routes re-exports its `router` from `__init__.py`, because a
  module running in its own process serves exactly that `router` under
  `/<module>`. A listener-only module needs none. Without the re-export the
  worker still starts, answers 404 on every `/<module>/...` path, and logs a
  warning.
- Declare each route without the `/<module>` prefix: the worker adds it.
- `main.py` does not run in a worker, so its middleware, its lifespan and
  anything else it sets up are absent there.

A worker runs only the listeners owned by its own module package. Two
listener shapes behave differently:

- A listener registered outside any module import (a plugin or hook) is
  local in every worker. A non-externalized event it handles is then never
  routed to the broker, so a listener for that event in another worker
  does not receive it.
- A listener in a plain, non-package file such as `myapp/shared.py`, or in
  a namespace folder without `__init__.py` such as `myapp/common/`, belongs
  to the module package whose import first loads that file in each process:
  the innermost module package on the import stack at that moment. Each
  worker decides this on its own, so if two modules each import the file
  directly, both workers run the listener, once each per broker delivery.
  If your module imports a sibling (`from myapp import orders`) whose import
  loads the file first, your module's own later import of the file does not
  make your worker run it. If your application package's `__init__.py` or
  the contracts package loads the file first, no module owns it and it
  behaves like a plugin listener: every worker runs it. To run such a
  listener in exactly one module's worker, define it inside that module
  package.

Keep listeners inside module packages, and mark an event `@externalized`
when modules in other workers handle it.

If you have direct cross-module function calls remaining, they will
break here — that's the cliff that `modulith doctor` was warning about.
Fix them by migrating to events first.

### Local SHM state and durability

The default `shm` adapter is local-host only. SQLite—not the mmap ring—is
authoritative for every publication, delivery, retry, and acknowledgement.
Every successful publish is committed before the ring receives its advisory
sequence hint. `synchronous=NORMAL` survives application, worker, supervisor,
and process restart on the same disk; set `sqlite_synchronous = "FULL"` when
the last commits must survive OS failure or power loss.

Delivery is at-least-once. A crash after a listener returns but before its ack
commits can deliver the event again, so make listeners idempotent. Every
publication, including one every group has acked, is retained for
`orphan_retention_seconds` (default one hour) and replayed to groups that
subscribe before expiry, while the store has room below its publish budget.

Set the paths and limits under `[tool.modulith.broker_options]`:

```toml
[tool.modulith.broker_options]
state_dir = "/private/app-state"
sqlite_path = "broker.db"
hint_path = "broker.hints"
max_payload_bytes = 16777216
max_store_bytes = 1073741824
```

Defaults are absolute, package-namespaced paths in the platform's per-user
state directory (`0700` directories and `0600` files on POSIX).
`max_payload_bytes` defaults to 16 MiB (maximum 1 GiB) and rejects oversized
messages before a publish transaction. `max_store_bytes` defaults to 1 GiB
(maximum 1 TiB) and bounds what publishes add to `broker.db`: publishes are
refused a small consumer reserve below it. Consumer writes can grow the file
past it while they drain work the store already holds, and the `-wal` file is
not counted. A subscribe replay stops short of the publish budget and logs a
WARNING with the group, the target, and the counts of publications it replayed
and skipped. Draining the replayed rows can still take the store past the
budget, as any consumer write can (error text, dead letters, and mark-mode
completions grow rows); publishes are then refused until prune frees pages or
`max_store_bytes` is raised. Environment overrides are
`MODULITH_BROKER_MAX_PAYLOAD_BYTES` and `MODULITH_BROKER_MAX_STORE_BYTES`.

For cross-host delivery, explicitly configure Redis Streams
(`modupy[redis]`) or a networked `database` URL.

---

## Step 7 (optional) — True microservice extraction

When organizational reasons (separate team, separate deployment
cadence, regulatory isolation) demand it, extract a module to its own
service:

1. The contracts module becomes a versioned shared library
2. The module gets its own repo, its own deploy pipeline
3. The remaining monolith publishes to a broker; the new service consumes
4. Database split happens here — usually the hardest part

`modulith extract <module>` scaffolds step 1-3's plumbing — it copies the
module, its contracts and the package-level helpers they import into a
standalone service tree with a generated wheel-buildable `pyproject.toml`,
`Dockerfile`, `.env.example` and `README.md` — but it refuses (exit 1,
overridable with `--force`) when the module still shares a table with
another module, since that's exactly the coupling a process split can't
paper over. It refuses on the same terms when the module itself still breaks
a boundary rule, when it imports another module, or when the shared-table
scan could not parse a file. A baselined violation counts: it would still
break the extracted service. If a `ForeignKey("table.col")` string literal
exists somewhere in your codebase pointing at a table another module owns,
`modulith verify` surfaces it as a `data-ownership` warning; run `modulith
verify --update-baseline` to grandfather existing findings the same way you
would any other ratcheted violation (Step 3). That silences CI only:
`modulith extract` still stops at them, so work through them before
extracting, or pass `--force` and fix them afterwards.

`modulith extract orders` runs these checks, in this order:

```mermaid
flowchart LR
    A["modulith extract orders"] --> B{"Blockers found<br>and no --force?"}
    B -->|"no"| C{"Output and source<br>paths safe?"}
    C -->|"yes"| D["Build in a<br>staging directory"]
    D --> E{"Staged module<br>imports cleanly?"}
    E -->|"yes"| F["Move staging<br>to the output"]
    B -->|"yes"| X["Exit 1"]
    C -->|"no, --force<br>cannot help"| X
    E -->|"no, --force<br>cannot help"| X
```

Extraction imports the configured application and module packages, so run it
only against trusted source. It stages output before publishing it and rejects
non-empty targets, output symlinks, output inside the source package, and
any symlink in the copied source; `--force` does not bypass these path safety
rules. Before publishing, it imports the staged module in a fresh interpreter
and fails if that import fails or loads code from the source tree outside the
extracted service; `--force` does not bypass that check either, so a
module-level import of another module fails even with `--force`.

modupy doesn't do the database split for you (that's a real data
migration project) but the contracts module, the events, and now
`modulith extract` give you the API boundary and the scaffolding. You're
extracting infrastructure, not code.

---

## Common pitfalls

**"We restructured but the verifier shows hundreds of violations."**
That's expected for a real codebase. Don't try to fix them all in one
PR. Generate the baseline, ratchet, fix one or two per sprint.

**"The outbox is dispatching duplicates."** It's at-least-once
delivery — listeners must be idempotent. The fix is listener-side, not
outbox-side. Use deterministic IDs and `INSERT ... ON CONFLICT DO NOTHING`
patterns or check-then-act with an idempotency key.

**"Sync FastAPI views can't await publish()."** Use `publish_sync()`.
It dispatches on its own background loop, so it works from a sync view or a
script, but it raises `RuntimeError` when called on an event loop's own thread
(use `await publish()` there).
It blocks until dispatch completes, bounded by its `timeout` keyword
(seconds, default 30.0) — on expiry the dispatch is cancelled and the
call raises `TimeoutError`; pass `timeout=None` to disable the bound.
Sync `@listener` functions are also accepted — they run in the event
loop's executor.

**"Tests are flaky after adding modupy."** Request the `modulith_app` fixture
in the flaky tests: it gives each test a fresh modupy runtime and resets it
afterwards. Any install of modupy registers the pytest plugin, but a fixture
only applies to a test that asks for it. `pip install 'modupy[test]'` adds
pytest and pytest-asyncio.

**"The audit tool's proposed structure looks wrong."** It's a
heuristic. Override it. The point is to start a conversation, not
prescribe a structure. Your team knows your domain.

---

## When modupy is the wrong choice

Skip modupy if:
- You're a solo developer on a small app — you don't have enough
  structural pain for the abstraction to pay off
- You're already on microservices — coming back to a monolith is rare
  and the migration cost would dwarf the benefit
- Your team is fully sync — the outbox requires async DB integration
- You have no events anywhere yet and aren't willing to introduce them
  — modupy is fundamentally event-driven; without that, you're just
  using folders and getting almost no benefit

If two or more of these apply, modupy is probably architecture for
its own sake. FastAPI plus folders plus discipline goes further than
people give it credit for.

---

## Getting help

- **SPEC.md** — the full design
- **examples/** — reference implementations of plugins and adapters
- **GitHub issues** — for bugs and adoption questions
