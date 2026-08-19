# Migration Guide

> How to adopt modulith on an existing FastAPI application. The path
> matters because brownfield is where most adoption happens; greenfield
> is rare.

This guide assumes you have a FastAPI app of moderate size (50k-200k
lines), structured as folders without enforced boundaries, with at
least some database access via SQLAlchemy. If you have something quite
different (Django, Flask without async, sync-only stack), the path
still works but specific commands differ — see the SPEC.md notes on
your stack.

The migration has seven steps. Steps 1-3 are mandatory for any
modulith adoption. Steps 4-5 are optional but recommended — they are
where the payoff is (events, then the transactional outbox). Steps 6-7
are optional and gated on real need.

---

## Step 1 — Install and audit (Friday afternoon, 2 hours)

```bash
uv add 'modupy[cli]'         # or: pip install 'modupy[cli]'
modulith audit                 # writes MIGRATION.md (use --output to change)
```

(The `cli` extra installs the `modulith` command's dependencies; the bare
`modulith` package is import-only. Don't shell-redirect stdout onto
`MIGRATION.md` — the command already writes the full report there and
echoes a short summary to stdout, so a redirect onto the same file
corrupts the report it just wrote.)

`modulith audit` reads your codebase non-destructively and produces a
Markdown report:
- A proposed module structure based on your folder layout and import patterns
- A list of cross-module imports that would become violations
- A list of database tables that multiple parts of the code touch (these
  are your future ownership decisions)
- A "modulith-readiness score" (0-100) based on how much of your
  cross-module communication already goes through indirection

Read the report with the team. Argue about the proposed module
boundaries. The audit is a starting point, not a verdict.

**Time-box:** the team should agree on a module structure within 2
hours. If you can't, the modulith pattern probably isn't the right
abstraction for your codebase, or the team isn't aligned on the
domain — neither of which modulith fixes.

## Step 2 — Restructure files (the boring weekend)

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
├── contracts/                  # new — for shared event types (created in step 3)
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
suite to verify nothing broke. **At this point modulith is doing nothing
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

Now boundaries are enforced **going forward**: any new violation fails
the build. Existing violations are tracked but don't block you.

This is where modulith starts being useful. The verifier prevents the
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
from inventory._internal.service import reserve_stock  # cross-module call

async def create_order(customer_id):
    order_id = await persist(...)
    await reserve_stock(order_id)  # tight coupling
    return order_id
```

After:
```python
# app/contracts/events.py
@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str
    customer_id: str
```

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

## Step 5 — Enable the transactional outbox (production-grade delivery)

When you have real users in production and event loss matters:

```bash
uv add 'modupy[postgres]'
```

```toml
[tool.modulith]
outbox = "postgres"
```

The completion mode (`"update"` keeps history visible; `"delete"` and
`"archive"` are the alternatives) is passed to `outbox.configure()` in
the wiring code below — the `[tool.modulith.outbox_options]` subtable
is parsed and validated but currently reserved: the runtime does not
read its keys yet.

Run the packaged schema migration. modulith ships its alembic config
*inside* the installed package (your project needs no alembic.ini), so
point alembic's `-c` at it and supply the database URL via the
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
`public`, add `-x schema=<name>` (or set `MODULITH_DB_SCHEMA`) — Postgres only;
other dialects log a warning and ignore it. This is separate from, but usually
paired with, the database broker's own `broker_options.schema` /
`MODULITH_BROKER_SCHEMA` knob.

Alembic's `-x` is a global option and must precede the command:

```bash
MODULITH_DB_URL='postgresql+psycopg://user:pass@localhost/mydb' \
  alembic -c "$(python -c 'import modulith.adapters, pathlib; print(pathlib.Path(modulith.adapters.__file__).parent / "alembic.ini")')" \
  -x schema=orders upgrade head
```

Schema names must be portable unquoted SQL identifiers. The same validation
applies to configuration, environment variables, Alembic `-x`, and direct
database-broker construction. Enabling a named schema does not move data: if
`public` already contains Modulith tables or Alembic history and the target has
no history, migration stops until you back up, explicitly move and verify the
tables, then rerun it.

Wire your SQLAlchemy session to modulith:
```python
# app/main.py
from modulith.adapters.postgres_outbox import (
    PostgresPublicationStore,
    bind_session,
    unbind_session,
)
from modulith.builtin import outbox
from modulith.serializers import JsonEventSerializer

from app.contracts.events import OrderCreated  # your event types

# At startup. allowed_event_types is JsonEventSerializer's deserialization
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

# In your dependency for getting a DB session
async def get_db():
    async with async_session_maker() as session:
        token = bind_session(session)
        try:
            yield session
        finally:
            # bind_session returned this token; reset it when the request ends.
            unbind_session(token)
```

Now `publish()` calls inside a transaction are atomically persisted.
Process crashes don't lose events. Rolled-back transactions don't leak
ghost events. Listeners are called at-least-once after commit.

**This is the feature that justifies modulith over "FastAPI plus
folders."** Without it, you have a structural pattern. With it, you
have actual delivery guarantees.

---

## Step 6 (optional) — Process-per-module (when one module needs more CPU)

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
URL prefix. Cross-module events flow through the configured broker. **No code
changes are needed if you've been following events for cross-module
communication** — an event whose consumer lives
in another worker routes to the broker automatically. The one exception
is fan-out: an event consumed *both* by a local listener *and* by a
remote worker must be marked `@externalized` (from `modulith`), or
`@externalized(target="scheme:destination")` to pin a destination:

```python
from modulith import event, externalized

@externalized          # local listeners still run; remote workers also consume
@event
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
```

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
commits can deliver the event again, so make listeners idempotent. A publication
without a registered group is retained for 24 hours and replayed to groups that
subscribe before expiry.

Use canonical private paths rather than the legacy names:

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
(maximum 1 TiB) and uses SQLite `max_page_count` to reject new writes when the
store is full. Environment overrides are
`MODULITH_BROKER_MAX_PAYLOAD_BYTES` and `MODULITH_BROKER_MAX_STORE_BYTES`.
Remove legacy `shm_slot_size`; it is deprecated and ignored.

### Migrating the old overflow-only SHM store

Stop the entire process fleet before opening an old store with the new runtime.
Back up the state directory, deploy one version everywhere, then restart. On
first open, the v0 `shm_message` overflow schema migrates transactionally to the
SQLite-authoritative schema; a failed migration rolls back.

Only rows that reached the old SQLite overflow store can migrate. Messages that
existed only in the old payload ring cannot be recovered. Do not run old and new
workers against the same files during migration.

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
module plus its contracts into a standalone service tree with a generated
wheel-buildable `pyproject.toml`, `Dockerfile`, and `README.md` — but it refuses (exit 1,
overridable with `--force`) when the module still shares a table with
another module, since that's exactly the coupling a process split can't
paper over. If a `ForeignKey("table.col")` string literal exists somewhere in
your codebase pointing at a table another module owns, `modulith verify`
surfaces it as a `data-ownership` warning; run `modulith verify
--update-baseline` to grandfather existing findings the same way you would
any other ratcheted violation (Step 3), then work through them before or
after extraction.

Extraction imports the configured application and module packages, so run it
only against trusted source. It stages output before publishing it and rejects
non-empty targets, output symlinks, output inside the source package, and
source symlinks that escape the package; `--force` does not bypass these path
safety rules.

Modulith doesn't do the database split for you (that's a real data
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
It detects context (running loop or not) and dispatches correctly.
It blocks until dispatch completes, bounded by its `timeout` keyword
(seconds, default 30.0) — on expiry the dispatch is cancelled and the
call raises `TimeoutError`; pass `timeout=None` to disable the bound.
Sync `@listener` functions are also accepted — they run in the event
loop's executor.

**"Tests are flaky after adding modulith."** Add the pytest plugin:
`pip install 'modupy[test]'`. The `modulith_app` fixture handles
state reset between tests, which fixes 90% of test isolation issues.

**"The audit tool's proposed structure looks wrong."** It's a
heuristic. Override it. The point is to start a conversation, not
prescribe a structure. Your team knows your domain.

---

## When modulith is the wrong choice

Skip modulith if:
- You're a solo developer on a small app — you don't have enough
  structural pain for the abstraction to pay off
- You're already on microservices — coming back to a monolith is rare
  and the migration cost would dwarf the benefit
- Your team is fully sync — the outbox requires async DB integration
  to be production-grade
- You have no events anywhere yet and aren't willing to introduce them
  — modulith is fundamentally event-driven; without that, you're just
  using folders and getting almost no benefit

If two or more of these apply, modulith is probably architecture for
its own sake. FastAPI plus folders plus discipline goes further than
people give it credit for.

---

## Getting help

- **SPEC.md** — every design decision documented
- **examples/** — reference implementations of plugins and adapters
- **GitHub issues** — for bugs and adoption questions
