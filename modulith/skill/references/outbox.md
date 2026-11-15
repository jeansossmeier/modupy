# The outbox: events that survive a crash

Read this when:

- an event must not be lost: the order was saved, so `OrderCreated` has to reach `payments`;
- you are turning the outbox on, or putting a database session around a `publish()`;
- a listener keeps failing, or `modulith outbox ...` shows rows you have to act on.

## Contents

- [When an event is lost without it](#when-an-event-is-lost-without-it)
- [Turn it on](#turn-it-on)
- [What "published inside a transaction" means](#what-published-inside-a-transaction-means)
- [Delivery, retries and dead letters](#delivery-retries-and-dead-letters)
- [The `modulith outbox` commands](#the-modulith-outbox-commands)
- [Listeners must be safe to run twice](#listeners-must-be-safe-to-run-twice)
- [One process per module](#one-process-per-module)
- [Tuning and claim strategies](#tuning-and-claim-strategies)
- [Tests](#tests)
- [What `modulith doctor` checks](#what-modulith-doctor-checks)
- [Mistakes that look right](#mistakes-that-look-right)

## When an event is lost without it

The default is `outbox = "memory"`: nothing is stored.
`publish()` runs every listener of the event, waits for all of them and re-raises the first error.

| Situation | Outbox off (`memory`) | Outbox on, published inside a bound session |
|---|---|---|
| A listener raises | `publish()` raises in your caller; nothing retries the listener | the failure is recorded and the listener is retried; your request has already returned |
| The process dies while listeners run | the ones that had not finished never run, and nothing remembers the event | the event is in the database and is delivered after the restart |
| Your transaction rolls back after the publish | the listeners already ran, on data that was never saved | the event is rolled back with the data and never delivered |

Check which one a project uses with `modulith info` (the `outbox:` line), or the startup log line `outbox=memory, broker=memory, topology=single`.

## Turn it on

All four steps are required. With step 4 skipped, nothing is saved and nothing warns: every event is delivered directly, as before.

**1. Install the database extra.**

```bash
pip install 'modupy[postgres]'    # PostgreSQL
pip install 'modupy[database]'    # MySQL, MariaDB or SQLite
```

**2. Point modupy at the database that holds your business tables.**

```toml
[tool.modulith]
outbox = "postgres"                              # the SQL outbox, whatever the database
outbox_url = "postgresql+asyncpg://app@db/app"   # an async SQLAlchemy URL
```

- `"postgres"` names the SQL adapter; the URL picks the database: `postgresql+asyncpg://...`, `mysql+aiomysql://...` or `sqlite+aiosqlite:///myapp.db`.
- The outbox tables must live in the same database as your data: one commit has to cover both.
- Keep passwords out of `pyproject.toml`: the environment variables `MODULITH_OUTBOX` and `MODULITH_OUTBOX_URL` override the file.

**3. Create the tables.**

```bash
$ modulith migrate
migrated sqlite:///myapp.db to head
```

`migrate` reads configuration only, so it runs before any table exists.
It migrates `outbox_url` (pass `--url <sqlalchemy url>` for another database, `--schema <name>` on PostgreSQL) and creates `event_publications`, `event_publications_archive` and the database broker's `broker_*` tables.
Run it again after every modupy upgrade, before the new version starts.
It never creates your own tables.

**4. Bind a session around each write, and start the outbox with the app.**

```python
# myapp/database.py: a plain file beside the modules, so it is not a module
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# The database [tool.modulith].outbox_url names.
URL = os.environ.get("MODULITH_OUTBOX_URL", "postgresql+asyncpg://app@db/app")

engine = create_async_engine(URL)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
```

```python
# myapp/orders/__init__.py
from uuid import uuid4

from modulith import publish
from modulith.builtin.outbox import bind_session, unbind_session

from myapp.contracts.events import OrderCreated
from myapp.database import sessionmaker
from myapp.orders.models import Order  # your SQLAlchemy model


async def create_order(customer_id: str) -> str:
    order_id = str(uuid4())
    async with sessionmaker() as session:
        token = bind_session(session)
        try:
            session.add(Order(id=order_id, customer_id=customer_id))
            await publish(OrderCreated(order_id=order_id))  # saved, not delivered yet
            await session.commit()  # the order and its event are saved together
        finally:
            unbind_session(token)
    return order_id
```

```python
# myapp/main.py
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from modulith import bootstrap
from modulith.builtin import outbox

from myapp.database import engine
from myapp.orders import router as orders_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    bootstrap()     # binds the outbox store from outbox_url
    outbox.start()  # retry loop, plus a sweep for what a crash left undelivered
    yield
    await outbox.shutdown()  # stops the loop, closes the engine modupy built
    await engine.dispose()   # your own engine


app = FastAPI(lifespan=lifespan)
app.include_router(orders_router, prefix="/orders")
```

Why each lifespan line is there:

- `bootstrap()` is otherwise lazy and first runs at the first `publish()`. Until then no store is bound and `outbox.start()` does nothing, so rows a crashed process left behind would wait for the next order.
- `outbox.start()` must run inside the server's event loop, which is why it sits in the lifespan and not at module scope.
- `outbox.shutdown()` is final for the process: do not start a second lifespan in the same process.

## What "published inside a transaction" means

A `publish()` is durable only when both hold: a session is bound with `bind_session()` in the current context, and that session is **committed after** the publish.

| What you wrote | What happens |
|---|---|
| bind, `publish()`, `commit()`, unbind | one row per listener is saved with your data; listeners run after the commit, in the background |
| `publish()` with no session bound | delivered directly, as with the memory outbox; nothing is saved; no warning |
| bind, `publish()`, then rollback or close without a commit | the event is discarded and a WARNING is logged (text below) |
| bind, `commit()`, then `publish()` with no later commit | same: discarded with the WARNING |

```text
WARNING:modulith.adapters.postgres:discarded 2 outbox publication(s) of myapp.contracts.events.OrderCreated: the bound session's transaction ended without committing (rolled back, or closed after its last commit). Publish before the final commit, or commit after the work that publishes (see the bind_session docs).
```

More rules that follow from this:

- `publish()` takes no `session=` argument and a listener receives only the event. The binding travels through the context, so always pair `bind_session` with `unbind_session` in a `try/finally`.
- **Only the hop you bind is durable.** When the `payments` listener publishes `PaymentReceived` with no session bound, that second event is delivered directly. To make it durable, the listener opens its own session and binds it, exactly as `create_order` does.
- Commit before the route returns. FastAPI runs a `yield` dependency's teardown after the response is sent, so a commit there can fail after the client already got a 200.
- Use `await publish()`. `publish_sync()` runs on another event loop, and one outbox engine must be used from one loop.
- With `auto_discover = false`, bootstrap binds no store from `outbox_url`. Call `outbox.configure(store, serializer)` yourself, or a publish inside a session is delivered directly, with one WARNING per process that starts `outbox is 'postgres' but no outbox store is bound`.

## Delivery, retries and dead letters

- After the commit, each listener's row is delivered at once, in a background task. The publishing request does not wait for it.
- A listener that raises costs its row one attempt. The row is due again 1 s after the first failure, then 2 s, 4 s and so on, doubling up to `max_retry_backoff_seconds` (300), and the next sweep picks it up. The sweep runs every `retry_interval_seconds` (30).
- After `dead_letter_after_attempts` (10) failed attempts the row becomes a **dead letter**: it is kept, never retried again on its own, and one ERROR is logged: `publication <id> dead-lettered after 10 attempt(s): <last error>`.
- A failing listener logs nothing before that. Look with `modulith outbox failing`.
- A process that dies mid-delivery holds its row until its claim lease ends (`claim_lease_seconds`, 60); the next sweep then delivers it again.
- Each row is one (event, listener) pair, so one failing listener never blocks or repeats the others.

## The `modulith outbox` commands

Every command runs in its own process against the database in `outbox_url`. Run them with the app's configuration and environment.

| Command | Use it to | Notes |
|---|---|---|
| `modulith outbox status` | see how much is waiting | counts `incomplete`, `completed`, `dead-lettered` |
| `modulith outbox failing` | find what is failing right now | rows that failed at least once and are not dead letters yet, with last error and next retry time |
| `modulith outbox dead-letter` | list what gave up | `--list` is the default |
| `modulith outbox dead-letter --retry-all` | replay every dead letter after fixing the cause | each gets a fresh attempt budget |
| `modulith outbox retry <publication-id>` | deliver one row now, ignoring its backoff | the id is the UUID the lists print |
| `modulith outbox purge --older-than 30d` | delete old completed rows | default `30d`; units `d`, `h`, `m`, `s` |

```bash
$ modulith outbox status
incomplete:    1
completed:     3
dead-lettered: 0

$ modulith outbox failing
1 failing publication(s):
  e0dfa129-c145-4d62-b14b-7334612e192f  myapp.contracts.events.OrderCreated  listener=myapp.payments.charge  attempts=1  last_error=card declined  next_retry_at=2026-11-14T02:56:33+00:00

$ modulith outbox dead-letter
1 dead-lettered publication(s):
  e0dfa129-c145-4d62-b14b-7334612e192f  myapp.contracts.events.OrderCreated  listener=myapp.payments.charge  attempts=2  last_error=card declined

$ modulith outbox dead-letter --retry-all
resubmitted 1 dead-lettered publication(s)
```

What to know before running them:

- `retry` and `dead-letter --retry-all` **run the listener inside the command's own process**. Run them where the listener's credentials, network access and environment are.
- `retry` exits 0 once the listener was run, even when it raised again. Check `modulith outbox status` afterwards. It exits 1 for an id that is not a UUID, an unknown or already completed row, or a row another process is delivering.
- With no store bound the commands exit 1 with a message that starts `no outbox store:`. The usual cause is a missing `outbox_url`.
- `completed` stays 0 under `completion_mode = "delete"`.

## Listeners must be safe to run twice

Delivery is at least once: a retry, a replay with `--retry-all`, or a crash between the listener's work and the completion mark runs the listener again with the same event.
Make the second run change nothing:

```python
# in myapp/orders/__init__.py
from modulith import listener

from myapp.contracts.events import PaymentReceived
from myapp.database import sessionmaker
from myapp.orders.models import Order


@listener
async def on_payment(event: PaymentReceived) -> None:
    async with sessionmaker() as session:
        order = await session.get(Order, event.order_id)
        if order is None or order.fulfilled:
            return  # already handled: a retry or replay changes nothing
        order.fulfilled = True
        await session.commit()
```

Check by a key the event carries (here the order id), or record processed ids in a table with a unique constraint. For a call to an outside service, pass that key as its idempotency key.

## One process per module

Under `--topology processes` no worker runs `main.py`, so the lifespan above never runs there. What takes its place:

- Each worker binds the store from `outbox_url` and starts the retry loop itself. `outbox_url` (or `MODULITH_OUTBOX_URL`) is therefore mandatory.
- A worker whose `outbox` is not `memory` and that has no store **refuses to start**: `outbox is 'postgres' but no outbox store is bound in this worker: ...`. The proxy stays up and answers 502 while the workers crash-loop; see [troubleshooting.md](troubleshooting.md).
- The session wiring in your module code is unchanged. For an event that other modules listen to, what is saved in your transaction is the send to the broker: it happens after the commit and is retried like a listener. See [processes-and-brokers.md](processes-and-brokers.md).

## Tuning and claim strategies

Settings go under `[tool.modulith.outbox_options]` and apply to the store built from `outbox_url`. An unknown key is an error at startup. The full list with every bound is in [configuration.md](configuration.md).

```toml
[tool.modulith.outbox_options]
dead_letter_after_attempts = 10    # default
retry_interval_seconds = 30        # default: how often the sweep runs
max_retry_backoff_seconds = 300    # default: longest wait between attempts
completion_mode = "update"         # "update" keeps delivered rows, "delete" removes them, "archive" moves them
sqlite_wal = true                  # SQLite only: readers stop blocking the commit
```

Every process that shares the table runs its own sweep; `claim_strategy` keeps them off each other's rows, and all of them must use the same one.

| `claim_strategy` | What it does | Use when |
|---|---|---|
| `"lease"` (default) | a sweep claims rows for `claim_lease_seconds` (60), renews while the listener runs, and only the claim holder may complete the row | always, unless you have a reason |
| `"advisory_lock"` | holds a PostgreSQL advisory lock per row during delivery | PostgreSQL only; refused at startup elsewhere |
| `"none"` | no coordination: two processes may deliver the same row at once | a single process, never more |

A lease is renewed while its listener runs, for at most ten lease lengths (600 s by default). A listener that runs longer may be delivered again by another process, so raise `claim_lease_seconds` for slower listeners.

## Tests

The pytest fixtures give every test a fresh runtime, which reads the same `pyproject.toml`. With `outbox_url` set, a test that publishes inside a bound session writes to that database.
Mark a test `@pytest.mark.modulith_no_outbox` to bind no store, so every `publish()` in it is delivered in memory.
Fixtures, `Scenario` and patterns are in [testing.md](testing.md).

## What `modulith doctor` checks

The `outbox health` line, from the same counts as `outbox status`:

| Line | Status | Meaning |
|---|---|---|
| `in-memory bus, no durable outbox` | ok | `outbox = "memory"` |
| `N incomplete, N completed, N dead-lettered` | ok | no dead letter, at most 1,000 incomplete |
| the same, with at least one dead letter or more than 1,000 incomplete | warn | something needs replaying, or delivery is falling behind |
| the same, with more than 100 dead letters | error | `doctor` exits 1 |
| `outbox configured as 'postgres' but no store is wired` | warn | `outbox_url` is missing |
| `outbox status query timed out after 10.0s` | error | the database did not answer |

Run `modulith doctor` after any change to outbox settings. All nine checks are in [deployment.md](deployment.md).

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| Setting `outbox = "postgres"` and `outbox_url`, and nothing else | without a bound session every `publish()` is still delivered directly; nothing is saved and nothing warns | wrap each write as in step 4, then confirm with `modulith outbox status` that `completed` grows |
| `await session.commit()` first, then `await publish(...)` | the publish joins a new transaction that is never committed, so the event is discarded | publish before the commit |
| Binding the session in a FastAPI dependency and committing in its teardown | the commit runs after the response; a failure there still answered 200 | bind, publish, commit and unbind inside the service function |
| Calling `outbox.configure(...)` or `outbox.start()` only in `main.py` | the CLI and every process-topology worker never run `main.py` | set `outbox_url`; keep only `bootstrap()`, `outbox.start()` and `outbox.shutdown()` in the lifespan |
| A `[tool.modulith.outbox]` table for tuning | `outbox` is the adapter name; the table is rejected at startup | `[tool.modulith.outbox_options]` |
| Pointing `outbox_url` at a separate "events" database | the event and the data no longer commit together | use the business database |
| A sync URL such as `postgresql://` or `sqlite:///` | the store needs an async driver | `postgresql+asyncpg://`, `mysql+aiomysql://`, `sqlite+aiosqlite:///` |
| Catching every exception inside a listener "so it never fails" | the row is marked delivered and the event is gone | let it raise: the outbox retries and dead-letters it |
| Running `modulith outbox dead-letter --retry-all` from a laptop to "poke production" | the listeners run on the laptop, with the laptop's environment | run it in the app's environment |
| Assuming a listener that publishes is durable because the first event was | each hop needs its own bound, committed session | bind a session in the listener too |
| Expecting `modulith outbox status` to see a store configured in a lifespan | the command is another process | set `outbox_url` |

Further reading: [Durable Single-Process](https://github.com/jeansossmeier/modupy/blob/main/docs/DEPLOYMENT.md#durable-single-process-outbox-pattern) and [ARCHITECTURE.md section 7](https://github.com/jeansossmeier/modupy/blob/main/docs/ARCHITECTURE.md#7-the-transactional-outbox).
