# One process per module, and the brokers between them

Read this when:

- the app runs, or should run, with `--topology processes` (or `topology = "processes"`);
- something works in one process and breaks under the process topology;
- you are choosing or configuring a broker, giving a module more processes, or handling broker dead letters.

## Contents

- [The two shapes](#the-two-shapes)
- [Start it](#start-it)
- [What runs](#what-runs)
- [What never reaches a worker](#what-never-reaches-a-worker)
- [Replicas and `own_process`](#replicas-and-own_process)
- [How an event crosses processes](#how-an-event-crosses-processes)
- [Brokers](#brokers)
- [Broker retries and dead letters](#broker-retries-and-dead-letters)
- [A retired module's consumer group](#a-retired-modules-consumer-group)
- [Health and readiness](#health-and-readiness)
- [What a process of its own contains](#what-a-process-of-its-own-contains)
- [Mistakes that look right](#mistakes-that-look-right)

## The two shapes

| | `topology = "single"` (default) | `topology = "processes"` |
|---|---|---|
| Processes | one: your `main.py` app | a supervisor with a reverse proxy, plus one worker per module |
| Started with | `uvicorn myapp.main:app`, `modulith dev`, `modulith run` | `modulith run myapp.main:app --topology processes` |
| `main.py` | runs | runs in **no** worker |
| Events between modules | in memory | through a broker, as JSON |
| A module's memory | shared by everything | private to each process |
| URLs | whatever `main.py` mounts | `/<module>/...`, always |

Module code, events and listeners are the same in both. Keep `main.py` mounting each module's `router` at `/<module>` so the URLs are the same too.

## Start it

```bash
$ modulith run myapp.main:app --topology processes
modulith → process topology: 3 worker(s) [inventory:9001, orders:9002, payments:9003], reverse proxy on http://0.0.0.0:8000
```

- `topology = "processes"` under `[tool.modulith]` (or `MODULITH_TOPOLOGY=processes`) makes it the default, so the flag can be dropped.
- `modulith dev myapp.main:app --topology processes` does the same on `127.0.0.1` and prints boundary violations as warnings; with `strict_boundaries` on, it refuses to start, as `run` does. `--reload` has no effect under the process topology: restart the command after a code change.
- `modulith dev myapp.main:app --isolate orders` starts only the `orders` worker; every other module's routes answer 404.
- Other flags: `--host`, `--port` (8000), `--worker-port-base` (9001), `--workers '{"payments": 2}'` (`run` only), `--log-level`. See [cli.md](cli.md).
- The app argument is still required. Workers do not serve that app; it names the package and is checked for setup the workers will not run.

## What runs

- **The main process** holds the supervisor and the reverse proxy on `--host`/`--port`. It imports none of your modules.
- **One worker per module**, each a uvicorn process on `127.0.0.1`, on consecutive ports from `worker_port_base` in alphabetical module order. The contracts module gets no worker.
- **A worker serves one thing over HTTP:** its module package's `router` attribute, mounted under `/<module>`. A module without a `router` (listener-only, like `payments`) still runs its listeners.
- **The proxy** forwards `/<module>/...` unchanged to that module's worker and spreads requests across its replicas. Any other path answers `404 {"detail":"no worker route for '/docs'"}`. It forwards HTTP only, not WebSockets, and refuses a request body over 10 MiB with 413 (`MODULITH_PROXY_MAX_BODY_BYTES` raises the cap).
- **API docs** are per module, on the public port: `/<module>/docs`, `/<module>/redoc`, `/<module>/openapi.json`. `/docs` is a 404.
- **Crashes:** the supervisor restarts a crashed worker after 1, 2, 4, 8, then 16 seconds (`worker orders exited with code 1; restarting in 2.0s`). After six crashes in a row it stops: `worker orders crashed 6 times in a row with no healthy run in between (last exit code 1) — giving up; not respawning.` A run of 60 seconds resets the count. Meanwhile the proxy answers `502 {"detail":"backend unreachable"}` for that module.
- **Logs:** the supervisor prints every worker line with the worker's name in front, such as `[orders]`.
- **Stop:** SIGTERM to the main process stops the proxy, then every worker; a worker still alive after 30 seconds is killed.

## What never reaches a worker

A worker builds its own app from the module's `router`. Nothing your `main.py` adds is there:

| In `main.py` | Under the process topology | Where it must live |
|---|---|---|
| a login check, rate limit or other `dependencies=` on `FastAPI(...)` or on `include_router(...)` | not run: the routes are open | on the module's own router (below) |
| middleware (CORS, sessions, request ids) | not run | in a reverse proxy in front of modupy |
| exception handlers | not run: the exception becomes a 500 | raise `HTTPException` in the module |
| the lifespan | not run | outbox: set `outbox_url` ([outbox.md](outbox.md)); other resources: create them when the module is imported, or on first use |
| routes defined on `app` itself, a prefix other than `/<module>` | do not exist | a module's `router` |
| `logging.basicConfig(...)` | not run: a worker prints only WARNING and above from your own loggers | configure logging where the module is imported |

```python
# myapp/auth.py: a plain file beside the modules, importable from any of them
from fastapi import Header, HTTPException


async def require_user(authorization: str = Header(default="")) -> None:
    if authorization != "Bearer secret":  # your real token check goes here
        raise HTTPException(status_code=401, detail="login required")
```

```python
# myapp/orders/api.py
from fastapi import APIRouter, Depends

from myapp.auth import require_user

router = APIRouter(dependencies=[Depends(require_user)])  # runs in every shape
```

```python
# myapp/orders/__init__.py, last line: the worker mounts exactly this attribute
from myapp.orders.api import router as router  # noqa: E402
```

`modulith run` and `modulith dev` check `main.py` at startup and warn, naming what it found:

```text
WARNING:  myapp.main:app sets up what no worker runs: under the process topology a worker serves only its modules' routers under /<module>, and myapp.main never runs there.
  middleware: CORSMiddleware
  dependencies added to the routes of orders: require_user
  exception handlers: OutOfStock
Put a check a module's routes need on that module's own router, APIRouter(dependencies=[Depends(...)]), raise HTTPException in the module instead of relying on an app exception handler, and put app-wide middleware such as CORS in a proxy in front of modupy. ...
```

The warning is only a warning: the workers start and serve the routes without the check. It does not report the lifespan, and it is skipped when importing `main.py` takes more than 5 seconds. Treat any such line as a bug to fix before shipping.

Two more startup lines to act on:

- `module 'orders' exposes no 'router' attribute — this worker serves no HTTP routes, so every request to /orders/... returns 404.` Expected for a listener-only module; otherwise re-export the router as above.
- `no module exposes a 'router' attribute (...)`: nothing is served at all.

## Replicas and `own_process`

```toml
[tool.modulith]
topology = "processes"
own_process = ["payments"]            # payments alone; all other modules share one worker
worker_liveness_timeout_seconds = 30  # restart a worker that stops answering

[tool.modulith.workers]
default = 1
payments = 2                          # two payments processes
```

```text
modulith → process topology: 2 worker(s) [inventory,orders:9001, payments:9002], reverse proxy on http://0.0.0.0:8000
```

- **Replicas.** Each replica is a separate process on its own port (`payments` above takes 9002 and 9003). The proxy spreads requests across them and each event is handled by one of them, so keep that module's state in the database, not in module-level variables. `--workers '{"payments": 2}'` replaces the whole table for one run.
- **`own_process`.** Without it, every module gets its own worker. With it, only the named modules do, and all the others share one worker and talk in memory, as in a single process. Use it to isolate one risky module without putting a broker hop between the rest. It has no environment variable.
- A shared worker serves its API docs under its first module's prefix only (`/inventory/docs` above; `/orders/docs` answers 404).
- A name in `workers`, `--workers` or `own_process` that is not a discovered module stops the start with exit 1 and lists the valid names.

## How an event crosses processes

1. **An event with no listener in the publishing process goes to the broker automatically.** `orders` publishes `OrderCreated`; only `payments` and `inventory` listen; nothing to mark.
2. **An event the publishing module also listens to stays in that process, unless it is marked `@externalized`.** Without the mark, the other modules' listeners never run and nothing warns.

   ```python
   # myapp/contracts/events.py
   from dataclasses import dataclass

   from modulith import event, externalized


   @externalized  # orders listens to it too, so send it to the other workers explicitly
   @event
   @dataclass(frozen=True)
   class OrderCreated:
       order_id: str
   ```

   In one process `@externalized` does nothing, so it is safe to add early. The publishing module's own listeners still run once only.
3. **Each module consumes in its own group**, `modulith-<module>` (for example `modulith-payments`). Replicas of a module share the group, so one of them handles each event.
4. **Events travel as JSON.** Fields may be `str`, `int`, `float`, `bool`, `None`, `datetime`, `date`, `UUID`, `Decimal`, `Enum`, a nested dataclass, a `list`, `tuple`, `set`, `frozenset` or `dict` of those. Anything else makes `publish()` raise `TypeError: cannot JSON-serialize Customer in event payload`. A payload over `max_payload_bytes` (16 MiB) is refused with a `ConfigurationError`.
5. **Delivery is asynchronous and at least once.** `publish()` returns once the broker has the event, before any other module handled it, so a read right after a write can find the old state. Listeners must be safe to run twice.
6. **Without the outbox, the send happens inside `publish()`**, and a broker failure raises in your caller. With the outbox and a bound session, the send is saved with your data and made after the commit ([outbox.md](outbox.md)).
7. **A direct call into another module runs in the caller's process.** If `payments` imports and calls `orders.is_fulfilled()`, it runs against the `payments` worker's own copy of `orders`' memory. Read shared state from the database, or react to events.

## Brokers

Set `broker` under `[tool.modulith]` or `MODULITH_BROKER`. When it is unset under the process topology, modupy picks `database` if a broker URL is configured and `shm` otherwise, and logs a warning that names the choice.

| Broker | Install | Reaches | Good for |
|---|---|---|---|
| `memory` | nothing | one process | `topology = "single"` only; with `processes` it is refused: `topology='processes' requires a cross-process broker, but broker='memory' was explicitly configured.` |
| `shm` | nothing | one host | the default: a SQLite file plus a small notification file in `state_dir` |
| `database` | `modupy[database]` | one host on a SQLite file; many hosts on PostgreSQL, MySQL 8.0.1+ or MariaDB 10.6+ | reusing the database you already run |
| `redis-streams` | `modupy[redis]`, Redis 6.2+ | many hosts | high volume, Redis already in place |

```toml
# shm: pin the store's location in production
[tool.modulith]
topology = "processes"
broker = "shm"

[tool.modulith.broker_options]
state_dir = "/var/lib/myapp/modulith"   # or MODULITH_BROKER_STATE_DIR
```

```toml
# database
[tool.modulith]
topology = "processes"
broker = "database"

[tool.modulith.broker_options]
url = "postgresql+asyncpg://app@db/app"   # or MODULITH_BROKER_URL; sqlite+aiosqlite:////path/broker.db on one host
```

```toml
# redis-streams
[tool.modulith]
topology = "processes"
broker = "redis-streams"

[tool.modulith.broker_options]
url = "redis://cache:6379"   # or REDIS_URL; default redis://localhost:6379
max_stream_len = 100000      # or MODULITH_STREAM_MAXLEN; default 10000
```

Options go in `[tool.modulith.broker_options]`; most also read `MODULITH_BROKER_<KEY>`, which wins. The full key list is in [configuration.md](configuration.md). What decides correctness:

- **`shm`: set `state_dir`.** The default directory is derived from the package's install path, so a redeploy to a new path opens an empty store and strands undelivered events. `modulith run` warns while the default is in use (`SHM broker state_dir is not set: ...`). modupy creates the directory with mode 0700; an existing one must already be 0700 and owned by the service user.
- **`shm` and a SQLite `database` broker work on one machine only.** For several machines or Kubernetes use PostgreSQL, MySQL or Redis ([deployment.md](deployment.md)).
- **`database`:** the broker creates its `broker_*` tables on first use, or `modulith migrate --url <broker url>` creates them ahead of time. With no `url` it falls back to a local SQLite file and warns; with `production = true` that is an error. Publishing an event that no module has subscribed to raises `NoSubscribersError: database broker target '...' has no subscribers` (`no_subscriber_policy`, default `"error"`).
- **`redis-streams`:** every stream is trimmed to `max_stream_len` entries whether or not they were consumed. Size it well above the worst backlog, or events are lost. Events are only as durable as Redis itself: turn on AOF persistence.
- Do not write a `[tool.modulith.broker]` table next to `broker = "..."`: TOML forbids one key being both. Use `broker_options`.

## Broker retries and dead letters

A listener that raises on a broker delivery is retried by the broker, independently of the outbox:

| | `shm`, `database` | `redis-streams` |
|---|---|---|
| Attempts before the delivery is a dead letter (`max_delivery_attempts`) | 5 | 5 |
| Wait before the next attempt | 0.05 s, doubling, capped at 5 s (`retry_backoff_base_seconds`, `retry_backoff_cap_seconds`) | until the message has been idle for `reclaim_min_idle_ms` (60000), plus the same backoff |
| A worker died mid-delivery | redelivered after `reclaim_stale_seconds` (60) | redelivered after `reclaim_min_idle_ms` |
| Payload that cannot be decoded | dead letter at once | dead letter at once |

What the supervisor's output shows:

```text
WARNING:  [inventory] listener reserve failed for OrderCreated: warehouse offline
WARNING:  [inventory] dispatch failed for cold:1:1 (attempt 1) -- warehouse offline
WARNING:  [inventory] delivery cold:1:5 of myapp.contracts.events.OrderCreated on myapp.contracts.events.OrderCreated for group modulith-inventory dead-lettered after 5 attempt(s): warehouse offline
```

```bash
$ modulith broker dead-letter
1 dead-lettered message(s):
  1  myapp.contracts.events.OrderCreated  target=myapp.contracts.events.OrderCreated  group=modulith-inventory  attempts=5  last_error=warehouse offline

$ modulith broker dead-letter --retry-all    # after fixing the listener
resubmitted 1 dead-lettered message(s)
```

- Run both with the service's broker configuration and environment (the same `state_dir`, URL and so on). They exit 1 when the store does not exist: `error: no shm broker store at ...`.
- `--retry-all` puts each dead letter back in its group's queue; the **running workers** deliver it. This differs from `modulith outbox dead-letter --retry-all`, which runs the listeners inside the command.
- On `redis-streams`, `--retry-all` resubmits a target only when its stream has one consumer group, and exits 1 naming the targets it left.
- A hanging `async def` listener blocks its delivery slot. Bound it: `@listener(timeout=30)` cancels the call after 30 seconds and counts a failed attempt. Only `async def` listeners take a timeout.

## A retired module's consumer group

On `shm` and `database`, every event is queued once per subscribed group. When a module is deleted or renamed, its group `modulith-<module>` keeps its subscription and collects events that nothing will ever consume.

1. Remove the module and restart `modulith run`.
2. Drop the group, with the service's broker configuration:

```bash
$ modulith broker drop-group modulith-inventory --force --yes
shm broker store: /var/lib/myapp/modulith/.modulith-shm-broker.db
dropped group 'modulith-inventory': 1 subscription(s), 1 pending or claimed delivery(ies)
```

- The queued messages are deleted, not delivered.
- Without `--yes` it asks for confirmation and states how many deliveries it will delete.
- Without `--force`, a group that a current module uses, or that a consumer served in the last 24 hours, is refused: `error: 'modulith-inventory' belongs to a module of the current deployment or a consumer served it in the last 24 h. ... Pass --force to drop it anyway.` A module you just retired is inside those 24 hours, so pass `--force` once you are sure nothing still runs it.
- `--target <event target>` (repeatable) drops only that target's subscription and backlog, for a module that stopped listening to one event.
- An unknown group exits 1: `error: group 'modulith-ghost' has no subscription or undelivered work in the shm broker's store; nothing was removed`.
- The process topology (`modulith run` or `modulith dev`) warns at startup about a group that no module uses and no consumer served for 24 hours: `broker group 'modulith-inventory' is not derived by any module of this deployment ... run: modulith broker drop-group modulith-inventory`.
- On `redis-streams` there is nothing to drop: `error: broker 'redis-streams' keeps no per-group subscription ledger; drop-group applies to shm and database`. Remove a stale group in Redis itself with `XGROUP DESTROY <stream> modulith-<module>`; a stream is named `modulith.events.<event module>.<EventClass>`.

## Health and readiness

The proxy serves three endpoints on the public port:

| Endpoint | Answers |
|---|---|
| `GET /_modulith/live` | `200 {"status":"ok"}` while the proxy runs; asks no worker |
| `GET /_modulith/health` | asks every worker; `200 {"status":"ok","backends":{"/inventory":"ok","/orders":"ok","/payments":"ok"}}`, or 503 with `"status":"degraded"` |
| `GET /_modulith/topology` | each prefix with its worker addresses |

- A module's state is `ok`, `unhealthy` (its worker answered non-200), `unreachable` (down or restarting), `failed (given up)` (the supervisor stopped restarting it) or `foreign deployment` (another program holds the worker's port).
- A module named in `own_process` that is down makes the body `degraded` but leaves the status 200, so the other modules stay in rotation.
- **These endpoints are not mounted on a non-loopback `--host` or with `production = true` unless `MODULITH_ACTUATOR_TOKEN` is set**, and `modulith run` binds `0.0.0.0` by default. They then answer `404 {"detail":"no worker route for '/_modulith/health'"}`. Modes and tokens are in [deployment.md](deployment.md).
- Each worker has its own `GET /health` on its internal port: `200 {"status":"ready","module":"inventory","ready":true}`, or 503 while its broker consumer is not ready. The proxy never forwards `/health`; on the public port it is a 404.
- **A worker that hangs is not restarted by default.** Set `worker_liveness_timeout_seconds` (or `MODULITH_WORKER_LIVENESS_TIMEOUT_SECONDS`): the supervisor then kills a worker whose `/health` has gone unanswered that long and restarts it as after a crash. A 503 counts as an answer.

## What a process of its own contains

| Contained: the other modules keep answering | Not contained |
|---|---|
| a crash of that module's process | the host running out of memory or CPU |
| a blocked or spinning event loop (restarted only with `worker_liveness_timeout_seconds`) | the database server running out of connections |
| that process's exhausted connection pool | the module's own routes, which fail while it is down |

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| Keeping the login check in `main.py` because "it works locally" | no worker runs `main.py`; the routes are open under the process topology | `APIRouter(dependencies=[Depends(require_user)])` on the module's router |
| Defining `router` in `orders/api.py` only | the worker mounts `myapp.orders.router`; every `/orders/...` is a 404 | re-export it from `orders/__init__.py` |
| Mounting a module at `/api/orders` in `main.py` | under the process topology it is `/orders` | mount every module at `/<module>` |
| Keeping state in a module-level `dict` | each process and each replica has its own copy | the database |
| Calling another module's function to read its in-memory state | the call runs in the caller's process, on an empty copy | read from the database, or listen to an event |
| A module listening to an event it publishes itself, unmarked | the event never leaves that worker | `@externalized` on the event |
| `broker = "memory"` with `topology = "processes"` | refused at startup | leave `broker` unset for `shm`, or name a real broker |
| An event field holding a model object or a custom class | `publish()` raises `TypeError: cannot JSON-serialize ...` across processes, while it worked in one | use plain values, dataclasses, enums, UUIDs, dates |
| Asserting on another module's state right after the HTTP call | the event has not been handled yet | poll, or wait for the resulting event |
| `--workers '{"payments": 2}'` to add to the table | it replaces `[tool.modulith.workers]` entirely | list every non-default module in the JSON |
| Probing `/health` on port 8000 | the proxy answers 404 for it | `/_modulith/live` and `/_modulith/health` |
| Running `shm`, or `database` on SQLite, across machines or pods | each host has its own file; events never arrive | PostgreSQL, MySQL or Redis |
| Expecting `modulith broker dead-letter --retry-all` to run the listeners | it only re-queues; the workers must be running | start the workers, then check `modulith broker dead-letter` again |
| Editing code under `modulith dev --topology processes` and waiting for a reload | there is none in this topology | restart the command |

Further reading: [Process-Per-Module Topology](https://github.com/jeansossmeier/modupy/blob/main/docs/DEPLOYMENT.md#process-per-module-topology) and [ARCHITECTURE.md sections 8 and 10](https://github.com/jeansossmeier/modupy/blob/main/docs/ARCHITECTURE.md#8-cross-process-delivery).
