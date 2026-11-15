# Troubleshooting: symptom, cause, fix

Read this when something does not behave as expected: an event that never arrives, a command that fails to import the app, a 404 or 502 behind the proxy, a refused start, a growing dead-letter pile.
Find the symptom below, run the diagnostic command, match the message, apply the fix.

## Start here

```bash
modulith info      # package, discovered modules, outbox, broker, topology
modulith verify    # boundary violations
modulith doctor    # the nine health checks; exit 1 on an error
```

The same three facts appear in the startup log as `discovered 4 module(s): contracts, inventory, orders, payments` and `outbox=memory, broker=memory, topology=single`.

| Symptom | Section |
|---|---|
| A listener never runs | [A listener never fires](#a-listener-never-fires) |
| A module is missing from `modulith info` | [A module is not discovered](#a-module-is-not-discovered) |
| `ModuleNotFoundError`, `failed to import`, `could not determine the application package` | [A CLI command cannot import the app](#a-cli-command-cannot-import-the-app) |
| `modulith verify` prints violations | [Boundary violations](#boundary-violations) |
| `boundary violations detected with strict_boundaries=True` | [`strict_boundaries` refuses to boot](#strict_boundaries-refuses-to-boot) |
| Events disappear after a crash or restart | [Events lost on a crash](#events-lost-on-a-crash) |
| 404 behind the proxy | [A 404 through the proxy](#a-404-through-the-proxy) |
| 502 `backend unreachable`, a worker keeps restarting | [A 502, or a worker that keeps restarting](#a-502-or-a-worker-that-keeps-restarting) |
| Routes answer without a login | [A login check stopped guarding routes](#a-login-check-stopped-guarding-routes) |
| `TypeError: cannot JSON-serialize ...` | [An event will not serialize](#an-event-will-not-serialize) |
| `database is locked` | ["database is locked" on SQLite](#database-is-locked-on-sqlite) |
| Dead letters pile up | [A dead-letter pile](#a-dead-letter-pile) |
| `broker group ... is not derived by any module` | [A retired module's consumer group](#a-retired-modules-consumer-group) |
| `modulith outbox ...` says `no outbox store:` | [Outbox commands find no store](#outbox-commands-find-no-store) |
| `/_modulith/health` answers 404 | [The health endpoints answer 404](#the-health-endpoints-answer-404) |

## A listener never fires

Work down the list; stop at the first cause that matches.

1. **The file holding the `@listener` is never imported.** A module's `__init__.py` must import it; discovery imports only the package itself.

   ```bash
   python -c "import sys, myapp.payments; print('myapp.payments.handlers' in sys.modules)"
   ```

   `False` means nothing imports `handlers.py`. Add `from myapp.payments import handlers as handlers  # noqa: E402` at the bottom of `myapp/payments/__init__.py`; at the top it fails with a circular import when `handlers.py` imports from its own package.
2. **The module is not discovered.** See the next section.
3. **The listener is annotated with a different class than the one published.** Events are matched by the exact class. A second `OrderCreated` defined inside `payments` is another class. Import the one from `myapp.contracts.events` on both sides.
4. **The publish was thrown away with its transaction** (outbox on). Look for `discarded N outbox publication(s) of myapp.contracts.events.OrderCreated: the bound session's transaction ended without committing` in the log. Publish before the final commit ([outbox.md](outbox.md)).
5. **It ran and failed** (outbox on). A failing listener logs nothing until it is a dead letter.

   ```bash
   modulith outbox failing        # last_error and next_retry_at per row
   modulith outbox dead-letter
   ```

6. **Under the process topology, the publishing module also listens to that event.** The event then stays in the publisher's worker and nothing warns. Mark the event `@externalized` ([processes-and-brokers.md](processes-and-brokers.md)).
7. **Under the process topology, the listener's worker is down, or the delivery failed five times.** Read the supervisor's output for `[payments]` lines, then:

   ```bash
   modulith broker dead-letter
   ```

8. **It did run, in another process.** Under the process topology a listener changes its own worker's memory. State kept in a module-level variable is invisible to other workers and replicas; keep it in the database. A read right after the request can also come before the event was handled: wait or poll.
9. **In a test, the listener was imported before the fixture reset the runtime.** Import app code inside the test function ([testing.md](testing.md)).

## A module is not discovered

```bash
$ modulith info
  package: myapp

  modules (4):
    - contracts  (myapp.contracts)  [no manifest]
    ...
```

A module is a **direct subpackage** of the application package. It is skipped, silently, when:

| The directory | Fix |
|---|---|
| has no `__init__.py` | add one (it may be empty) |
| has a name starting with `_` | rename it; a leading underscore means private |
| is a single file, `myapp/shipping.py` | make it a package: `myapp/shipping/__init__.py` |
| is nested deeper, `myapp/domain/shipping/` | move it directly under `myapp/` |

Also check:

- the `package:` line names the package you expect. If not, set `package = "myapp"` under `[tool.modulith]`;
- `auto_discover` is not set to `false`;
- `[no manifest]` is normal: manifests are optional.

A module that is found but fails to import stops every command with:

```text
error: module discovery recorded import failure(s):
  - module 'myapp.orders' failed to import: ModuleNotFoundError("No module named 'stripe'")
```

Fix the import named there; the full traceback is printed above the message.

## A CLI command cannot import the app

Every command except `init`, `add module`, `skill install`, `audit` and `migrate` imports your modules. The CLI finds the package through the `MODULITH_PACKAGE` environment variable, else `[tool.modulith].package`, else `[project].name` (with `-` turned into `_`), in the nearest `pyproject.toml` at or above the working directory, and puts that file's directory on the import path. Nothing else.

| Message | Cause | Fix |
|---|---|---|
| `error: the application package 'my_shop' failed to import (ModuleNotFoundError: No module named 'my_shop').` | the project name is not the import package | `[tool.modulith] package = "shop"` |
| the same, with the right package name | the package is not next to `pyproject.toml` (a `src/` layout), or you ran the command outside the project | `pip install -e .`, or run from the project root |
| the same, naming a package you do not know | a nearer `pyproject.toml` in a subdirectory was picked up | run from the directory of the right `pyproject.toml`, or set `MODULITH_PACKAGE` |
| `module discovery recorded import failure(s): ... ModuleNotFoundError("No module named 'stripe'")` | one of the app's dependencies is missing in the environment the `modulith` command runs in | install the app's dependencies in the same environment as modupy |
| `error: could not determine the application package. Set [tool.modulith].package or [project].name in pyproject.toml, or the MODULITH_PACKAGE environment variable.` | no `pyproject.toml` with a name was found | as the message says |
| `modulith CLI requires the 'cli' extra.` | typer is not installed | `pip install 'modupy[cli]'` |

`pip install modulith` installs an unrelated project. The distribution is `modupy`; the import package and the command are `modulith`.

## Boundary violations

```bash
$ modulith verify

payments:
  [ERROR] no-internal-imports: payments imports _orders from orders, reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/payments/__init__.py:4)

1 error(s), 0 warning(s)
```

It exits 1 on any `[ERROR]`. Fix the code at the location shown: use the other module's public function, or an event from the contracts module. Each rule and its fix is in [modules-and-contracts.md](modules-and-contracts.md). Do not silence a rule or widen the baseline to make a new violation pass; the baseline is for adopting existing code ([adopting-existing-code.md](adopting-existing-code.md)).

## `strict_boundaries` refuses to boot

```text
error: boundary violations detected with strict_boundaries=True:
  - [ERROR] payments: no-internal-imports: payments imports _orders from orders, ... (myapp/payments/__init__.py:4)
```

- **Cause:** `strict_boundaries = true` (or `MODULITH_STRICT_BOUNDARIES=true`) stops the start on **any** violation, warnings included.
- **Where it appears:** `modulith run --topology processes`, `modulith dev --topology processes`, a single-process app at its first `bootstrap()` or `publish()`, and the commands `info`, `openapi`, `k8s-manifest`, `outbox ...` and `broker ...`.
- **What still runs:** `modulith verify`, `doctor`, `docs`, `extract` and `migrate`. Single-process `modulith dev` only logs the violations.
- **Fix:** run `modulith verify`, fix what it lists, start again. With warning-level violations use `modulith verify --fail-on-warnings`: plain `verify` exits 0 on warnings, while the boot refuses them.
- While adopting modupy in existing code, leave `strict_boundaries` off and use the ratchet ([adopting-existing-code.md](adopting-existing-code.md)).

## Events lost on a crash

```bash
modulith info            # read the "outbox:" line
modulith outbox status   # with the outbox on: does "completed" grow as you publish?
```

| Finding | Cause | Fix |
|---|---|---|
| `outbox: memory` | nothing is stored; an event dies with the process, and a failing listener is not retried | turn the outbox on ([outbox.md](outbox.md)) |
| outbox on, but `completed` and `incomplete` stay 0 while the app publishes | `publish()` runs with no session bound, so it is delivered directly | `bind_session(session)` around the write, and commit after the publish |
| `discarded N outbox publication(s) ...` in the log | the session was rolled back, or closed with no commit after the publish | publish before the final commit |
| rows sit in `incomplete` after a restart | `bootstrap()` and `outbox.start()` are missing from the lifespan, so nothing sweeps until the next publish | add both to the lifespan's startup |
| process topology, `shm` broker, events gone after a redeploy | the store sat in the default directory, which depends on the install path (`SHM broker state_dir is not set: ...` at startup) | set `state_dir` under `[tool.modulith.broker_options]` |
| process topology, no outbox | the send to the broker happens inside `publish()`, separately from your commit | turn the outbox on, with `outbox_url`, so the send is saved with your data |
| `redis-streams` broker | streams are trimmed to `max_stream_len`, and Redis without AOF forgets on restart | raise `max_stream_len`; enable Redis persistence |

## A 404 through the proxy

Read the body first; it says who answered.

**`{"detail":"no worker route for '/docs'"}` comes from the proxy:** no module owns that path.

- The path does not start with `/<module>`. Under the process topology every route lives under its module's name. `/docs`, `/openapi.json` and `/` do not exist; use `/orders/docs`.
- The route is defined on `app` in `main.py`, or `main.py` mounts the module under another prefix such as `/api/orders`. No worker runs `main.py`. Move the route onto a module's `router`, and mount modules at `/<module>`.
- The path is `/health`. The proxy never forwards it.
- The module was left out with `--isolate`, or it was added after `modulith run` started. Restart the command.
- The path is `/_modulith/...`: see [The health endpoints answer 404](#the-health-endpoints-answer-404).

**`{"detail":"Not Found"}` comes from the module's worker:** the worker has no such route.

- The module package has no `router` attribute. Startup said: `module 'orders' exposes no 'router' attribute — this worker serves no HTTP routes, so every request to /orders/... returns 404.` Add `from myapp.orders.api import router as router` at the end of `myapp/orders/__init__.py`.
- The router carries its own prefix, `APIRouter(prefix="/orders")`. The worker adds `/orders` again, giving `/orders/orders/...`. Remove the router's prefix and let `main.py` pass `prefix="/orders"`.
- Several modules share a worker (`own_process`): only the first one serves API docs, so `/orders/docs` can be a 404 while `/inventory/docs` works.

To see the routing table on a loopback bind: `curl http://127.0.0.1:8000/_modulith/topology`.

## A 502, or a worker that keeps restarting

`502 {"detail":"backend unreachable"}` means the module's worker is not accepting connections. The proxy stays up while a worker crash-loops, so the cause is in the supervisor's output, in the lines prefixed with the module's name:

```text
WARNING:  [orders] modulith.config.ConfigurationError: outbox is 'postgres' but no outbox store is bound in this worker: the application's main.py (its lifespan, middleware and outbox wiring) does not run under --topology processes. Set [tool.modulith].outbox_url (env MODULITH_OUTBOX_URL) ...
WARNING:  worker orders exited with code 1; restarting in 2.0s
```

| In the output | Fix |
|---|---|
| `outbox is 'postgres' but no outbox store is bound in this worker` | set `outbox_url` or `MODULITH_OUTBOX_URL`; `outbox.configure()` in `main.py` is not seen by workers |
| an `ImportError` or other traceback from your module | the module must import on its own: a worker imports only its module and the contracts, never `main.py` |
| `worker orders crashed 6 times in a row with no healthy run in between ... giving up; not respawning.` | fix the cause, then restart `modulith run`; the supervisor will not try again |
| `port 8000 is already in use; choose another --port` or `proxy port ... is also assigned to worker` | pick another `--port`, or move the workers with `--worker-port-base` |
| nothing, and the worker simply hangs | set `worker_liveness_timeout_seconds` so the supervisor restarts a worker that stops answering |

Other proxy answers: `503 {"detail":"no worker of this deployment serves /orders"}` (another program holds that worker's port), `503 {"detail":"proxy connection pool exhausted"}` (more than `MODULITH_PROXY_MAX_CONNECTIONS` requests in flight to one worker), `413 {"detail":"request body too large"}` (over `MODULITH_PROXY_MAX_BODY_BYTES`, 10 MiB by default).

## A login check stopped guarding routes

- **Symptom:** under `--topology processes` a route answers 200 without credentials; in one process it answered 401.
- **Diagnose:** `curl -i` the route on the public port with no credentials, and read the start of the `modulith run` output:

  ```text
  WARNING:  myapp.main:app sets up what no worker runs: ...
    dependencies added to the routes of orders: require_user
  ```

- **Cause:** the check is in `main.py`: a `dependencies=[...]` on `FastAPI(...)` or `include_router(...)`, or a middleware. No worker runs `main.py`.
- **Fix:** put it on the module's own router, which every shape serves: `router = APIRouter(dependencies=[Depends(require_user)])`. Keep `require_user` in a file both can import, such as `myapp/auth.py`. Then repeat the `curl` and expect 401. Full example in [processes-and-brokers.md](processes-and-brokers.md).

The same applies to exception handlers (the route answers 500 instead of your mapped status) and to middleware such as CORS (headers missing).

## An event will not serialize

```text
TypeError: cannot JSON-serialize Customer in event payload
```

- **When:** at `publish()`, as soon as the event has to leave the process or be stored: under the process topology, or with the outbox on. In one process with the memory outbox the same event worked, because nothing serialized it. In a route the caller sees a 500.
- **Cause:** a field holds something JSON cannot carry: an ORM model, a custom class, `bytes`. Allowed: `str`, `int`, `float`, `bool`, `None`, `datetime`, `date`, `UUID`, `Decimal`, `Enum`, nested dataclasses, and `list`, `tuple`, `set`, `frozenset` or `dict` of those.
- **Fix:** carry ids and plain values in the event, and let the listener load the rest. Prove it in a test:

  ```python
  from modulith.serializers import JsonEventSerializer

  from myapp.contracts.events import OrderCreated

  serializer = JsonEventSerializer(allowed_event_types=[OrderCreated])
  event = OrderCreated(order_id="ord-1")
  data = serializer.serialize(event)  # raises TypeError for a field JSON cannot carry
  assert serializer.deserialize(data, "myapp.contracts.events.OrderCreated") == event
  ```

Related messages: `cannot JSON-serialize dict key of type ... in event payload` (use `str`, numbers, dates, UUIDs or enums as keys); `... payload is N bytes, exceeding max_payload_bytes=16777216` (the event is too big: send an id, not the data); `undeserializable message ...` in a worker (the consumer could not rebuild the event, usually because publisher and consumer run different versions of the contracts).

## "database is locked" on SQLite

```text
sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) database is locked
```

SQLite allows one writer at a time; a second writer waits 5 seconds, then fails with this.

| Situation | Why | Fix |
|---|---|---|
| Memory outbox: you `flush()` (or otherwise write), then `publish()`, and a listener writes in its own session | `publish()` runs the listeners before your commit, while your transaction holds the write lock; the listener waits 5 s and fails, and `publish()` re-raises it | publish before the first write is flushed; or turn the outbox on, so listeners run after the commit |
| Outbox on SQLite | in the default journal mode a reader blocks a commit | `sqlite_wal = true` under `[tool.modulith.outbox_options]` |
| Process topology, several workers writing one SQLite file | all writers queue for one lock | fine for development; use PostgreSQL or MySQL for real load |

SQLite files, including the `shm` broker's store and a SQLite `database` broker, also cannot be shared across machines.

## A dead-letter pile

There are two queues. Find out which one holds the pile, then read `last_error`.

| Where | Holds | List | Replay after fixing the cause |
|---|---|---|---|
| the outbox | listeners that failed 10 times after an outbox publish; under the process topology also failed sends to the broker | `modulith outbox dead-letter` | `modulith outbox dead-letter --retry-all` (runs the listeners **in that command**) |
| the broker | listeners in other workers that failed 5 times | `modulith broker dead-letter` | `modulith broker dead-letter --retry-all` (re-queues; the **running workers** deliver) |

The log line for each: `publication <id> dead-lettered after 10 attempt(s): <error>` (outbox) and `delivery <id> of <event> on <target> for group modulith-<module> dead-lettered after 5 attempt(s): <error>` (broker).

1. Group by `last_error`: one cause usually explains the pile (a bug in one listener, an outside service that was down, an event that cannot be decoded).
2. Fix and deploy the cause first. Replaying before that just fails again and, in the outbox, uses up a fresh budget.
3. Replay. Listeners must be safe to run twice, because some of these events may have been half-handled.
4. Check: `modulith outbox status` shows `dead-lettered: 0`; `modulith broker dead-letter` shows `no dead-lettered messages`.
5. To retry one outbox row: `modulith outbox retry <publication-id>`. To see what is on its way to becoming a dead letter: `modulith outbox failing`.

`modulith doctor` warns on any outbox dead letter and reports an error above 100. A dead letter that should never be delivered stays until you delete its row yourself; no command discards a single one.

## A retired module's consumer group

```text
WARNING:  broker group 'modulith-inventory' is not derived by any module of this deployment and no consumer served it in the last 24 h; it holds 12 pending or claimed delivery(ies) that no consumer claimed. Every later publication to its targets is queued for it too, and those rows keep the shm store from pruning its publications. If the module is retired for good, run: modulith broker drop-group modulith-inventory
```

- **Cause:** a module was deleted or renamed. On the `shm` and `database` brokers its group `modulith-<old name>` still subscribes, so every event it listened to keeps being queued for it and is never removed.
- **Fix:** the drop deletes the group's queued messages without delivering them, so first confirm with the user that the module is retired for good. Then, with the service's broker configuration and environment:

  ```bash
  modulith broker drop-group modulith-inventory
  ```

- `error: 'modulith-inventory' belongs to a module of the current deployment or a consumer served it in the last 24 h. ...`: the module still exists, or it ran within the last day. For a module you just retired, add `--force` once nothing runs it any more.
- `error: no shm broker store at ...; nothing was removed.`: the command ran with a different `state_dir` or user than the service.
- The startup warning appears only under the process topology (`modulith run` or `modulith dev`), and only 24 hours after the group was last served. Do not wait for it: drop the group as part of retiring the module.
- If the module is only switched off for a while, do nothing: it receives its backlog when it returns.
- Details and `--target` in [processes-and-brokers.md](processes-and-brokers.md).

## Outbox commands find no store

```text
no outbox store: [tool.modulith].outbox is 'postgres' but no store is bound in this process. Set [tool.modulith].outbox_url (env MODULITH_OUTBOX_URL) ...
```

Each `modulith outbox` command is its own process and sees only configuration, never a store that `main.py` wired in a lifespan. Set `outbox_url`, or export `MODULITH_OUTBOX_URL` in the shell that runs the command. With `the default 'memory' outbox persists nothing` in the message, the outbox is simply off.

`modulith migrate` failing with `error: no database URL to migrate.` has the same cause: pass `--url`, or set `outbox_url`.

## The health endpoints answer 404

`GET /_modulith/health` returns `404 {"detail":"no worker route for '/_modulith/health'"}` and startup logged:

```text
WARNING:  actuator disabled: actuator_mode='auto' will not serve an unauthenticated /_modulith/* when production=True or the proxy binds a non-loopback host ('0.0.0.0'), and no token is configured. Set MODULITH_ACTUATOR_TOKEN ...
```

`modulith run` binds `0.0.0.0` by default, where the endpoints need a token. Set `MODULITH_ACTUATOR_TOKEN` and send `Authorization: Bearer <token>`, or choose another `actuator_mode` ([deployment.md](deployment.md)). A `401 {"detail":"unauthorized"}` means the token is set and the header is missing or wrong. In one process these endpoints do not exist.

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| Adding `time.sleep` or a retry loop in the caller because "the listener is slow to fire" | it hides the cause, and in one process listeners already ran before `publish()` returned | find the cause in [A listener never fires](#a-listener-never-fires) |
| Importing the listener's module from `main.py` to make it register | no worker and no CLI command runs `main.py` | import it from the module's own `__init__.py` |
| Fixing a CLI `ModuleNotFoundError` with `sys.path` edits in app code | the next command or worker fails again | set `[tool.modulith] package`, or install the project |
| Disabling a verify rule, or setting `strict_boundaries = false`, to get a green start | the coupling is still there and blocks process isolation and `extract` later | fix the import |
| Replaying dead letters first, investigating later | the same failure repeats, now with side effects | read `last_error`, fix, then replay |
| Deleting the `shm` store or truncating `event_publications` to "clear the queue" | undelivered events are destroyed | replay or drop the specific group |
| Catching the `TypeError` around `publish()` | the event is never sent | make the event JSON-friendly |
| Switching `outbox` back to `memory` to stop `database is locked` | you lose durability, and the memory outbox has its own locking case | apply the fix from the table above |
| Treating a 404 from the proxy and a 404 from a worker alike | the fixes are opposite | read the `detail` text |
| Reading only the last line of `modulith run` output | the cause of a crash loop is in the `[module]` lines above the `restarting in` line | scroll up to the first traceback |
