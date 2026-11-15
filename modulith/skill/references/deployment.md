# Deploying and operating a modupy app

Read this when:

- you are preparing a release: the run command, production settings, health probes, secrets;
- `modulith doctor` printed a warning or an error and you need to know what it means;
- you are generating Kubernetes manifests or one OpenAPI file, adding tracing, or splitting a module into its own service with `modulith extract`.

## Contents

- [Pick the shape](#pick-the-shape)
- [`modulith run` in production](#modulith-run-in-production)
- [The production flag](#the-production-flag)
- [`modulith doctor`](#modulith-doctor)
- [Securing the actuator endpoints](#securing-the-actuator-endpoints)
- [Observability](#observability)
- [`modulith k8s-manifest`](#modulith-k8s-manifest)
- [`modulith openapi`](#modulith-openapi)
- [Spreading across machines](#spreading-across-machines)
- [`modulith extract <module>`](#modulith-extract-module)
- [Mistakes that look right](#mistakes-that-look-right)

## Pick the shape

| Need | Setting | Details |
|---|---|---|
| One process, nothing else to run | none | below |
| No lost events | `outbox = "postgres"`, `outbox_url`, session wiring | [outbox.md](outbox.md) |
| A busy or risky module in its own process, on one machine | `topology = "processes"` | [processes-and-brokers.md](processes-and-brokers.md) |
| Modules on several machines or pods | `broker = "database"` on PostgreSQL or MySQL, or `"redis-streams"` | [Spreading across machines](#spreading-across-machines) |
| One module as its own deployable | `modulith extract <module>` | [below](#modulith-extract-module) |

## `modulith run` in production

```bash
$ modulith run myapp.main:app
modulith run → myapp.main:app on http://0.0.0.0:8000
```

In one process this hands over to uvicorn with `--host 0.0.0.0 --port 8000 --log-level info`; `uvicorn myapp.main:app --host 0.0.0.0` is the same thing. With `--topology processes` (or `topology = "processes"` in `pyproject.toml`) it runs the supervisor, the proxy and one worker per module.

`modulith dev` is for development only: it binds `127.0.0.1`, reloads on change in one process, and turns boundary violations into warnings.

A container image needs your package, its dependencies and its `pyproject.toml` (modupy reads `[tool.modulith]` from the nearest one at or above the working directory):

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY myapp/ ./myapp/
RUN pip install --no-cache-dir .
EXPOSE 8000
CMD ["modulith", "run", "myapp.main:app", "--topology", "processes"]
```

Your project's own `dependencies` must list `modupy` with the extras it uses, for example `modupy[fastapi,cli,postgres]`. Pass secrets as environment variables at run time: `MODULITH_OUTBOX_URL`, `MODULITH_BROKER_URL` or `REDIS_URL`, `MODULITH_ACTUATOR_TOKEN`.

Before each release, in this order:

1. `modulith verify` and the tests pass.
2. `modulith doctor` exits 0, run with the production environment variables set.
3. `modulith migrate` has run against the production database, if the outbox or the database broker is on. Run it again after every modupy upgrade.
4. Under the process topology: `state_dir` is set for the `shm` broker, `MODULITH_ACTUATOR_TOKEN` is set if anything probes `/_modulith/*`, and the startup output has no `sets up what no worker runs` warning.

## The production flag

`production = true` under `[tool.modulith]`, or `MODULITH_PRODUCTION=1` (`1`, `true`, `yes` turn it on; `0`, `false`, `no` turn it off; anything else is an error). It is off by default. There is no `--production` option. It turns these on:

| With `production = true` | Otherwise |
|---|---|
| Startup fails unless `outbox` is set explicitly: `Cannot start in production mode with the default in-memory outbox — events would be lost on restart.` Set `outbox = "postgres"`, or opt in with `outbox = "memory"`. | the memory outbox is the silent default |
| `broker = "database"` with no URL is an error: `Cannot start in production with broker='database' and no URL` under the process topology, `broker='database' in production requires an explicit URL` in one process | it falls back to a local SQLite file with a warning |
| `/_modulith/*` needs a token even on `127.0.0.1` | open on loopback |
| `strict_boundaries = true` always stops the boot on a violation | single-process `modulith dev` only warns |
| `doctor` reports a single-host broker under Kubernetes as an error | a warning |

Every command that bootstraps the app applies the first two, so `modulith info` and `modulith doctor` fail the same way when run with the production environment.

## `modulith doctor`

```bash
$ modulith doctor
modulith doctor

✓ boundary health — 0 violation(s)
✓ process-split readiness — 100% of cross-module interactions via events — microservice-ready
✓ schema drift — 2 event schema(s) stable
⚠ outbox health — 0 incomplete, 3 completed, 1 dead-lettered
✓ listener registration — no manifests declared
✓ shm notifier — no shm broker registered
✓ actuator token — single-process topology, no actuator proxy
✓ single-host broker — single-process topology
✓ redis retention — broker is not redis-streams

overall: warn
```

- **Exit code:** 1 when any check is an error (`✗`), 0 otherwise. Warnings (`⚠`) are printed and pass. 2 is an internal failure.
- **It reads `pyproject.toml` and the environment, not the flags of `modulith run`.** To have it check a process-topology deployment that is selected only by `--topology processes`, run `MODULITH_TOPOLOGY=processes modulith doctor`.
- It imports your modules, so run it where the app's dependencies are installed. It still runs under `strict_boundaries = true`.
- `--baseline PATH` names the ratchet baseline for the first check (default `.modulith-baseline.json`).

| Check | ok | warn | error |
|---|---|---|---|
| `boundary health` | no violation | only warnings; or errors that are all in the baseline file | an error-level violation that is not in the baseline. Fix with [modules-and-contracts.md](modules-and-contracts.md) |
| `process-split readiness` | at least 80% of cross-module interactions are `publish()` calls rather than direct imports of another module | under 80%, or a shared table when no import or event links the modules; the details name the modules | never |
| `schema drift` | event fields unchanged since the last run | an event's fields changed or an event was removed: plan a compatible rollout | the cache file is corrupt |
| `outbox health` | no dead letter, at most 1,000 incomplete | a dead letter, more than 1,000 incomplete, or a durable outbox with no store bound | more than 100 dead letters, or no answer in 10 s. See [outbox.md](outbox.md) |
| `listener registration` | every listener a manifest declares is registered | never | a declared listener was not registered, usually because its file is not imported |
| `shm notifier` | the `shm` broker's notification file is attached | not attached: delivery still works, more slowly. Stop every worker and delete the file it names | never |
| `actuator token` | single process, or mode `open`/`disabled`, or a token is set | `actuator_mode = "auto"` and no token: `/_modulith/*` will not be mounted on `0.0.0.0` | `actuator_mode = "token"` and no token: `modulith run` will refuse to start |
| `single-host broker` | single process, a networked broker, or no container detected | `shm` or a SQLite `database` broker inside a container | the same under Kubernetes with `production = true` |
| `redis retention` | not Redis, or `max_stream_len` at least 10,000 | `max_stream_len` below 10,000, or the live backlog is at least half of it | the live backlog is at least 90% of `max_stream_len` |

Side effects to know:

- `schema drift` keeps a local cache, `.modulith-schemas.json`, in the working directory. The first run there records the events and reports nothing; each later run reports a change once and then updates the file. Add the file to `.gitignore`. On a fresh checkout, as in CI, this check therefore never reports drift.
- A check that raises is reported as an error for that check; the others still run.
- The Redis check queries the server when it can reach one and falls back to the configured values when it cannot. With a durable outbox, `outbox health` queries the outbox store, so an unreachable outbox database is an error. `doctor` does not otherwise test that a broker or database is reachable.

Run it after any change to configuration, the outbox, brokers or the topology, and in CI next to `modulith verify`.

## Securing the actuator endpoints

Under the process topology the proxy serves `/_modulith/live`, `/_modulith/health` and `/_modulith/topology` (what they return: [processes-and-brokers.md](processes-and-brokers.md)). `topology` reveals module names and internal ports, so access is controlled by `actuator_mode` under `[tool.modulith]`, or `MODULITH_ACTUATOR_MODE`:

| Mode | Behaviour |
|---|---|
| `auto` (default) | open only when the proxy binds a loopback host and `production` is off. Otherwise a token is required; without one the routes are **not mounted** and startup logs `actuator disabled: ...` |
| `token` | a token is always required; `modulith run` refuses to start without one |
| `open` | no authentication |
| `disabled` | never mounted |

```bash
export MODULITH_ACTUATOR_TOKEN="$(openssl rand -hex 32)"
curl -H "Authorization: Bearer $MODULITH_ACTUATOR_TOKEN" http://localhost:8000/_modulith/health
```

- The token comes only from the `MODULITH_ACTUATOR_TOKEN` environment variable. A request without the right `Authorization: Bearer` header gets `401 {"detail":"unauthorized"}`.
- `modulith run` binds `0.0.0.0` by default, so with no token the default result is unmounted routes: `404 {"detail":"no worker route for '/_modulith/health'"}`. A probe pointed there fails until you set a token or choose `open`.
- An unmounted actuator does not stop the app: the module routes work.
- In one process there is no proxy and no actuator: modupy adds no HTTP routes, so probe an endpoint of your own.

## Observability

modupy emits OpenTelemetry spans and no metrics.

```bash
pip install 'modupy[otel]'
```

| Span | One per | Attributes |
|---|---|---|
| `modulith.event.publish` | `publish()` call | `event.type`, `event.module`, `modulith.duration_ms` |
| `modulith.event.dispatch` | listener call, whether in memory, from the outbox or from a broker | `event.type`, `listener.name`, `publication.id`; status ERROR when the listener raises |

- modupy configures no exporter. Set up a tracer provider in your app (`trace.set_tracer_provider(...)` with your exporter); without one the spans are no-ops.
- A dispatch span is a child of its publish span, also after an outbox commit, on a retry and across processes. Spans your listener starts are children of the dispatch span.
- `observability` under `[tool.modulith]` (or `MODULITH_OBSERVABILITY`): unset means on when OpenTelemetry is installed; `false` creates no spans; `true` makes startup fail when OpenTelemetry is missing: `observability = true needs OpenTelemetry, which is not installed.`
- Under the process topology, a provider set up only in `main.py` does not exist in the workers. Set it up in code every module imports.
- The proxy and the workers write uvicorn access lines without query strings. `--log-level warning` on `modulith run` silences INFO lines in every process.

## `modulith k8s-manifest`

```bash
$ modulith k8s-manifest --image myapp:1.0.0 --namespace shop --host shop.example.com
wrote 3 module manifest(s) to modulith-k8s.yaml
```

It writes, into one YAML file:

- one **Deployment** and one **Service** per module (`myapp-orders`, `myapp-payments`, ...), with `replicas` from `[tool.modulith.workers]`. `own_process` is ignored: every module gets its own Deployment;
- one **Ingress** (`myapp-ingress`) routing `/<module>` to each Service;
- each container runs `python -m uvicorn modulith._worker:create_app --factory --host 0.0.0.0 --port 8000` with `MODULITH_MODULE`, `MODULITH_APP_PACKAGE`, `MODULITH_TOPOLOGY`, `MODULITH_BROKER`, `MODULITH_CONTRACTS_MODULE` and the non-secret broker options as environment variables;
- probes on each pod: readiness `GET /health`, liveness a TCP check. There is no supervisor, no proxy and no `/_modulith/*` in this layout.

Options: `--output` (default `modulith-k8s.yaml`, `-` for stdout), `--image` (default `<package>:latest`), `--namespace`, `--port` (8000), `--host` (Ingress host).

Before applying:

1. Configure a broker that pods can share. With `shm`, `memory`, or a `sqlite` broker URL the command exits 1; for `shm` the message is `the 'shm' broker only works within a single process/host and cannot back a Kubernetes deployment ...`.
2. Create the Secret the manifest reads the broker URL from. The URL is never written into the file:

   ```bash
   kubectl create secret generic myapp-broker --from-literal=url=<broker connection URL> --namespace shop
   ```

3. If the outbox is on, give the pods `MODULITH_OUTBOX_URL` through the optional `myapp-env` Secret (unless the image's `pyproject.toml` sets `outbox_url`). A pod with a durable outbox and no store refuses to start.

   ```bash
   kubectl create secret generic myapp-env --from-literal=MODULITH_OUTBOX_URL=<async SQLAlchemy URL> --namespace shop
   ```

4. Build the image as above; the pods read `[tool.modulith]` from the image's `pyproject.toml`.

Regenerate and roll out after adding or removing a module. A retired module's consumer group is not reported in this layout; drop it yourself ([processes-and-brokers.md](processes-and-brokers.md)).

## `modulith openapi`

```bash
$ modulith openapi --output openapi.json
wrote OpenAPI for 2 module(s) to openapi.json
skipped (no router): payments
```

- Merges every module's routes into one OpenAPI document, with paths under `/<module>` and each schema name prefixed with `<module>_`. Use it for API portals and client generators; under the process topology no running process serves a merged document.
- Options: `--output` (default `openapi.json`), `--title` (default: the package name), `--api-version` (default: `[project].version`, else `0.0.0`).
- It imports your modules and needs FastAPI installed. It exits 1 when no module has a `router`, or when two modules cannot be merged (`error: could not merge OpenAPI documents: ...`).
- It reads each module's `router`, not `main.py`, so routes defined on the app itself are absent.

## Spreading across machines

1. Use a networked broker: `broker = "database"` with a PostgreSQL or MySQL `url`, or `broker = "redis-streams"`. The `shm` broker and a SQLite file stay on one host.
2. Place the modules:
   - Kubernetes: `modulith k8s-manifest`;
   - any host: start one module's worker directly, with the same `pyproject.toml` and broker settings:

     ```bash
     MODULITH_MODULE=payments MODULITH_APP_PACKAGE=myapp \
       uvicorn modulith._worker:create_app --factory --host 0.0.0.0 --port 8000
     ```

     It serves `/payments/...` and its own `/health`, and consumes its module's events. Routing `/<module>` to the right host is your load balancer's job; `modulith run`'s proxy only reaches workers on its own machine.
3. Every host needs the same code version of the contracts, the same broker settings, and, with the outbox, the same `outbox_url`.
4. Delivery is at least once, so listeners must be safe to run twice.

## `modulith extract <module>`

Turns one module into a standalone service: a directory with its own `pyproject.toml` and `Dockerfile` that runs the module with `modulith._worker:create_app` and talks to the rest of the app through the broker.

```bash
$ modulith extract payments --output payments-service
source package: /srv/myapp/myapp
extracted 'payments' to payments-service (8 file(s)):
  myapp/__init__.py
  myapp/payments/__init__.py
  myapp/contracts/__init__.py
  myapp/contracts/events.py
  pyproject.toml
  Dockerfile
  .env.example
  README.md
```

**Before you run it**

- The main app already runs with `topology = "processes"` and a networked broker. `extract` copies the configured `broker` and `outbox` into the service and picks its dependencies from them; it does not refuse a `memory` or `shm` configuration, it just writes a service that cannot receive anything (`MODULITH_BROKER=memory` in `.env.example`).
- `modulith verify` is clean for that module and it shares no table with another module.
- The module's third-party dependencies are installed where you run `extract`: it imports the extracted copy to check it.
- `--output` (default `<module>-service`) does not exist or is empty, and is outside the package.

**What it writes**

- the module, the contracts package, and every package-level helper file they import (listed in the generated README under "Copied helper modules");
- `pyproject.toml`: `modupy` pinned to the installed version with the needed extras, plus the source project's `[project].dependencies`, and a `[tool.modulith]` table carrying the package name, the explicitly set `outbox`, `broker`, `production` and similar app-level options (not `topology` or `outbox_url`), and the broker options without the URL;
- `Dockerfile`: installs the service and runs `uvicorn modulith._worker:create_app --factory --host 0.0.0.0 --port 8000` with `MODULITH_MODULE` and `MODULITH_APP_PACKAGE` set;
- `.env.example`: `MODULITH_BROKER`, `MODULITH_BROKER_URL`, `MODULITH_OUTBOX`, `MODULITH_OUTBOX_URL` and the other variables to fill in;
- `README.md`: how to run it, and "Extraction notes" for anything you must finish by hand.

`main.py` is not copied. The service serves the module's routes under `/<module>` and `/health` at the root.

**What it refuses, exit 1**

| Refusal | Message contains | `--force` |
|---|---|---|
| the module has boundary violations | `error: extraction blocked:` then each violation | overrides |
| the module imports another module | `imports declared module(s): orders (...)` | overrides only an import inside a function |
| the module shares a table with another module | `shares table(s) with another module:` | overrides |
| the extracted copy does not import, or loads code from the source tree outside the service | `the extracted service cannot import myapp.payments:` | never |
| unknown module | `error: unknown module 'billing'. Available modules:` | never |
| `--output` exists and is not empty, is a symlink, or is inside the package | `--output ... already exists and is not empty`, `is a symlink` or `is inside source package` | never |
| a symlink in the copied sources | `source symlink ... is not supported` | never |
| the app's own `__init__.py` holds code (anything but a docstring) | `package initializer ... contains imports or executable behavior` | never |
| the package spans several directories | `package 'myapp' spans N directories` | never |

`--force` writes what it overrode into the generated README. It never makes a broken service importable: a module-level `from myapp.orders import ...` still fails the import check. Fix the cause instead: replace the cross-module call with an event or a contract ([modules-and-contracts.md](modules-and-contracts.md)), and give the module its own tables.

**After it succeeds**

1. `cd payments-service && cp .env.example .env`, then fill in the same broker settings the main app uses, and `MODULITH_OUTBOX_URL` if the outbox is on.
2. `docker build -t payments-service . && docker run --env-file .env -p 8000:8000 payments-service`.
3. **Delete the module from the main app** and redeploy it. While both run, they share the consumer group `modulith-payments` and split the module's events between them.
4. Route `/payments` to the new service in your load balancer or Ingress: the main app's proxy no longer has a worker for it.
5. Keep the contracts in step: the service has its own copy of `myapp/contracts`.

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| Treating a `doctor` warning as a failed build, or an `overall: warn` as fine forever | warnings exit 0 by design, and each one names a real gap | read the line, fix or accept it knowingly; gate CI on the exit code |
| Running `doctor` on a laptop to judge production | it reads the local `pyproject.toml` and environment | run it with the production environment variables |
| Pointing a Kubernetes probe at `/_modulith/health` with no token configured | on `0.0.0.0` the route is not mounted and answers 404 | set `MODULITH_ACTUATOR_TOKEN`, or `actuator_mode = "open"` on a private network |
| Probing `/health` through the proxy | the proxy answers 404 for it | `/_modulith/live` for liveness, `/_modulith/health` for readiness |
| Setting `production = true` and leaving `outbox` unset | startup fails by design | set `outbox` explicitly, to `"postgres"` or to `"memory"` |
| `modulith k8s-manifest` on the default configuration | the default `shm` broker cannot span pods; exit 1 | configure `database` (PostgreSQL or MySQL) or `redis-streams` first |
| Putting the broker URL into the generated YAML | the manifest reads it from the `<package>-broker` Secret on purpose | create the Secret |
| Expecting `own_process` or `modulith run` behaviour inside the generated pods | each pod is one module's worker, alone | scale with `[tool.modulith.workers]`; probe `/health` on the pod |
| `modulith extract payments --force` to get past a blocker | the service then lacks code or tables it needs | remove the coupling, then extract without `--force` |
| Leaving the module in the main app after extracting it | both consume the same group, so each event reaches only one of them | delete it from the main app |
| Extracting while the app still uses the `memory` or `shm` broker | the service gets a broker that cannot reach the app | switch the app to a networked broker first |
| Baking `MODULITH_OUTBOX_URL` or a token into the image | secrets in image layers | pass them at run time |
| Assuming tracing works because `modupy[otel]` is installed | no tracer provider, no spans | configure a provider and exporter in the app |

Further reading: [docs/DEPLOYMENT.md](https://github.com/jeansossmeier/modupy/blob/main/docs/DEPLOYMENT.md) covers Docker users and permissions, graceful shutdown timing, Redis and PostgreSQL tuning, and scaling.
