# The `modulith` command

Read this when:

- you need the exact command, flag, default or exit code;
- a command failed and you have to tell a violation from a usage error from a configuration error;
- you are deciding which command to run next, or whether a command is safe to run here.

## Contents

- [Run these, in this order](#run-these-in-this-order)
- [Install and invoke](#install-and-invoke)
- [How the CLI finds the project and the package](#how-the-cli-finds-the-project-and-the-package)
- [Which commands import your code](#which-commands-import-your-code)
- [Exit codes](#exit-codes)
- [Commands](#commands): [`init`](#modulith-init), [`add module`](#modulith-add-module), [`skill install`](#modulith-skill-install), [`info`](#modulith-info), [`verify`](#modulith-verify), [`doctor`](#modulith-doctor), [`dev`](#modulith-dev), [`run`](#modulith-run), [`docs`](#modulith-docs), [`audit`](#modulith-audit), [`migrate`](#modulith-migrate), [`outbox`](#modulith-outbox), [`broker`](#modulith-broker), [`extract`](#modulith-extract), [`k8s-manifest`](#modulith-k8s-manifest), [`openapi`](#modulith-openapi)
- [Mistakes that look right](#mistakes-that-look-right)

## Run these, in this order

| When | Run | What you need from it |
|---|---|---|
| First, in any project you did not just create | `modulith info` | the package, its modules, and `outbox` / `broker` / `topology`. Exit 1 means no package was found or it does not import: fix that first |
| There is no project yet | `modulith init myapp .` in the directory you were given, or `modulith init myapp`, then `cd myapp`; add `--empty` when the user's domain is not orders and payments | a runnable project with tests; every later command runs from the project root, the directory with its `pyproject.toml` |
| A new module | `modulith add module shipping` | the module with a router and its test, mounted in `main.py` |
| After every change, before you call the work done | `modulith verify` | exit 0 and `✓ no boundary violations` |
| At the same moment | `pytest` | exit 0 and the number of tests that ran, which must not be 0 |
| After a change to `[tool.modulith]`, the outbox wiring, the topology, a broker, a manifest or an event's fields | `modulith doctor` | exit 0; read every `⚠` line, they pass but they are findings |
| To watch it serve requests | `modulith dev myapp.main:app` | a server that does not return: run it in the background and stop it |

## Install and invoke

```bash
pip install 'modupy[fastapi,cli]'   # cli: the command; fastapi: FastAPI and uvicorn, which dev and run start
```

- `modulith ...` and `python -m modulith ...` are the same program. Use the second when the script is not on `PATH`, for example with a virtualenv that is not activated: `.venv/bin/python -m modulith verify`.
- `modulith --version` prints `modulith <version>`. `modulith --help` and `modulith <command> --help` print the options below.
- The distribution is `modupy`. `pip install modulith` installs an unrelated project.

## How the CLI finds the project and the package

1. **The project** is the directory of the nearest `pyproject.toml`, looking in the current directory and then each parent. The CLI reads `[tool.modulith]` from that file only, and puts that directory first on the import path, so a package that sits beside `pyproject.toml` imports without `PYTHONPATH` and without being installed. Run commands from the project root or below it.
2. **The package** is `MODULITH_PACKAGE`, else `[tool.modulith] package`, else `[project] name` with `-` turned into `_`. For `[project] name` the search continues upward past a `pyproject.toml` that has none.
3. With none of the three, every command that needs the package exits 1:

```text
error: could not determine the application package. Set [tool.modulith].package or [project].name in pyproject.toml, or the MODULITH_PACKAGE environment variable.
```

`dev` and `run` are the exception: with no package configured they take it from `APP_MODULE` (`myapp.main:app` gives `myapp`).
Settings, their environment variables and their precedence are in [configuration.md](configuration.md).

## Which commands import your code

| Import your modules (run them where the app's dependencies are installed, on code you trust) | Do not |
|---|---|
| `info`, `verify`, `docs`, `doctor`, `extract`, `openapi`, `k8s-manifest`, every `outbox` and `broker` command, `dev`, `run` | `init`, `add module`, `skill install`, `audit` (parses files, never imports them), `migrate` (reads configuration only) |

- A module that fails to import stops every command in the left column with exit 1 and the exception. Fix the import before anything else.
- With `strict_boundaries = true`, a boundary violation (warnings included) stops `info`, `openapi`, `k8s-manifest`, the `outbox` and `broker` commands, and `run` / `dev` under the process topology, with exit 1 and `error: boundary violations detected with strict_boundaries=True:`. `verify`, `docs`, `doctor` and `extract` still run and report the violations themselves. Single-process `dev` only warns.

## Exit codes

| Code | Meaning | What to do |
|---|---|---|
| 0 | success; warnings may have been printed | read the output for `[WARNING]` and `⚠` lines |
| 1 | a violation, an invalid value for an option, a configuration error, or nothing to act on | read the message; it names the key, file or module |
| 2 | a usage error (missing argument, unknown option), or an internal error with a traceback | fix the command line; a traceback is a bug to report, not a boundary violation |

`modulith dev` and single-process `modulith run` replace themselves with uvicorn, so after a successful start their exit code is uvicorn's.

## Commands

### `modulith init`

```text
modulith init NAME [DIR] [--empty] [--no-skill] [--install] [--ci github]
```

Writes the starter project into a new `./NAME`, or into `DIR` (`.` for the current directory): three modules (`orders`, `payments`, `inventory`), a `contracts` package with `OrderCreated` and `PaymentReceived`, a FastAPI app and five tests, with no database or broker to set up. It adds a `.gitignore`, unless one exists, and installs this skill as `skill install` does.

| Option | Effect |
|---|---|
| `--empty` | no sample modules: `main.py` with an app and no routers, an empty `contracts/events.py` and `tests/test_app.py`, one test |
| `--no-skill` | does not install this skill |
| `--install` | creates `.venv` and installs the dependencies: `uv sync --extra test` when `uv` is on `PATH`, else `python -m venv .venv` and pip; the `next:` steps then use `uv run`, or activate `.venv` |
| `--ci github` | adds `.github/workflows/ci.yml`, which runs `uv sync --extra test`, `uv run modulith verify`, `uv run pytest`, `uvx ruff check NAME tests` and `uvx ruff format --check NAME tests` |

```text
$ modulith init myapp
created myapp (11 file(s)):
  .gitignore
  pyproject.toml
  myapp/__init__.py
  myapp/main.py
  myapp/contracts/__init__.py
  myapp/contracts/events.py
  myapp/orders/__init__.py
  myapp/orders/api.py
  myapp/payments/__init__.py
  myapp/inventory/__init__.py
  tests/test_quickstart_flow.py
installed the modupy skill in myapp/.claude/skills/modupy
installed the modupy skill in myapp/.agents/skills/modupy
next:
  cd myapp
  pip install 'modupy[fastapi,cli]' pytest httpx2
  pytest
  modulith dev myapp.main:app
  modulith add module NAME  # a new module with its routes and a test
```

- `NAME` is the import package: lowercase letters, digits and `_`, starting with a letter. Without `DIR` the project root is `./NAME` and the package is `./NAME/NAME`.
- `DIR` may exist, such as a fresh repository holding `.git` and a README; it becomes the project root. When it is the current directory, the `next:` list has no `cd` line. A `.gitignore` already there is kept, and `init` says so: have it ignore `.venv/` and `.modulith-schemas.json`. An earlier install of this skill is kept too (``kept the existing modupy skill in .claude/skills/modupy; `modulith skill install` updates it``).
- Exit 1, with nothing written: `./NAME` exists (`error: myapp already exists; init never overwrites it`); `DIR` holds a `pyproject.toml`, `NAME`, `tests` or, with `--ci github`, `.github/workflows/ci.yml`, or a file stands where one of the project's directories goes, such as `.github` (`error: <path> already exists; init never overwrites it`, naming the first of them, such as `error: tests already exists; ...`); a `modupy` in a skills directory is not this skill; the name is not a lowercase package name (`My-App`); the name is `tests` (`error: 'tests' is the project's test directory; pick another package name`), one of the starter's dependencies (`modupy`), or already importable, such as `json` or an installed package (`error: 'json' is already an importable module, which the project would hide`).
- Exit 1 after writing the project: the `--install` command failed (`error: ...; the project is written, so install its dependencies yourself:`, then the `cd` and the commands still to run).
- Run it when the user wants a new project. It never touches an existing one.

### `modulith add module`

```text
modulith add module NAME
```

Adds a module to the project of the nearest `pyproject.toml`: `NAME/__init__.py`, whose public function `status()` its router calls, and `NAME/api.py` with `router` and one route, `GET /NAME`, in the application package (`./PACKAGE` or `./src/PACKAGE`), and `tests/test_NAME.py`, which requests that route. When the package's `main.py` builds `app = FastAPI(...)` at top level, it mounts the router there, at `/NAME`, with the import where ruff's isort puts it; a line past 88 characters is split, with a trailing comma.

```text
$ modulith add module shipping
created module shipping:
  myapp/shipping/__init__.py
  myapp/shipping/api.py
  tests/test_shipping.py
mounted its router at /shipping in myapp/main.py
next:
  pytest
  modulith verify
```

- Replace `status()` and the route with the module's real code. Keep the router import as the last line of `__init__.py`: `api.py` imports from the package.
- With a `uv.lock` beside the `pyproject.toml`, the `next:` steps are `uv run pytest` and `uv run modulith verify`.
- When no `main.py` builds `app = FastAPI(...)` at top level (an app a factory builds included), or it is read-only, it still writes the module, exits 0, and prints on stderr the two lines to add where the app is built. `tests/test_NAME.py` then serves the router on an app of its own, so it passes before they are added.
- Exit 1, with nothing written: no `pyproject.toml` (``error: no pyproject.toml here or above; start a project with `modulith init` ``), no package found, a name that is not a lowercase module name, the contracts package's name (`error: 'contracts' is the contracts package; pick another module name`), or a module or test file that exists, or a file where its directory goes (`error: myapp/shipping already exists; add never overwrites it`).
- For a module that only listens and serves no routes, create the package by hand ([recipes.md](recipes.md#6-add-a-module-that-only-listens)).

### `modulith skill install`

```text
modulith skill install [--user] [--dir PATH]...
```

Copies this skill to `<skills directory>/modupy`.

| Option | Default | Effect |
|---|---|---|
| (none) | | writes `.claude/skills/modupy` and `.agents/skills/modupy` beside the nearest `pyproject.toml`, else in the current directory |
| `--user` | off | writes the same two under the home directory, for every project |
| `--dir PATH` | none | writes `PATH/modupy` only; repeatable; not with `--user` |

It prints `installed <path>` or, when it replaced an earlier install, `updated <path>`, one line per directory.
Exit 1, with nothing written anywhere: a `modupy` path that is not this skill is in the way, or `--user` and `--dir` are both given.

### `modulith info`

```text
modulith info
```

No options. Prints the package, each module with `[manifest]` or `[no manifest]`, the resolved `outbox`, `broker`, `topology`, `observability` and `production`, the loaded plugins and the registered broker schemes.

```text
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
```

A module you expected and do not see is not a package directly under the application package, or its name starts with `_`.

### `modulith verify`

```text
modulith verify [--mode strict|ratchet] [--baseline PATH] [--update-baseline] [--fail-on-warnings]
```

| Option | Default | Effect |
|---|---|---|
| `--mode` | `strict` | `strict` reports every violation; `ratchet` reports only those beyond the baseline file |
| `--baseline` | `.modulith-baseline.json` | the baseline file `ratchet` reads and `--update-baseline` writes |
| `--update-baseline` | off | records every current violation as accepted and exits 0, whatever was found |
| `--fail-on-warnings` | off | also exit 1 when only `[WARNING]` violations are reported |

```text
$ modulith verify

payments:
  [ERROR] no-internal-imports: payments imports _orders from orders, reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/payments/__init__.py:4)

1 error(s), 0 warning(s)
```

- Exit 0: no `[ERROR]` was reported. Warnings print and pass unless `--fail-on-warnings`.
- Exit 1: an `[ERROR]`; an invalid `--mode`; a corrupt baseline; an unwritable `--baseline` path.
- `--mode ratchet` with no baseline file reports everything, like `strict`.
- Each rule and its fix: [modules-and-contracts.md](modules-and-contracts.md). Baselines and ratchet mode: [adopting-existing-code.md](adopting-existing-code.md).

### `modulith doctor`

```text
modulith doctor [--baseline PATH]
```

`--baseline` (default `.modulith-baseline.json`) is the file boundary health compares against.
Runs nine checks: boundary health, process-split readiness, schema drift, outbox health, listener registration, shm notifier, actuator token, single-host broker, redis retention.

```text
$ modulith doctor
modulith doctor

✓ boundary health — 0 violation(s)
✓ process-split readiness — 100% of cross-module interactions via events — microservice-ready
✓ schema drift — recorded 2 event schema(s)
✓ outbox health — in-memory bus, no durable outbox
✓ listener registration — no manifests declared
✓ shm notifier — no shm broker registered
✓ actuator token — single-process topology, no actuator proxy
✓ single-host broker — single-process topology
✓ redis retention — broker is not redis-streams

overall: ok
```

- Exit 1 only when a check reports an error (`overall: error`). `overall: warn` exits 0.
- It writes `.modulith-schemas.json` in the current directory: the first run records each event's fields, and a later run reports a changed or removed event once, as a warning, then records the new shape.
- What each check means: [deployment.md](deployment.md).

### `modulith dev`

```text
modulith dev APP_MODULE [--topology single|processes] [--isolate MODULE] [--reload/--no-reload]
                        [--host HOST] [--port PORT] [--log-level LEVEL] [--worker-port-base PORT]
```

| Option | Default | Effect |
|---|---|---|
| `APP_MODULE` | required | the ASGI app as `package.module:attribute`, for example `myapp.main:app` |
| `--topology` | `[tool.modulith] topology`, else `single` | `processes` starts one worker per module behind a proxy |
| `--isolate` | none | process topology with only this module started; the other modules' routes answer 404 |
| `--reload/--no-reload` | `--reload` | uvicorn's reload; single process only |
| `--host` | `127.0.0.1` | bind address |
| `--port` | `8000` | public port |
| `--log-level` | `info` | `debug`, `info`, `warning`, `error` or `critical`, for this process and every worker |
| `--worker-port-base` | `[tool.modulith] worker_port_base`, else `9001` | first worker port under the process topology |

Single process: prints `modulith: discovered modules: ...`, prints any boundary violations as warnings that never stop the server, prints `modulith dev → myapp.main:app on http://127.0.0.1:8000 (reload=on)`, then becomes uvicorn.
Exit 1 before starting: an invalid `--topology` or `--log-level`, an empty `APP_MODULE`, a configuration error, or uvicorn not installed.

### `modulith run`

```text
modulith run APP_MODULE [--topology single|processes] [--workers JSON] [--host HOST] [--port PORT]
                        [--log-level LEVEL] [--worker-port-base PORT]
```

Same options as `dev`, without `--isolate` and `--reload`, and with two differences:

| Option | Default | Effect |
|---|---|---|
| `--host` | `0.0.0.0` | bind address |
| `--workers` | none | a JSON object such as `'{"orders": 4}'`; process topology only. It replaces the whole `[tool.modulith.workers]` table. Each key is a discovered module or `default`, each value a positive integer |

`run` prints no boundary warnings. Single process prints `modulith run → myapp.main:app on http://0.0.0.0:8000` and becomes uvicorn.
Under the process topology, `run` and `dev` print one line and then serve until SIGINT or SIGTERM, after which they exit 0:

```text
$ modulith run myapp.main:app --topology processes --host 127.0.0.1
modulith → process topology: 3 worker(s) [inventory:9001, orders:9002, payments:9003], reverse proxy on http://127.0.0.1:8000
```

- Right after that line the proxy can still refuse connections for a few seconds, then answers 502 until the workers are up. Wait for `GET /_modulith/health` to answer 200. With the default `actuator_mode` it is served without a token only on a loopback `--host`, which is why the example passes one.
- Exit 1 before starting: invalid `--workers` JSON, a `--workers` key that is not a module (`error: workers names 'nope', which is not a module of package 'myapp'; discovered modules: inventory, orders, payments`), or anything `dev` rejects.
- Workers, the proxy and brokers: [processes-and-brokers.md](processes-and-brokers.md). Production: [deployment.md](deployment.md).

### `modulith docs`

```text
modulith docs [--output-dir PATH]
```

Writes Mermaid diagrams and one page per module to `--output-dir` (default `docs/modulith`): `architecture.mmd`, `events.mmd` and `modules/<module>.md`. Exit 1 when the directory cannot be written.

### `modulith audit`

```text
modulith audit [PATH] [--output PATH] [--force]
```

Reads an existing codebase without importing it and writes a report: proposed modules, the cross-module imports that would be violations, shared tables, and a readiness score from 0 to 100.

| Option | Default | Effect |
|---|---|---|
| `PATH` | the application package under the current directory | the directory whose subdirectories are the module candidates |
| `--output` | `MIGRATION.md` | the report file |
| `--force` | off | replace an existing report |

Exit 1: the report exists and `--force` is absent, or `PATH` is missing or not a directory. See [adopting-existing-code.md](adopting-existing-code.md).

### `modulith migrate`

```text
modulith migrate [REVISION] [--url URL] [--schema SCHEMA]
```

| Option | Default | Effect |
|---|---|---|
| `REVISION` | `head` | the migration revision to reach |
| `--url` | `[tool.modulith] outbox_url`, or `MODULITH_OUTBOX_URL` | the SQLAlchemy URL to migrate |
| `--schema` | none | target schema, PostgreSQL only |

Creates the outbox tables and the `broker_` tables of the database broker. It creates none of your own tables.
It needs Alembic and SQLAlchemy (`modupy[postgres]` or `modupy[database]`), swaps the URL's async driver for the sync one itself, and is safe to run again.

```text
$ modulith migrate
migrated sqlite:///myapp.db to head
```

Exit 1: no URL (`error: no database URL to migrate. ...`), the drivers are missing, or the migration failed (`error: migration failed: ...`).

### `modulith outbox`

Every subcommand needs a durable outbox bound in the command's own process, which `outbox_url` does. With the default `memory` outbox each exits 1 with `no outbox store: ...`; that is not a fault in the app.

| Command | Does | Exit 1 when |
|---|---|---|
| `modulith outbox status` | prints `incomplete:`, `completed:` and `dead-lettered:` counts | |
| `modulith outbox failing` | lists events that failed and will be retried, with attempts, last error and next retry time | the store cannot list them |
| `modulith outbox dead-letter [--list]` | lists the events whose listeners gave up | |
| `modulith outbox dead-letter --retry-all` | runs those listeners again, inside this command | `--list` is also given |
| `modulith outbox retry PUBLICATION_ID` | delivers one publication now, skipping its backoff | the id is not a UUID, is unknown or complete, or another process holds it |
| `modulith outbox purge [--older-than 30d]` | deletes completed publications older than a count and a unit: `7d`, `24h`, `30m`, `90s` | the duration is malformed |

`--retry-all` and `retry` run your listeners in the command's process, so run them where the app's settings and credentials are. Details: [outbox.md](outbox.md).

### `modulith broker`

Both subcommands act on the configured broker as the process topology uses it.

```text
modulith broker dead-letter [--list] [--retry-all]
modulith broker drop-group GROUP [--target TARGET]... [--force] [--yes]
```

- `dead-letter` lists the deliveries the broker gave up on, or resubmits them with `--retry-all`, on the `shm`, `database` and `redis-streams` brokers. On `redis-streams`, `--retry-all` skips a target whose stream has several consumer groups and exits 1 naming it.
- `drop-group` deletes a retired module's consumer group (`modulith-<module>`) with its subscriptions and undelivered messages, on `shm` and `database` only. It asks for confirmation unless `--yes` (`-y`), refuses a group a current module or a recent consumer uses unless `--force`, and with `--target` (repeatable) removes only those targets. It is destructive: run it only when the user asked for it.
- Exit 1: the broker's local store does not exist, the broker does not support the command, the drop was refused or declined, or nothing was removed.

Details: [processes-and-brokers.md](processes-and-brokers.md).

### `modulith extract`

```text
modulith extract MODULE [--output PATH] [--force]
```

Writes a standalone service built from one module into `--output` (default `<module>-service`): the module, the contracts package, a `pyproject.toml`, a `Dockerfile`, a `.env.example` and a `README.md`.
Exit 1 on an unknown module (`error: unknown module 'nope'. Available modules: ...`), and on boundary violations, shared tables or imports of other modules unless `--force`.
Exit 1 whatever `--force` says when the extracted module does not import on its own, or when `--output` exists and is not empty. See [deployment.md](deployment.md).

### `modulith k8s-manifest`

```text
modulith k8s-manifest [--output PATH] [--image IMAGE] [--namespace NAME] [--port PORT] [--host HOST]
```

| Option | Default | Effect |
|---|---|---|
| `--output` | `modulith-k8s.yaml` | output file, or `-` for stdout |
| `--image` | `<package>:latest` | container image |
| `--namespace` | none | namespace for every object |
| `--port` | `8000` | container port every worker listens on |
| `--host` | none | Ingress host |

One Deployment and Service per module, plus one Ingress. Exit 1 with the `shm` broker, which cannot cross pods: configure `database` or `redis-streams` first. See [deployment.md](deployment.md).

### `modulith openapi`

```text
modulith openapi [--output PATH] [--title TITLE] [--api-version VERSION]
```

| Option | Default |
|---|---|
| `--output` | `openapi.json` |
| `--title` | the package name |
| `--api-version` | `[project] version`, else `0.0.0` |

Merges every module's routes into one OpenAPI file. It needs FastAPI, and exits 1 when no module exposes a `router`.

```text
$ modulith openapi
wrote OpenAPI for 2 module(s) to openapi.json
skipped (no router): payments
```

## Mistakes that look right

| Looks right | What actually happens | Do this |
|---|---|---|
| Running `modulith info` in the directory where you ran `modulith init myapp` | exit 1, no package: the project is `./myapp` | `cd myapp` first; the package is `myapp/myapp` |
| Running commands from a subdirectory that has its own `pyproject.toml` | that nearer file is the project, and its `[tool.modulith]` (or the lack of one) wins | run from the project whose `pyproject.toml` holds `[tool.modulith]` |
| Reading `modulith dev`'s clean start as "the boundaries are fine" | `dev` prints violations as warnings and starts anyway | `modulith verify` is the check; require exit 0 |
| `modulith verify --update-baseline` to get back to exit 0 | it accepts every violation present, including the one you just wrote | fix the import; touch the baseline only when the user is adopting modupy on existing code |
| Treating exit 2 as "violations found" | exit 2 is a wrong command line or a crash | re-read the usage; violations are exit 1 |
| Starting `modulith dev` or `modulith run` and waiting for it to finish | it serves until it is stopped | start it in the background, wait for the port, send requests, then stop it |
| Sending requests the moment the process topology prints its line | the connection is refused, then answered 502, until the workers are listening | poll `/_modulith/health` until it answers 200 |
| `modulith run main:app` or `modulith run myapp/main.py` | uvicorn cannot import the app | `package.module:attribute`: `myapp.main:app` |
| `--workers '{"orders": 4}'` to raise one module | every other entry of `[tool.modulith.workers]` is dropped for that run | pass the whole map, or edit the table |
| `modulith outbox status` exits 1 on a fresh project, so "the outbox is broken" | the default outbox is `memory` and stores nothing | nothing to fix; turn the outbox on only when asked ([outbox.md](outbox.md)) |
| `modulith migrate` to create the app's tables | it creates only the outbox and broker tables | create your tables with your own migrations |
| `modulith broker drop-group GROUP --yes --force` to silence a startup warning | the group's undelivered messages are deleted | ask the user; a module that is only switched off still needs its backlog |
| `modulith info` passes, so `modulith run --topology processes` will start | `info` does not check `own_process` and `workers` names against the modules | the names are checked when the workers are derived; read the first error line of `run` |
