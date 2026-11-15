# Configuration: every setting

Read this when:

- you are adding or changing a key under `[tool.modulith]`, or a `MODULITH_*` environment variable;
- startup fails with a `ConfigurationError`, or a setting seems to have no effect;
- you need the smallest configuration for one process, the durable outbox, one process per module, or a shared broker.

## Contents

- [Where settings come from](#where-settings-come-from)
- [How the application package is found](#how-the-application-package-is-found)
- [`[tool.modulith]`](#toolmodulith)
- [`[tool.modulith.outbox_options]`](#toolmodulithoutbox_options)
- [`[tool.modulith.workers]`, `verify`, `subscriptions`](#toolmodulithworkers-verify-subscriptions)
- [`[tool.modulith.broker_options]`](#toolmodulithbroker_options)
- [Other environment variables](#other-environment-variables)
- [Settings that interact](#settings-that-interact)
- [A complete example](#a-complete-example)
- [The smallest configuration for each setup](#the-smallest-configuration-for-each-setup)
- [Mistakes that look right](#mistakes-that-look-right)

## Where settings come from

Highest priority first:

1. `configure(...)` keyword arguments, in the same process, before bootstrap.
2. `MODULITH_*` environment variables.
3. `[tool.modulith]` in `pyproject.toml`.
4. The defaults. A project with no `[tool.modulith]` at all runs: one process, in-memory events, nothing stored.

Rules that decide what is actually read:

- **One file.** The nearest `pyproject.toml` at or above the current working directory, and only that one, even when it has no `[tool.modulith]`. A `pyproject.toml` in a subdirectory you `cd` into hides the project's. Run the app, the CLI and the tests from the project root.
- **Every process reads it again.** The app, each `modulith` command and each worker of the process topology resolve configuration on their own. `modulith run --topology processes` also hands its workers `MODULITH_BROKER`, `MODULITH_OUTBOX`, `MODULITH_OUTBOX_URL` and every `broker_options` key as `MODULITH_BROKER_<KEY>`; a value already in the environment wins.
- **Environment variables.** An empty or whitespace-only value counts as unset. Booleans accept `1`, `true`, `yes` and `0`, `false`, `no` in any case; anything else stops the start: `MODULITH_PRODUCTION='ture' is not a valid boolean. Use one of 1/true/yes for true or 0/false/no for false, or unset the variable.` Only the scalar keys have one; the tables, and `own_process`, do not.
- **Typos fail.** An unknown key, a wrong type or a value outside its range raises at startup, in every command that loads configuration:

  ```text
  Unknown config keys: ['topolgy']. Valid keys: [...]. Did you mean: 'topolgy' -> 'topology'?
  strict_boundaries must be bool, got str: 'true'
  Unknown config subtable [tool.modulith.worker] — did you mean [tool.modulith.workers]? ...
  [tool.modulith.outbox] is not a valid subtable: `outbox` is the scalar adapter name (e.g. outbox = "postgres"). Put outbox tuning in [tool.modulith.outbox_options] instead.
  ```

  Not checked, so a typo there is ignored without a word: the keys inside `[tool.modulith.broker_options]`, every key of `[tool.modulith.verify]` except `disabled_rules`, and a subtable name close to no known one. A `pyproject.toml` that does not parse is an error, never a silent fallback to defaults.

`configure()` is for values computed at startup:

```python
# myapp/__init__.py, so every process that imports the package runs it
import os

from modulith import configure

configure(production=os.environ.get("DEPLOY_ENV") == "prod")
```

- It accepts every `[tool.modulith]` key, the tables as dicts, plus `disable_plugins=[...]`.
- It affects only the process that calls it. A call in `main.py` never reaches the `modulith` commands or the workers, which do not run `main.py`; put shared settings in `pyproject.toml`.
- After bootstrap (the first `publish()` or `bootstrap()`) it raises `configure() cannot be called after modulith has bootstrapped. Call it at application startup, before any @listener registration or publish() call.`

## How the application package is found

| Where | Order |
|---|---|
| Your app (`uvicorn`, `bootstrap()`, the first `publish()`) and your tests | `configure(package=...)` or `MODULITH_PACKAGE` or `[tool.modulith] package`; else the top-level package of the first calling frame outside modupy and the standard library; else `[project] name` with `-` turned into `_` |
| `modulith` commands | `MODULITH_PACKAGE` or `[tool.modulith] package`; else `[project] name`; `dev` and `run` also accept the package of `APP_MODULE`. Never the call stack |

The call-stack step is right when `myapp/orders/__init__.py` calls `publish()`. It is wrong when the nearest caller is a test file: `await publish(...)` in a test body, or `scenario.publish(...)`. The test module then becomes the application package:

```text
detected application package 'test_payments'
no modules discovered under 'test_payments'
```

Only listeners in modules the test itself imported are registered, so `expected event PaymentReceived not seen within 2s`. The test fixtures also drop only `test_payments.*` after the test, so your modules' state leaks into the next test and results depend on test order.

The fix, also needed whenever the import package is not `[project] name`:

```toml
[tool.modulith]
package = "myapp"
```

`modulith info` prints the package the CLI uses (`package: myapp`); the app logs `detected application package 'myapp'` at INFO.

## `[tool.modulith]`

| Key | Type, default | Environment variable | What it does |
|---|---|---|---|
| `package` | string, detected | `MODULITH_PACKAGE` | the application package; see above |
| `contracts_module` | string, `"contracts"` | `MODULITH_CONTRACTS_MODULE` | the subpackage of shared events and types; every module may import it and it gets no worker. Must be a dotted Python name |
| `outbox` | string, `"memory"` | `MODULITH_OUTBOX` | `"memory"` stores nothing. `"postgres"` is the SQL outbox for PostgreSQL, MySQL and SQLite alike ([outbox.md](outbox.md)) |
| `outbox_url` | string, unset | `MODULITH_OUTBOX_URL` | async SQLAlchemy URL of your business database (`postgresql+asyncpg://`, `mysql+aiomysql://`, `sqlite+aiosqlite:///`). With `outbox` not `"memory"`, every process binds the outbox store here |
| `topology` | `"single"` or `"processes"`, `"single"` | `MODULITH_TOPOLOGY` | one process, or one worker per module behind a proxy ([processes-and-brokers.md](processes-and-brokers.md)). `"subinterpreters"` is refused: `not yet implemented` |
| `broker` | string, `"memory"` | `MODULITH_BROKER` | `memory`, `shm`, `database`, `redis-streams`. Unset under `processes`: `database` when a broker URL is configured, else `shm`, with a WARNING naming the choice |
| `own_process` | list of module names, `[]` | none | under `processes`, only these modules get a worker of their own; the rest share one. Empty: one worker per module |
| `worker_liveness_timeout_seconds` | number > 0, unset | `MODULITH_WORKER_LIVENESS_TIMEOUT_SECONDS` | the supervisor kills and restarts a worker whose `/health` stays unanswered this long. Unset: a hung worker is left alone |
| `worker_port_base` | integer 1-65535, `9001` | `MODULITH_WORKER_PORT_BASE` | first loopback port of the workers; each replica takes the next one |
| `subscription_source` | `"manifest"`, `"config"`, `"listener"`; `"manifest"` | `MODULITH_SUBSCRIPTION_SOURCE` | where extra broker targets a worker consumes are declared: `declare_module(broker_targets=...)`, `[tool.modulith.subscriptions]`, or `@listener(broker_targets=...)` |
| `actuator_mode` | `"auto"`, `"token"`, `"open"`, `"disabled"`; `"auto"` | `MODULITH_ACTUATOR_MODE` | who may call the proxy's `/_modulith/*` ([deployment.md](deployment.md)) |
| `auto_discover` | boolean, `true` | `MODULITH_AUTO_DISCOVER` | `false`: bootstrap imports no module, and in one process and in the CLI no outbox store is bound from `outbox_url` |
| `production` | boolean, `false` | `MODULITH_PRODUCTION` | refuses an unset `outbox` and a `database` broker without URL ([deployment.md](deployment.md)) |
| `observability` | boolean, unset | `MODULITH_OBSERVABILITY` | unset: spans when OpenTelemetry is installed; `false`: none; `true`: startup fails without it (`observability = true needs OpenTelemetry, which is not installed.`) |
| `verify_manifests` | boolean, `true` | `MODULITH_VERIFY_MANIFESTS` | check every `_manifest.py` against the code at bootstrap |
| `strict_boundaries` | boolean, `false` | `MODULITH_STRICT_BOUNDARIES` | refuse to start on any boundary violation, warnings included ([troubleshooting.md](troubleshooting.md)) |

## `[tool.modulith.outbox_options]`

Applied to the store modupy builds from `outbox_url`. An application that calls `outbox.configure()` itself keeps its own settings, and bootstrap logs a WARNING naming the keys it ignored. What the retry cycle does with them: [outbox.md](outbox.md).

| Key | Default | Accepted |
|---|---|---|
| `dead_letter_after_attempts` | `10` | integer > 0 |
| `retry_interval_seconds` | `30` | number > 0: how often the sweep runs |
| `retry_stale_seconds` | `30` | number > 0: a periodic sweep takes only rows at least this old |
| `max_retry_backoff_seconds` | `300` | number > 0: the longest wait between attempts |
| `claim_strategy` | `"lease"` | `"lease"`, `"advisory_lock"` (PostgreSQL only), `"none"` (one process only) |
| `claim_lease_seconds` | `60` | number > 0, at most `86400`; renewed while the listener runs, for at most ten leases |
| `claim_batch_size` | `100` | integer > 0 |
| `completion_mode` | `"update"` | `"update"` keeps delivered rows, `"delete"` removes them, `"archive"` moves them |
| `sqlite_wal` | `false` | `true` or `false`; SQLite only, ignored elsewhere |

Any other key fails: `Unknown outbox_options keys: ['retry_interval']. Valid keys: [...]. Did you mean: 'retry_interval' -> 'retry_interval_seconds'?`

## `[tool.modulith.workers]`, `verify`, `subscriptions`

```toml
[tool.modulith.workers]
default = 1        # every module not listed; default 1
payments = 2       # two payments processes

[tool.modulith.verify]
disabled_rules = ["data-ownership"]   # off for verify, doctor and the strict_boundaries check

[tool.modulith.subscriptions]
payments = ["database:refunds.requested"]   # read only with subscription_source = "config"
```

- **`workers`**: module name to a positive integer. Applies only under `topology = "processes"`; `modulith run --workers '{"payments": 2}'` replaces the whole table for that run. A name that is not a module passes every check until the process topology starts, then exits 1: `workers names 'notifications', which is not a module of package 'myapp'; discovered modules: ...`.
- **`verify`**: only `disabled_rules` is read, as a list of rule names; any other key here is ignored. Disabling a rule is not a fix ([modules-and-contracts.md](modules-and-contracts.md)).
- **`subscriptions`**: module name to a list of `scheme:destination` targets the module's worker also consumes. A target without a scheme fails: `subscriptions targets must be non-empty 'scheme:destination' strings; ...`. The scheme must be the configured `broker`.

## `[tool.modulith.broker_options]`

Read only by the broker that `broker` selects. **Keys are not validated as names: a misspelled key is silently ignored.** Copy names from the tables below. `[tool.modulith.broker]` is accepted as another spelling, but it cannot sit beside `broker = "..."`: TOML forbids one key being both.

Environment overrides:

- `shm` and `database`: every key below also reads `MODULITH_BROKER_<KEY>` (the key in capitals: `MODULITH_BROKER_STATE_DIR`, `MODULITH_BROKER_URL`), which wins over the table.
- `redis-streams`: `url` reads `REDIS_URL`, `stream_prefix` reads `MODULITH_STREAM_PREFIX`, `max_stream_len` reads `MODULITH_STREAM_MAXLEN` (each falls back to its `MODULITH_BROKER_<KEY>`); `dlq_max_stream_len`, `max_payload_bytes` and the two `retry_backoff_*` keys read `MODULITH_BROKER_<KEY>`; `poll_block_ms`, `reclaim_min_idle_ms` and `max_delivery_attempts` come from the table only.

**`shm` and `database`:**

| Key | Default | What it does |
|---|---|---|
| `state_dir` | a private per-user directory whose name derives from the install path | `shm`: where the store lives, created 0700. `database`: where the SQLite fallback goes when there is no `url`. Set it in production |
| `max_delivery_attempts` | `5` | failed deliveries before a dead letter |
| `retry_backoff_base_seconds`, `retry_backoff_cap_seconds` | `0.05`, `5` | wait before the next attempt, doubling up to the cap; each at most `86400`, base not above cap |
| `reclaim_stale_seconds` | `60` | a delivery whose worker died is redelivered after this; a running listener's claim is renewed for at most ten times this |
| `dispatch_concurrency` | `10` | listeners one consumer runs at once |
| `batch_size`, `poll_interval_ms` | `100`, `20` | rows claimed per poll, and the poll cadence |
| `completion_mode` | `"delete"` | `"delete"` removes an acknowledged row, `"mark"` keeps it until pruned |
| `retention_age_seconds`, `prune_interval_seconds` | `259200` (3 days), `300` | dead and done rows are pruned after this age; `prune_interval_seconds = 0` stops pruning |
| `orphan_retention_seconds` | `shm` `3600`, `database` `86400` | how long a publication is kept for a group that subscribes late |
| `max_payload_bytes` | `16777216` | largest serialized event; `shm` at most `1000000000` |
| `sqlite_synchronous` | `"NORMAL"` | `"FULL"` keeps the last commits through a power loss |

**`shm` only:** `sqlite_path` (default `.modulith-shm-broker.db`, relative to `state_dir`), `hint_path` (`.modulith-shm-broker.hints`), `max_store_bytes` (`1073741824`, the size cap of the store, where every publication holds space for `orphan_retention_seconds`; at most 1 TiB), `shm_capacity` (`8192`). A database URL here is refused: `broker='shm' accepts only filesystem paths, not SQLAlchemy or network URLs.`

**`database` only:**

| Key | Default | What it does |
|---|---|---|
| `url` (or `dsn`) | unset: a SQLite file in `state_dir`, with a WARNING; refused with `production = true` | async SQLAlchemy URL; PostgreSQL or MySQL for several hosts |
| `schema` | unset (`MODULITH_DB_SCHEMA`) | PostgreSQL schema of the `broker_*` tables |
| `pool_size`, `max_overflow` | SQLAlchemy's; `1` and `0` on a SQLite file | connection pool |
| `busy_timeout_ms` | `250` | SQLite wait for the write lock |
| `no_subscriber_policy` | `"error"` | publishing to a target no group subscribes to: `"error"` raises `NoSubscribersError`, `"wait"` polls, `"store"` keeps it for later groups |
| `no_subscriber_wait_timeout_seconds`, `no_subscriber_wait_poll_interval_ms` | `30`, `100` | for `"wait"` |
| `orphan_replay_policy` | `"ttl_all_groups"` | for `"store"`: also `"first_groups"`, `"expected_groups"` |
| `expected_consumer_groups` | unset | table of target to list of groups, for `"expected_groups"` |

**`redis-streams`:** `url` (`redis://localhost:6379`), `stream_prefix` (`"modulith.events"`), `max_stream_len` (`10000`; every stream is trimmed to it, consumed or not), `dlq_max_stream_len` (ten times `max_stream_len`), `max_payload_bytes` (`16777216`), `poll_block_ms` (`1000`), `reclaim_min_idle_ms` (`60000`), `max_delivery_attempts` (`5`), `retry_backoff_base_seconds` and `retry_backoff_cap_seconds` (`0.05`, `5`).

## Other environment variables

| Variable | What it does |
|---|---|
| `MODULITH_ACTUATOR_TOKEN` | bearer token for the proxy's `/_modulith/*`; the only place it can be set ([deployment.md](deployment.md)) |
| `MODULITH_PROXY_MAX_BODY_BYTES` | largest request body the proxy forwards; default 10 MiB, larger gets 413 |
| `MODULITH_PROXY_MAX_CONNECTIONS` | concurrent proxied requests per worker replica; default 1000 |
| `MODULITH_DB_SCHEMA` | schema the packaged migrations and the `database` broker use on PostgreSQL when none is given |
| `MODULITH_MODULE`, `MODULITH_APP_PACKAGE` | which module(s) a worker started with `uvicorn modulith._worker:create_app --factory` hosts, and the package; `modulith run`, `k8s-manifest` and `extract` set them |

## Settings that interact

- `topology = "processes"` with `broker` unset picks `shm`, or `database` when `broker_options.url`/`dsn` or `MODULITH_BROKER_URL` is set, and logs `topology='processes' with no broker configured — defaulting to the durable SHM broker ...`. An explicit `broker = "memory"` there is refused: `topology='processes' requires a cross-process broker, but broker='memory' was explicitly configured.`
- `production = true` with `outbox` unset: `Cannot start in production mode with the default in-memory outbox — events would be lost on restart.` Opt in with `outbox = "memory"` if that is really meant. With `broker = "database"` under `processes` and no URL: `Cannot start in production with broker='database' and no URL ...`.
- `outbox` other than `"memory"` needs `outbox_url` (or `MODULITH_OUTBOX_URL`): workers refuse to start without it, and `modulith outbox ...` and `migrate` find nothing. With `auto_discover = false` the single process and the CLI bind no store from it.
- `own_process`, `workers` and `worker_liveness_timeout_seconds` do nothing under `topology = "single"`, so one file can serve `uvicorn` locally and `modulith run --topology processes` in production.
- `@listener(timeout=...)` should stay below ten times `reclaim_stale_seconds` (broker) and ten times `claim_lease_seconds` (outbox). A listener still running past that may be delivered a second time beside itself.
- `strict_boundaries = true` honours `[tool.modulith.verify] disabled_rules`: a disabled rule no longer stops the start either.

## A complete example

Every key below is real; most projects need only a few of them.

```toml
[project]
name = "myapp"
version = "0.1.0"

[tool.modulith]
package = "myapp"                     # the import package; also protects tests from mis-detection
contracts_module = "contracts"        # myapp/contracts/ holds the shared events
outbox = "postgres"                   # store events in the same transaction as the data
outbox_url = "postgresql+asyncpg://app@db/app"   # the business database; MODULITH_OUTBOX_URL wins
topology = "processes"                # workers behind a proxy; "single" is the default
broker = "database"                   # how events cross processes
own_process = ["assistant"]           # only assistant gets a worker of its own
worker_liveness_timeout_seconds = 30  # restart a worker that stops answering /health
strict_boundaries = true              # refuse to start on a boundary violation
production = true                     # outbox must be explicit, database broker needs a url

[tool.modulith.outbox_options]
dead_letter_after_attempts = 10       # default
max_retry_backoff_seconds = 300       # default

[tool.modulith.broker_options]
url = "postgresql+asyncpg://app@db/app"   # MODULITH_BROKER_URL wins; keep passwords there
max_delivery_attempts = 5                 # default

[tool.modulith.workers]
default = 1
assistant = 2
```

## The smallest configuration for each setup

**One process, in-memory events (the default).** Only `package = "myapp"` under `[tool.modulith]`: the app runs without it, but tests that publish from their own body then take the test file for the package (above).

**One process with the durable outbox.** Then run `modulith migrate` and wire the session and lifespan ([outbox.md](outbox.md)).

```toml
[tool.modulith]
package = "myapp"
outbox = "postgres"
outbox_url = "sqlite+aiosqlite:///myapp.db"   # MODULITH_OUTBOX_URL in production
```

**One process per module, on one machine.**

```toml
[tool.modulith]
package = "myapp"
topology = "processes"
broker = "shm"

[tool.modulith.broker_options]
state_dir = "/var/lib/myapp/modulith"   # or MODULITH_BROKER_STATE_DIR
```

**A broker shared by several machines.** Pass the URL through the environment: `MODULITH_BROKER_URL` for `database`, `REDIS_URL` for `redis-streams` (`pip install 'modupy[redis]'`).

```toml
[tool.modulith]
package = "myapp"
topology = "processes"
broker = "database"   # pip install 'modupy[database]'; MODULITH_BROKER_URL=postgresql+asyncpg://...
```

After any change: `modulith info` shows the package, `outbox`, `broker` and `topology` the CLI resolved, and `modulith doctor` checks them. For a setting chosen only by an environment variable, run both with that variable set.

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| `strict_boundaries = "true"` | a string is not a boolean; startup fails | `strict_boundaries = true` |
| `[tool.modulith.outbox]` with `retry_interval_seconds` | `outbox` is the adapter name; the table is refused | `[tool.modulith.outbox_options]` |
| `broker = "redis-streams"` plus a `[tool.modulith.broker]` table | TOML forbids one key as both; the file does not parse | `[tool.modulith.broker_options]` |
| Trusting a `broker_options` key because startup passed | names there are not checked; `state_dirr` is ignored | copy the key from the tables above, then read the startup log |
| `MODULITH_OWN_PROCESS=assistant` | `own_process` has no environment variable; it is ignored | `own_process = ["assistant"]` in `pyproject.toml` |
| `configure(outbox_url=...)` in `main.py` | the CLI and every worker never run it | `outbox_url` in `pyproject.toml`, or `MODULITH_OUTBOX_URL` |
| Leaving `package` unset because the CLI finds `[project] name` | the running app and tests detect the package from the call stack, not from that name | `package = "myapp"` |
| Putting the database password in `outbox_url` or `broker_options.url` in the file | it is committed with the code | `MODULITH_OUTBOX_URL`, `MODULITH_BROKER_URL`, `REDIS_URL` at run time |
| `outbox_url = "postgresql://..."` | the store needs an async driver | `postgresql+asyncpg://`, `mysql+aiomysql://`, `sqlite+aiosqlite:///` |
| `production = true` with `outbox` unset | refused by design | `outbox = "postgres"`, or `outbox = "memory"` on purpose |
| `topology = "processes"` with `broker = "memory"` | refused | leave `broker` unset for `shm`, or name a real broker |
| Running `modulith doctor` from a subdirectory with its own `pyproject.toml` | that file is the one read | run from the project root |
| Adding `[tool.modulith.verify] mode = "ratchet"` | ignored; only `disabled_rules` is read there | pass `--mode=ratchet` to `modulith verify` |

Depth: [ARCHITECTURE.md section 3](https://github.com/jeansossmeier/modupy/blob/main/docs/ARCHITECTURE.md#3-configuration-resolution) (how settings resolve) and [API_REFERENCE.md](https://github.com/jeansossmeier/modupy/blob/main/docs/API_REFERENCE.md#configuration).
