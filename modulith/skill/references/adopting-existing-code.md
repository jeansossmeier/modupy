# Adopting modupy in existing code

Read this when:

- the repository already has application code and the task is to bring modupy into it;
- the project has a `.modulith-baseline.json` or a `MIGRATION.md`, or CI runs `modulith verify --mode=ratchet`;
- `modulith verify --mode=ratchet` failed and you must choose between fixing the code and updating the baseline.

Contents: [Rules](#rules-for-the-whole-adoption) · [Audit](#step-1-audit) · [Modules](#step-2-move-code-into-modules) · [Baseline](#step-3-baseline-and-ratchet) · [When the ratchet fails](#when-the-ratchet-fails) · [strict_boundaries](#keep-strict_boundaries-off) · [Events](#step-4-replace-cross-module-calls-with-events) · [Outbox and processes](#steps-5-and-6-the-outbox-then-processes) · [Mistakes](#mistakes-that-look-right)

## Rules for the whole adoption

1. Never run `modulith init` in existing code. It writes the sample starter project, into `./NAME` or, with `.`, among your files; it does not add modupy to the code you have.
2. Go in this order, one step per change: audit, modules, baseline, events, outbox, processes. Steps 5 and 6 only when the need is real.
3. Never restructure the whole codebase in one change. Move one domain at a time, and never mix moving files with changing logic.
4. Never add a new violation to the baseline to make it pass. The baseline holds the violations that existed when adoption began; it only shrinks.
5. Keep `strict_boundaries` off until `modulith verify --fail-on-warnings` passes without a baseline.

Install the command (`pip install 'modupy[cli]'`, or `uv add 'modupy[cli]'`), then name the import package. CLI commands otherwise take `[project] name` and fail when it differs (`error: the application package 'shop_api' failed to import (ModuleNotFoundError: No module named 'shop_api')`), and a test that publishes from its own body takes the test file for the package ([modules-and-contracts.md](modules-and-contracts.md)):

```toml
[tool.modulith]
package = "myapp"
```

The package must import from the project root. In a `src/` layout that is not installed, every command fails the same way until you install the project or run with `PYTHONPATH=src`.

## Step 1: audit

Run it from the project root. It parses the code with `ast` and imports nothing, so it is safe on any codebase.

```text
$ modulith audit
audited <project>/myapp
readiness score: 0/100
2 cross-module import pattern(s), 0 shared table(s)
wrote audit report to MIGRATION.md
```

- Without a path it audits the application package: the single package under `src/`, else the one top-level package. When choosing, it ignores `tests`, `docs`, `scripts`, `examples`, `migrations`, virtualenvs, hidden and build directories, and script directories without `__init__.py` (`tools/`). The `audited` line names what it chose. `tests/` directories are never analysed.
- `modulith audit myapp` audits exactly that directory: its subdirectories are the module candidates.
- It writes the report itself and prints only the summary above. It never replaces a report: the second run exits 1 with `error: MIGRATION.md already exists; pass --force to overwrite it or --output to write the report elsewhere.` Use `--force` to replace it or `--output audit.md` to write elsewhere.
- Also exit 1: `path does not exist: nosuchdir`, `not a directory: myapp/main.py (pass the directory to audit)`, and an `--output` in a missing directory (`error: could not write audit report to reports/audit.md (...). Create the directory or pass a writable --output path.`).

What `MIGRATION.md` holds, for a layered app with `views/`, `services/` and `models/`:

```text
# modupy Audit Report

## Summary

Audited root: `myapp`

**Readiness score: 0/100**
...
## Proposed Module Structure

- `models` (4 files)
- `myapp` (2 files)
- `services` (4 files)
- `views` (4 files)

## Cross-Module Imports (would become violations)

| From | To | Count | Sample |
| --- | --- | --- | --- |
| `myapp` | `views` | 3 | `main.py` |
| `views` | `services` | 3 | `views/inventory.py` |

## Shared Tables (need ownership decisions)
...
## Listener-Shaped Code (events conversion candidates)
...
## Recommended Next Steps
```

How to read it:

- **Proposed modules** are the top-level subdirectories of the audited directory. Here they are layers, not domains: the real modules are `orders`, `payments` and `inventory`, cutting across all three. Propose the domain split to the user; the audit is a starting point, not a verdict.
- **The row named after the package** (`myapp`) is the files at its root, such as `main.py`. They are not a module, and `main.py` may import every module's router.
- **Cross-module imports** count import statements between candidates. Imports of the contracts package (`[tool.modulith] contracts_module`, default `contracts`) are not counted.
- **Shared tables** are table names written as literals (`Table("orders", ...)`, `__tablename__`, `ForeignKey("orders.id")`) in more than one candidate. Each needs one owning module.
- **Listener-shaped code** lists files with a function named `on_*`, `handle_*` or `process_*`, or decorated with a name containing `receiver`, `signal`, `listener`, `connect` or `subscribe`.
- **Readiness score** = listener-shaped files ÷ (cross-module imports + listener-shaped files) × 100, and 100 when there are neither. Shared tables are not in it. Cross-module imports with no listener-shaped files score 0. It gates nothing; use it to compare runs.
- **Recommended Next Steps** are the same in every report and start with the baseline. On code that is not yet in module packages, do step 2 first.
- The report has relative paths and no timestamp, so it can be committed and diffed.

The score is `n/a` on the console (`not applicable` in the report) when there are fewer than two module candidates: a flat package with every file directly in it, a package with one subpackage, `modulith audit .` above `src/`, or no Python files at all. It is also withheld when no import crosses candidates but some imports name packages below the audited directory that are not candidates. Except for an empty tree, the console prints the reason as a `warning:` line:

```text
$ modulith audit
audited <project>/myapp
readiness score: n/a (no module boundaries to score; see the warning)
0 cross-module import pattern(s), 0 shared table(s)
wrote audit report to MIGRATION.md
warning: only one module candidate (`myapp`) was found, so every import stays inside it and there are no boundaries to score. Run `modulith audit <dir>` on the directory whose subdirectories are your intended modules.
```

Rerun with the directory the warning asks for, or, for a flat package, decide the modules with the user and move code first (step 2).

## Step 2: move code into modules

Move files into one package per domain. Change imports, nothing else, and run the existing tests after each move.

```text
myapp/
├── main.py                 # mounts each module's router; not a module
├── orders/
│   ├── __init__.py         # required: without it the directory is not a module
│   ├── api.py              # was views/orders.py
│   └── _internal/
│       ├── __init__.py
│       ├── models.py       # was models/order.py
│       └── service.py      # was services/order_service.py
├── payments/               # same pattern
└── inventory/              # same pattern
```

- Do one domain per change. Each change moves files only and keeps the tests green.
- Confirm with `modulith info` that the modules are listed. Before this step `modulith verify` reports `✓ no boundary violations` on a layered app, because each layer is taken for a module and layers import each other's public names. That clean result means nothing.
- Code that other modules call today ends up in `_internal`, so `modulith verify` now lists the existing coupling:

```text
$ modulith verify

inventory:
  [ERROR] no-internal-imports: inventory imports 'myapp.orders._internal.models', reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/inventory/_internal/models.py:3)
  [ERROR] no-cyclic-dependency: cyclic dependency among modules: inventory, orders, payments. The module dependency graph must be acyclic — break the cycle by moving shared types to contracts or inverting a dependency via events.

orders:
  [ERROR] no-internal-imports: orders imports 'myapp.inventory._internal.service', reaching into inventory's private package. Cross-module access must go through inventory's public API.  (myapp/orders/_internal/service.py:1)
  [ERROR] no-internal-imports: orders imports 'myapp.payments._internal.service', reaching into payments's private package. Cross-module access must go through payments's public API.  (myapp/orders/_internal/service.py:2)

payments:
  [ERROR] no-internal-imports: payments imports 'myapp.orders._internal.models', reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/payments/_internal/models.py:3)

5 error(s), 0 warning(s)
```

Do not fix these now. Record them (step 3), then remove them one per change.

## Step 3: baseline and ratchet

Make the baseline once the moves are done: it identifies each violation by its file path, so a file moved later re-reports its violations.

```text
$ modulith verify --update-baseline
baseline updated: 5 violation(s) recorded in .modulith-baseline.json
$ modulith verify --mode=ratchet
✓ no boundary violations
```

- `--update-baseline` writes every current violation and exits 0 whatever `--mode` says, so it is the same as the `modulith verify --mode=ratchet --update-baseline` that `MIGRATION.md` suggests. `--baseline PATH` uses another file (default `.modulith-baseline.json`).
- Commit `.modulith-baseline.json`. Its paths are relative to the project root, so it matches on every machine and in CI.
- Add the ratchet to CI: `modulith verify --mode=ratchet`. Any error-level violation the baseline does not hold fails it with exit 1; add `--fail-on-warnings` to fail on new warnings too.
- `modulith doctor` reads the same file and reports `⚠ boundary health — 5 violation(s), all baselined`.

Each entry is a fingerprint plus a count:

```json
{
  "count": 1,
  "location": "myapp/orders/_internal/service.py",
  "message_hash": "74a5c12d",
  "module": "orders",
  "rule": "no-internal-imports"
}
```

The fingerprint is the rule, the module, the file (no line number; empty for a cycle) and a hash of the message. So:

- edits that shift a baselined line up or down still pass;
- a second identical import in a file that already has one is new and fails: the count is part of the ratchet;
- moving or renaming the file, renaming a module, or anything that changes the message makes the old entry stop matching;
- a missing baseline file counts as empty, so `--mode=ratchet` reports everything, like strict mode;
- a corrupt file exits 1 with `error: baseline file .modulith-baseline.json is not valid JSON (...)` and a hint to regenerate it. Restore it from git instead: regenerating it on a branch with new violations would bake them in.

## When the ratchet fails

```text
$ modulith verify --mode=ratchet

payments:
  [ERROR] no-internal-imports: payments imports 'myapp.orders._internal.service', reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/payments/api.py:1)

1 error(s), 0 warning(s)
```

Only violations beyond the baseline are printed. For each one:

1. **New coupling from your change** (as above): fix the code. Each rule's fix is in [modules-and-contracts.md](modules-and-contracts.md). Do not touch the baseline.
2. **An old violation reported again** because you moved or renamed its file, or because your fix changed a cycle (after one event replaces `orders` → `inventory`, the cycle message changes from `inventory, orders, payments` to `orders, payments` and is printed as new): run `modulith verify --update-baseline`, then check that the number it prints is not above the sum of the `count` fields before, and that `git diff .modulith-baseline.json` shows only changed locations or hashes and removed entries.
3. **After you fixed a violation** the ratchet already passes, since fewer is fine. Still run `modulith verify --update-baseline` in the same change so the file shrinks and the diff shows what was fixed.

## Keep strict_boundaries off

`strict_boundaries = true` makes the start check every violation, warnings included, and it ignores the baseline. With baselined violations left:

```text
$ modulith verify --mode=ratchet
✓ no boundary violations
$ modulith info
error: boundary violations detected with strict_boundaries=True:
  - [ERROR] inventory: no-internal-imports: inventory imports 'myapp.orders._internal.models', ... (myapp/inventory/_internal/models.py:3)
  ...
```

The same `ConfigurationError` fails the app at its first `publish()`, so every test that publishes fails. Turn it on only when `modulith verify --fail-on-warnings` passes without a baseline. Details: [troubleshooting.md](troubleshooting.md).

## Step 4: replace cross-module calls with events

Take one cross-module call per change, the first row of the audit table or a baselined `no-internal-imports`. Before, `orders` called into `inventory`'s private service:

```python
# myapp/orders/_internal/service.py, before
from myapp.inventory._internal.service import reserve_stock
```

After: an event in contracts, published by `orders`, handled by a listener in `inventory`.

```python
# myapp/contracts/events.py (with an empty myapp/contracts/__init__.py beside it)
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str
```

```python
# myapp/orders/_internal/service.py
from modulith import publish

from myapp.contracts.events import OrderCreated

_orders: dict[str, str] = {}


async def create_order(customer_id: str) -> str:
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id
    await publish(OrderCreated(order_id=order_id))  # was: await reserve_stock(order_id)
    return order_id
```

```python
# myapp/inventory/handlers.py
from modulith import listener

from myapp.contracts.events import OrderCreated
from myapp.inventory._internal.service import reserve_stock


@listener
async def on_order_created(event: OrderCreated) -> None:
    await reserve_stock(event.order_id)
```

```python
# myapp/inventory/__init__.py
from myapp.inventory import handlers as handlers  # registers the listener
```

Then run the existing tests, `modulith verify --mode=ratchet`, and `modulith verify --update-baseline` (rule 2 or 3 above). Without the import in `inventory/__init__.py` the listener never registers and nothing reports it, so a test must prove the stock is reserved; the [recipes.md](recipes.md) flow tests show how. `modulith doctor`'s process-split readiness rises as calls become events.

- In one process `publish()` waits for every listener and re-raises the first failure, as the direct call did.
- A sync view cannot `await`; it calls `publish_sync(event)` from `modulith`.
- A `_manifest.py` (MIGRATION.md's third step) is optional. If it sets `declared_dependencies`, list every module the module imports today, or the ratchet fails on the new `undeclared-dependency` errors ([modules-and-contracts.md](modules-and-contracts.md)).

## Steps 5 and 6: the outbox, then processes

- **Outbox**, once losing an event matters, such as with real users in production: [outbox.md](outbox.md). Listeners then run after the commit and are retried, at least once, so they must be safe to run twice.
- **Process per module**, only when one module needs its own CPU or isolation: [processes-and-brokers.md](processes-and-brokers.md). First remove that module's direct cross-module imports (`modulith doctor` lists them per module): in its own process, a call into another module runs against that process's own copy of it, so in-memory state differs with no error.

## Mistakes that look right

| The move | Why it is wrong | The right move |
|---|---|---|
| `modulith init` in the repository | With your package's name it refuses (`error: myapp already exists; init never overwrites it`); any other name writes the sample project, `./shop/pyproject.toml` with `./shop/shop/`, or with `.` a `pyproject.toml`, `shop/` and `tests/` among your files | Delete only what it wrote: the `created` list it printed, and the skill directories its `installed the modupy skill in` lines name; follow this file |
| Running `modulith verify --update-baseline` to make your change pass | It records your new violation as accepted, and CI stops reporting it | Fix the code; re-baseline only after rule 2 or 3 of the ratchet section |
| Moving every directory and converting calls in one change | A broken test cannot be traced to one move, and the baseline churns | One domain per change; files first, logic later |
| Generating the baseline before moving files | Every moved file's violations come back as new | Move into modules first, then make the baseline |
| Trusting `✓ no boundary violations` on a layered layout | Layers count as modules and import each other's public names | Restructure by domain, then verify |
| Turning on `strict_boundaries` to "enforce boundaries now" | The start refuses every violation, baseline or not; the app and tests stop working | Ratchet in CI; strict only once verify is clean |
| Disabling a rule in `[tool.modulith.verify] disabled_rules` to get green | It hides that rule's future violations everywhere | Baseline today's violations instead |
| Fixing all baselined violations in one change | Large, risky diffs | One violation or one call per change, each shrinking the baseline |
| Taking the audit's proposed modules as the design | They are folder names, often layers | Agree the domain modules with the user |

Depth: [MIGRATION_GUIDE.md](https://github.com/jeansossmeier/modupy/blob/main/MIGRATION_GUIDE.md).
