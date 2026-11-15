---
name: modupy
description: Use when working in a Python project whose pyproject.toml lists modupy as a dependency or has a [tool.modulith] table, when code imports modulith (for example from modulith import event, listener, publish), when the modulith command is run or mentioned, or when the user asks to build a modular monolith or a new FastAPI backend with modupy. Also use for any question or change about modupy module boundaries, private module code, events and listeners, the contracts package, modulith verify violations, the outbox and its dead letters, process-per-module topology and workers, brokers (shm, database, redis-streams), deploying a modupy app, or adopting modupy in an existing codebase. The PyPI distribution is modupy; its import package and command are modulith.
---

# modupy

modupy (distribution `modupy`; import package, command and `[tool.modulith]` table: `modulith`) builds a FastAPI backend as one application package made of modules. A module is a direct subpackage of that package with an `__init__.py` (`myapp/orders/`). Its names without a leading `_` are its public API; every `_` name (`_orders`, `_internal/`, `_tracking.py`) is private to it. Modules talk through events: classes marked `@event` in the `contracts` package (`myapp/contracts/`), which one module publishes and others receive with `@listener`. A module that needs an answer during a request may call another module's public function. The same code runs in one process or with one process per module. In the second shape every process holds its own copy of each module's memory and never runs `main.py`, so anything modules share only in memory, or only through `main.py`, breaks there without an error. Every rule below follows from that.

## The loop: every task, in this order

1. **Orient.** From the project root (the directory of the nearest `pyproject.toml`), run `modulith info` and read `[tool.modulith]` in `pyproject.toml`. `info` prints the package, its modules, and the `outbox`, `broker` and `topology` in use. Where no `pyproject.toml` exists it exits 1 with `could not determine the application package`: there is no modupy project here yet, and step 2 tells a new project from existing code. In a project that lists modupy or has `[tool.modulith]`, exit 1 means the package was not found or does not import: fix that first ([troubleshooting.md](references/troubleshooting.md)). Without `modulith` on `PATH`, use `python -m modulith ...` with the project's interpreter; when neither is installed, `pip install 'modupy[fastapi,cli]'` first.
2. **Route by situation.**
   - No project yet (an empty directory, or the user wants a new modupy backend): [Start a new project](#start-a-new-project).
   - Code exists that does not use modupy: follow [adopting-existing-code.md](references/adopting-existing-code.md). Never run `modulith init` there.
   - The project has a `.modulith-baseline.json`: it is mid-adoption. Run `modulith verify --mode=ratchet` wherever this file says `modulith verify`, and read adopting-existing-code.md before touching the baseline.
   - A modupy project: go on.
3. **Make the change the modupy way.** Obey the [hard rules](#hard-rules). Take the files, code and checks of a routine change from [recipes.md](references/recipes.md). Before touching the outbox, the topology, brokers or deployment, open the reference the [router](#which-reference-to-open) names. One module, event, listener or route per step, each with its test.
4. **Pass the done-gate**, from the project root, after the last edit:
   1. `modulith verify`: the output must say `✓ no boundary violations`. Exit 1 is an `[ERROR]` violation, a module that does not import, or a bad option value or baseline file; exit 2 is a wrong command line or a crash with a traceback. Exit 0 can still print `[WARNING]` lines: read and fix them.
   2. `pytest`: read the summary line. It needs a passed count above 0 and no failure or error. `no tests ran` exits 5 and is not a pass. Every new listener needs a test that proves it fires: `modulith verify` passes when a listener never registers.
   3. `modulith doctor`, after any change to `[tool.modulith]`, the outbox, the topology, a broker, a manifest or an event's fields: exit 0, and read every `⚠` line (warnings pass, but each one names a real gap). It writes a `.modulith-schemas.json` cache in the project root: keep that file out of git.
   4. After an HTTP change: start `modulith dev myapp.main:app` in the background (it serves until stopped), wait for `http://127.0.0.1:8000`, send real requests to every changed route, check status and body, then stop the server.
   5. When the project configures ruff, as the starter does: `ruff check --fix myapp tests` and `ruff format myapp tests` (`pip install ruff` if it is missing), then rerun the steps above if they changed files. Name the code: with ruff's preview mode on, `ruff format .` also rewrites the code blocks of the installed skill under `.claude/` and `.agents/`.

   The work is not done until every step is green on the final code. When one is red, fix the code and run the whole gate again; never fix the check.

## Hard rules

Never:

- **Import another module's private names or internals** (`from myapp.orders import _orders`, `myapp.orders._internal`): they are the part the owner may change without telling anyone, and `verify` reports `no-internal-imports`. Reaching them through `orders._orders`, `importlib` or a helper in `main.py` breaks the same way, unseen.
- **Share tables or in-memory state between modules** (module-level dicts, caches, singletons, locks; foreign keys, joins or writes into another module's tables): each process imports its own copy, so a write in one is invisible in another, and a module split into its own service takes its tables with it.
- **Call another module to make it react** (`orders` calling `payments.charge()`): publish an event and let the other module listen; the publisher imports no other module and works unchanged in every topology.
- **Put a live object on an event** (a session, an ORM row, a client): with the outbox or a broker the event travels as JSON, and `publish()` raises `TypeError: cannot JSON-serialize ...`.
- **Remove or rename a field of an existing event, or add one without a default**: a stored event that no longer fits its class fails to load with `TypeError`.
- **Silence `modulith verify`**: no rule in `[tool.modulith.verify] disabled_rules`, no `modulith verify --update-baseline` or baseline edit to accept a violation you introduced, no `if TYPE_CHECKING:` around the import, no renaming `_orders` to `orders_store`, no copying private code into your module or logic into contracts. Each makes the report green and leaves the coupling; apply the fix for that rule from [modules-and-contracts.md](references/modules-and-contracts.md).
- **Run `modulith init` inside existing code**: it writes the sample starter project, into `./NAME` or, with `.`, among your files; it does not add modupy to the code you have.
- **Put a check that every route of a module needs in `main.py`** (a login or rate limit as middleware, `FastAPI(dependencies=...)` or `include_router(..., dependencies=...)`): a module in its own process never runs `main.py`, so there its routes are open. Put it on the module's router: `APIRouter(dependencies=[Depends(require_user)])`.
- **Turn on the outbox, the process topology or a networked broker** (`outbox = "postgres"`, `topology = "processes"`, `broker = "database"` or `"redis-streams"`) unless the user asks for it or the reference for the situation says it is required: each needs more infrastructure and makes delivery asynchronous and at-least-once.
- **Delete queued events or dead letters without asking the user first** (`modulith broker drop-group`, deleting the `shm` broker's store, deleting rows from `event_publications`): those events are destroyed, not delivered.
- **Turn on `strict_boundaries`** while any violation or baseline entry remains: the start, and every test that publishes, then fails.
- **Catch exceptions to hide a failing listener** (around `publish()`, or everything inside a listener): the failure is hidden and, with the outbox, the event is marked delivered and lost. Fix the listener and let it raise.

Always:

- **Define an event that a second module uses in `myapp/contracts/`, and import it from there on both sides**: dispatch is by exact class, and contracts imports no module.
- **Move data between modules on events first.** Put ids and plain values on the event (`str`, `int`, `float`, `bool`, `None`, `datetime`, `date`, `UUID`, `Decimal`, enums, nested dataclasses, lists of those) and let the consumer keep its own copy in a listener; that works in every topology.
- **Call another module's public function** (`from myapp.orders import is_fulfilled`) **only when the caller needs the answer during the request and that function reads shared storage such as a database**: the call runs on the caller's own copy of the other module, so over an in-memory dict it answered 200 in one process and 404 with one process per module.
- **Import every file that defines a `@listener` from its module's `__init__.py`** (`from myapp.payments import handlers as handlers  # noqa: E402`, at the bottom when that file imports from its own package): modupy imports only `__init__.py` and `_manifest.py`, so otherwise the listener never runs and nothing reports it.
- **Re-export `router` from the module's `__init__.py`, declare its paths without the module prefix, and mount it in `main.py` at `/<module>`**: a module's own process serves exactly `myapp.<module>.router` under `/<module>`, so the URLs stay the same in every topology.
- **Set `package` under `[tool.modulith]`**: without it the running app takes the package of the code that first calls `publish()`, which in a test can be the test file.
- **Write listeners that are safe to run twice**: the outbox and the brokers deliver at least once.
- **In tests, request `scenario` or `modulith_app` and import application code inside the test function**: the fixtures reset the runtime and re-import your modules, and a top-level import registers listeners the reset then drops.
- **Keep shared infrastructure that is not business logic** (database engine, settings, an auth dependency) **in a plain file at the package root**, such as `myapp/db.py`: it is not a module, every module may import it, and each process builds its own.

## Start a new project

`modulith init NAME [DIR]` writes the quickstart: modules `orders`, `payments` and `inventory`, events `OrderCreated` and `PaymentReceived`, a FastAPI app and 5 tests, with no database or broker to set up. `--empty` leaves the sample out: an app with no modules and 1 test. The project root gets `pyproject.toml`, the package `NAME/`, `tests/`, a `.gitignore` and this skill in `.claude/skills/modupy` and `.agents/skills/modupy` (an existing `.gitignore` or skill install is kept), so the skill travels with the project. It never overwrites: it exits 1 with `error: <path> already exists; init never overwrites it` when `./NAME` exists, or when `DIR` already holds a `pyproject.toml`, a `NAME` or a `tests`. Something is already there: go back to step 2 of the loop, and never delete it to make room.

1. Pick `NAME`, the import package: it matches `[a-z][a-z0-9_]*`, is not `tests`, and is not already importable (`json`, an installed package).
2. Create the project where the user wants it, with `--empty` when the user's domain is not orders and payments.
   - In the directory you were given (an empty workspace, a fresh repository with only `.git` or a README): `modulith init NAME .` makes it the project root.
   - In a new directory: `modulith init NAME`, then `cd NAME`.

   Every later command runs in the project root, the directory with the new `pyproject.toml`. Add `--ci github` when the user wants CI.
3. Keep `package = "NAME"` under `[tool.modulith]`, which `init` writes: rename it with the package, never delete it.
4. Install the dependencies: `--install` on the `init` line creates `.venv` (with uv, run every later command as `uv run ...`), or install into the environment that runs `modulith`: `pip install 'modupy[fastapi,cli]' pytest httpx2`.
5. Add each module with `modulith add module NAME`: it writes the module with a router, its test, and the mount in `main.py`. Then add each event, listener and route with [recipes.md](references/recipes.md), each with its test.
6. Pass the done-gate. The untouched starter gives `5 passed` and `✓ no boundary violations`; the `--empty` one gives `1 passed`, and each `add module` adds one test.

## Code shapes

An event, in contracts:

```python
# myapp/contracts/events.py
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str


@event
@dataclass(frozen=True)
class PaymentReceived:
    order_id: str
    amount_cents: int = 0  # added to an existing event, so it has a default
```

A module: private state, public functions, a publish, a listener, the router re-export:

```python
# myapp/orders/__init__.py
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived

_orders: dict[str, str] = {}  # private: order_id -> customer_id
_fulfilled: set[str] = set()


async def create_order(customer_id: str) -> str:  # public: plain data in and out
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id  # write the module's own state first
    await publish(OrderCreated(order_id=order_id))  # orders never learns who listens
    return order_id


def is_fulfilled(order_id: str) -> bool:
    return order_id in _fulfilled


@listener
async def on_payment(event: PaymentReceived) -> None:  # the annotation is the subscription
    _fulfilled.add(event.order_id)


# At the bottom, because api.py imports from this package. A worker serves this attribute.
from myapp.orders.api import router as router  # noqa: E402
```

A listener in another module. In a separate `handlers.py`, it needs `from myapp.payments import handlers as handlers  # noqa: E402` in `myapp/payments/__init__.py`. Sync code publishes with `publish_sync(event)` from `modulith`; `@listener(timeout=30)` bounds an `async def` listener.

```python
# myapp/payments/__init__.py
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived


@listener
async def charge(event: OrderCreated) -> None:
    await publish(PaymentReceived(order_id=event.order_id))
```

Routes on the module's own router, mounted at `/<module>`:

```python
# myapp/orders/api.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from myapp.orders import create_order, is_fulfilled

router = APIRouter()  # a check every route needs: APIRouter(dependencies=[Depends(require_user)])


class NewOrder(BaseModel):
    customer_id: str


@router.post("")  # no /orders prefix here: main.py adds it
async def post_order(body: NewOrder) -> dict[str, str]:
    return {"order_id": await create_order(body.customer_id)}


@router.get("/{order_id}/fulfilment")
async def get_fulfilment(order_id: str) -> dict[str, str | bool]:
    if not is_fulfilled(order_id):
        raise HTTPException(status_code=404, detail="order not fulfilled")
    return {"order_id": order_id, "fulfilled": True}
```

```python
# myapp/main.py: not a module; it only mounts each module's router
from fastapi import FastAPI

from myapp.orders import router as orders_router

app = FastAPI()
app.include_router(orders_router, prefix="/orders")
```

Tests: trigger, then expect an event; import application code inside the test:

```python
# tests/test_orders.py
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp, Scenario


def test_an_order_is_paid_and_fulfilled(scenario: Scenario, modulith_app: ModulithTestApp) -> None:
    from myapp.contracts.events import OrderCreated, PaymentReceived
    from myapp.orders import create_order, is_fulfilled

    payment = scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert payment.order_id == "ord-1"
    assert OrderCreated(order_id="ord-1") in modulith_app.published_events
    assert is_fulfilled("ord-1")  # in one process every listener has finished by now


def test_posting_an_order(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)

    assert client.post("/orders", json={"customer_id": "alice"}).json() == {"order_id": "ord-1"}
    assert client.get("/orders/ord-1/fulfilment").status_code == 200
```

`.call(fn, *args)` runs your function, `.publish(event)` publishes from the test (needs `package` set), `.matching(predicate)` filters, and a miss raises `AssertionError: expected event PaymentReceived not seen within 2s`.

## Which reference to open

- [modules-and-contracts.md](references/modules-and-contracts.md): adding or moving code and deciding which module, file or package it belongs in; any `modulith verify` violation and the right fix for its rule; choosing between an event and a public function.
- [recipes.md](references/recipes.md): a routine change: new project, module, event, listener, test, HTTP endpoint, cross-module access, private helper, rename or removal.
- [adopting-existing-code.md](references/adopting-existing-code.md): the repository had code before modupy, or has a `.modulith-baseline.json` or a `MIGRATION.md`, or `modulith verify --mode=ratchet` failed.
- [outbox.md](references/outbox.md): an event must not be lost, turning the outbox on, a database session around `publish()`, outbox dead letters.
- [processes-and-brokers.md](references/processes-and-brokers.md): `--topology processes`, workers, `own_process`, brokers, or something that works in one process and breaks under the process topology.
- [deployment.md](references/deployment.md): preparing a release, `modulith doctor` output, Kubernetes manifests, OpenAPI, tracing, actuator security, extracting a module into a service.
- [troubleshooting.md](references/troubleshooting.md): something does not behave as expected: an event never arrives, a command cannot import the app, a 404 or 502 behind the proxy, a refused start, a growing dead-letter pile.
- [cli.md](references/cli.md): the exact command, flag, default or exit code.
- [configuration.md](references/configuration.md): any `[tool.modulith]` key or `MODULITH_*` variable, a `ConfigurationError`, or the smallest configuration for a setup.
- [testing.md](references/testing.md): writing or fixing tests for events, listeners, routes, the outbox or a worker, or a test that passes alone and fails in the full run.
- [ai-features.md](references/ai-features.md): adding a model, agent or local-inference call, or a model call that hangs, crashes, drains the connection pool or must be retried.

This skill matches the installed modupy version (`modulith --version`). After upgrading modupy, run `modulith skill install` again.
