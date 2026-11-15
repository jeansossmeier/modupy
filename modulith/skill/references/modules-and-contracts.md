# Modules and contracts

Read this when:

- you are adding or moving code and need to know which module, file or package it belongs in;
- `modulith verify` reported a violation and you need the correct fix for that rule;
- one module needs something another module owns, and you are choosing between an event, a function call and a shared type.

Contents: [Layout](#project-layout) · [Discovery](#how-modupy-finds-the-package-and-its-modules) · [Public API](#a-modules-public-api) · [Contracts](#the-contracts-package) · [Events and listeners](#events-publish-and-listeners) · [Routers](#routers) · [Manifests](#manifests) · [Boundary rules](#the-boundary-rules) · [Interaction](#how-two-modules-may-interact) · [Not shared](#what-modules-must-not-share) · [Mistakes](#mistakes-that-look-right)

## Project layout

```text
myproject/
├── pyproject.toml          # [tool.modulith] package = "myapp" names the application package
├── myapp/                  # the application package
│   ├── __init__.py
│   ├── main.py             # the FastAPI app; mounts each module's router. Not a module.
│   ├── contracts/          # events and types that modules share. Imports no module.
│   │   ├── __init__.py
│   │   └── events.py
│   ├── orders/             # a module
│   │   ├── __init__.py     # public API; imports handlers; re-exports `router`
│   │   ├── api.py          # FastAPI routes on `router`
│   │   ├── handlers.py     # @listener functions
│   │   ├── _internal/      # private: any name that starts with `_`
│   │   └── _manifest.py    # optional: declare_module(...)
│   ├── payments/
│   │   └── __init__.py     # a small module can keep everything here
│   └── inventory/
│       └── __init__.py
└── tests/
```

## How modupy finds the package and its modules

- **The package.** CLI commands use `MODULITH_PACKAGE`, else `[tool.modulith] package`, else `[project] name` from the nearest `pyproject.toml` at or above the current directory, with `-` turned into `_`. The CLI puts that `pyproject.toml`'s directory on the import path, so run commands from the project. When the import package is not `[project] name`, set `[tool.modulith] package = "myapp"`.
- **The package at runtime.** The app and tests read `MODULITH_PACKAGE` and `[tool.modulith] package` too, but without them they take the top-level package of the code that calls the first `publish()`, and only then `[project] name`. A test that publishes from its own body (`scenario.publish(...)`, `publish_sync(...)`, `await publish(...)`) is then taken for the package: no module is discovered, and only modules the test imported have listeners. Set `[tool.modulith] package = "myapp"` in every project.
- **The modules.** Every directory directly under the package that has an `__init__.py` and a name not starting with `_`. These are not modules: plain files (`main.py`, `db.py`), `_`-prefixed packages, nested packages (they belong to their parent module), and a directory without `__init__.py`, which is skipped with no message.
- **When.** At the first `publish()` or an explicit `modulith.bootstrap()`, modupy imports each module's `__init__.py`, then its `_manifest.py` when present. It imports no other file.

Check what was found:

```text
$ modulith info
modulith
  package: myapp

  modules (4):
    - contracts  (myapp.contracts)  [no manifest]
    - inventory  (myapp.inventory)  [no manifest]
    - orders  (myapp.orders)  [no manifest]
    - payments  (myapp.payments)  [no manifest]
```

A module whose import raises stops `info`, `verify`, `doctor` and the app itself with `error: module discovery recorded import failure(s):` and the exception; fix the import. A wrong package name gives `error: the application package 'shop_api' failed to import (ModuleNotFoundError: ...)`.

## A module's public API

Other modules may import any name without a leading underscore from `myapp.orders` or its public submodules. Everything with a leading underscore is private to the module: a `_internal/` package, a `_tracking.py` file, a `_orders` variable. Keep the public surface in `__init__.py`: functions that take and return plain data.

```python
# myapp/orders/__init__.py
_fulfilled: set[str] = set()  # private state


def is_fulfilled(order_id: str) -> bool:  # public: other modules may call it
    return order_id in _fulfilled
```

`modulith verify` reads only the `.py` files inside module packages. Files at the package root (`main.py`) and `tests/` are not checked, so a test may import a private name, though testing through the public API survives refactors.

## The contracts package

`myapp/contracts/` holds what more than one module must agree on. It is discovered as a module but gets no process of its own, and every module may import it. Its name comes from `[tool.modulith] contracts_module` (default `"contracts"`).

| Belongs in contracts | Does not belong |
|---|---|
| events that one module publishes and another listens to | business logic, queries, tables |
| types in more than one module's signatures: enums, small frozen dataclasses, shared exception classes | `@listener` functions (a listener there belongs to no module) |
| | any import from `myapp.orders` or another module |

An event is a class marked `@event`; a frozen dataclass is the recommended shape:

```python
# myapp/contracts/events.py
from dataclasses import dataclass

from modulith import event


@event
@dataclass(frozen=True)
class OrderCreated:
    order_id: str
```

- Keep fields plain data. With the outbox or a broker the event is stored as JSON: `str`, `int`, `float`, `bool`, `None`, lists, tuples, `datetime`, `date`, `UUID`, `Decimal`, enums and nested dataclasses round-trip. A live object (a session, an ORM row) works in one process and then fails with `TypeError: cannot JSON-serialize object in event payload`.
- An event is a contract with every listener. Once events are stored (outbox, broker), add a field only with a default; a stored event that lacks a field without a default, or carries a field the class dropped, fails to load with `TypeError`.
- An event only one module uses may live in that module. Move it to contracts when a second module needs it.

## Events, publish and listeners

```python
# myapp/orders/__init__.py
from modulith import publish

from myapp.contracts.events import OrderCreated

_orders: dict[str, str] = {}


async def create_order(customer_id: str) -> str:
    order_id = f"ord-{len(_orders) + 1}"
    _orders[order_id] = customer_id
    await publish(OrderCreated(order_id=order_id))  # orders does not know who listens
    return order_id
```

```python
# myapp/payments/__init__.py
from modulith import listener, publish

from myapp.contracts.events import OrderCreated, PaymentReceived


@listener
async def charge(event: OrderCreated) -> None:
    await publish(PaymentReceived(order_id=event.order_id))
```

Listener rules:

- **The event type is the first parameter's annotation**, a class defined at module scope. Without it the module fails to import: `TypeError: @listener 'untyped' must annotate its event parameter so modulith knows which event type to route.`
- **A listener registers when its file is imported, and modupy imports only `__init__.py`** (and `_manifest.py`). A listener in `handlers.py` needs `from myapp.orders import handlers as handlers` in the module's `__init__.py`. Without it the listener never runs and nothing reports it: `modulith verify` passes and `publish()` raises nothing. If `handlers.py` imports from its own package, put that import at the bottom of `__init__.py` with `# noqa: E402`.
- **Dispatch is by exact class.** A listener annotated with a base class does not receive subclass events.
- **`async def` is preferred.** A plain `def` listener is accepted and runs on a worker thread.
- **`@listener(timeout=30)`** bounds one call of an `async def` listener, in seconds. Past it the listener is cancelled and fails with `modulith.ListenerTimeoutError` (`listener slow_check did not finish within 0.05s`). Cancellation lands at the next `await`; work handed to a thread keeps running. On a plain `def` listener it is a `TypeError` at import.
- **Sync callers** use `from modulith import publish_sync` (`publish_sync(event, timeout=30.0)`). On an event loop's own thread it raises `RuntimeError`; use `await publish(event)` there.

What a failure does in one process (the default: no outbox, `topology = "single"`):

1. `publish()` runs every listener of that event concurrently and waits for all of them, including the events they publish in turn.
2. Each failure is logged. The other listeners still finish.
3. `publish()` then re-raises the first failure, in registration order, to the caller. A route that called it answers 500, although the publisher's earlier writes and the other listeners' effects stay.

So in one process a slow or failing listener slows or fails the publisher. With the outbox the listener runs after the commit and is retried ([outbox.md](outbox.md)); in a process of its own the broker retries it ([processes-and-brokers.md](processes-and-brokers.md)). Both deliver at least once, so write listeners that are safe to run twice.

## Routers

```python
# myapp/orders/api.py
from fastapi import APIRouter, Depends, Header, HTTPException

from myapp.orders import is_fulfilled


def require_user(x_user: str | None = Header(default=None)) -> str:
    if x_user is None:
        raise HTTPException(status_code=401, detail="not signed in")
    return x_user


router = APIRouter(dependencies=[Depends(require_user)])  # runs for every route of this module


@router.get("/{order_id}/fulfilment")  # no /orders prefix here
async def get_fulfilment(order_id: str) -> dict[str, str | bool]:
    return {"order_id": order_id, "fulfilled": is_fulfilled(order_id)}
```

```python
# bottom of myapp/orders/__init__.py, because api.py imports from this package
from myapp.orders.api import router as router  # noqa: E402
```

```python
# myapp/main.py
from fastapi import FastAPI

from myapp.orders import router as orders_router

app = FastAPI()
app.include_router(orders_router, prefix="/orders")  # prefix = the module's name
```

- **Re-export `router` from the module's `__init__.py`.** A module running in its own process serves exactly that attribute under `/<module>`. Without it the process answers 404 on `/<module>/...` and logs `module 'payments' exposes no 'router' attribute`, which is expected only for a module that just listens.
- **Mount it in `main.py` under `/<module>`** so URLs are the same in every topology.
- **A check every route needs (login, rate limit) goes on the module's own `router`,** as above. A module in its own process never runs `main.py`, so `main.py`'s middleware, exception handlers, lifespan and the `dependencies=` it passes to `include_router` do not exist there.

## Manifests

Optional. `myapp/orders/_manifest.py` declares the module's contract; modupy imports it after the module and checks it at every start.

```python
# myapp/orders/_manifest.py
from modulith import declare_module

from myapp import orders

declare_module(
    publishes=["OrderCreated"],  # must be importable as myapp.orders.OrderCreated
    consumes=["PaymentReceived"],  # descriptive only
    listeners=[orders.on_payment],  # each must have registered
    owns_tables=["orders_order"],  # feeds the data-ownership rule
    declared_dependencies=["contracts"],  # feeds the undeclared-dependency rule
)
```

- `publishes` names must exist in the module package's namespace, so `__init__.py` imports each event it publishes.
- `declared_dependencies` omitted leaves the dependency rule off. `[]` means "imports no other module". Entries are module names (`"shipping"`); `contracts` is always allowed.
- A mismatch stops every command and the app's start: `error: Manifest verification failed:` followed by `[myapp.orders] event type 'OrderRefunded' declared in myapp.orders but not defined in package namespace (...)` or `[myapp.orders] listener on_payment (file:line) declared in myapp.orders but not registered against the event bus — module may have failed to import`. Fix the manifest or the code so they agree.
- Call `declare_module` once per module. `modulith info` shows `[manifest]` beside the module.

## The boundary rules

`modulith verify` parses the source of every module and prints violations grouped by module:

```text
$ modulith verify

payments:
  [ERROR] no-internal-imports: payments imports _orders from orders, reaching into orders's private package. Cross-module access must go through orders's public API.  (myapp/payments/__init__.py:1)

1 error(s), 0 warning(s)
```

Exit code 1 when any `[ERROR]` is reported, else 0. Warnings pass unless you add `--fail-on-warnings`. A clean run prints `✓ no boundary violations`. In a project with a `.modulith-baseline.json`, run `modulith verify --mode=ratchet` instead ([adopting-existing-code.md](adopting-existing-code.md)).

| Rule | Level | Fires when a module... |
|---|---|---|
| `no-internal-imports` | ERROR | imports a `_` name, or a path with a `_` segment, from another module |
| `use-contracts` | ERROR | imports a Capitalized name from another module and uses it only in annotations or not at all, or imports `*` from it |
| `undeclared-dependency` | ERROR | has a manifest with `declared_dependencies` and imports a module not listed there |
| `contracts-is-sink` | ERROR | is the contracts package and imports any module |
| `no-cyclic-dependency` | ERROR | is part of a loop of runtime imports between modules |
| `data-ownership` | WARNING | defines or references a table another module's `owns_tables` lists, or defines one its own non-empty `owns_tables` omits |
| `own-process-import` | WARNING | imports, at runtime, a module named in `[tool.modulith] own_process` |
| `parse-error` | ERROR | contains a `.py` file that does not parse; cannot be disabled |

Messages and correct fixes:

- **`no-internal-imports`**: `payments imports _orders from orders, reaching into orders's private package. ...` or `payments imports 'myapp.shipping._tracking', reaching into shipping's private package. ...`. Fix: use what the owner already exposes. If nothing fits, add a public function to the owner's `__init__.py` that returns the answer, not the private object, or get the data from an event.
- **`use-contracts`**: `payments imports type(s) Shipment from shipping. Shared types must live in the 'contracts' module so modules depend on contracts, not each other's internals.` or `payments imports * from shipping. Wildcard cross-module imports hide which names are shared ...`. Fix: move the class into `myapp/contracts/` and import it from there in both modules; replace `*` with the names you use.
- **`undeclared-dependency`**: `payments imports from shipping, which is not in its declared_dependencies. Add 'shipping' to the manifest or remove the import.` Fix: if the dependency is intended, add it to that module's manifest; otherwise remove the import and use an event.
- **`contracts-is-sink`**: `contracts imports from orders. The contracts module is a dependency sink: ...`. Fix: move the shared code into contracts, or delete the import. When the imported module imports contracts back, as every module that uses an event does, a runtime import never reaches this rule: discovery stops first with `error: module discovery recorded import failure(s):` and `cannot import name ... from partially initialized module ... (most likely due to a circular import)`.
- **`no-cyclic-dependency`**: `cyclic dependency among modules: orders, shipping. The module dependency graph must be acyclic — break the cycle by moving shared types to contracts or inverting a dependency via events.` It has no file location and is listed under the first member by name. Fix: replace one direction of the loop with an event.
- **`data-ownership`**: `payments references table 'orders_order', owned by orders. Access another module's data through its events or public API (best-effort static check).`, `payments defines table 'orders_order', which is already owned by orders's manifest. ...`, `orders defines table 'orders_audit' but does not declare it in owns_tables. Declare it or move it to its owning module.`, or `table 'x' is declared in owns_tables by multiple modules: ...`. Fix: drop the foreign key or copy of the table and keep a plain id column; list every table a module defines in its `owns_tables`. The rule sees only SQLAlchemy `Table("name", ...)`, `__tablename__ = "name"` and `ForeignKey("table.col")` literals, never raw SQL.
- **`own-process-import`**: `orders imports 'myapp.shipping', but shipping runs in a process of its own (own_process): the import loads shipping's code into orders's process too. Share the events through the contracts module instead.` Fix: replace the import with an event.
- **`parse-error`**: `myapp/payments/broken.py could not be parsed: invalid syntax (broken.py, line 1)`. Fix the syntax; until then no rule sees that file.

Wrong fixes. Each makes the report green and leaves the coupling in place:

- Adding the rule to `[tool.modulith.verify] disabled_rules`. That turns it off for the whole project, in `verify`, `doctor` and the `strict_boundaries` start check.
- Running `modulith verify --update-baseline` or editing `.modulith-baseline.json` so a violation you just introduced is accepted.
- Wrapping the import in `if TYPE_CHECKING:`. `no-internal-imports`, `use-contracts`, `undeclared-dependency` and `contracts-is-sink` still report it.
- Hiding the access: `from myapp import orders` then `orders._orders`, a computed `importlib.import_module(name)`, or a helper in `main.py` that reaches in. The verifier cannot see these, and they break the same way.
- Renaming `_orders` to `orders_store` so the import is "public". Expose a function that answers the question instead of the state itself.
- Copying the private code into your module, or moving logic into `contracts`.

## How two modules may interact

| The need | Do this | Notes |
|---|---|---|
| Others should react to something `orders` did | `orders` publishes an event from contracts | First choice. The publisher imports no other module and works unchanged in every topology. |
| `orders` needs data that `shipping` owns, and may lag by one event | Put the data on the event `shipping` already publishes; `orders` stores its own copy in a listener | Works when the modules run in separate processes or services. |
| `orders` needs an answer from `shipping` during a request | Call a public function: `from myapp.shipping import tracking_for` | Passes `verify`. `orders` now depends on `shipping`: see the costs below. |
| Both need the same type | Move the type to contracts | |

Costs of a direct public call, which events do not have:

- The call runs in the caller's process against the caller's own copy of the other module. It is correct only if that function reads shared storage, such as a database. Measured on this layout: `orders` calling `shipping.tracking_for()` over an in-memory dict answered 200 in one process and 404 with one process per module.
- It needs `"shipping"` in the caller's `declared_dependencies` when it has a manifest, fails `no-cyclic-dependency` if `shipping` also imports `orders`, and warns `own-process-import` if `shipping` is in `own_process`.
- `modulith doctor` counts it (`orders: 1 direct cross-module import(s)`) and lowers the process-split readiness score, and `modulith extract` refuses a module that imports another one.

## What modules must not share

| Never shared | Why it matters once modules run in separate processes |
|---|---|
| Tables. Each table has one owning module; no foreign keys, joins or writes into another module's tables | A module split into its own service takes its tables with it. Anything else reading them breaks, and `modulith extract` refuses the split. |
| In-memory state: module-level dicts, caches, singletons, locks | Each process imports its own copy. A write in one process is invisible in another, with no error. |
| Private code (`_` names) | It is the part the owner may change without telling anyone. |
| What `main.py` sets up: middleware, lifespan, app state | A module's own process never runs `main.py`. |

Shared infrastructure that is not business logic (the database engine, a session helper, settings) goes in a plain file at the package root, such as `myapp/db.py`. It is not a module, modules may import it, and every process gets its own instance of whatever it creates.

## Mistakes that look right

| Looks right | Do this instead |
|---|---|
| Putting `@listener` functions in `handlers.py` and stopping there | Import `handlers` from the module's `__init__.py`; then run the test that proves the listener fires |
| Importing the event class from the module that publishes it (`from myapp.orders import OrderCreated`) | Import it from `myapp.contracts.events` |
| Defining a new shared event next to its publisher | Define it in `myapp/contracts/events.py` |
| Creating a module directory without `__init__.py` | Add `__init__.py`; confirm with `modulith info` |
| Reading another module's dict or table "just for a query" | Ask through its public function, or keep your own copy fed by its events |
| Adding a login check as middleware in `main.py` | Put it on each module's `APIRouter(dependencies=[...])` |
| Declaring routes as `/orders/{id}` inside the orders router | Declare `/{id}`; the `/orders` prefix is added where the router is mounted |
| Making `orders` call `payments.charge()` directly so the flow is "explicit" | Publish `OrderCreated` and let `payments` listen |
| Catching the exception around `publish()` to hide a failing listener | Fix the listener, or move slow and fragile work behind the outbox |
| Putting a live session or ORM object on an event | Put ids and plain values on it |
| Silencing a violation with `disabled_rules`, the baseline or `TYPE_CHECKING` | Apply the fix listed for that rule |

Depth: [docs/ARCHITECTURE.md](https://github.com/jeansossmeier/modupy/blob/main/docs/ARCHITECTURE.md) (how bootstrap, dispatch and verification work) and [docs/COOKBOOK.md](https://github.com/jeansossmeier/modupy/blob/main/docs/COOKBOOK.md).
