# Recipes

Read this when:

- you are about to make a routine change in a modupy project and want the files to touch, the code and the commands to run;
- the change is one of: new project, new module, event, listener, test, HTTP endpoint, cross-module access, private helper, rename or removal.

For the reasons behind a step, and for fixing a `modulith verify` violation, read [modules-and-contracts.md](modules-and-contracts.md).

Contents: [Every recipe](#before-and-after-every-recipe) · [1 New project](#1-start-a-new-project) · [2 Module](#2-add-a-module) · [3 Event](#3-add-an-event) · [4 Listener](#4-add-a-listener) · [5 Test](#5-test-an-event-flow) · [6 Listener-only module](#6-add-a-module-that-only-listens) · [7 Endpoint](#7-add-an-http-endpoint) · [8 Cross-module access](#8-let-one-module-use-what-another-owns) · [9 Private helper](#9-add-a-private-helper) · [10 Rename or remove](#10-rename-or-remove-a-module) · [Mistakes](#mistakes-that-look-right)

The recipes build one example: the starter project (`orders`, `payments`, `inventory`; events `OrderCreated`, `PaymentReceived`) gains a `shipping` module that ships paid orders and publishes `OrderShipped`.

## Before and after every recipe

```bash
modulith info      # before: the package, its modules, the active configuration
modulith verify    # after: expect "✓ no boundary violations"
pytest             # after: expect every test passed, and more than zero ran
```

- Run them from the project root. If the project has a `.modulith-baseline.json`, use `modulith verify --mode=ratchet` ([adopting-existing-code.md](adopting-existing-code.md)).
- After changing `[tool.modulith]`, the outbox, the topology, a broker, a manifest or an event's fields, also run `modulith doctor`.
- When the project uses ruff, as the starter does, run `ruff check --fix myapp tests` and `ruff format myapp tests` too. Name the code: with ruff's preview mode on, `ruff format .` also rewrites the code blocks of the installed skill under `.claude/` and `.agents/`.

## 1. Start a new project

Only where no project exists yet. For existing code read [adopting-existing-code.md](adopting-existing-code.md) and do not run `init`.

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

- `NAME` is the package name. Without a second argument `init` creates `./myapp` as the project root. To build in the directory you were given, such as an empty workspace or a fresh repository, run `modulith init myapp .` inside it: that directory becomes the project root (files such as `.git`, a README or a `.gitignore` stay), and the `next:` list has no `cd` line.
- `--empty` writes no sample modules, `--install` creates `.venv` with the dependencies (with uv, the `next:` steps start with `uv run`), `--ci github` adds a GitHub Actions workflow that runs verify, pytest and ruff, and `--no-skill` leaves this skill out: [cli.md](cli.md#modulith-init).
- In the project root, `pytest` reports `5 passed`, `modulith verify` is clean, and `modulith dev myapp.main:app` serves `http://127.0.0.1:8000` with reload.
- `init` never overwrites. It exits 1 with `error: myapp already exists; init never overwrites it` (an existing empty `./myapp` included), with `error: <path> already exists; init never overwrites it` naming the `pyproject.toml`, `myapp` or `tests` that the directory given already holds (`error: tests already exists; ...`), with `error: 'MyApp' is not a lowercase Python package name, such as 'myapp'` (the name must match `[a-z][a-z0-9_]*`), or with `error: 'json' is already an importable module, which the project would hide`. An earlier install of this skill in the directory given is kept, not replaced.

The new `pyproject.toml` names the package:

```toml
[tool.modulith]
package = "myapp"
```

Keep it. Without it, a test that publishes from its own body (`scenario.publish(...)`, `publish_sync(...)`, `await publish(...)`) makes modupy take the test module for the application package: no module is discovered, only modules the test imported have listeners, and the test fails, for example with `expected event PaymentReceived not seen within 2s`.

The starter is sample code: `orders`, `payments`, `inventory`, the events `OrderCreated` and `PaymentReceived`, and `tests/test_quickstart_flow.py`. When the user's domain differs, start with `modulith init myapp --empty` instead, which writes none of it and one test, `tests/test_app.py`, then add the real modules with recipes 2 to 7, each with its test. When the sample is already there, delete the modules it does not need, their router imports and mounts in `main.py`, their events in `myapp/contracts/events.py` and `tests/test_quickstart_flow.py` (its worker test names `orders` and `inventory`). With no test left, `pytest` prints `no tests ran` and exits 5: that is not a pass.

## 2. Add a module

Files: one new package directly under `myapp/`. Nothing registers it; discovery finds it. For a module that serves routes, let the CLI write it:

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

It writes recipe 7's shape: `api.py` with `router` and a `GET /shipping` route that calls the module's placeholder `status()`, the router re-export as the last line of `__init__.py`, the mount in `main.py` with the import where ruff sorts it, and a test that requests the route. Replace `status()` and the route with the module's own code, and keep the re-export last. A module that only listens needs no router: create its package by hand, as in recipe 6:

```python
# myapp/shipping/__init__.py
_shipments: dict[str, str] = {}  # order_id -> tracking number


def tracking_for(order_id: str) -> str | None:
    return _shipments.get(order_id)
```

`modulith info` now lists `- shipping  (myapp.shipping)  [no manifest]`. If it does not, the directory lacks `__init__.py` or its name starts with `_`. Keep the module's public functions in `__init__.py` and the rest private (recipe 9).

## 3. Add an event

Files: `myapp/contracts/events.py`, the publishing module, a test.

Define the event in contracts, not in the module that publishes it:

```python
# myapp/contracts/events.py, add at the end
@event
@dataclass(frozen=True)
class OrderShipped:
    order_id: str
    tracking_number: str
```

Publish it from the owning module, after the module's own state is written:

```python
# myapp/shipping/__init__.py
from modulith import publish

from myapp.contracts.events import OrderShipped

_shipments: dict[str, str] = {}  # order_id -> tracking number


async def ship(order_id: str) -> str:
    tracking_number = f"trk-{len(_shipments) + 1}"
    _shipments[order_id] = tracking_number  # your real persistence goes here
    await publish(OrderShipped(order_id=order_id, tracking_number=tracking_number))
    return tracking_number


def tracking_for(order_id: str) -> str | None:
    return _shipments.get(order_id)
```

```python
# tests/test_shipping_flow.py
from modulith.testing import Scenario


def test_shipping_an_order_publishes_order_shipped(scenario: Scenario) -> None:
    from myapp.contracts.events import OrderShipped
    from myapp.shipping import ship, tracking_for

    shipped = scenario.call(ship, "ord-1").expect_event(OrderShipped).within(seconds=2)

    assert shipped == OrderShipped(order_id="ord-1", tracking_number="trk-1")
    assert tracking_for("ord-1") == "trk-1"
```

- Put on the event what listeners will need (ids and plain values), so they never call back into the publisher. From sync code use `publish_sync(event)` from `modulith`.
- To change an existing event, add fields with a default. Removing or renaming a field breaks events already stored by the outbox or a broker.
- With the outbox on, publish inside the bound database session: [outbox.md](outbox.md).

## 4. Add a listener

Files: the listening module's `handlers.py` and `__init__.py`, a test (recipe 5).

```python
# myapp/shipping/handlers.py
from modulith import listener

from myapp.contracts.events import PaymentReceived
from myapp.shipping import ship


@listener
async def on_payment(event: PaymentReceived) -> None:
    await ship(event.order_id)
```

Import it from the module's `__init__.py`, or it never registers:

```python
# myapp/shipping/__init__.py, add at the end
# Importing handlers registers its @listener functions. It sits at the bottom
# because handlers.py imports `ship` from this package.
from myapp.shipping import handlers as handlers  # noqa: E402
```

- The annotation on the event parameter is the subscription; it must be the event class. A module with one or two listeners can define them in `__init__.py` itself, as `payments` does.
- Write the listener so that running it twice for the same event is harmless: with the outbox or a broker, delivery is at least once.
- Bound a slow call with `@listener(timeout=30)` (`async def` only).

## 5. Test an event flow

A listener is not done until a test proves it fires. Without the `__init__.py` import from recipe 4, `modulith verify` still passes and only this test fails, with `AssertionError: expected event OrderShipped not seen within 2s`.

```python
# tests/test_shipping_flow.py, add at the end
def test_a_paid_order_gets_shipped(scenario: Scenario) -> None:
    from myapp.contracts.events import OrderShipped
    from myapp.orders import create_order
    from myapp.shipping import tracking_for

    shipped = (
        scenario.call(create_order, "alice")
        .expect_event(OrderShipped)
        .matching(lambda event: event.order_id == "ord-1")
        .within(seconds=2)
    )

    assert tracking_for("ord-1") == shipped.tracking_number
```

- Request the `scenario` fixture, or `modulith_app` for HTTP tests (recipe 7). Each gives the test a fresh runtime and fresh copies of your modules, so import application code inside the test function, not at the top of the file.
- Trigger with `scenario.call(function, *args)` or `scenario.publish(event)`, name the event with `.expect_event(Type)`, optionally filter with `.matching(predicate)`, and finish with `.within(seconds=N)`, which returns the matching event or raises the `AssertionError` above.
- In one process the whole chain of listeners has finished when `.within()` returns, so assert on state through public functions right after it.
- Fixtures, markers and async tests: [testing.md](testing.md).

## 6. Add a module that only listens

Files: one new package, a test. No router, no change to `main.py`, and no other module imports it.

```python
# myapp/notifications/__init__.py
from modulith import listener

from myapp.contracts.events import OrderShipped

_sent: list[str] = []


def sent_messages() -> list[str]:
    return list(_sent)


@listener
async def notify_customer(event: OrderShipped) -> None:
    # your real email or push notification goes here
    _sent.append(f"order {event.order_id} shipped: {event.tracking_number}")
```

- Test it by publishing the event itself: `scenario.publish(OrderShipped(order_id="ord-1", tracking_number="trk-1")).expect_event(OrderShipped).within(seconds=2)`, then `assert sent_messages() == ["order ord-1 shipped: trk-1"]`.
- This is the shape for side effects such as email, analytics or an LLM call: the publisher is untouched, and the module can later run in its own process without code changes. There its worker logs `module 'notifications' exposes no 'router' attribute`, which is expected for this shape.

## 7. Add an HTTP endpoint

Files: the module's `api.py` and `__init__.py`, `myapp/main.py`, a test.

```python
# myapp/shipping/api.py
from fastapi import APIRouter, Depends, Header, HTTPException

from myapp.shipping import tracking_for


def require_user(x_user: str | None = Header(default=None)) -> str:
    if x_user is None:
        raise HTTPException(status_code=401, detail="not signed in")
    return x_user


# The check lives on the router, so it also runs when this module is served
# from its own process, where main.py's middleware and dependencies are absent.
router = APIRouter(dependencies=[Depends(require_user)])


@router.get("/{order_id}")
async def get_tracking(order_id: str) -> dict[str, str]:
    tracking_number = tracking_for(order_id)
    if tracking_number is None:
        raise HTTPException(status_code=404, detail="order not shipped")
    return {"order_id": order_id, "tracking_number": tracking_number}
```

```python
# myapp/shipping/__init__.py, add at the end
# `router` is what a worker process serves under /shipping.
from myapp.shipping.api import router as router  # noqa: E402
```

```python
# myapp/main.py, add: the import with the other router imports, the mount with the other mounts
from myapp.shipping import router as shipping_router

app.include_router(shipping_router, prefix="/shipping")
```

```python
# tests/test_shipping_api.py
from fastapi.testclient import TestClient
from modulith.testing import ModulithTestApp


def test_tracking_needs_a_user_and_a_shipped_order(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)
    client.post("/orders", json={"customer_id": "alice"})

    assert client.get("/shipping/ord-1").status_code == 401
    tracking = client.get("/shipping/ord-1", headers={"x-user": "alice"})
    assert tracking.status_code == 200
    assert tracking.json() == {"order_id": "ord-1", "tracking_number": "trk-1"}
```

- Declare paths without the module prefix (`/{order_id}`, not `/shipping/{order_id}`) and mount the router at `/<module name>`.
- A module that already has a router, such as one `modulith add module` wrote, needs only the new route and a test.
- Routes call their own module's public functions; for another module's data follow recipe 8. Drop `require_user` when the route is public: the example shows where such a check has to live.

## 8. Let one module use what another owns

Example: `orders` wants the tracking number, which `shipping` owns. Take the first way that fits.

**Way 1: the data rides on the event, and the consumer keeps its own copy.** Files: the consuming module only.

```python
# myapp/orders/__init__.py, add OrderShipped to the contracts import, then add above the router re-export
_tracking: dict[str, str] = {}  # order_id -> tracking number, this module's own copy


def tracking_number_of(order_id: str) -> str | None:
    return _tracking.get(order_id)


@listener
async def on_shipped(event: OrderShipped) -> None:
    _tracking[event.order_id] = event.tracking_number
```

If the event does not carry the field yet, add it in contracts with a default (recipe 3). `orders` imports nothing from `shipping`, and a route in `orders` that returns `tracking_number_of(order_id)` answered the same in one process and with one process per module.

**Way 2: call the other module's public function**, `from myapp.shipping import tracking_for`. Use it when the caller needs the answer during the request and the function reads shared storage. `modulith verify` accepts it. Know what it costs: with that import over the in-memory dict above, `GET /orders/ord-1/tracking` answered 200 in one process and 404 with one process per module, because each process holds its own copy of `shipping`'s state. `modulith doctor` then reports `⚠ process-split readiness — 75% of cross-module interactions via events` and `orders: 1 direct cross-module import(s)`. If the caller has a manifest, add `"shipping"` to its `declared_dependencies`. If `shipping` already imports `orders`, it is a cycle: use way 1.

**Way 3: both need the same type.** Move the class into `myapp/contracts/` and import it from there in both modules.

Never: import a `_` name from the other module, read or join its tables, or reach its state through `myapp.shipping._shipments`.

## 9. Add a private helper

Files: a `_`-prefixed file or package inside the module.

```python
# myapp/shipping/_tracking.py
def new_tracking_number(sequence: int) -> str:
    return f"trk-{sequence}"
```

Import it inside the module with `from myapp.shipping._tracking import new_tracking_number` and use it, for example `tracking_number = new_tracking_number(len(_shipments) + 1)` in `ship()`.

- For several private files use a `_internal/` package with its own `__init__.py`.
- Only this module may import it. From another module, `modulith verify` reports `no-internal-imports: payments imports 'myapp.shipping._tracking', reaching into shipping's private package.`
- A helper several modules need is not private to one of them. Plain infrastructure (database engine, settings) goes in a file at the package root such as `myapp/db.py`; a shared type goes in contracts; shared behaviour stays in one module, behind a public function or an event.

## 10. Rename or remove a module

**Rename** (`notifications` to `alerts`):

1. Move the package and its tests: `mv myapp/notifications myapp/alerts` (`git mv` in a git repository).
2. Update every import: `grep -rn --include='*.py' "myapp.notifications\|import notifications" myapp tests`.
3. Update the places that name a module as a string: the `prefix="/notifications"` mount in `main.py`, `[tool.modulith.workers]` keys, `own_process`, and other manifests' `declared_dependencies`. Do not hand-edit `.modulith-baseline.json`: its entries hash messages that name the module, so mid-adoption regenerate it as rule 2 of [When the ratchet fails](adopting-existing-code.md#when-the-ratchet-fails) says.
4. Run `ruff check --fix myapp tests` (import order changes with the name), `modulith info`, `modulith verify`, `pytest`.

**Remove:**

1. Find what depends on it: imports (`grep -rn --include='*.py' "myapp.alerts" myapp tests`), events only it publishes (their listeners go quiet) and events only it listens to.
2. Delete the package and its tests, its router mount in `main.py`, and its names in `[tool.modulith]`.
3. Delete from contracts only the events that no remaining module publishes or listens to.
4. Run `modulith info`, `modulith verify`, `pytest`.

What the checks do not catch:

- `modulith verify` and `modulith doctor` pass with a stale name in `workers` or `own_process`. It fails only when the process topology starts: `error: workers names 'notifications', which is not a module of package 'myapp'; discovered modules: alerts, inventory, orders, payments, shipping`. A stale import in `tests/` shows up in `pytest` as `ModuleNotFoundError`, not in `verify`.
- The module's name is its URL prefix. A rename changes `/notifications/...` to `/alerts/...` for clients.
- With the outbox or a broker in use, stored events refer to listeners by their Python path. Let pending events drain before renaming or removing a module that listens: [outbox.md](outbox.md), [processes-and-brokers.md](processes-and-brokers.md).

## Mistakes that look right

| Looks right | Do this instead |
|---|---|
| Running `modulith init` to "add modupy" to an existing repository | Follow [adopting-existing-code.md](adopting-existing-code.md) |
| Registering a new module in a list or config file | Run `modulith add module NAME`, or create the package with `__init__.py`; confirm with `modulith info` |
| Writing a listener and trusting `modulith verify` | Add the flow test from recipe 5; `verify` cannot see a listener that never registers |
| Importing `handlers` or `router` at the top of `__init__.py` when they import from the package | Put those imports at the bottom, with `# noqa: E402` |
| Calling `payments.charge()` from `orders` to make a flow explicit | Publish an event and let `payments` listen |
| Fixing the 404 from way 2 by sharing the dict through a root-level file | Each process still has its own copy; use way 1 or shared storage |
| Publishing from the test body (`scenario.publish(...)`, `publish_sync(...)`, `await publish(...)`) in a project without `[tool.modulith] package` | Set `[tool.modulith] package = "myapp"` (recipe 1); otherwise the test file is taken for the application package and only modules the test imported have listeners. `scenario.call(module_function, ...)` works either way |
| Declaring success because the exit code was 0 | Read the output: `✓ no boundary violations`, and a pytest line with a non-zero passed count |
| Renaming a module by replacing imports only | Also update the string names in `main.py`, `[tool.modulith]` and manifests |

More worked examples: [docs/COOKBOOK.md](https://github.com/jeansossmeier/modupy/blob/main/docs/COOKBOOK.md) and the [quickstart example](https://github.com/jeansossmeier/modupy/tree/main/examples/quickstart).
