# Testing a modupy application

Read this when:

- you are writing or fixing tests for code that publishes events or has `@listener` functions;
- a test passes alone and fails in the full run, or `expected event X not seen within Ns` makes no sense;
- you need to test HTTP routes, the outbox, one module as a worker, or a listener that fails or times out.

## Contents

- [Setup](#setup)
- [The fixtures and what they reset](#the-fixtures-and-what-they-reset)
- [Scenario: trigger, then expect](#scenario-trigger-then-expect)
- [What was published, and what was not](#what-was-published-and-what-was-not)
- [Listeners that fail or time out](#listeners-that-fail-or-time-out)
- [Async tests](#async-tests)
- [HTTP tests](#http-tests)
- [Apps with the outbox](#apps-with-the-outbox)
- [One module as a worker](#one-module-as-a-worker)
- [One module with its neighbours mocked](#one-module-with-its-neighbours-mocked)
- [Markers and the ini option](#markers-and-the-ini-option)
- [Mistakes that look right](#mistakes-that-look-right)

Every example below ran green in a project laid out as `myapp/orders`, `myapp/payments`, `myapp/inventory` and `myapp/contracts/events.py` (with `OrderCreated` and `PaymentReceived`), where `create_order()` publishes `OrderCreated`, `payments.charge` answers with `PaymentReceived`, and `orders.on_payment` marks the order fulfilled.

## Setup

The pytest plugin ships inside modupy and loads by itself in every pytest run where modupy is installed (entry point `pytest11`, name `modulith`). Do not also list it: `pytest_plugins = ["modulith.testing"]` stops pytest with `ValueError: Plugin already registered under a different name: modulith.testing=...`. To switch it off in a suite that does not use it: `pytest -p no:modulith`.

Install `pip install 'modupy[test]' httpx2` (`modupy[test]` is pytest and pytest-asyncio), then:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]       # the directory that holds myapp/; without it: ModuleNotFoundError: No module named 'myapp'
asyncio_mode = "auto"    # without it an `async def` test fails: "async def functions are not natively supported."

[tool.modulith]
package = "myapp"        # needed as soon as a test publishes an event itself; see below
```

**Set `package`.** Without it, the running app finds the application package from the call stack, and a publish whose nearest caller is a test (`await publish(...)` in a test body, or `scenario.publish(...)`) makes the test module the package: the log says `detected application package 'test_orders'` and `no modules discovered under 'test_orders'`, only the listeners of modules the test imported register, and the teardown cleans up the wrong package, so state leaks between tests. `scenario.call(create_order, ...)` happens to work without it, because the publish's caller is then `myapp.orders`. Setting `package` removes the difference ([configuration.md](configuration.md#how-the-application-package-is-found)).

## The fixtures and what they reset

| Fixture | Before the test | After the test |
|---|---|---|
| `modulith_app` | resets the runtime (no bootstrap, no listeners, no `configure()` values, no outbox store, brokers closed) and installs a spy that records events; returns a `ModulithTestApp` | resets the runtime again, then removes from `sys.modules` every module of the application package that was first imported during the test |
| `scenario` | requests `modulith_app`; returns a `Scenario` | as `modulith_app` |
| `modulith_module` | nothing; returns a context manager | the context manager restores `sys.modules` on exit |

What follows from the reset and re-import:

- **Import your app inside the test function**, or in a fixture that requests `modulith_app`. A test file that does `from myapp.orders import create_order` at the top registers `orders.on_payment` at collection; the fixture's reset then drops that registration, the cached module is never imported again, and `is_fulfilled("ord-1")` stays `False`.
- **A fixture that imports your app must request `modulith_app`.** Otherwise it can run before the reset (it does when the test lists it before `scenario` or `modulith_app`), its import registers the listeners, the reset drops them, and the expected event never arrives.
- **Each test starts with fresh module state**, so in-memory counters and dicts restart (`ord-1` in every test). Without the fixture they carry over: two tests that each call `create_order` get `ord-1` and `ord-2`.
- Modules of other packages (SQLAlchemy, pydantic, modupy itself) are never removed.

## Scenario: trigger, then expect

```python
from modulith.testing import ModulithTestApp, Scenario


def test_creating_an_order_charges_it(scenario: Scenario, modulith_app: ModulithTestApp) -> None:
    from myapp.contracts.events import OrderCreated, PaymentReceived
    from myapp.orders import create_order

    payment = (
        scenario.call(create_order, "alice")
        .expect_event(PaymentReceived)
        .matching(lambda e: e.order_id == "ord-1")
        .within(seconds=2)
    )

    assert payment == PaymentReceived(order_id="ord-1")
    assert OrderCreated(order_id="ord-1") in modulith_app.published_events
    assert ("myapp.payments.charge", OrderCreated(order_id="ord-1")) in modulith_app.listener_calls


def test_a_payment_fulfils_the_order(scenario: Scenario) -> None:
    from myapp.contracts.events import PaymentReceived
    from myapp.orders import is_fulfilled

    scenario.publish(PaymentReceived(order_id="ord-7")).expect_event(PaymentReceived).within(
        seconds=2
    )

    assert is_fulfilled("ord-7")
```

| Step | What it does |
|---|---|
| `.call(fn, *args, **kwargs)` | the trigger: runs `fn` on a background thread; when it returns a coroutine, runs that to completion. Prefer this: it drives the same code path as your app |
| `.publish(event)` | the trigger: publishes `event` as if from the test (needs `package` set) |
| `.expect_event(EventType)` | required: the event type to wait for |
| `.matching(predicate)` | optional: the event must also satisfy `predicate` |
| `.within(seconds)` | fires the trigger, then waits for a matching event published after it; returns that event. One budget covers both |

- On a miss: `AssertionError: expected event PaymentReceived not seen within 2s`.
- An exception from the trigger or from a listener is raised as is, never turned into that `AssertionError`.
- `within()` without `expect_event` raises `ValueError: call expect_event(...) before within(...)`; without a trigger, `ValueError: call publish(...) or call(...) before within(...)`.
- The trigger runs only inside `within()`, so nothing happens if you leave `within()` off.

## What was published, and what was not

`ModulithTestApp` (the `modulith_app` fixture) records everything since the test began:

- `published_events`: every event published.
- `published_events_of_type(EventType)`: those of one type, in a list.
- `listener_calls`: `(listener name, event)` tuples, the name fully qualified: `("myapp.orders.on_payment", PaymentReceived(order_id="ord-1"))`.
- `reset()`: empties both lists, to assert on a second step only.

Do not assert on the order of `published_events`. In memory, an event is recorded once all its listeners have finished, so a nested event comes first: `[PaymentReceived(order_id='ord-1'), OrderCreated(order_id='ord-1')]`. With the outbox the same flow recorded `OrderCreated` first.

To prove an event was **not** published, assert after the trigger has finished. In memory, `publish()` returns only after every listener ran, so an empty list is final:

```python
def test_a_rejected_request_publishes_nothing(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    response = TestClient(app).post("/orders", json={})

    assert response.status_code == 422
    assert modulith_app.published_events == []
```

With the outbox or a broker, delivery happens later; assert absence only after waiting for an event that the same flow publishes after it.

## Listeners that fail or time out

In one process without the outbox, `publish()` waits for every listener and re-raises the first error, so the caller of `create_order` sees it. A listener defined inside the test is registered for that test only:

```python
import asyncio

import pytest


def test_a_failing_listener_fails_the_publisher(scenario: Scenario) -> None:
    from modulith import listener

    from myapp.contracts.events import OrderCreated
    from myapp.orders import create_order

    @listener
    async def declined(event: OrderCreated) -> None:
        raise RuntimeError("card declined")

    with pytest.raises(RuntimeError, match="card declined"):
        scenario.call(create_order, "alice").expect_event(OrderCreated).within(seconds=2)


def test_a_listener_past_its_timeout(scenario: Scenario) -> None:
    from modulith import ListenerTimeoutError, listener

    from myapp.contracts.events import OrderCreated
    from myapp.orders import create_order

    @listener(timeout=0.1)
    async def slow(event: OrderCreated) -> None:
        await asyncio.sleep(5)

    with pytest.raises(ListenerTimeoutError, match="did not finish within 0.1s"):
        scenario.call(create_order, "alice").expect_event(OrderCreated).within(seconds=2)
```

The full message is `listener slow did not finish within 0.1s`. To make your real listener fail on demand (a model that hangs, a provider that errors), use a stand-in read from an environment variable at import; because `modulith_app` re-imports your modules per test, `monkeypatch.setenv(...)` before the import works ([ai-features.md](ai-features.md#stand-in-models-and-fault-injection)). With the outbox or a broker, a failing listener does not fail the publisher: it is retried and then dead-lettered ([outbox.md](outbox.md)).

## Async tests

With `asyncio_mode = "auto"`, `await` your code and assert on `modulith_app`:

```python
async def test_create_order_in_an_async_test(modulith_app: ModulithTestApp) -> None:
    from myapp.contracts.events import PaymentReceived
    from myapp.orders import create_order, is_fulfilled

    order_id = await create_order("alice")

    assert order_id == "ord-1"
    assert modulith_app.published_events_of_type(PaymentReceived) == [
        PaymentReceived(order_id="ord-1")
    ]
    assert is_fulfilled("ord-1")
```

Do not use `scenario.publish(...)` in an `async def` test: `RuntimeError: publish_sync() called from inside an async context on the same thread as the event loop. Use await publish(event) instead.`

## HTTP tests

```python
from fastapi.testclient import TestClient


def test_http_flow_reaches_orders_and_inventory(modulith_app: ModulithTestApp) -> None:
    from myapp.main import app

    client = TestClient(app)

    created = client.post("/orders", json={"customer_id": "alice"})

    assert created.json() == {"order_id": "ord-1"}
    assert client.get("/orders/ord-1/fulfilment").json() == {"order_id": "ord-1", "fulfilled": True}
    assert client.get("/inventory/ord-1").status_code == 200
```

- Request `modulith_app` even when the test does not read it: it gives the test a fresh runtime and fresh module state.
- Use `with TestClient(app) as client:` when `main.py` has a lifespan (`bootstrap()`, `outbox.start()`). Without `with`, the lifespan never runs.
- **Why `httpx2`.** Starlette's `TestClient`, which FastAPI re-exports, imports `httpx2` first. With only `httpx` installed it warns `Using httpx with starlette.testclient is deprecated; install httpx2 instead.`, which fails a suite that turns warnings into errors; with neither it raises. `modupy[fastapi]` installs `httpx` (modupy's proxy uses it), not `httpx2`, so add `httpx2` to your test dependencies.

## Apps with the outbox

Point the outbox at a throwaway SQLite file, create its tables with `modulith migrate`, and run the app through its lifespan. Delivery happens after the commit, on the retry loop, so poll instead of asserting at once:

```python
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from modulith.cli import app as cli
from typer.testing import CliRunner


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    path = tmp_path / "myapp.db"
    monkeypatch.setenv("MODULITH_OUTBOX_URL", f"sqlite+aiosqlite:///{path}")
    result = CliRunner().invoke(cli, ["migrate"])  # creates the outbox tables
    assert result.exit_code == 0, result.output
    return path


def eventually(check, seconds: float = 5.0):
    deadline = time.monotonic() + seconds
    while not (value := check()) and time.monotonic() < deadline:
        time.sleep(0.05)
    return value


def test_an_order_is_fulfilled_after_the_commit(
    modulith_app: ModulithTestApp, database: Path
) -> None:
    from myapp.main import app

    with TestClient(app) as client:  # `with` runs the lifespan: bootstrap() and outbox.start()
        created = client.post("/orders", json={"customer_id": "alice"})
        fulfilled = eventually(lambda: client.get("/orders/ord-1/fulfilment").status_code == 200)

    assert created.json() == {"order_id": "ord-1"}
    assert fulfilled


@pytest.mark.modulith_no_outbox
def test_payment_logic_without_the_outbox(scenario: Scenario) -> None:
    from myapp.contracts.events import PaymentReceived
    from myapp.orders import create_order, is_fulfilled

    scenario.call(create_order, "alice").expect_event(PaymentReceived).within(seconds=2)

    assert is_fulfilled("ord-1")
```

- The app's own engine must read the same variable at import (`os.environ.get("MODULITH_OUTBOX_URL", ...)`); the per-test re-import picks up the `monkeypatch` value.
- `scenario.call(create_order, "alice")...within(seconds=5)` also works with the `database` fixture; give it a few seconds.
- Pointed at a database that was never migrated, an outbox test fails with `sqlite3.OperationalError: no such table: event_publications`. Without the fixture, it uses `outbox_url` from `pyproject.toml`, which may be your development database.
- `from modulith.cli import app` needs `modupy[cli]`.

## One module as a worker

`modulith run --topology processes` serves each module from `modulith._worker:create_app`, a documented, stable entry point. Build the same app in a test to check that a module's routes stand alone:

```python
def test_an_orders_worker_serves_orders_and_not_inventory(
    modulith_app: ModulithTestApp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modulith._worker import create_app

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))  # the broker's state lands in tmp
    monkeypatch.setenv("MODULITH_MODULE", "orders")
    monkeypatch.setenv("MODULITH_APP_PACKAGE", "myapp")

    client = TestClient(create_app())

    paths = client.get("/orders/openapi.json").json()["paths"]

    assert "/orders" in paths
    assert "/orders/{order_id}/fulfilment" in paths
    assert not any(path.startswith("/inventory") for path in paths)
    assert client.get("/inventory/ord-1").status_code == 404
```

The worker mounts the module package's `router` under `/<module>` and never runs `main.py`, so middleware, lifespan and auth kept there are absent. A route that works through `main.py` and fails here depends on something only `main.py` sets up.

## One module with its neighbours mocked

```python
def test_inventory_with_orders_mocked(modulith_module) -> None:
    with modulith_module("myapp.inventory", mock_modules=["myapp.orders"]):
        from myapp.inventory import can_ship
        from myapp.orders import is_fulfilled  # the MagicMock's attribute

        is_fulfilled.return_value = True

        assert can_ship("ord-1") is True
        is_fulfilled.assert_called_once_with("ord-1")
```

Inside the block, the other modules of the package are unloaded, each name in `mock_modules` is a `MagicMock`, and the target is imported fresh against the mocks. Reach a mock with `from myapp.orders import ...`. `import myapp.orders` followed by `myapp.orders.is_fulfilled` raises `AttributeError: module 'myapp' has no attribute 'orders'` when `myapp` was not imported before the block.

## Markers and the ini option

- `@pytest.mark.modulith_isolated`: runs the test in a fresh `python -m pytest` subprocess (it sees `MODULITH_ISOLATED_SUBPROCESS=1`), for import-time state that a re-import cannot reset.
- `modulith_isolated_timeout` (ini option, seconds, default `300`): the subprocess limit. Past it: `AssertionError: isolated subprocess for <test id> timed out after 1.0s (tune via the modulith_isolated_timeout ini option)`.
- `@pytest.mark.modulith_no_outbox`: no outbox store is bound in this test, so events dispatch in memory and the outbox tables are not needed.

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| `from myapp.orders import create_order` at the top of the test file | registers listeners before the fixture's reset; they never run | import inside the test function |
| `await publish(OrderCreated(...))` or `scenario.publish(...)` with `package` unset | the test module becomes the application package | `package = "myapp"` under `[tool.modulith]` |
| A fixture that imports `myapp` and does not request `modulith_app` | it can run before the reset and register listeners that are then dropped | `def my_fixture(modulith_app, ...)` |
| `pytest_plugins = ["modulith.testing"]` in `conftest.py` | the plugin is already registered; pytest refuses to start | nothing; it loads by itself |
| `assert modulith_app.published_events == [OrderCreated(...), PaymentReceived(...)]` | nested events are recorded first in memory, last with the outbox | `published_events_of_type(...)` or `in` |
| `TestClient(app)` for an app whose lifespan starts the outbox | the lifespan only runs inside `with` | `with TestClient(app) as client:` |
| Asserting right after a request when the outbox is on | delivery happens after the commit, on the retry loop | poll with a deadline, or `scenario.call(...)` with `.within(seconds=5)` |
| `scenario.publish(...)` inside an `async def` test | it calls `publish_sync`, which refuses to run on the loop's thread | `await` your code and assert on `modulith_app` |
| Testing without `modulith_app` because the test "does not publish" | module state and listeners leak from earlier tests | request `modulith_app` in every test that imports `myapp` |
| `time.sleep(1)` before asserting | slow and still flaky | `scenario...within(seconds=...)` or a polling helper |

Depth: [COOKBOOK.md section 9](https://github.com/jeansossmeier/modupy/blob/main/docs/COOKBOOK.md#9-test-an-event-flow-with-the-pytest-plugin) and [modulith/testing.py](https://github.com/jeansossmeier/modupy/blob/main/modulith/testing.py).
