# AI features: model calls in a modupy app

Read this when:

- you are adding a model call, an agent or local inference to a modupy application;
- a model call hangs, runs slowly, crashes the process or drains the database pool;
- you are choosing between `@listener(timeout=...)`, `asyncio.timeout`, `own_process` and `worker_liveness_timeout_seconds`.

## Contents

- [Where a model call goes](#where-a-model-call-goes)
- [`@listener(timeout=...)` and its limits](#listenertimeout-and-its-limits)
- [When someone waits: `asyncio.timeout`](#when-someone-waits-asynciotimeout)
- [Close the database session before the call](#close-the-database-session-before-the-call)
- [A process of its own](#a-process-of-its-own)
- [Retries call the model again](#retries-call-the-model-again)
- [Stand-in models and fault injection](#stand-in-models-and-fault-injection)
- [Mistakes that look right](#mistakes-that-look-right)

modupy has no AI integration and needs none: no model client, no prompt API, no framework. A model call is ordinary `async` Python inside a listener, whether it uses a vendor SDK, plain HTTP, LangGraph or a local model. The Python examples below use a stand-in `generate()`, and each of them ran in a test project.

## Where a model call goes

In a listener, in a module of its own that nothing else imports:

```python
# myapp/assistant/__init__.py
import os

from modulith import listener

from myapp.assistant.model import generate
from myapp.contracts.events import OrderCreated

TIMEOUT = float(os.environ.get("ASSISTANT_TIMEOUT", "30"))

_notes: dict[str, str] = {}


@listener(timeout=TIMEOUT)
async def write_thank_you_note(event: OrderCreated) -> None:
    if event.order_id in _notes:
        return  # delivered again: at least once
    _notes[event.order_id] = await generate(f"Write a thank-you note for order {event.order_id}")
```

A module of its own, because process isolation, worker counts and `modulith extract` all work per module, never per function. `orders` keeps publishing `OrderCreated` and never learns the assistant exists.

What the same listener does in each setup:

| Setup | What happens to the order request |
|---|---|
| One process, no outbox (the default) | `publish()` waits for every listener and re-raises the first error. A slow model slows `create_order()`; a failed or timed-out call fails it: the caller gets `ListenerTimeoutError: listener write_thank_you_note did not finish within 0.2s` |
| One process with the outbox | the listener runs after the order commits, in the background. A failure is retried with backoff, 10 attempts by default, then dead-lettered ([outbox.md](outbox.md)) |
| `topology = "processes"` | the event crosses a broker to the assistant's worker. The broker retries, 5 attempts by default ([processes-and-brokers.md](processes-and-brokers.md)) |

So the outbox or the process topology has to be on before a model call reaches production: tell the user, and turn it on once they agree. In the default setup the model call is on the request path.

## `@listener(timeout=...)` and its limits

`timeout` is seconds, a finite number above 0, and only for `async def` listeners. When it expires, the listener's task is cancelled and the delivery fails with `ListenerTimeoutError` (importable from `modulith`), which counts as one failed attempt: retried by the outbox or broker, raised to the publisher in one process.

Refused at import:

```text
TypeError: listener write_note is a plain def, which cannot take a timeout: its call runs on a thread that cannot be stopped, so the retried delivery would run alongside the call still running. Make it async def, or leave it unbounded and run its module in a process of its own (own_process).
ValueError: listener timeout must be a finite number above 0, got 0
```

What the timeout cannot do:

- **Stop blocking code.** Cancellation is cooperative: it lands at the listener's next `await`. A synchronous SDK call, `time.sleep()` or CPU-bound inference on the event loop's thread is never interrupted, and blocks every other request of that process meanwhile. In a single-process test app, a model call stuck in `time.sleep()` made every request time out, and Ctrl+C did not stop the server within 60 seconds.
- **Stop work handed to a thread.** `await asyncio.to_thread(sdk_call)` is cancelled at the `await`, but the thread runs on to the end: in a test, the threaded call finished its work after `ListenerTimeoutError` had been raised. Its side effects still happen, possibly while the retry runs.
- **Force a listener to stop.** One that catches `asyncio.CancelledError` and returns is only logged: `listener ... returned after its 0.1s timeout: it caught the cancellation instead of stopping, so its work ran past the budget`, and the delivery counts as done. Let `CancelledError` propagate.
- **Outlive the claim.** A running delivery's claim is renewed for at most ten times its base: 10 × `claim_lease_seconds` for the outbox, 10 × `reclaim_stale_seconds` for the `shm` and `database` brokers (60 s each by default, so 600 s). A listener still running past that can be delivered a second time beside itself. Keep `timeout` well below it.

For blocking or CPU-bound calls, use `async` client libraries, or put the module in a process of its own and leave the timeout off (below).

## When someone waits: `asyncio.timeout`

`@listener(timeout=...)` covers listeners only. A route that calls the model while the user waits needs its own deadline:

```python
# myapp/assistant/api.py
import asyncio
import os

from fastapi import APIRouter, HTTPException

from myapp.assistant.model import generate

SUMMARY_TIMEOUT = float(os.environ.get("ASSISTANT_SUMMARY_TIMEOUT", "10"))

router = APIRouter()


@router.get("/summary/{order_id}")
async def read_summary(order_id: str) -> dict[str, str]:
    try:
        async with asyncio.timeout(SUMMARY_TIMEOUT):
            summary = await generate(f"Summarize order {order_id}")
    except TimeoutError:
        raise HTTPException(
            status_code=504, detail=f"the model did not answer within {SUMMARY_TIMEOUT:g}s"
        ) from None
    return {"order_id": order_id, "summary": summary}
```

With a hung model and a 0.3 s limit, this answered `504 {"detail": "the model did not answer within 0.3s"}` after 0.31 s. The same cooperative-cancellation limits apply. Re-export the router from `myapp/assistant/__init__.py` (`from myapp.assistant.api import router as router`) so a worker of the process topology serves it.

## Close the database session before the call

A SQLAlchemy session holds a pool connection from its first query until it closes. Held across a model call, every call in flight pins one connection, and ordinary requests wait for the pool until they fail. With `pool_size=1` and one listener holding its session across a slow call, a concurrent request got:

```text
TimeoutError: QueuePool limit of size 1 overflow 0 reached, connection timed out, timeout 0.30
```

The same request answered 200 when the listener closed its session first. Read, close, call, then write in a new short transaction:

```python
import os

from modulith import listener, publish
from modulith.builtin.outbox import bind_session, unbind_session

from myapp.assistant import model
from myapp.assistant.storage import Note, sessionmaker
from myapp.contracts.events import NoteWritten, OrderCreated

TIMEOUT = float(os.environ.get("ASSISTANT_TIMEOUT", "30"))


@listener(timeout=TIMEOUT)
async def write_thank_you_note(event: OrderCreated) -> None:
    async with sessionmaker() as session:  # 1. read, then give the connection back
        if await session.get(Note, event.order_id) is not None:
            return  # delivered again: at least once
    text = await model.generate(f"Write a thank-you note for order {event.order_id}")  # 2. nothing held
    async with sessionmaker() as session:  # 3. a short transaction for the result
        token = bind_session(session)
        try:
            session.add(Note(order_id=event.order_id, text=text))
            await publish(NoteWritten(order_id=event.order_id))
            await session.commit()
        finally:
            unbind_session(token)
```

- `Note` is the module's own table, with `order_id` as its primary key; `NoteWritten` is an event in `myapp/contracts/events.py`.
- With the outbox on, `bind_session` stores `NoteWritten` in the same commit as the note ([outbox.md](outbox.md)).
- The existence check skips the model on a redelivery: publishing the same `OrderCreated` twice called the model once and published one `NoteWritten`. Two overlapping deliveries can both pass the check; the primary key makes the second commit fail instead of storing a second note.

## A process of its own

```toml
[tool.modulith]
package = "myapp"
own_process = ["assistant"]            # only the assistant gets a worker of its own
worker_liveness_timeout_seconds = 30   # kill and restart a worker that stops answering /health

[tool.modulith.workers]
assistant = 2                          # optional: two assistant processes
```

Run it with `modulith run --topology processes`; with `broker` unset it uses the `shm` broker and says so in a WARNING. The other modules share one worker. Both settings do nothing under `topology = "single"`, so the same file serves `uvicorn` in development.

What it contains, from a run where the model call blocked its event loop in `time.sleep()` (`own_process = ["assistant"]`, `worker_liveness_timeout_seconds = 3`):

- `POST /orders`, `GET /orders/ord-1/fulfilment` and `GET /inventory/ord-1` kept answering 200.
- `GET /_modulith/health` answered 200 with `{"status":"degraded","backends":{"/assistant":"unreachable","/inventory":"ok","/orders":"ok","/payments":"ok"}}`.
- The supervisor logged `worker assistant has not answered /health for 3s (pid 338840); killing it, and restarting it as after a crash`, then `worker assistant exited with code -9; restarting in 1.0s`.

What it does not contain:

- The module's own routes, which fail while its worker is down.
- A blocked worker, when `worker_liveness_timeout_seconds` is unset (the default): it stays blocked until someone restarts it.
- The host running out of memory or CPU, or the database server running out of connections: every process shares those.

Other things to know:

- A worker never runs `main.py`. Create the model client in the module (on first use, or at import), not in the lifespan.
- One worker runs at most `dispatch_concurrency` deliveries at once (default 10, under `[tool.modulith.broker_options]`, for `shm` and `database`). That caps concurrent model calls per process; `[tool.modulith.workers]` multiplies it.
- Synchronous or CPU-bound local inference (a `def` listener, a sync SDK) cannot take a timeout. Give its module `own_process` and set `worker_liveness_timeout_seconds`, so a stuck call costs one restart instead of the API.

## Retries call the model again

Every delivery after the first runs the whole listener again, including the model call:

- The outbox tries up to `dead_letter_after_attempts` times (10), with backoff up to `max_retry_backoff_seconds` (300). The brokers try `max_delivery_attempts` times (5). A timeout is a failed attempt, so a provider that hangs costs up to attempts × timeout per event.
- Make the listener idempotent: look for the stored result before calling the model, and back the check with a unique key.
- No exception tells modupy "do not retry". For a permanent failure (a rejected prompt, a content-policy refusal), catch it, store the outcome, and return.

Dead letters of the outbox:

```text
$ modulith outbox dead-letter
1 dead-lettered publication(s):
  3e93b869-...  myapp.contracts.events.OrderCreated  listener=myapp.assistant.write_thank_you_note  attempts=1  last_error='MODEL_API_KEY'
```

`modulith outbox dead-letter --retry-all` gives each one a fresh retry budget and runs its listener **inside that command's process**. The command therefore needs everything the listener needs: model credentials, network access to the provider, the database URL. Run without `MODEL_API_KEY` (and `dead_letter_after_attempts = 1`), it printed `resubmitted 1 dead-lettered publication(s)`, the replay failed again with `'MODEL_API_KEY'` and went straight back to the dead letters, and the command still exited 0. Run with it, the note was written by the CLI process and `modulith outbox status` showed `dead-lettered: 0`. Check `modulith outbox status` after every replay.

The brokers keep their own dead letters: `modulith broker dead-letter --retry-all` hands each delivery back to the consumer group whose delivery died, so it runs in that module's workers.

## Stand-in models and fault injection

Keep a stand-in client beside the real one, so tests and local runs need no API key and can make the model fail on purpose:

```python
# myapp/assistant/model.py
import asyncio
import os
import signal
import time

FAULTS = ("hang", "block", "slow", "crash")
FAULT = os.environ.get("ASSISTANT_FAULT", "")
if FAULT not in ("", *FAULTS):
    raise ValueError(f"ASSISTANT_FAULT must be empty or one of {', '.join(FAULTS)}, got {FAULT!r}")


async def generate(prompt: str) -> str:
    if FAULT == "hang":
        await asyncio.Event().wait()  # a request the provider never answers
    elif FAULT == "block":
        time.sleep(3600)  # a sync SDK call on the event loop's thread
    elif FAULT == "slow":
        await asyncio.sleep(float(os.environ.get("ASSISTANT_SLOW_SECONDS", "5")))
    elif FAULT == "crash":
        os.kill(os.getpid(), signal.SIGKILL)  # what the OOM killer does
    return f"Thank you for order {prompt.split()[-1]}!"
```

- Validate the variable at import, so a typo (`hnag`) stops the start instead of silently running without a fault.
- In tests, set the variable with `monkeypatch.setenv(...)`, then import the module inside the test; `modulith_app` re-imports your modules every test, so each test can choose its fault ([testing.md](testing.md)):

```python
def test_a_hung_model_call_fails_the_publisher_in_one_process(
    scenario: Scenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ASSISTANT_FAULT", "hang")
    monkeypatch.setenv("ASSISTANT_TIMEOUT", "0.2")
    from modulith import ListenerTimeoutError

    from myapp.contracts.events import OrderCreated
    from myapp.orders import create_order

    with pytest.raises(ListenerTimeoutError):
        scenario.call(create_order, "alice").expect_event(OrderCreated).within(seconds=5)
```

- To count calls, replace the function: `monkeypatch.setattr(model, "generate", counting_model)` works when the listener calls `model.generate(...)` through the module, not a name imported with `from ... import generate`.
- Locally, start the real topology with a fault, `ASSISTANT_FAULT=block modulith run --topology processes`, and watch which routes keep answering.

Full applications built this way: [examples/agent_desk](https://github.com/jeansossmeier/modupy/blob/main/examples/agent_desk/README.md) (a LangGraph agent in a listener, with a timeout and a stand-in model) and [examples/ai_studio](https://github.com/jeansossmeier/modupy/blob/main/examples/ai_studio/README.md) (a model reply listener, a summary route with `asyncio.timeout`, and synchronous local moderation in its own process).

## Mistakes that look right

| Looks right | Why it is wrong | Do this |
|---|---|---|
| Calling the model in `create_order()` or the order route | the user waits for it, and its failure fails the order | publish an event; call the model in a listener of its own module |
| A model listener in the default single process | `publish()` waits for it and re-raises its error | propose the outbox or `topology = "processes"` to the user before production |
| `@listener(timeout=30)` on a `def` listener | refused: a thread cannot be stopped | `async def`, or `own_process` without a timeout |
| `@listener(timeout=30)` around a sync SDK call | the timeout cannot interrupt a blocked event loop | an async client, or `own_process` with `worker_liveness_timeout_seconds` |
| `await asyncio.to_thread(sdk_call)` with a timeout | the thread runs on after the timeout, beside the retry | make the side effect idempotent, or isolate the module |
| A bare `except:`, `except BaseException:` or `except asyncio.CancelledError:` around the model call | it swallows the cancellation, so the timeout no longer stops the listener | catch the provider's own exceptions, or `Exception`, which excludes `CancelledError` |
| `async with session:` around the whole listener, model call included | each call pins a pool connection; requests time out | read, close, call, then open a new session to write |
| Assuming each event calls the model once | every retry and redelivery calls it again | check for the stored result first; unique key |
| `modulith outbox dead-letter --retry-all` from a shell without the API key | the replay runs in that shell's process and fails again, exit code 0 | export the credentials first; then check `modulith outbox status` |
| Creating the model client in `main.py`'s lifespan | workers never run `main.py` | create it in the module |
| `own_process = ["assistant"]` and nothing else | a blocked worker is never restarted | also set `worker_liveness_timeout_seconds` |
| A listener timeout of 15 minutes | past 10 × the claim lease the event can be delivered twice at once | keep it well below 600 s with the defaults, or raise the lease |

Depth: [README "Ready for AI"](https://github.com/jeansossmeier/modupy/blob/main/README.md#ready-for-ai), [An AI agent in its own process](https://github.com/jeansossmeier/modupy/blob/main/docs/posts/an-ai-agent-in-its-own-process.md) and [When model calls pile up](https://github.com/jeansossmeier/modupy/blob/main/docs/posts/when-model-calls-pile-up.md).
