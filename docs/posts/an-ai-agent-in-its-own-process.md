# An AI agent in its own process

*October 2026. The features measured here are on modupy's main branch, after
0.12.0.*

An AI agent in a web application fails in ways a database query rarely does. A
model provider accepts a request and never answers. An SDK without async
support blocks the event loop while it waits for tokens. A model too big for
the container gets the process killed by the kernel. The first of those costs
only the agent's own work when its code is async, because the event loop goes
on serving while the call waits. The other two, when the agent runs in the
API's process, are the API's failure too.

The usual answer is a queue and a separate worker service: a second thing to
deploy, its own entry point, and a contract between the two. modupy, a
modular-monolith framework for Python, now offers a smaller step. The agent
stays a module of the same codebase, gets its work through the transactional
outbox, and runs in a process of its own because one line of configuration
says so. This post measures what that step buys, with a LangGraph agent and
three injected faults, and is specific about what it does not buy.

## The example

[`examples/agent_desk`](../../examples/agent_desk/) is a small help desk with
three modules:

- `tickets` stores a ticket and publishes `TicketOpened` in the same database
  transaction, through the outbox, so the event exists exactly when the ticket
  does.
- `agent` runs a LangGraph `StateGraph` on each new ticket, which classifies
  the ticket and drafts a first reply, and publishes `TicketTriaged`.
- `activity` logs both events.

The graph is plain LangGraph:
[`graph.py`](../../examples/agent_desk/desk/agent/graph.py) imports nothing
from modupy. What connects it to the rest is one listener,

```python
@listener(timeout=TIMEOUT)
async def triage(event: TicketOpened) -> None:
    async with sessionmaker() as session:
        # Delivery is at least once, so a redelivered ticket is not triaged twice.
        if await session.get(Triage, event.ticket_id) is not None:
            return
    state = await graph.ainvoke({"subject": event.subject, "body": event.body})
    ...  # store the triage and publish TicketTriaged in one transaction
```

and a few lines in `pyproject.toml`:

```toml
[tool.modulith]
own_process = ["agent"]
worker_liveness_timeout_seconds = 5
```

Under `uvicorn desk.main:app`, all three modules share one process. Under
`modulith run desk.main:app --topology processes`, the agent gets a worker
process of its own, `tickets` and `activity` share a second one, and a reverse
proxy in front sends `/tickets` and `/activity` there. The supervisor that
starts the workers restarts one that exits, and the liveness timeout has it
kill and restart one whose `/health` has gone unanswered for 5 seconds. The
code is the same in both topologies.

The model is a stand-in that answers from keywords, so that the benchmark
measures the architecture, not a provider's latency. An environment variable
makes every model call fail in one of three ways:

| Fault | What the model call does | Stands for |
|---|---|---|
| `hang` | `await asyncio.Event().wait()` | a request the provider accepts and never answers |
| `block` | `time.sleep(3600)` on the event loop's thread | a blocking SDK call inside async code |
| `crash` | `os.kill(os.getpid(), signal.SIGKILL)` | the kernel's out-of-memory killer |

## The benchmark

[`benchmark.py`](../../examples/agent_desk/benchmark.py) runs each topology
with each fault in a fresh copy of the example and its database. It opens one
ticket so that the agent starts failing, then sends 50 requests a second for 30
seconds. Requests go out on schedule whether or not earlier ones have returned,
so a stalled server shows up as latency rather than as fewer requests. One
request in ten opens a ticket, which keeps the agent fed; the rest read one.
The client gives each request two seconds, and a request that fails or runs out
of time counts as two seconds, so a dead server cannot post fast percentiles.
The agent's timeout is 2 seconds rather than the example's 30
(`DESK_AGENT_TIMEOUT`), so that a hung call is cancelled and retried within a
row.

Everything ran on one Linux desktop with 8 cores, Python 3.11, LangGraph
1.2.16 and a SQLite database in WAL mode, with a browser and other programs
running. That noise moves single runs around, so the benchmark ran three
times, one after another. Each row sent 1,500 requests per run. Every cell
gives the three runs in order.

| Topology | Fault | Failed requests | p50 ms | p95 ms | p99 ms | The agent |
|---|---|---|---|---|---|---|
| one process | none | 0 / 0 / 0 | 6.0 / 7.3 / 8.0 | 16.4 / 18.1 / 38.1 | 25.7 / 20.8 / 128.1 | triaged all 151 tickets |
| one process | hang | 0 / 0 / 0 | 4.3 / 7.5 / 7.3 | 12.1 / 15.6 / 15.8 | 25.1 / 18.1 / 18.5 | triaged none |
| one process | block | all 1,500 | 2,000 | 2,000 | 2,000 | stopped the API answering |
| one process | crash | all 1,500 | 2,000 | 2,000 | 2,000 | took the API down |
| own process | none | 0 / 0 / 0 | 9.2 / 14.4 / 11.1 | 17.5 / 141.1 / 31.2 | 20.6 / 252.4 / 103.7 | triaged all 151 tickets |
| own process | hang | 0 / 0 / 0 | 8.6 / 12.2 / 10.7 | 17.7 / 38.6 / 19.8 | 35.7 / 104.6 / 23.7 | triaged none |
| own process | block | 0 / 0 / 0 | 10.3 / 12.3 / 12.1 | 18.9 / 72.7 / 76.7 | 22.7 / 139.4 / 215.3 | triaged none, killed 3 / 3 / 4 times |
| own process | crash | 0 / 0 / 0 | 9.5 / 10.2 / 10.6 | 18.8 / 18.4 / 20.7 | 24.1 / 23.9 / 26.1 | triaged none, crashed 5 / 5 / 5 times |

The first run was quiet. The second and third overlapped heavy use of a
browser on the same desktop, which lifted even the rows without a fault: with
a healthy agent in its own process, the p95 went from 17.5 ms in the first run
to 141.1 and 31.2 ms. The first run shows the shape; the other two show how far
noise moves it.

The `block` row's p95 also rose in those two runs, to 72.7 and 76.7 ms, so a
separate run logged every request and timed the server's log lines, to see
whether killing the agent slows the API. Across the agent's three kills in
that row, no request failed, and the slowest request of each second stayed
under 50 ms, but for one at 95 ms that came 4 seconds after a kill. The rise
reads as the same noise.

## Reading it

**Hang.** The API stays flat in both topologies, because a hung call is an
`await`: while it waits, the event loop goes on serving requests. After 2
seconds the timeout cancels the call and fails the attempt, which is retried:
through the outbox in one process, by the broker in the other. No ticket was
triaged, because every attempt hangs again. In the processes topology the
broker dead-letters a ticket after its fifth attempt. The first ticket got
there about 25 seconds into the logged run, later than five 2-second attempts
take, because each retry waited for one of the agent's ten listener slots
behind the hung calls of newer tickets. This is the case where a timeout really
stops a hung agent: the listener is `async def`, so cancelling it ends the
work.

**Block.** In one process the API stops answering, and every request fails. A
blocked thread runs nothing else, the event loop included, so no timeout fires
and no request is served. With the agent in its own process, no request failed,
and in the first run the p95 was 18.9 ms against 17.5 ms with a healthy agent.
The blocked agent stops answering its own `/health` too, so the supervisor
kills it once 5 seconds have passed and starts another, which takes the next
ticket and blocks on it: 3, 3 and 4 kills in the three runs. What a killed
agent had not acknowledged stays in the broker for the one that replaces it. A
kill counts as a crash, so if every ticket keeps blocking the agent, the
supervisor stops restarting it after six in a row, as below.

**Crash.** In one process the agent's crash is the API's crash: the server is
gone after the first ticket, and every request after it fails. Nothing
restarts it in this benchmark. Under systemd or Kubernetes it would come back,
but every request in flight would fail with it, and the next ticket would take
it down again, so this row measures the outage, not a recovery. In its own
process the agent dies alone: no request failed, and the p95 stayed between
18.4 and 20.7 ms in all three runs, against 17.5 ms with a healthy agent in the
first. Each new ticket crashes the agent again, so the supervisor restarted it
five times in each run, with a delay that doubles from one second, and would
stop after the sixth crash in a row. A single bad ticket ends the loop sooner:
the broker counts each crash against the delivery that was running,
dead-letters it after its fifth attempt, and the agent stays up. The example's
README walks through that case.

**The cost.** With a healthy agent, the API answered in 9.2 ms at the median in
the processes topology against 6.0 ms in one process, in the first run,
because every request now crosses the reverse proxy, one more local HTTP hop.
At p95 the two were 1.1 ms apart (17.5 against 16.4), and at p99 the processes
topology came out lower (20.6 against 25.7). The noisy runs widen the gap. The
second process also costs its memory, which this benchmark does not measure.

## What a timeout cannot stop

The `hang` result depends on the listener being `async def`, and on how
cancellation works there. The timeout cancels the listener's task, which raises
`CancelledError` at the `await` it is waiting on. That is cooperative: code that
catches `CancelledError` and carries on keeps running, and code that never
returns to the event loop, like the `block` fault, never reaches an `await` for
the cancellation to land on. The liveness timeout is what ends that one. Work
an async listener hands to a thread is not stopped either: a LangChain chat
model without native async support runs its sync call that way, so the timeout
fails the listener on time while the call runs on, and the retry runs beside
it.

A plain `def` listener cannot take a timeout at all: modupy refuses
`@listener(timeout=...)` on one. It runs on a thread of the event loop's
default executor, and Python has no way to kill a thread, so a timeout could
only stop waiting while the stuck call kept its thread, and the retry would run
beside it. That pool is shared with every other sync listener and with
asyncio's DNS lookups, so in one process enough stuck calls fill it and stall
work that has nothing to do with the agent. A stuck thread does not stop the
event loop, so the worker keeps answering `/health` and the liveness timeout
does not fire. The only way to end the call is to end its process. With the
agent in a process of its own, restarting that worker ends only the agent, and
its `/health` reports `degraded` once a listener has run past its claim's
renewal limit (600 seconds by default), which tells you when. So a timeout
stops a hung agent when the agent is async; when it is not, run the agent in
its own process and restart it when it is stuck.

## Caveats

- One machine, one SQLite file, and a model that answers at once. A real model
  takes seconds per call, so a healthy agent holds more work in flight.
  Compare the rows with each other; the absolute numbers belong to this desktop.
- Every process here ran on one host. A process boundary contains a crash or a
  blocked event loop. It does not contain a host, or a container, that runs out
  of memory or CPU.
- SQLite takes one writer at a time. A write that finds another in progress
  waits for it, up to the driver's 5-second busy timeout, which is longer than
  the client's 2 seconds. If your own benchmark shows errors without a fault,
  look at write contention before you look at the agent.

## Run it

`own_process`, listener timeouts and the liveness timeout are not in 0.12.0 on
PyPI yet, so this installs modupy from a clone:

```bash
git clone https://github.com/jeansossmeier/modupy
cd modupy
pip install -e '.[fastapi,cli,postgres]' aiosqlite 'langgraph>=1.2,<2'
cd examples/agent_desk
python benchmark.py --seconds 30
```

The example's [README](../../examples/agent_desk/README.md) walks through the
crash by hand, in both topologies, and CI runs every command in it.
