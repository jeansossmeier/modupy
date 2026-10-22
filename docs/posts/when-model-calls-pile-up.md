# When model calls pile up

*October 2026. Measured with modupy 0.13.0, and benchmark options the example
gained after that release.*

An `async` model call costs next to nothing while it waits: the event loop goes
on serving other requests. What the call holds while it waits is another
matter. When a model provider slows down, calls pile up, and if each one holds
a database connection from the pool the API reads from, the API runs out of
connections although none of its own code got slower.

This post measures that with a common mistake built in on purpose, first with
the agent in the API's process and then in a process of its own. It also
measures the real fix, which needs no second process.

## The mistake

A SQLAlchemy session takes a connection from the pool at its first query and
keeps it until its transaction ends or it closes. A session opened at the top
of a handler or a listener and closed at its end can hold the connection
through everything in between, a model call included:

```python
async with sessionmaker() as session:
    if await session.get(Triage, event.ticket_id) is not None:
        return
    state = await graph.ainvoke(...)  # the connection waits with the model
```

The triage listener in [`examples/agent_desk`](../../examples/agent_desk/)
does not do this: it closes its session before the model calls. Setting
`DESK_AGENT_HOLD_SESSION=1` makes it run them inside the session, as above.

## The benchmark

[`benchmark.py`](../../examples/agent_desk/benchmark.py) serves a fresh copy
of the example for each row, in one process under `uvicorn` or with the agent
in its own process under `modulith run --topology processes`. For this post:

- The desk runs on a fresh database on Postgres 16, with the stock limit of
  100 connections. The desk's engine has SQLAlchemy's default pool: 5
  connections, up to 10 more under load, and a wait of up to 30 seconds for a
  free one. The outbox has an engine of its own, with a pool of the same size.
- The agent holds its session across its model calls (`--hold-session`).
- Under the `slow` fault every model call answers 15 seconds late. A triage
  makes two calls, so it takes 30 seconds.
- The benchmark sends 10 requests a second for 45 seconds, whether or not
  earlier ones have returned. One in ten opens a ticket, so a new ticket
  arrives every second; the rest read one. A request that errors or takes over
  5 seconds counts as failed, and as 5 seconds.
- The agent's timeout is 40 seconds, so a slow triage can finish.
- A thread counts the desk database's open connections every half second.

Everything ran on one Linux desktop with 8 cores, Python 3.11, SQLAlchemy
2.1.6, psycopg 3.3.6, LangGraph 1.2.16 and Postgres 16 in Docker. Other work
kept the machine busy: its 1-minute load average read between 13.5 and 19.3 at
the start and end of each run. The benchmark ran three times, one after
another, each followed by the control. Each row sent 450 requests per run, and
every cell gives the three runs in order.

## Results

| Agent | Fault | Failed requests | p50 ms | p95 ms | Tickets triaged | Peak connections |
|---|---|---|---|---|---|---|
| in the API's process | none | 1 / 0 / 0 | 31.0 / 44.4 / 18.9 | 3,503.8 / 123.4 / 53.8 | all 46 | 26 / 8 / 6 |
| in the API's process | slow | 280 / 291 / 276 | 5,000 | 5,000 | 2 / 5 / 2 of 20 / 18 / 20 | 30 / 30 / 30 |
| in its own process | none | 0 / 0 / 0 | 46.4 / 34.9 / 48.7 | 273.5 / 116.2 / 157.6 | all 46 | 20 / 14 / 13 |
| in its own process | slow | 0 / 0 / 0 | 34.3 / 38.7 / 38.0 | 75.0 / 87.8 / 120.1 | 10 / 10 / 10 of 46 | 20 / 22 / 23 |
| in the API's process, session closed (the control) | slow | 0 / 0 / 0 | 43.3 / 22.0 / 20.6 | 372.9 / 62.2 / 81.0 | 26 / 26 / 26 of 46 | 19 / 10 / 12 |

## Reading it

**No fault.** One request of the 2,700 sent without a fault failed, in the
first run's one-process row, whose p95 also reached 3.5 seconds where the
other two runs gave 123.4 and 53.8 ms. Noise on this desktop moves single rows
that far, so read the failure counts first.

**Slow, in the API's process.** The outbox starts each ticket's triage as
soon as the ticket's transaction commits, and nothing caps how many run at
once. With a ticket a second and each triage holding a connection for 30
seconds, triages alone would need 30 connections, and the desk's pool gives
out 15. Requests that need one wait for up to the pool's 30 seconds, far past
the client's 5. Between 61 and 65% of requests failed, opening tickets as well as
reading them, so only 18 to 20 tickets were opened. The agent finished 2 to 5
of them: once its model calls returned, it needed a connection again to store
the result, and queued for it behind everything else. At the peak the
database had 30 connections open, the most the desk's pool and the outbox's
pool can hold between them.

**Slow, in its own process.** No request failed in any run, and the p95
stayed between 75.0 and 120.1 ms. The agent ran at most 10 triages at a time,
so its held sessions took at most 10 connections, from the pool of its own
process; the pool behind `/tickets` belongs to the other worker and stayed
free. The other tickets waited in the broker. The agent finished 10 tickets
per run: the first ten, 30 seconds after each started, while the next ten were
still waiting on the model when the row ended. It fell behind, and the API
kept answering. The same topology without a fault was slower at p95, 116.2 to
273.5 ms, in every run; there the agent triaged all 46 tickets and the other
worker recorded each result, so both workers had more to do.

**The fix.** With the session closed before the model calls, the example's
default, one process failed no request in any run either, with a p95 between
62.2 and 372.9 ms and at most 19 connections open. The agent finished 26
tickets in each run, more than in either topology with the session held:
nothing capped it, and its waiting calls held nothing. Holding nothing while a
model answers is the fix. A process of its own is the backstop, for the call
that holds something you did not see.

## What the comparison changes

Two things differ between the topologies, not one. In one process the outbox
hands each ticket to the agent as soon as the ticket's transaction commits,
and nothing limits how many triages run at once. In its own process the agent
takes at most 10 deliveries from the broker at a time, the default
`dispatch_concurrency`, and the rest wait in the broker, holding no
connection. So the agent got both a pool of its own and a cap, and this
benchmark does not separate the two. In one process, a cap in the listener,
such as a semaphore taken before the session opens, would have kept the agent
to 10 of the 15 connections too. These runs do not measure that.

## What a process of its own does not fix

- The agent still falls behind. At 30 seconds a triage and 10 at a time, it
  finishes 10 tickets every 30 seconds while one arrives every second, so the
  backlog grows until the model speeds up again. The process boundary protects
  the API, not the agent's throughput.
- Every pool still draws on one Postgres server and its connection limit.
  Processes that each keep a pool of 15 reach the default limit of 100 sooner
  than one process does.
- The mistake was built in. With the session closed before the model calls,
  one process was fine too, as the control row shows.

## Run it

The example and its benchmark live in modupy's repository, so this runs them
from a clone, with Postgres in Docker:

```bash
git clone https://github.com/jeansossmeier/modupy
cd modupy
pip install -e '.[fastapi,cli,postgres]' aiosqlite 'langgraph>=1.2,<2'
docker run -d --name desk-postgres -e POSTGRES_PASSWORD=secret -p 127.0.0.1:5432:5432 postgres:16
cd examples/agent_desk
python benchmark.py --postgres postgresql://postgres:secret@127.0.0.1:5432/postgres \
  --hold-session --faults none,slow --rate 10 --seconds 45 \
  --client-timeout 5 --agent-timeout 40
python benchmark.py --postgres postgresql://postgres:secret@127.0.0.1:5432/postgres \
  --faults slow --topologies single --rate 10 --seconds 45 \
  --client-timeout 5 --agent-timeout 40
docker rm -f desk-postgres
```

The second command is the control, with the session closed before the model
calls. [An AI agent in its own process](an-ai-agent-in-its-own-process.md)
measures the same example while the agent hangs, blocks and crashes.
