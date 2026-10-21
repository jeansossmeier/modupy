# The agent desk

A help desk with an AI agent in it. `tickets` stores a ticket and publishes
`TicketOpened` in the same transaction, through the outbox. `agent` runs a
LangGraph graph on each new ticket and publishes `TicketTriaged` with a
category, a priority and a first reply. `tickets` records the result and
`activity` logs both events. No module imports another, only the shared
events.

The example is about what happens when the agent misbehaves. Model calls hang,
block their thread, or take their process down with them. In one process,
some of those take the API down too. `own_process = ["agent"]` in
`pyproject.toml` gives the agent a process of its own under
`--topology processes`, while `tickets` and `activity` share another, and the
API keeps answering through all three failures. [The write-up](../../docs/posts/an-ai-agent-in-its-own-process.md)
has the numbers.

- [`desk/contracts/__init__.py`](desk/contracts/__init__.py): the two events
- [`desk/tickets/`](desk/tickets): `open_ticket`, the `record_triage` listener, and the `POST /tickets` and `GET /tickets/{id}` routes
- [`desk/agent/graph.py`](desk/agent/graph.py): the agent, plain LangGraph with nothing of modupy in it
- [`desk/agent/__init__.py`](desk/agent/__init__.py): the `triage` listener that runs the graph, with a timeout
- [`desk/agent/model.py`](desk/agent/model.py): a stand-in chat model that fails on demand
- [`desk/activity/__init__.py`](desk/activity/__init__.py): the listeners that log each ticket's events, and their route
- [`benchmark.py`](benchmark.py): the API's latency while the agent hangs, blocks or crashes
- [`tests/`](tests): the flow, and a hung model call, under `pytest`

The model in `desk/agent/model.py` answers from keywords, so the desk runs
without an API key. Set `DESK_AGENT_FAULT` to `hang`, `block` or `crash` and
every model call fails that way; any other value that is not empty stops the
agent from importing. To use a real model, assign any LangChain chat
model to `model` in `desk/agent/graph.py`; the graph does not change.

Every command below runs from this directory, `examples/agent_desk` in a clone
of the repository (the [examples index](../README.md) shows how), exactly as
written. CI executes them and checks that every line of output shown appears,
in order. Real output can carry more, such as server logs and process ids. The
steps share one `desk.db`, so each one opens a ticket with an id of its own.

## Install and look around

```bash
pip install 'modupy[fastapi,cli,postgres]' aiosqlite 'langgraph>=1.2,<2'
```

The project is not installed: `uvicorn` and `modulith` both put the current
directory on the import path, and `pyproject.toml` names the package (`desk`).
It also sets `outbox = "postgres"`, the SQL outbox, here on the SQLite file
`desk.db`, so each event is written in the transaction that causes it.

```bash
$ modulith info
modulith
  package: desk

  modules (4):
    - activity  (desk.activity)  [no manifest]
    - agent  (desk.agent)  [no manifest]
    - contracts  (desk.contracts)  [no manifest]
    - tickets  (desk.tickets)  [no manifest]

  configuration:
    outbox:        postgres
    broker:        memory
    topology:      single
    observability: None
    production:    False
$ modulith verify
✓ no boundary violations
```

## Run its tests

```bash
pip install pytest pytest-timeout httpx2
pytest
```

The second test sets `DESK_AGENT_FAULT=hang` and a 2-second timeout, and reads
`GET /tickets/t-1` while the model call hangs. The call is then cancelled and
its outbox row records the failure for a retry. The others check that a
redelivered ticket is not triaged twice and that an unknown fault name stops
the agent from importing.

## Run it in one process

`modulith migrate` creates the outbox tables and `python -m desk.schema` the
modules' own.

```bash
$ modulith migrate
migrated sqlite:///desk.db to head
$ python -m desk.schema
```

```bash
uvicorn desk.main:app
```

```bash
$ curl -sX POST localhost:8000/tickets -H 'content-type: application/json' \
      -d '{"ticket_id": "t-1", "subject": "Refund please", "body": "I was charged twice."}'
{"ticket_id":"t-1"}
```

The agent runs after the ticket's transaction commits, so give it a moment:

```bash
$ curl -s localhost:8000/tickets/t-1
{"ticket_id":"t-1","category":"billing","priority":"normal","reply":"Thanks for writing in about 'Refund please'; we are on it."}
$ curl -s localhost:8000/activity/t-1
{"ticket_id":"t-1","activity":["opened","triaged as billing/normal"]}
```

## Give the agent its own process

Nothing in `desk/` changes. `own_process = ["agent"]` gives the agent one
worker, and `tickets` and `activity` share the other, so two processes do the
work instead of three. Startup logs warnings about the default broker and about
the agent having no routes; neither affects this walkthrough.

```bash
$ modulith run desk.main:app --topology processes --host 127.0.0.1
modulith → process topology: 2 worker(s) [activity,tickets:9001, agent:9002], reverse proxy on http://127.0.0.1:8000
```

```bash
$ curl -sX POST localhost:8000/tickets -H 'content-type: application/json' \
      -d '{"ticket_id": "t-2", "subject": "Site is down", "body": "Every page returns a 500."}'
{"ticket_id":"t-2"}
$ curl -s localhost:8000/tickets/t-2
{"ticket_id":"t-2","category":"outage","priority":"high","reply":"Thanks for writing in about 'Site is down'; we are on it."}
$ curl -s localhost:8000/activity/t-2
{"ticket_id":"t-2","activity":["opened","triaged as outage/high"]}
```

The proxy's `/_modulith/health` reports each worker by the prefixes it serves.
Without a token it answers only on a loopback address, which is why these
commands pass `--host 127.0.0.1`:

```bash
$ curl -s localhost:8000/_modulith/health
{"status":"ok","backends":{"/activity":"ok","/tickets":"ok","/agent":"ok"}}
```

## When the agent crashes

`DESK_AGENT_FAULT=crash` makes the model call kill its own process with
`SIGKILL`, as the kernel's out-of-memory killer does to a process that loads
too big a model. With the agent in its own process, only the agent dies, and
the supervisor restarts it:

```bash
export DESK_AGENT_FAULT=crash
```

```bash
$ modulith run desk.main:app --topology processes --host 127.0.0.1
modulith → process topology: 2 worker(s) [activity,tickets:9001, agent:9002], reverse proxy on http://127.0.0.1:8000
WARNING:  worker agent exited with code -9; restarting in 1.0s
```

```bash
$ curl -sX POST localhost:8000/tickets -H 'content-type: application/json' \
      -d '{"ticket_id": "t-3", "subject": "Invoice", "body": "Where is my invoice?"}'
{"ticket_id":"t-3"}
$ curl -s localhost:8000/activity/t-3
{"ticket_id":"t-3","activity":["opened"]}
$ curl -s localhost:8000/tickets/t-3
{"ticket_id":"t-3","category":null,"priority":null,"reply":null}
```

The API answers, and the ticket waits: each restarted agent takes it up again
and crashes again, and the supervisor waits longer before each restart (1, 2,
4, 8 and 16 seconds). A delivery reclaimed from a crashed consumer counts as an
attempt, so the fifth crash is the last: the broker dead-letters the delivery
and logs that it did, and the agent stays up. `modulith broker dead-letter
--retry-all` resubmits it once the agent is fixed. In one process, the process
the agent kills is the API:

```bash
uvicorn desk.main:app
```

```bash
$ curl -sX POST localhost:8000/tickets -H 'content-type: application/json' \
      -d '{"ticket_id": "t-4", "subject": "Login fails", "body": "Error on every login."}'
{"ticket_id":"t-4"}
$ curl -s localhost:8000/tickets/t-4
```

The ticket was stored, then the agent's crash took the server with it, so the
last `curl` finds nothing listening and exits with code 7.

The other two faults:

- `hang`: the model call waits forever on an answer that never comes. The
  `triage` listener has `@listener(timeout=30)` and is `async`, so the timeout
  cancels the call and the attempt is retried later: by the broker when the
  agent has a process of its own, by the outbox in one process. This stops a
  hung async agent in one process as well.
- `block`: the model call holds the event loop's thread, as a blocking SDK call
  inside async code does. No timeout can fire while the loop is blocked, so in
  one process the API stops answering. In its own process the API keeps
  answering, and `worker_liveness_timeout_seconds = 5` in `pyproject.toml` has
  the supervisor kill the agent once its `/health` has gone unanswered that
  long, then restart it as after a crash.

## Fix the agent

Both crashed tickets are still waiting: `t-3` in the broker, which the agent
never acknowledged, and `t-4` in the outbox, whose delivery the crash
interrupted. Each comes back once its claim expires, after seconds here
because `pyproject.toml` shortens the outbox's `claim_lease_seconds` and the
broker's `reclaim_stale_seconds`; both default to a minute. Clear the fault and
run the desk again:

```bash
export DESK_AGENT_FAULT=
```

```bash
modulith run desk.main:app --topology processes --host 127.0.0.1
```

```bash
$ curl -s localhost:8000/tickets/t-3
{"ticket_id":"t-3","category":"billing","priority":"normal","reply":"Thanks for writing in about 'Invoice'; we are on it."}
$ curl -s localhost:8000/tickets/t-4
{"ticket_id":"t-4","category":"outage","priority":"high","reply":"Thanks for writing in about 'Login fails'; we are on it."}
```

## Measure it

```bash
python benchmark.py --seconds 5
```

For each topology and fault, the script serves a fresh copy of this directory,
opens a ticket so the agent starts failing, then sends 50 requests a second for
the given time: one in ten opens a ticket, the rest read one. It prints the
error rate, the p50, p95 and p99 latencies, how many opened tickets the agent
triaged, and how often the agent's worker crashed. A request that fails or
takes over two seconds counts as two seconds. The agent runs with a 2-second
timeout, which each row records as `agent_timeout_s`, so a hung call is
cancelled and retried within the row. Each retry waits for a free listener
slot behind the hung calls of newer tickets, so in a 30-second row the first
ticket's delivery fails its fifth and last attempt about 25 seconds in, and the
server log records the dead letter. `--port-base` picks the three ports each run serves on. It writes the same rows
to `bench-results.json`. The run takes about two minutes; the write-up reports
three runs with `--seconds 30`.
