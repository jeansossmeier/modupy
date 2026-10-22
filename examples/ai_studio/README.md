# The AI studio

A shop's product reviews with three AI features in the same codebase, each one
a module beside the business code instead of a Python service of its own.
`reviews` stores a review and publishes `ReviewPosted` in the same transaction,
through the outbox. Three AI modules of different shapes work on it:

- `moderation` runs a local model on each new review and publishes
  `ReviewModerated`. `reviews` then publishes or hides the review.
- `replies` runs a LangGraph agent on each new review and publishes
  `ReplyDrafted` with a reply that `reviews` stores.
- `summaries` answers `GET /summaries/{product}` by asking a model to summarize
  the product's published reviews while the caller waits.

The modules share only the events in `contracts` and one public function,
`published_reviews`, which `summaries` calls on `reviews`. `modulith verify`
checks that no module reaches into another's internals.

Each AI module fails in its own way, so each gets the containment that fits:

- `summaries` calls a hosted model on the request path. A call that hangs is
  cancelled by `asyncio.timeout` and the route answers 504; that works in any
  process, the API's included.
- `replies` calls a hosted model from a listener. `@listener(timeout=30)`
  cancels a hung call, and the outbox retries it later.
- `moderation` runs its model in its own process. Local inference is
  synchronous code that holds its thread while it runs, and native code can
  crash the process it runs in; no timeout stops either. `own_process =
  ["moderation"]` in `pyproject.toml` gives it a worker of its own under
  `--topology processes`, while `reviews`, `replies` and `summaries` share
  another.

- [`studio/contracts/__init__.py`](studio/contracts/__init__.py): the three events
- [`studio/reviews/`](studio/reviews): `post_review`, `published_reviews`, the listeners that record the verdict and the reply, and the `POST /reviews` and `GET /reviews/{id}` routes
- [`studio/moderation/`](studio/moderation): the `moderate` listener and a stand-in local classifier
- [`studio/replies/`](studio/replies): the `draft_reply` listener with its timeout, the LangGraph graph, and a stand-in chat model
- [`studio/summaries/`](studio/summaries): the `GET /summaries/{product}` route with its timeout, and a stand-in summarizer
- [`studio/faults.py`](studio/faults.py): the fault switch every stand-in model reads
- [`tests/`](tests): each module, a hung reply and a hung summary, under `pytest`

The models answer from keywords and ratings, so the studio runs without an API
key or a model download. Set `STUDIO_MODERATION_FAULT`, `STUDIO_REPLIES_FAULT`
or `STUDIO_SUMMARIES_FAULT` to `hang`, `block` or `crash` and every call to
that module's model fails that way; any other value that is not empty stops
the module from importing. To use real models, replace `is_abusive` in
`studio/moderation/model.py` and `summarize` in `studio/summaries/model.py`,
and assign any LangChain chat model to `model` in `studio/replies/graph.py`.

Every command below runs from this directory, `examples/ai_studio` in a clone
of the repository (the [examples index](../README.md) shows how), exactly as
written. CI executes them and checks that every line of output shown appears,
in order. Real output can carry more, such as server logs and process ids. The
steps share one `studio.db`, so each one posts a review with an id of its own.

## Install and look around

```bash
pip install 'modupy[fastapi,cli,postgres]' aiosqlite 'langgraph>=1.2,<2'
```

The project is not installed: `uvicorn` and `modulith` both put the current
directory on the import path, and `pyproject.toml` names the package (`studio`).
It also sets `outbox = "postgres"`, the SQL outbox, here on the SQLite file
`studio.db`, so each event is written in the transaction that causes it.

```bash
$ modulith info
modulith
  package: studio

  modules (5):
    - contracts  (studio.contracts)  [no manifest]
    - moderation  (studio.moderation)  [no manifest]
    - replies  (studio.replies)  [no manifest]
    - reviews  (studio.reviews)  [no manifest]
    - summaries  (studio.summaries)  [no manifest]

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

The tests run every module in one process. Two of them set a fault:
`STUDIO_REPLIES_FAULT=hang` with a 2-second timeout, where the reply is
cancelled and its outbox row records the failure for a retry while the review
is still moderated and summarized, and `STUDIO_SUMMARIES_FAULT=hang`, where the
summary route answers 504 once its timeout runs out.

## Run it in one process

`modulith migrate` creates the outbox tables and `python -m studio.schema` the
reviews table.

```bash
$ modulith migrate
migrated sqlite:///studio.db to head
$ python -m studio.schema
```

```bash
uvicorn studio.main:app
```

```bash
$ curl -sX POST localhost:8000/reviews -H 'content-type: application/json' \
      -d '{"review_id": "r-1", "product": "mug", "rating": 5, "text": "Lovely glaze."}'
{"review_id":"r-1"}
```

The AI modules run after the review's transaction commits, so give them a
moment:

```bash
$ curl -s localhost:8000/reviews/r-1
{"review_id":"r-1","product":"mug","rating":5,"status":"published","reply":"Thank you for reviewing the mug!"}
$ curl -sX POST localhost:8000/reviews -H 'content-type: application/json' \
      -d '{"review_id": "r-2", "product": "mug", "rating": 1, "text": "Total scam."}'
{"review_id":"r-2"}
$ curl -s localhost:8000/reviews/r-2
{"review_id":"r-2","product":"mug","rating":1,"status":"hidden","reply":"We are sorry the mug let you down; our team will be in touch."}
```

Moderation hid `r-2`, so the summary leaves it out:

```bash
$ curl -s localhost:8000/summaries/mug
{"product":"mug","summary":"1 of 1 published reviews of the mug are positive."}
```

## When the summarizer hangs

`STUDIO_SUMMARIES_FAULT=hang` makes the summary model wait forever on an
answer that never comes, as a hosted model can:

```bash
export STUDIO_SUMMARIES_FAULT=hang
```

```bash
uvicorn studio.main:app
```

The route's `asyncio.timeout` cancels the call after
`STUDIO_SUMMARIES_TIMEOUT` (10) seconds and answers 504, and the rest of the
API answers as before:

```bash
$ curl -s localhost:8000/summaries/mug
{"detail":"the model did not answer within 10s"}
$ curl -s localhost:8000/reviews/r-1
{"review_id":"r-1","product":"mug","rating":5,"status":"published","reply":"Thank you for reviewing the mug!"}
```

A timeout cancels only a call that is waiting. A `block` fault holds the event
loop's thread, and a `crash` kills the process; in one process, either one
stops the whole API. The summarizer and the reply agent wait on hosted models,
where a timeout covers a call that hangs, so they share a process with
`reviews`. The local moderation model holds the event loop's thread while it
runs and can crash its process, so it gets a process of its own.

## Give moderation its own process

Nothing in `studio/` changes. `own_process = ["moderation"]` gives moderation
one worker, and `replies`, `reviews` and `summaries` share the other. Startup
logs warnings about the default broker and about modules with no routes;
neither affects this walkthrough.

```bash
export STUDIO_SUMMARIES_FAULT=
```

```bash
$ modulith run studio.main:app --topology processes --host 127.0.0.1
modulith → process topology: 2 worker(s) [moderation:9001, replies,reviews,summaries:9002], reverse proxy on http://127.0.0.1:8000
```

```bash
$ curl -sX POST localhost:8000/reviews -H 'content-type: application/json' \
      -d '{"review_id": "r-3", "product": "mug", "rating": 2, "text": "The handle broke."}'
{"review_id":"r-3"}
$ curl -s localhost:8000/reviews/r-3
{"review_id":"r-3","product":"mug","rating":2,"status":"published","reply":"We are sorry the mug let you down; our team will be in touch."}
```

The proxy's `/_modulith/health` reports each worker by the prefixes it serves.
Without a token it answers only on a loopback address, which is why these
commands pass `--host 127.0.0.1`:

```bash
$ curl -s localhost:8000/_modulith/health
{"status":"ok","backends":{"/moderation":"ok","/replies":"ok","/reviews":"ok","/summaries":"ok"}}
```

## When moderation crashes

`STUDIO_MODERATION_FAULT=crash` makes the moderation model kill its own process
with `SIGKILL`, as the kernel's out-of-memory killer does to a process that
loads too big a model. Only moderation dies, and the supervisor restarts it:

```bash
export STUDIO_MODERATION_FAULT=crash
```

```bash
$ modulith run studio.main:app --topology processes --host 127.0.0.1
modulith → process topology: 2 worker(s) [moderation:9001, replies,reviews,summaries:9002], reverse proxy on http://127.0.0.1:8000
WARNING:  worker moderation exited with code -9; restarting in 1.0s
```

The review is stored, the agent drafts its reply and the summary answers. Only
the verdict is missing:

```bash
$ curl -sX POST localhost:8000/reviews -H 'content-type: application/json' \
      -d '{"review_id": "r-4", "product": "mug", "rating": 4, "text": "Keeps tea hot."}'
{"review_id":"r-4"}
$ curl -s localhost:8000/reviews/r-4
{"review_id":"r-4","product":"mug","rating":4,"status":"pending","reply":"Thank you for reviewing the mug!"}
$ curl -s localhost:8000/summaries/mug
{"product":"mug","summary":"1 of 2 published reviews of the mug are positive."}
```

`r-4` waits in the broker: each restarted moderation worker takes it up again
and crashes again, and the supervisor waits longer before each restart (1, 2,
4, 8 and 16 seconds). The fifth crash is the last: the broker dead-letters the
delivery and logs that it did, and `modulith broker dead-letter --retry-all`
resubmits it once moderation is fixed. In one process, a crash like this takes
the API down with it, as [the agent desk](../agent_desk/README.md#when-the-agent-crashes)
shows with its agent.

## Fix moderation

`r-4` is still waiting in the broker, which moderation never acknowledged. It
comes back once its claim expires, after seconds here because `pyproject.toml`
shortens the broker's `reclaim_stale_seconds`, which defaults to a minute.
Clear the fault and run the studio again:

```bash
export STUDIO_MODERATION_FAULT=
```

```bash
modulith run studio.main:app --topology processes --host 127.0.0.1
```

```bash
$ curl -s localhost:8000/reviews/r-4
{"review_id":"r-4","product":"mug","rating":4,"status":"published","reply":"Thank you for reviewing the mug!"}
$ curl -s localhost:8000/summaries/mug
{"product":"mug","summary":"2 of 3 published reviews of the mug are positive."}
```
