# Examples

Three runnable projects, each one step up in scale, plus three single-file references for extending modupy. CI executes every command in each project's README exactly as written, so the README is the tutorial and the test at once.

| Project | Scale | What it teaches | Infrastructure |
|---|---|---|---|
| [`quickstart`](quickstart/) | Small: 3 modules | The root README's pitch as a real project: events between modules, listeners, per-module routers, and the same code run one process per module. | None |
| [`demo_app`](demo_app/) | Mid: 3 modules | Per-module persistence, idempotent listeners, a durable outbox, `modulith migrate`, one process per module with a scaled module, the outbox CLI, and swapping in Postgres and Redis. | A SQLite file; Docker for the last stage |
| [`marketplace`](marketplace/) | Large: 7 modules | A saga with compensation, durable cascades, scaled workers, dead-letter recovery, a boundary rule shipped as a plugin, tracing in every process, and extracting a module into its own service. | Docker (Postgres) |

Start with the smallest project that covers your question. Each one assumes you know the projects before it, and none repeats their lessons.

## Extension references

- [`naming_convention_verifier.py`](naming_convention_verifier.py): a custom rule for `modulith verify`, shipped as a plugin.
- [`redis_streams_broker.py`](redis_streams_broker.py): the producer side of a third-party broker adapter.
- [`versioned_json_serializer.py`](versioned_json_serializer.py): a custom outbox storage serializer.

## Which modupy they need

The projects use APIs added after modupy 0.10.0: `bind_session` from `modulith.builtin.outbox`, the retry, dead-letter and completion keys of `[tool.modulith.outbox_options]`, and `modulith migrate`. Run them against this repository or against the first release after 0.10.0.
