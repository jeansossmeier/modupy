---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-02
category: Observability
impact: Low - Trace and metric interpretation differs between outbox and memory dispatch
effort: Low - Standardize naming and linkage
---

# listener.name differs and spans not linked across outbox/broker

## Description
Multiple observability inconsistencies:
1. `listener.name` differs between in-memory dispatch and outbox dispatch
2. Spans are not linked across the outbox or the broker
3. `Configuration.observability` is read only by `info` command, not used for tracing configuration

## Affected Areas
- `modulith/runtime.py` (listener instrumentation)
- `modulith/builtin/outbox.py` (span linking)
- `modulith/adapters/db_broker.py` (span linking)

## Proposed Solution
Standardize listener naming, add parent-child links for cross-process spans, and use `Configuration.observability` consistently in all observability paths.

## Context
Derived by reading the code, not by a run. The exact symbols are not pinned yet; the files above are where each behavior lives. [Assertion-Only]

## Resolution (2026-10-02)
All three inconsistencies are fixed:
1. **`listener.name`:** in-memory, outbox and broker dispatch all name a listener by the outbox's stored id from `modulith.builtin.outbox._listener_id`: `module.qualname`, with an `owner:` prefix for a bound method or callable instance.
2. **Span links:** each outbox row stores the W3C trace context of the publish span that wrote it, in `EventPublication.trace_context` (migration `0008_outbox_trace_context`). Broker messages carry that context in `traceparent` and `tracestate` headers, which the outbox route takes from the stored row and the inline route from the live publish span. The dispatch span of a delivery after commit, on a retry or in a consuming process is therefore a child of the publish span. `outbox._ensure_retry_loop` starts the retry task in an empty `contextvars.Context`, so retried deliveries no longer inherit the span of the publish that started the loop.
3. **`Configuration.observability`:** bootstrap honours it. `false` skips the tracing plugin, `true` without OpenTelemetry raises a `ConfigurationError` naming `pip install 'modupy[otel]'`, and unset auto-detects as before.

Regression tests:
- `tests/test_observability.py`: `test_dispatch_listener_name_is_the_outbox_listener_id_in_memory`, `test_dispatch_listener_name_matches_for_broker_delivered_events`, `test_dispatch_listener_name_carries_owner_prefix_for_bound_methods`, and the four `test_observability_*` switch tests.
- `tests/test_observability_publish_span.py`: `test_after_commit_dispatch_span_is_a_child_of_the_publish_span`, `test_retry_dispatch_span_is_a_child_of_the_publish_span`, `test_retry_loop_dispatch_span_has_no_parent_from_the_creating_publish`, `test_broker_route_row_carries_the_publish_span_carrier`, `test_rows_carry_no_trace_context_when_tracing_is_off`, `test_garbage_trace_context_dispatches_with_a_parentless_span`.
- `tests/test_broker_trace_propagation.py`: both routes on the shm and database brokers, identical headers on a retried send, no headers with tracing off, unusable carriers and headers, and the redis-streams hop (`test_redis_streams_hop_joins_the_dispatch_span_to_the_publish_span`, integration).
- `tests/test_postgres_outbox_adapter.py` and `tests/test_migrations.py`: the stored context survives save, claim, dead-lettering and archiving, and migration 0008 adds and drops its column.

Residual, accepted: a third-party broker adapter that drops message headers loses the link, so its consumers' dispatch spans have no parent.
