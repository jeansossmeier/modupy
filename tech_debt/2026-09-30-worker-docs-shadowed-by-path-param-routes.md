---
type: tech-debt
debt_status: open
created: 2026-09-30
updated: 2026-09-30
category: Architecture
impact: Low - A worker's docs are unreachable when its module has a top-level GET /{param} route
effort: Low - Register the doc paths first unless the module defines the same exact path
---

# A worker's docs routes lose to a module's path-parameter routes

## Description
`modulith/_worker.py::create_app` registers `/<module>/openapi.json`, `/<module>/docs` and `/<module>/redoc` after `include_router`, on purpose. Its comment says a module's own routes must win any collision, and names a module called `docs` or one defining its own `/docs` route.

The comment does not name the common case: a top-level path-parameter route. `GET /orders/{order_id}` also matches `/orders/openapi.json`, so the module's handler receives `order_id="openapi.json"` and answers 404. The same happens to `/docs` and `/redoc`. Most CRUD modules have such a route, so their worker docs are unreachable through the proxy.

## Affected Areas
- `modulith/_worker.py::create_app`

## Proposed Solution
Register the three doc routes before the module router, skipping any path the module router defines exactly. A module's own `/docs` route still wins, and a path parameter no longer captures the docs.

Alternatively, document the limitation beside the docs URLs and point to `modulith openapi`, which builds the document without serving it.

## Context
Found while writing the `examples/demo_app` flow tests. The orders worker answered `/orders/openapi.json` with the module's 404, so the tests read `create_app().openapi()` instead.

The registration order is read from `create_app`. [Tool-Verified]

The observed 404 comes from the test author's report. [Assertion-Only] Highly Probable, since Starlette matches routes in registration order.
