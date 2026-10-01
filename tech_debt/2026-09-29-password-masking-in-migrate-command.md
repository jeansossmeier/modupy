---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Security
impact: Low - Passwords in query parameters are not masked in CLI output
effort: Low - Extend masking to handle query parameter secrets
---

# Password masking in migrate command doesn't cover query parameters

## Description
`modulith migrate` prints its target URL through `make_url(url).render_as_string(hide_password=True)`. That masks only the user-info password. A password passed as a query parameter prints in clear: `postgresql+psycopg://user@host/db?password=secret` renders unchanged, while `postgresql+psycopg://user:secret@host/db` renders as `user:***@host`.

## Affected Areas
- `modulith/cli.py::_masked_url`, used by the `migrate` command's output.

## Proposed Solution
Either: 1) also redact query parameters whose names mark secrets (`password`, `token`, `api_key`), or 2) print only the dialect, host and database name.

## Context
Verified with SQLAlchemy 2.1.1's `make_url` on both URL shapes. [Tool-Verified]

## Resolution
Extended `_masked_url` to parse and redact query parameters whose names match secret keywords (case-insensitive): `password`, `passwd`, `pwd`, `secret`, `token`, `apikey`, `api_key`. All secret parameters now render as `***` while non-secret parameters remain visible. Tests cover user-info passwords, query parameters with all secret keyword variants, and non-secret parameters.
