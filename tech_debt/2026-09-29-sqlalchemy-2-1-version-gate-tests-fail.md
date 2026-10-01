---
type: tech-debt
debt_status: resolved
created: 2026-09-29
updated: 2026-10-01
category: Testing
impact: High - 50+ test failures in default lane when SQLAlchemy 2.1.1 is installed (CI-affecting)
effort: Medium - Add raising=False flag and guard deletion
---

# SQLAlchemy 2.1.1 skip-locked version-gate tests fail

## Description
Under SQLAlchemy 2.1.1 (what an unlocked install resolves today), skip-locked version-gate tests fail with ~50 FAILED/ERROR results because they use `monkeypatch.delattr` on an attribute (`_mariadb_normalized_version_info`) that SQLAlchemy 2.1 removed. The attribute does not exist, so `delattr` raises AttributeError.

The product code handles this gracefully through `getattr(..., None)` defaults in the same file, so the test gate is being overly strict.

SQLAlchemy 2.1 did not drop the name everywhere; it moved it. `MySQLDialect` no longer carries it as a class attribute, which is why the 50 default-lane cases raise `type object 'MySQLDialect' has no attribute '_mariadb_normalized_version_info'`. A new mixin, `sqlalchemy/dialects/mysql/_mariadb_shim.py::MariaDBShim`, defines it as a read-only `@property` that returns `server_version_info`.

That property causes a second symptom in the integration lane. `test_mysql_server_without_skip_locked_is_rejected_at_startup` sets the name on an aiomysql dialect instance and fails, for MySQL 5.7 and MariaDB 10.5, with `property '_mariadb_normalized_version_info' of 'MySQLDialect_aiomysql' object has no setter`.

## Affected Areas
- `tests/test_db_broker.py`: the `test_skip_locked_gate_*` tests, including `test_skip_locked_gate_reads_the_version_without_sqlalchemys_mariadb_field`.
- `tests/test_db_broker_integration.py::test_mysql_server_without_skip_locked_is_rejected_at_startup`: 2 cases in the integration lane.
- `modulith/adapters/db_broker.py::_supports_skip_locked`, which reads the attribute through a `getattr` default and so already tolerates its absence.

## Proposed Solution
Use `monkeypatch.delattr(..., raising=False)` to handle missing attributes gracefully, matching the production code's defensive pattern.

That covers only the default-lane `delattr` cases. The integration test needs the property replaced on the class that resolves it, or on a subclass, because an instance-level `setattr` cannot override a read-only property.

## Context
CI installs dependencies without a lock file, so it resolves SQLAlchemy 2.1.1 and these tests fail there. The same tests pass under SQLAlchemy versions that still define the attribute. [Tool-Verified]

On 2026-09-30 the full integration lane gave 2 failed and 114 passed, with 0 skipped; the 2 failures are the integration cases above. Neither test file was changed on `feat/examples-ladder`. [Tool-Verified]

## Resolution (2026-10-01)
The description above understated the impact: on SQLAlchemy 2.1 the product refused supported servers, not only the tests. SQLAlchemy 2.1.1 stores in `server_version_info` only the three numbers before the `MariaDB` token, so `10.6.12-8-MariaDB-enterprise` read as 6.12.8, and `_supports_skip_locked` refused a MariaDB Enterprise 10.6 server at consumer start. [Tool-Verified]

`DatabaseBroker._mysql_server_version` now reads the server's `VERSION()` once per broker, and the gate takes the release from that string (`modulith/adapters/db_broker.py::_mysql_family_release`), so SQLAlchemy 2.0 and 2.1 give the same answer. The integration test simulates an older server by rewriting that `SELECT VERSION()` in a `before_cursor_execute` listener instead of writing SQLAlchemy's private dialect fields. Unit and integration tests pass on SQLAlchemy 2.0.51 and 2.1.1. [Tool-Verified]

Still open upstream: SQLAlchemy 2.1.1 itself raises `NotImplementedError` while connecting to a MariaDB Enterprise 11 server, because it reads `11.4.2-1-MariaDB-enterprise` as 4.2.1, below its 5.0.2 minimum. [Tool-Verified]
