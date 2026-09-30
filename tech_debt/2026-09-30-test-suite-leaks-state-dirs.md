---
type: tech-debt
debt_status: open
created: 2026-09-30
updated: 2026-09-30
category: Testing
impact: Low - Every default test run leaves a broker state directory in the developer's home
effort: Low - One autouse fixture in tests/conftest.py, plus finding the leaking test
---

# The test suite writes broker state into the real home directory

## Description
A default-lane test starts the SHM broker for a `fakeapp` project without redirecting modulith's state directory. Each run of `pytest -m "not integration"` leaves one new `fakeapp-<hash>/` under `~/.local/state/modulith/`, holding an empty `.modulith-shm-broker.db` and a 128 KB `.modulith-shm-broker.hints`. The hash comes from the temporary project path, so no run ever reuses a directory.

## Affected Areas
- `tests/conftest.py`: no autouse fixture redirects the state directory, and only 5 test files set `XDG_STATE_HOME` themselves
- `modulith/adapters/_state_path.py::_default_state_home`

## Proposed Solution
Add an autouse fixture in `tests/conftest.py` that points the state directory into `tmp_path`.

Setting `XDG_STATE_HOME` covers Linux only. On macOS, `_default_state_home` returns `~/Library/Application Support`, and on Windows it returns `LOCALAPPDATA`. The fixture must therefore also set `HOME` and `LOCALAPPDATA`, or patch `_default_state_home`.

To find the leaking test, run the suite in halves and watch for a new directory.

## Context
On 2026-09-30 this machine held 1,473 directories under `~/.local/state/modulith/` (88 MB), and 1,440 of them were `fakeapp-<hash>`. The oldest was from 2026-07-21. A full default run created exactly one new `fakeapp-<hash>/`. [Tool-Verified]

The other 33 came from hand runs of example apps (`myapp`, `shop`, `marketplace`) outside the test suite.
