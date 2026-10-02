---
type: tech-debt
debt_status: resolved
created: 2026-09-30
updated: 2026-10-02
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

Setting `XDG_STATE_HOME` covers Linux only. On macOS, `_default_state_home` returns `~/Library/Application Support`, and on Windows it returns `LOCALAPPDATA`. The fixture must therefore also set `HOME` and `LOCALAPPDATA`, or patch `_default_state_home`. `tests/test_shm_broker.py::_redirect_default_state_home` already sets all three for four SHM tests and can move into the fixture.

To find the leaking test, run the suite in halves and watch for a new directory.

## Context
On 2026-09-30 this machine held 1,473 directories under `~/.local/state/modulith/` (88 MB), and 1,440 of them were `fakeapp-<hash>`. The oldest was from 2026-07-21. A full default run created exactly one new `fakeapp-<hash>/`. [Tool-Verified]

The other 33 came from hand runs of example apps (`myapp`, `shop`, `marketplace`) outside the test suite.

## Resolution (2026-10-02)
`tests/conftest.py::_private_state_home` is an autouse fixture that points `XDG_STATE_HOME` and `LOCALAPPDATA` at a per-test temporary directory. The broker state of every test, and of any worker a test starts, therefore stays out of the developer's home on Linux and Windows.

A full default run on 2026-10-02, under an outer `XDG_STATE_HOME` pointing at an empty sentinel directory, left 0 new entries under `~/.local/state/modulith` and 0 in the sentinel. [Tool-Verified]

Residual, accepted: on macOS, `modulith.adapters._state_path._default_state_home` derives the state home from `HOME` alone, and the fixture cannot redirect `HOME` without breaking the tests that run git. A test there that starts the SHM broker without setting its own state directory still writes under `~/Library/Application Support/modulith`.
