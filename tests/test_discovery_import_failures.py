"""W2 audit fixes: broken module / manifest imports must not be silent.

Covers three audit findings against modulith/builtin/discovery.py:

* A4-r5-207 — the discover hook's "never raises" contract must hold for
  ANY exception raised by the top-level application package, not just
  ImportError.
* A4-r3-133 — a module whose own package fails to import must fail
  bootstrap loudly instead of being reported as a healthy discovered
  module with its listeners silently missing.
* A4-r1-10 — a module whose ``_manifest.py`` raises during import must
  fail bootstrap loudly instead of being silently exempted from the very
  manifest verification that is documented to catch failed imports.
"""

from __future__ import annotations

import asyncio

import pytest

from modulith import ConfigurationError
from modulith import manifest as manifest_module


def _run(coro):
    """Drive a coroutine on a private loop without touching the policy loop.

    ``asyncio.run`` calls ``set_event_loop(None)`` on exit, which breaks
    older tests in the same session that still rely on
    ``asyncio.get_event_loop()``.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture(autouse=True)
def reset_manifests():
    """Ensure the manifest registry is clean around each test."""
    manifest_module._reset_for_testing()
    yield
    manifest_module._reset_for_testing()


# ---------------------------------------------------------------------------
# A4-r5-207 — top-level package failures never propagate from the hook
# ---------------------------------------------------------------------------


def test_discover_returns_empty_when_app_package_raises_non_import_error(
    make_fake_app,
) -> None:
    """A4-r5-207: the hook docstring promises 'never raises', but only
    ImportError was caught around the top-level import — a NameError (a
    real bug in the app's __init__) propagated out of the hook."""
    from modulith.builtin.discovery import modulith_discover_modules

    pkg = make_fake_app(
        {},
        package_name="buggyrootapp",
        extra_files={"__init__.py": "raise NameError('oops, a real bug in app init')\n"},
    )

    assert modulith_discover_modules(pkg) == []


# ---------------------------------------------------------------------------
# A4-r3-133 — module package import failure fails bootstrap loudly
# ---------------------------------------------------------------------------


def test_broken_module_package_fails_bootstrap(make_fake_app) -> None:
    """A4-r3-133: a module whose __init__ raises used to be recorded as a
    healthy discovered module while its listeners silently never
    registered — bootstrap succeeded with only a log line. SPEC promises
    startup fails with a clear error when a module fails to import."""
    from modulith import configure, publish

    make_fake_app({"orders": "raise RuntimeError('simulated bug: module fails to import')\n"})
    configure(package="fakeapp")

    with pytest.raises(ConfigurationError, match="failed to import"):
        _run(publish(object()))  # triggers bootstrap


def test_broken_module_does_not_abort_healthy_sibling_discovery(make_fake_app) -> None:
    """A4-r3-133 (companion): the loud failure is aggregated AFTER the walk —
    the healthy sibling module is still imported before bootstrap fails,
    and the error names the broken module."""
    import sys as _sys

    from modulith import configure, publish

    pkg = make_fake_app(
        {
            "good": "value = 1\n",
            "broken": "raise RuntimeError('boom at import time')\n",
        }
    )
    configure(package=pkg)

    with pytest.raises(ConfigurationError, match=f"{pkg}.broken"):
        _run(publish(object()))
    assert f"{pkg}.good" in _sys.modules


# ---------------------------------------------------------------------------
# A4-r1-10 — _manifest.py import failure fails bootstrap loudly
# ---------------------------------------------------------------------------


def test_broken_manifest_file_fails_bootstrap(make_fake_app) -> None:
    """A4-r1-10: a _manifest.py that raises before declare_module() used to
    leave the module absent from the manifest registry, so verification —
    documented to catch exactly 'module silently failed to import' — never
    ran for it and bootstrap succeeded."""
    from modulith import configure, publish

    make_fake_app(
        {"orders": "value = 1\n"},
        extra_files={
            "orders/_manifest.py": "raise RuntimeError('simulated bug inside _manifest.py')\n"
        },
    )
    configure(package="fakeapp")

    with pytest.raises(ConfigurationError, match="_manifest"):
        _run(publish(object()))
