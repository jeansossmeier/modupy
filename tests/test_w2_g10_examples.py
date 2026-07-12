"""Regression tests for the W2 G10 examples-group audit findings.

Covers examples/ adapter-authoring samples: they must compose with the
shipped built-ins rather than colliding with them.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from modulith.brokers import BrokerRegistry
from modulith.manager import create_plugin_manager

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"


def _load_example_adapter() -> ModuleType:
    """Import examples/redis_streams_broker.py from disk (not on sys.path)."""
    spec = importlib.util.spec_from_file_location(
        "example_redis_streams_adapter", EXAMPLES_DIR / "redis_streams_broker.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_adapter_coexists_with_builtin_redis_broker(make_fake_app, monkeypatch) -> None:
    """A1-r4-167: the example adapter must not collide with the built-in.

    examples/redis_streams_broker.py used to register the same
    ``redis-streams`` scheme as the shipped ``modulith.adapters.redis_broker``
    built-in. Following the SPEC/README-documented selection path
    (``broker = "redis-streams"``) with the example adapter installed then
    crashed the ``modulith_register_brokers`` hook with DuplicateBrokerError.
    The example registers its own ``redis-streams-example`` scheme so both
    can coexist in one registry.
    """
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    configure(package="fakeapp", broker="redis-streams")
    _runtime.ensure_bootstrapped()

    example = _load_example_adapter()
    pm = create_plugin_manager(extra_plugins=[example], load_entrypoints=False, load_builtins=True)

    registry = BrokerRegistry()
    # Before the fix this raised DuplicateBrokerError: scheme 'redis-streams'
    # is already registered to RedisStreamsBroker.
    pm.hook.modulith_register_brokers(registry=registry)

    assert "redis-streams" in registry.schemes()  # built-in adapter
    assert "redis-streams-example" in registry.schemes()  # example adapter
