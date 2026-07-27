"""Regression tests for examples/ adapter-authoring samples.

Covers examples/ adapter-authoring samples: they must compose with the
shipped built-ins rather than colliding with them, and the snippets in their
docstrings must name the scheme the sample actually registers.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from textwrap import dedent
from types import ModuleType
from typing import Any

from modulith.brokers import BrokerRegistry
from modulith.manager import create_plugin_manager

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"

BUILTIN_SCHEME = "redis-streams"
EXAMPLE_SCHEME = "redis-streams-example"


def _load_example_adapter() -> ModuleType:
    """Import examples/redis_streams_broker.py from disk (not on sys.path)."""
    spec = importlib.util.spec_from_file_location(
        "example_redis_streams_adapter", EXAMPLES_DIR / "redis_streams_broker.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _indented_block(doc: str, marker: str) -> str:
    """Return the dedented indented block of ``doc`` that contains ``marker``.

    Docstring code samples are inert text — nothing imports or executes them —
    so extracting a block and running it is the only way to keep a sample
    honest. Blocks are the maximal runs of indented/blank lines, which is
    exactly how the samples in examples/ are written.
    """
    blocks: list[list[str]] = [[]]
    for line in doc.splitlines():
        if line.startswith("    ") or not line.strip():
            blocks[-1].append(line)
        else:
            blocks.append([])
    for block in blocks:
        text = dedent("\n".join(block)).strip()
        if marker in text:
            return text
    raise AssertionError(f"no indented block in the docstring contains {marker!r}")


def test_example_adapter_coexists_with_builtin_redis_broker(make_fake_app, monkeypatch) -> None:
    """The example adapter must not collide with the built-in during bootstrap.

    examples/redis_streams_broker.py used to register the same
    ``redis-streams`` scheme as the shipped ``modulith.adapters.redis_broker``
    built-in. Following the SPEC/README-documented selection path
    (``broker = "redis-streams"``) with the example adapter installed then
    crashed the ``modulith_register_brokers`` hook with DuplicateBrokerError.
    The example registers its own ``redis-streams-example`` scheme so both
    can coexist in one registry.

    The example is injected into the *runtime's own* plugin manager (the same
    ``_extra_plugins`` seam modulith.testing uses) rather than a standalone
    one, because the collision happened inside ``ensure_bootstrapped``: a
    registry built on the side would stay green even if bootstrap's own
    registration path regressed.
    """
    make_fake_app({"orders": ""})
    from modulith import configure
    from modulith.runtime import _runtime

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    _runtime._extra_plugins.append(_load_example_adapter())

    configure(package="fakeapp", broker=BUILTIN_SCHEME)
    # Before the fix this raised DuplicateBrokerError: scheme 'redis-streams'
    # is already registered to RedisStreamsBroker.
    _runtime.ensure_bootstrapped()

    registry = _runtime.broker_registry
    assert registry is not None
    assert BUILTIN_SCHEME in registry.schemes()  # built-in adapter
    assert EXAMPLE_SCHEME in registry.schemes()  # example adapter


def test_example_adapter_registers_only_its_own_scheme() -> None:
    """In isolation the example must claim ``redis-streams-example`` alone.

    Guards the other half of the coexistence contract: an example that also
    grabbed the built-in scheme would pass the bootstrap test above only
    until plugin load order changed.
    """
    example = _load_example_adapter()
    pm = create_plugin_manager(extra_plugins=[example], load_entrypoints=False, load_builtins=False)
    registry = BrokerRegistry()
    pm.hook.modulith_register_brokers(registry=registry)

    assert registry.schemes() == [EXAMPLE_SCHEME]


def test_example_docstring_snippet_routes_to_the_example_scheme() -> None:
    """The usage snippet in the example's docstring must execute and name the
    scheme the example registers.

    The snippet is what a reader copies. It once carried
    ``target="redis-streams:..."`` — the *built-in* scheme — so copying it
    routed events straight past the adapter the file exists to demonstrate.
    Nothing imports a docstring, so only executing it catches that.
    """
    example = _load_example_adapter()
    assert example.__doc__ is not None
    snippet = _indented_block(example.__doc__, "@externalized")

    namespace: dict[str, Any] = {}
    exec(compile(snippet, "<redis_streams_broker docstring>", "exec"), namespace)

    declared = [
        obj
        for obj in namespace.values()
        if isinstance(obj, type) and hasattr(obj, "__modulith_broker_target__")
    ]
    assert len(declared) == 1, "snippet must declare exactly one externalized event"
    target: str = declared[0].__modulith_broker_target__
    assert target.split(":", 1)[0] == EXAMPLE_SCHEME, (
        f"docstring snippet routes to {target!r}, which is not the "
        f"{EXAMPLE_SCHEME!r} scheme this example registers"
    )
