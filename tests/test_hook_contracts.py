"""Contract tests for hookspec guarantees (W2 fix batch G09_contract).

The hookspec docstrings in modulith/hooks.py make explicit promises:
observe-shaped hooks (dispatch/complete/error) observe, they don't gate —
a plugin's failure must never prevent the listener from running or mask
the listener's own exception. And modulith/manager.py promises that the
``disable`` list skips plugins *during loading* and that hookimpl name
typos surface loudly rather than silently never firing.

Each test cites the audit finding id it reproduces.
"""

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

import pluggy
import pytest

from modulith import create_plugin_manager, hookimpl
from modulith.runtime import _runtime

# ---------------------------------------------------------------------------
# Observe-only hook contract: dispatch/complete must not gate or mask
# ---------------------------------------------------------------------------


def _register_via_bootstrap(monkeypatch, plugin, name: str) -> None:
    """Make runtime bootstrap register ``plugin`` on its plugin manager."""
    import modulith.runtime as rt
    from modulith.manager import create_plugin_manager as original

    def patched(**kwargs):
        pm = original(**kwargs)
        pm.register(plugin, name=name)
        return pm

    monkeypatch.setattr(rt, "create_plugin_manager", patched)


@pytest.mark.asyncio
async def test_raising_dispatch_hookimpl_does_not_gate_listener(fake_app, monkeypatch):
    """A1-r4-165: a modulith_on_listener_dispatch hookimpl that raises must
    not prevent the listener from running.

    hooks.py documents: "The listener invocation happens regardless of what
    this hook does — it observes, it doesn't gate."
    """

    class _RaisingDispatch:
        @hookimpl
        def modulith_on_listener_dispatch(self, event, listener_name, publication) -> None:
            raise RuntimeError("tracer failed to start span")

    _register_via_bootstrap(monkeypatch, _RaisingDispatch(), "raising-dispatch")
    _runtime.configure(package="fakeapp")

    from fakeapp.orders import OrderCreated  # type: ignore[import-not-found]

    # Must not raise: the hook observes, it doesn't gate.
    await _runtime.publish(OrderCreated(order_id="o1"))

    from fakeapp.inventory import received  # type: ignore[import-not-found]

    assert len(received) == 1, "listener never ran — dispatch hookimpl exception gated it"


@pytest.mark.asyncio
async def test_raising_complete_hookimpl_does_not_mask_listener_error(make_fake_app, monkeypatch):
    """A1-r4-166: a modulith_on_listener_complete hookimpl that raises must
    not mask the listener's own exception.

    hooks.py documents that the complete hook, "like the other observe
    hooks[,] must not re-raise" — the original listener error must still
    reach the publish() caller even when a plugin's span-closing hook blows
    up.
    """
    errors_seen: list[BaseException] = []

    class _RaisingComplete:
        @hookimpl
        def modulith_on_listener_error(self, event, listener_name, publication, exception) -> None:
            errors_seen.append(exception)

        @hookimpl
        def modulith_on_listener_complete(
            self, event, listener_name, publication, exception
        ) -> None:
            raise RuntimeError("span exporter network hiccup while closing span")

    _register_via_bootstrap(monkeypatch, _RaisingComplete(), "raising-complete")
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class Boom:
                    x: int
            """,
            "handlers": """
                from modulith import listener
                from fakeapp.orders import Boom

                @listener
                async def explode(event: Boom) -> None:
                    raise ValueError("kaboom-original")
            """,
        }
    )
    _runtime.configure(package="fakeapp")

    from fakeapp.orders import Boom  # type: ignore[import-not-found]

    # The listener's own ValueError must propagate — not the plugin's
    # RuntimeError from the complete hook.
    with pytest.raises(ValueError, match="kaboom-original"):
        await _runtime.publish(Boom(x=1))

    assert [type(e).__name__ for e in errors_seen] == ["ValueError"]


@pytest.mark.asyncio
async def test_raising_error_hookimpl_does_not_mask_listener_error(make_fake_app, monkeypatch):
    """Companion to A1-r4-166 for the error hook's own documented contract:
    "exceptions from this hook are swallowed to prevent one plugin's failure
    from masking another's" (hooks.py, modulith_on_listener_error).
    """

    class _RaisingError:
        @hookimpl
        def modulith_on_listener_error(self, event, listener_name, publication, exception) -> None:
            raise RuntimeError("alerting backend unreachable")

    _register_via_bootstrap(monkeypatch, _RaisingError(), "raising-error")
    make_fake_app(
        {
            "orders": """
                from dataclasses import dataclass
                from modulith import event

                @event
                @dataclass(frozen=True)
                class Boom:
                    x: int
            """,
            "handlers": """
                from modulith import listener
                from fakeapp.orders import Boom

                @listener
                async def explode(event: Boom) -> None:
                    raise ValueError("kaboom-original")
            """,
        }
    )
    _runtime.configure(package="fakeapp")

    from fakeapp.orders import Boom  # type: ignore[import-not-found]

    with pytest.raises(ValueError, match="kaboom-original"):
        await _runtime.publish(Boom(x=1))


# ---------------------------------------------------------------------------
# Observe-only publish-error hook (Task 6 runtime-sync)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_error_fires_observe_only_hook_without_masking_failure(
    make_fake_app, monkeypatch
):
    """A publish that fails between the before/after event hooks (a durable
    persist failure, an inline broker-route failure) must fire the new
    ``modulith_on_publish_error`` hook — and a raising hookimpl must not
    mask the original failure, matching every other observe-only hook's
    contract."""
    from modulith import ConfigurationError

    seen: list[tuple[str, str]] = []

    class _RaisingPublishErrorObserver:
        @hookimpl
        def modulith_on_publish_error(self, event, exception) -> None:
            seen.append((type(event).__name__, type(exception).__name__))
            raise RuntimeError("observer's own bug must not surface")

    _register_via_bootstrap(monkeypatch, _RaisingPublishErrorObserver(), "raising-publish-error")
    make_fake_app({"orders": ""})
    _runtime.configure(
        package="fakeapp", topology="processes", broker="ghost-scheme", auto_discover=False
    )

    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Ghost:
        x: int

    with pytest.raises(ConfigurationError, match="ghost-scheme"):
        await _runtime.publish(Ghost(x=1))

    assert seen == [("Ghost", "ConfigurationError")]


# ---------------------------------------------------------------------------
# Entry-point plugin loading: disable must skip during loading (A1-r2-72)
# ---------------------------------------------------------------------------


def _write_fake_dist(tmp_path: Path, plugin_name: str, module_name: str) -> Path:
    """Lay out a real installed distribution advertising a modulith entry point.

    Returns the directory to put on sys.path. The plugin module touches a
    ``<module>.imported`` marker file at import time so tests can detect
    whether the import (and its side effects) actually ran.
    """
    site = tmp_path / "site"
    site.mkdir()
    (site / f"{module_name}.py").write_text(
        dedent(
            """
            from pathlib import Path

            from modulith import hookimpl

            # Import-time side effect the tests detect.
            Path(__file__).with_suffix(".imported").touch()


            @hookimpl
            def modulith_register_brokers(registry) -> None:
                pass
            """
        )
    )
    dist_info = site / f"{plugin_name}-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {plugin_name}\nVersion: 1.0\n"
    )
    (dist_info / "entry_points.txt").write_text(f"[modulith]\n{plugin_name} = {module_name}\n")
    return site


def test_entrypoint_plugin_loads_without_disable(tmp_path, monkeypatch):
    """Positive control for A1-r2-72: the fake distribution really is
    discovered, imported, and registered when not disabled."""
    site = _write_fake_dist(tmp_path, "fake_ep_pos", "fake_ep_pos_mod")
    monkeypatch.syspath_prepend(str(site))
    try:
        pm = create_plugin_manager(load_builtins=False, load_entrypoints=True)

        assert pm.has_plugin("fake_ep_pos")
        assert (site / "fake_ep_pos_mod.imported").exists()
    finally:
        sys.modules.pop("fake_ep_pos_mod", None)


def test_disable_skips_entrypoint_plugin_import(tmp_path, monkeypatch):
    """A1-r2-72: ``disable`` promises to skip plugins *during loading* —
    a disabled entry-point plugin's module must never be imported, so its
    import-time side effects must never run."""
    site = _write_fake_dist(tmp_path, "fake_ep_dis", "fake_ep_dis_mod")
    monkeypatch.syspath_prepend(str(site))
    try:
        pm = create_plugin_manager(
            load_builtins=False,
            load_entrypoints=True,
            disable=["fake_ep_dis"],
        )

        assert not pm.has_plugin("fake_ep_dis")
        assert not (site / "fake_ep_dis_mod.imported").exists(), (
            "disabled entry-point plugin was imported — its import-time side effects ran"
        )
    finally:
        sys.modules.pop("fake_ep_dis_mod", None)


def test_disable_does_not_block_explicit_extra_plugins(tmp_path, monkeypatch):
    """Regression guard for A1-r2-72's fix: blocking entry-point names must
    not prevent an *explicitly passed* extra plugin from registering under
    the same canonical name — extras are intentional and register last."""

    class _Extra:
        @hookimpl
        def modulith_register_brokers(self, registry) -> None:
            pass

    extra = _Extra()
    pm = create_plugin_manager(
        extra_plugins=[extra],
        load_builtins=False,
        load_entrypoints=True,
        disable=[pm_name := pluggy.PluginManager("modulith").get_canonical_name(extra)],
    )

    assert pm.is_registered(extra), f"extra plugin {pm_name!r} was blocked by disable"


# ---------------------------------------------------------------------------
# Hookimpl name typos must be rejected loudly (A1-r5-201)
# ---------------------------------------------------------------------------


def test_typoed_hookimpl_name_is_rejected_loudly():
    """A1-r5-201: a hookimpl whose name matches no declared hookspec (a
    typo) must fail manager creation with PluginValidationError, not be
    silently accepted and never invoked."""

    class _TypoPlugin:
        @hookimpl
        def modulith_verfy_module(self, module, all_modules):  # typo: "verfy"
            return []

    with pytest.raises(pluggy.PluginValidationError, match="modulith_verfy_module"):
        create_plugin_manager(
            extra_plugins=[_TypoPlugin()],
            load_builtins=False,
            load_entrypoints=False,
        )
