"""Tests for configuration loading and validation."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from modulith import ConfigurationError
from modulith.config import load_configuration


# Reset cwd-dependent state by running each test in a tmp_path. This
# keeps pyproject.toml from a parent directory from leaking in.
@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Strip any MODULITH_* env vars the test runner inherits.
    for key in list(os.environ):
        if key.startswith("MODULITH_"):
            monkeypatch.delenv(key)
    yield


# ----- Defaults --------------------------------------------------------------


def test_defaults_when_no_config_present() -> None:
    """With no pyproject.toml, no env, no overrides, all defaults apply."""
    cfg = load_configuration()
    assert cfg.package is None  # auto-detect signal
    assert cfg.outbox == "memory"
    assert cfg.topology == "single"
    assert cfg.broker == "memory"
    assert cfg.auto_discover is True
    assert cfg.production is False
    assert cfg.explicit_keys == frozenset()


# ----- pyproject.toml --------------------------------------------------------


def test_reads_pyproject_modulith_section(tmp_path: Path) -> None:
    """[tool.modulith] in pyproject.toml populates Configuration."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.modulith]\npackage = "myapp"\noutbox = "postgres"\n')
    cfg = load_configuration()
    assert cfg.package == "myapp"
    assert cfg.outbox == "postgres"
    # Keys read from pyproject are tracked as explicit.
    assert cfg.is_explicit("package")
    assert cfg.is_explicit("outbox")
    # Unset keys still get defaults.
    assert cfg.topology == "single"
    assert not cfg.is_explicit("topology")


def test_missing_pyproject_falls_through_to_defaults(tmp_path: Path) -> None:
    """No pyproject.toml is fine — defaults apply."""
    # tmp_path has no pyproject.toml.
    cfg = load_configuration()
    assert cfg.outbox == "memory"


def test_malformed_pyproject_is_silently_skipped(tmp_path: Path) -> None:
    """A broken pyproject doesn't crash modulith — build tools surface that."""
    (tmp_path / "pyproject.toml").write_text("not [valid toml at all")
    # Should not raise; just falls through to defaults.
    cfg = load_configuration()
    assert cfg.outbox == "memory"


# ----- contracts_module (convention with a configurable default) -------------


def test_contracts_module_defaults_to_convention() -> None:
    """Unset, the contracts module follows the 'contracts' convention."""
    cfg = load_configuration(package="x")
    assert cfg.contracts_module == "contracts"
    assert not cfg.is_explicit("contracts_module")


def test_contracts_module_override() -> None:
    """The convention is a default, not a hardcode — it can be overridden."""
    cfg = load_configuration(package="x", contracts_module="shared")
    assert cfg.contracts_module == "shared"
    assert cfg.is_explicit("contracts_module")


def test_contracts_module_from_pyproject(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\ncontracts_module = "shared"\n')
    cfg = load_configuration()
    assert cfg.contracts_module == "shared"


def test_contracts_module_from_env(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_CONTRACTS_MODULE", "kernel")
    cfg = load_configuration(package="x")
    assert cfg.contracts_module == "kernel"


# ----- Environment variables -------------------------------------------------


def test_env_vars_override_pyproject(tmp_path: Path, monkeypatch) -> None:
    """MODULITH_* env vars take priority over pyproject.toml."""
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\noutbox = "postgres"\n')
    monkeypatch.setenv("MODULITH_OUTBOX", "mongodb")
    cfg = load_configuration()
    assert cfg.outbox == "mongodb"


def test_production_env_var_parses_truthy(monkeypatch) -> None:
    """MODULITH_PRODUCTION accepts 1, true, yes (case-insensitive)."""
    for value in ("1", "true", "TRUE", "yes", "Yes"):
        monkeypatch.setenv("MODULITH_PRODUCTION", value)
        # Need to also set outbox so production check passes.
        monkeypatch.setenv("MODULITH_OUTBOX", "postgres")
        cfg = load_configuration()
        assert cfg.production is True, f"expected truthy for {value!r}"


# ----- Explicit overrides ----------------------------------------------------


def test_explicit_overrides_beat_everything(tmp_path: Path, monkeypatch) -> None:
    """Kwargs to load_configuration() win over pyproject and env."""
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\noutbox = "postgres"\n')
    monkeypatch.setenv("MODULITH_OUTBOX", "mongodb")
    cfg = load_configuration(outbox="memory")
    assert cfg.outbox == "memory"


# ----- Validation ------------------------------------------------------------


def test_unknown_keys_raise_with_helpful_message() -> None:
    """Typos in config keys are caught immediately."""
    with pytest.raises(ConfigurationError, match="Unknown config keys"):
        load_configuration(outboxx="postgres")  # typo


def test_invalid_topology_raises() -> None:
    """Topology must be one of the known modes."""
    with pytest.raises(ConfigurationError, match="invalid topology"):
        load_configuration(topology="quantum")


def test_production_with_default_outbox_refuses_to_start() -> None:
    """Production mode + memory outbox = data loss waiting to happen."""
    with pytest.raises(ConfigurationError, match="production mode"):
        load_configuration(production=True)


def test_production_with_explicit_memory_outbox_is_allowed() -> None:
    """User can opt in to memory outbox in production by being explicit."""
    # No exception — they said yes, we trust them.
    cfg = load_configuration(production=True, outbox="memory")
    assert cfg.outbox == "memory"
    assert cfg.production is True


def test_production_with_durable_outbox_is_allowed() -> None:
    """The normal production case: explicit durable outbox."""
    cfg = load_configuration(production=True, outbox="postgres")
    assert cfg.outbox == "postgres"


def test_process_topology_with_default_memory_broker_refuses_to_start() -> None:
    """topology=processes + the default memory broker can't deliver cross-process.

    Regression: this combination used to validate clean and then silently
    no-op delivery deep in the runtime instead of failing fast.
    """
    with pytest.raises(ConfigurationError, match="requires a cross-process broker"):
        load_configuration(topology="processes")


def test_subinterpreters_topology_with_memory_broker_refuses_to_start() -> None:
    """Same coupling applies to the subinterpreters topology."""
    with pytest.raises(ConfigurationError, match="requires a cross-process broker"):
        load_configuration(topology="subinterpreters", broker="memory")


def test_process_topology_with_real_broker_is_allowed() -> None:
    """A process topology with a non-memory broker validates clean."""
    cfg = load_configuration(topology="processes", broker="redis-streams")
    assert cfg.topology == "processes"
    assert cfg.broker == "redis-streams"


def test_single_topology_with_memory_broker_is_allowed() -> None:
    """The default single-process topology is fine on the memory broker."""
    cfg = load_configuration(topology="single")
    assert cfg.broker == "memory"


# ----- Boolean coercion (T0.3) -----------------------------------------------


def test_production_env_var_is_strict_bool_not_string(monkeypatch) -> None:
    """MODULITH_PRODUCTION=true must produce True (bool), not 'true' (str).

    Locks in correct boolean coercion: cfg.production should pass
    ``is True`` identity comparison, not just be truthy.
    """
    monkeypatch.setenv("MODULITH_PRODUCTION", "true")
    monkeypatch.setenv("MODULITH_OUTBOX", "postgres")  # required in prod
    cfg = load_configuration()
    assert cfg.production is True
    assert type(cfg.production) is bool


def test_production_env_var_falsy_value_is_strict_false(monkeypatch) -> None:
    """MODULITH_PRODUCTION=false (or unrecognized) produces False (bool)."""
    monkeypatch.setenv("MODULITH_PRODUCTION", "false")
    cfg = load_configuration()
    assert cfg.production is False
    assert type(cfg.production) is bool


# ----- pyproject.toml subtable handling (T0.7) -------------------------------


def test_pyproject_outbox_subtable_maps_to_outbox_options(tmp_path: Path) -> None:
    """[tool.modulith.outbox_options] can coexist with scalar outbox."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\noutbox = "postgres"\n'
        "[tool.modulith.outbox_options]\n"
        'completion_mode = "delete"\n'
    )
    cfg = load_configuration()
    assert cfg.outbox == "postgres"
    assert cfg.outbox_options == {"completion_mode": "delete"}


def test_pyproject_subtable_outbox_alone_maps_to_options(tmp_path: Path) -> None:
    """A bare [tool.modulith.outbox] subtable populates outbox_options
    while leaving the scalar outbox field at its default."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.outbox]\ncompletion_mode = "delete"\nretry_interval_seconds = 60\n'
    )
    cfg = load_configuration()
    assert cfg.outbox == "memory"  # scalar default
    assert cfg.outbox_options == {
        "completion_mode": "delete",
        "retry_interval_seconds": 60,
    }


def test_pyproject_workers_subtable_maps_to_workers_field(tmp_path: Path) -> None:
    """[tool.modulith.workers] populates the workers field directly."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith.workers]\ndefault = 1\nreports = 4\n")
    cfg = load_configuration()
    assert cfg.workers == {"default": 1, "reports": 4}


def test_pyproject_broker_subtable_maps_to_broker_options(tmp_path: Path) -> None:
    """[tool.modulith.broker] populates broker_options (the SPEC's broker config).

    Regression: the subtable was previously dropped (no broker_options field),
    so SPEC-documented TOML broker settings were silently ignored.
    """
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.broker]\nurl = "redis://cache:6379"\nconsumer_group = "modulith-orders"\n'
    )
    cfg = load_configuration()
    assert cfg.broker_options == {
        "url": "redis://cache:6379",
        "consumer_group": "modulith-orders",
    }
    # Subtable form leaves the broker *name* at its default (set out-of-band).
    assert cfg.broker == "memory"


def test_pyproject_unknown_subtable_is_silently_dropped(tmp_path: Path) -> None:
    """Future subtables (e.g. [tool.modulith.verify]) shouldn't break bootstrap
    in older modulith versions. They're forward-compatibility space."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\noutbox = "postgres"\n'
        '[tool.modulith.verify]\nmode = "ratchet"\n'
        "[tool.modulith.unknown_future_feature]\nflag = true\n"
    )
    cfg = load_configuration()
    assert cfg.outbox == "postgres"


def test_pyproject_with_only_empty_subtables_uses_defaults(tmp_path: Path) -> None:
    """Regression: this repo's own pyproject.toml has [tool.modulith.verify],
    [tool.modulith.outbox], [tool.modulith.workers] with all keys commented
    out. The resulting empty subtables must not break bootstrap."""
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n[tool.modulith.verify]\n[tool.modulith.outbox]\n[tool.modulith.workers]\n"
    )
    cfg = load_configuration()
    assert cfg.outbox == "memory"
    assert cfg.outbox_options == {}
    assert cfg.workers == {}
