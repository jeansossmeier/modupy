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


def test_malformed_pyproject_raises_configuration_error(tmp_path: Path) -> None:
    """A4-r4-172 / design decision 1: TOML parse errors are LOUD.

    A malformed pyproject.toml must raise ConfigurationError instead of
    silently discarding the whole [tool.modulith] table (which would
    revert production/outbox to unsafe defaults with zero warning).
    Contract changed by user decision 1 — this test previously encoded
    the silent-skip behavior.
    """
    (tmp_path / "pyproject.toml").write_text("not [valid toml at all")
    with pytest.raises(ConfigurationError, match=r"pyproject\.toml"):
        load_configuration()


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
    """MODULITH_PRODUCTION=false produces False (bool).

    (Docstring updated per the A4-r2-80 adjudication: unrecognized values
    now raise ConfigurationError instead of coercing to False — see
    test_garbage_boolean_env_var_raises.)
    """
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


def test_pyproject_subtable_outbox_is_rejected_as_legacy(tmp_path: Path) -> None:
    """Design decision 1: [tool.modulith.outbox_options] is the ONLY options
    subtable — the legacy [tool.modulith.outbox] subtable raises loudly,
    pointing at the new spelling. Contract changed by user decision 1 —
    this test previously encoded the legacy subtable mapping to
    outbox_options."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.outbox]\ncompletion_mode = "delete"\nretry_interval_seconds = 60\n'
    )
    with pytest.raises(ConfigurationError, match="outbox_options"):
        load_configuration()


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
    [tool.modulith.outbox_options], [tool.modulith.workers] with all keys
    commented out. The resulting empty subtables must not break bootstrap.
    (Updated per user decision 1: the legacy [tool.modulith.outbox] spelling
    is now rejected, so the empty-subtable case uses outbox_options.)"""
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n[tool.modulith.verify]\n"
        "[tool.modulith.outbox_options]\n[tool.modulith.workers]\n"
    )
    cfg = load_configuration()
    assert cfg.outbox == "memory"
    assert cfg.outbox_options == {}
    assert cfg.workers == {}


# ----- W2 audit fixes: loud TOML errors, subtable contract (A4-r4-172, -------
# ----- A4-r2-79, A4-r5-208, design decision 1) --------------------------------


def test_scalar_and_subtable_outbox_collision_is_loud(tmp_path: Path) -> None:
    """A4-r4-172: the documented scalar+subtable dup-key collision is LOUD.

    TOML forbids `outbox = "postgres"` plus `[tool.modulith.outbox]` in one
    file (duplicate key). tomllib raises TOMLDecodeError; that must surface
    as ConfigurationError — not silently reset production=true to False.
    """
    (tmp_path / "pyproject.toml").write_text(
        "[tool.modulith]\n"
        'package = "myapp"\n'
        "production = true\n"
        'outbox = "postgres"\n'
        "\n"
        "[tool.modulith.outbox]\n"
        'completion_mode = "immediate"\n'
    )
    with pytest.raises(ConfigurationError, match=r"pyproject\.toml"):
        load_configuration()


def test_broker_and_broker_options_subtables_together_raise(tmp_path: Path) -> None:
    """A4-r2-79: both broker-options spellings in one file must not silently
    overwrite each other — whichever came last used to win with zero warning."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.broker]\nurl = "redis://a:6379"\n'
        '[tool.modulith.broker_options]\nconsumer_group = "grp"\n'
    )
    with pytest.raises(ConfigurationError, match="broker_options"):
        load_configuration()


def test_misspelled_subtable_raises_did_you_mean(tmp_path: Path) -> None:
    """A4-r5-208: a typo of a real subtable ([tool.modulith.worker] for
    workers) must raise loudly, not be silently dropped as forward-compat."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith.worker]\ndefault = 1\n")
    with pytest.raises(ConfigurationError, match="workers"):
        load_configuration()


def test_scalar_field_written_as_subtable_raises(tmp_path: Path) -> None:
    """A4-r5-208 (companion): a scalar option written as a table is user
    error, not forward-compat space — reject it loudly."""
    (tmp_path / "pyproject.toml").write_text('[tool.modulith.package]\nname = "myapp"\n')
    with pytest.raises(ConfigurationError, match="package"):
        load_configuration()


def test_verify_subtable_stays_reserved_noop(tmp_path: Path) -> None:
    """[tool.modulith.verify] remains a documented forward-compat no-op."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\noutbox = "postgres"\n[tool.modulith.verify]\nmode = "ratchet"\n'
    )
    cfg = load_configuration()
    assert cfg.outbox == "postgres"


# ----- W2 audit fixes: dict-typed field validation (A4-r4-173) ----------------


def test_scalar_outbox_options_in_pyproject_raises(tmp_path: Path) -> None:
    """A4-r4-173: outbox_options = "string" (forgot the table header) must be
    rejected at load time, not crash later with AttributeError on .get()."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\noutbox_options = "this-should-be-a-table-not-a-string"\n'
    )
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration()


def test_scalar_workers_kwarg_raises() -> None:
    """A4-r4-173: the dict-typed check also guards explicit kwargs."""
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration(workers=3)


def test_scalar_broker_options_kwarg_raises() -> None:
    """A4-r4-173: broker_options gets the same dict type check."""
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration(broker_options="redis://x")


# ----- W2 audit fixes: env var handling (A4-r4-174, A4-r2-80 adjudicated) -----


def test_empty_string_env_vars_are_documented_as_unset(monkeypatch) -> None:
    """A4-r4-174: MODULITH_X="" (e.g. from a CI template with a missing
    source variable) is treated as unset by documented contract — the field
    keeps its default and is NOT marked explicit."""
    monkeypatch.setenv("MODULITH_PACKAGE", "")
    monkeypatch.setenv("MODULITH_TOPOLOGY", "")
    monkeypatch.setenv("MODULITH_OUTBOX", "")
    cfg = load_configuration()
    assert cfg.package is None
    assert cfg.topology == "single"
    assert cfg.outbox == "memory"
    assert not cfg.is_explicit("package")
    assert not cfg.is_explicit("topology")
    assert not cfg.is_explicit("outbox")


def test_empty_production_env_var_is_unset_not_false(monkeypatch) -> None:
    """A4-r2-80 (adjudicated): MODULITH_PRODUCTION="" = unset (documented)."""
    monkeypatch.setenv("MODULITH_PRODUCTION", "")
    cfg = load_configuration()
    assert cfg.production is False
    assert not cfg.is_explicit("production")


def test_garbage_boolean_env_var_raises(monkeypatch) -> None:
    """A4-r2-80 (adjudicated): a non-empty unrecognized boolean value must
    raise ConfigurationError, not silently coerce to False — a typo like
    'ture' would otherwise flip the production safety check off."""
    monkeypatch.setenv("MODULITH_PRODUCTION", "ture")
    with pytest.raises(ConfigurationError, match="MODULITH_PRODUCTION"):
        load_configuration()


def test_garbage_auto_discover_env_var_raises(monkeypatch) -> None:
    """A4-r4-174: strict boolean parsing applies to every boolean env var."""
    monkeypatch.setenv("MODULITH_AUTO_DISCOVER", "banana")
    with pytest.raises(ConfigurationError, match="MODULITH_AUTO_DISCOVER"):
        load_configuration()


def test_boolean_env_var_accepts_explicit_false_words(monkeypatch) -> None:
    """A4-r2-80 (adjudicated): 0/false/no parse to explicit False."""
    for value in ("0", "no", "FALSE", " false "):
        monkeypatch.setenv("MODULITH_PRODUCTION", value)
        cfg = load_configuration()
        assert cfg.production is False, f"expected False for {value!r}"
        assert cfg.is_explicit("production"), f"expected explicit for {value!r}"


# ----- W2 audit fixes: unshipped subinterpreters topology (S2-r1-49) ----------


def test_subinterpreters_topology_is_rejected_as_unimplemented() -> None:
    """S2-r1-49: SPEC/ROADMAP declare subinterpreters unshipped, but config
    resolution accepted it and the CLI routed it through the real process
    supervisor. It must fail loudly even with a valid cross-process broker."""
    with pytest.raises(ConfigurationError, match="not yet implemented"):
        load_configuration(topology="subinterpreters", broker="redis-streams")
