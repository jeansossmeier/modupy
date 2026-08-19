"""Tests for configuration loading and validation."""

from __future__ import annotations

import os
import subprocess
import tomllib
from pathlib import Path, PurePosixPath

import pytest

from modulith import ConfigurationError
from modulith.config import _redact_broker_url, load_configuration


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
    assert cfg.subscription_source == "manifest"
    assert cfg.subscriptions == {}
    assert cfg.actuator_mode == "auto"
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
    """TOML parse errors are LOUD.

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
    cfg = load_configuration(package="x", contracts_module="shared.kernel")
    assert cfg.contracts_module == "shared.kernel"
    assert cfg.is_explicit("contracts_module")


def test_contracts_module_from_pyproject(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\ncontracts_module = "shared"\n')
    cfg = load_configuration()
    assert cfg.contracts_module == "shared"


def test_contracts_module_from_env(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_CONTRACTS_MODULE", "kernel")
    cfg = load_configuration(package="x")
    assert cfg.contracts_module == "kernel"


@pytest.mark.parametrize(
    "value",
    [
        "",
        "/shared",
        r"shared\kernel",
        "shared/kernel",
        "shared..kernel",
        ".shared",
        "shared.",
        "shared\nkernel",
        "shared.class",
    ],
)
def test_contracts_module_rejects_non_importable_names(value: str) -> None:
    with pytest.raises(ConfigurationError, match="contracts_module"):
        load_configuration(package="x", contracts_module=value)


def test_contracts_module_from_pyproject_rejects_path(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.modulith]\ncontracts_module = "../outside"\n')

    with pytest.raises(ConfigurationError, match="contracts_module"):
        load_configuration()


def test_contracts_module_from_env_rejects_keyword(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_CONTRACTS_MODULE", "shared.class")

    with pytest.raises(ConfigurationError, match="contracts_module"):
        load_configuration(package="x")


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


def test_process_topology_defaults_to_shm_broker() -> None:
    """topology=processes with no broker configured auto-selects the shm
    broker (shared-memory ring buffer) — the broker is NOT marked as explicitly set."""
    cfg = load_configuration(topology="processes")
    assert cfg.broker == "shm"
    assert cfg.is_explicit("broker") is False


def test_env_driven_process_topology_also_defaults_broker(monkeypatch) -> None:
    """MODULITH_TOPOLOGY=processes (no kwargs) takes the same defaulting path —
    the injection keys off the merged explicit dict, not the call site."""
    monkeypatch.setenv("MODULITH_TOPOLOGY", "processes")
    cfg = load_configuration()
    assert cfg.broker == "shm"
    assert cfg.is_explicit("broker") is False


def test_default_broker_warning_fires_only_on_defaulted_path(caplog) -> None:
    """The defaulting warning names the broker file; an explicit broker stays
    silent — 'the warnings on startup tell users exactly which defaults are
    active' (module docstring contract)."""
    import logging

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        load_configuration(topology="processes")
    assert any(".modulith-shm-broker.db" in r.getMessage() for r in caplog.records)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        load_configuration(topology="processes", broker="database")
    assert caplog.records == []


def test_default_broker_warning_is_announced_once_per_boot(caplog) -> None:
    """One boot resolving the configuration repeatedly announces the default once.

    ``modulith run --topology=processes`` resolves the same configuration three
    times before the first worker starts (the CLI places the topology, then
    finds the application package, then ``Runtime.ensure_bootstrapped()``
    resolves it again), and each resolution reaches the same defaulting branch.
    Three identical warnings read as three separate problems.
    """
    import logging

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        for _ in range(3):
            assert load_configuration(topology="processes").broker == "shm"

    announced = [r for r in caplog.records if ".modulith-shm-broker.db" in r.getMessage()]
    assert len(announced) == 1, [r.getMessage() for r in announced]


def test_a_different_broker_default_is_still_announced(caplog) -> None:
    """Suppression is per decision, so a reload landing elsewhere still warns.

    The URL-inferred 'database' adapter and the SHM fallback are different
    answers to the same question; announcing one must not silence the other.
    """
    import logging

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        load_configuration(topology="processes")
        load_configuration(
            topology="processes",
            broker_options={"url": "postgresql+asyncpg://db/prod"},
        )

    messages = [r.getMessage() for r in caplog.records]
    assert any(".modulith-shm-broker.db" in m for m in messages), messages
    assert any("inferring the 'database' adapter" in m for m in messages), messages


def test_subinterpreters_without_broker_raises_not_implemented() -> None:
    """topology=subinterpreters with NO broker no longer hits the cross-process
    broker guard (that now needs an explicit memory broker) — it falls through
    to the 'not yet implemented' rejection. Pins the new error identity."""
    with pytest.raises(ConfigurationError, match="not yet implemented"):
        load_configuration(topology="subinterpreters")


def test_process_topology_with_explicit_memory_broker_refuses_to_start() -> None:
    """An explicit broker='memory' with processes topology still fails fast."""
    with pytest.raises(ConfigurationError, match="requires a cross-process broker"):
        load_configuration(topology="processes", broker="memory")


def test_subinterpreters_topology_with_memory_broker_refuses_to_start() -> None:
    """Same coupling applies to the subinterpreters topology."""
    with pytest.raises(ConfigurationError, match="requires a cross-process broker"):
        load_configuration(topology="subinterpreters", broker="memory")


def test_production_allows_implicit_shm_with_explicit_outbox() -> None:
    """The private durable SHM default is valid in production on one host."""
    cfg = load_configuration(topology="processes", production=True, outbox="postgres")
    assert cfg.broker == "shm"
    assert cfg.is_explicit("broker") is False


def test_production_with_explicit_database_broker_requires_url() -> None:
    """Production + broker='database' without a URL is refused — otherwise
    registration would invent a per-host SQLite file."""
    with pytest.raises(ConfigurationError, match="no URL"):
        load_configuration(
            topology="processes", production=True, broker="database", outbox="postgres"
        )


def test_production_with_explicit_database_broker_and_url_is_allowed() -> None:
    """Explicit broker='database' + URL in production + processes is accepted."""
    cfg = load_configuration(
        topology="processes",
        production=True,
        broker="database",
        outbox="postgres",
        broker_options={"url": "postgresql+asyncpg://db/prod"},
    )
    assert cfg.broker == "database"
    assert cfg.topology == "processes"


def test_blank_broker_name_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="non-empty"):
        load_configuration(broker="   ")


def test_process_topology_with_url_infers_database_and_redacts_credentials(caplog) -> None:
    """A legacy URL-only config selects database without logging its password."""
    import logging

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        cfg = load_configuration(
            topology="processes",
            broker_options={"url": "postgresql+asyncpg://alice:secret@db/prod"},
        )
    assert cfg.broker == "database"
    assert cfg.is_explicit("broker") is False
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "secret" not in joined
    assert "alice" not in joined
    assert "***@db" in joined


def test_process_topology_with_plain_filesystem_url_infers_shm(tmp_path: Path) -> None:
    """The SHM adapter's legacy ``url`` path alias is not a database connection."""
    cfg = load_configuration(
        topology="processes",
        broker_options={"url": str(tmp_path / "broker.db")},
    )

    assert cfg.broker == "shm"
    assert cfg.broker_options["url"] == str(tmp_path / "broker.db")


def test_env_broker_url_does_not_claim_embedded_sqlite(caplog, monkeypatch) -> None:
    """MODULITH_BROKER_URL alone must not emit the embedded-SQLite-file warning."""
    import logging

    monkeypatch.setenv("MODULITH_BROKER_URL", "postgresql+asyncpg://db/prod")
    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        cfg = load_configuration(topology="processes")
    assert cfg.broker == "database"
    assert cfg.is_explicit("broker") is False
    assert not any(".modulith-shm-broker.db" in r.getMessage() for r in caplog.records)
    assert any("broker URL" in r.getMessage() for r in caplog.records)


def test_env_plain_filesystem_url_takes_precedence_over_dsn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The URL alias remains first in env precedence without becoming a DSN."""
    monkeypatch.setenv("MODULITH_BROKER_URL", str(tmp_path / "broker.db"))
    monkeypatch.setenv("MODULITH_BROKER_DSN", "postgresql+asyncpg://db/prod")

    cfg = load_configuration(topology="processes")

    assert cfg.broker == "shm"


def test_env_broker_dsn_infers_database(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_BROKER_DSN", "postgresql+asyncpg://db/prod")

    cfg = load_configuration(topology="processes")

    assert cfg.broker == "database"
    assert cfg.is_explicit("broker") is False


def test_broker_options_dsn_infers_database() -> None:
    cfg = load_configuration(
        topology="processes",
        broker_options={"dsn": "postgresql+asyncpg://db/prod"},
    )

    assert cfg.broker == "database"
    assert cfg.is_explicit("broker") is False


def test_production_allows_inferred_database_with_url(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_BROKER_URL", "postgresql+asyncpg://db/prod")

    cfg = load_configuration(topology="processes", production=True, outbox="postgres")

    assert cfg.broker == "database"
    assert cfg.is_explicit("broker") is False


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://db/prod",
        "sqlite+aiosqlite:///tmp/broker.db",
        "redis://cache:6379/0",
    ],
)
def test_explicit_shm_rejects_sqlalchemy_and_network_urls(url: str) -> None:
    with pytest.raises(ConfigurationError, match="shm"):
        load_configuration(
            topology="processes",
            broker="shm",
            broker_options={"url": url},
        )


def test_explicit_shm_accepts_plain_filesystem_url_alias(tmp_path: Path) -> None:
    cfg = load_configuration(
        topology="processes",
        broker="shm",
        broker_options={"url": str(tmp_path / "broker.db")},
    )

    assert cfg.broker_options["url"] == str(tmp_path / "broker.db")


def test_implicit_shm_validates_canonical_path_options() -> None:
    with pytest.raises(ConfigurationError, match="state_dir"):
        load_configuration(
            topology="processes",
            broker_options={"state_dir": 42},
        )


@pytest.mark.parametrize(
    ("url", "secrets"),
    [
        ("postgresql://:passwordonly@db/prod", ("passwordonly",)),
        ("postgresql://alice:s%65cret@db/prod", ("s%65cret", "secret")),
        (
            "postgresql://db/prod?access_token=querysecret&ssl=true",
            ("querysecret",),
        ),
        (
            "postgresql://alice:usersecret@db/prod?sslmode=querysecret#fragmentsecret",
            ("alice", "usersecret", "querysecret", "fragmentsecret"),
        ),
        (
            "postgresql://alice:malformed-secret@@db/prod",
            ("alice", "malformed-secret"),
        ),
        ("host=db password=rawsecret", ("rawsecret",)),
    ],
)
def test_inferred_broker_warning_redacts_adversarial_urls(
    caplog, url: str, secrets: tuple[str, ...]
) -> None:
    import logging

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        load_configuration(topology="processes", broker_options={"url": url})

    joined = " ".join(record.getMessage() for record in caplog.records)
    for secret in secrets:
        assert secret not in joined


def test_broker_url_redaction_masks_all_userinfo_query_values_and_fragment() -> None:
    redacted = _redact_broker_url(
        "postgresql://alice:password@db/prod"
        "?sslmode=verify-full&application_name=orders#client-certificate"
    )

    assert redacted == ("postgresql://***@db/prod?sslmode=%2A%2A%2A&application_name=%2A%2A%2A#***")


@pytest.mark.parametrize(
    "url",
    [
        "host=db password=secret",
        "postgresql://db:invalid-port/prod",
        "postgresql://[invalid/prod",
        "postgresql://db/prod\ninjected",
    ],
)
def test_broker_url_redaction_fails_closed_for_unparseable_input(url: str) -> None:
    assert _redact_broker_url(url) == "<redacted broker URL>"


@pytest.mark.parametrize(
    ("options", "option_name"),
    [
        ({"sqlite_synchronous": "OFF"}, "sqlite_synchronous"),
        ({"completion_mode": []}, "completion_mode"),
        ({"max_payload_bytes": 0}, "max_payload_bytes"),
        ({"max_payload_bytes": 1024**3 + 1}, "max_payload_bytes"),
        ({"max_store_bytes": True}, "max_store_bytes"),
        ({"max_store_bytes": 1024**4 + 1}, "max_store_bytes"),
        ({"shm_capacity": 0}, "shm_capacity"),
        ({"shm_capacity": 10**12}, "shm_capacity"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_size": 10**12}, "batch_size"),
        ({"dispatch_concurrency": 0}, "dispatch_concurrency"),
        ({"dispatch_concurrency": 10**12}, "dispatch_concurrency"),
        ({"max_delivery_attempts": 0}, "max_delivery_attempts"),
        ({"poll_interval_ms": float("inf")}, "poll_interval_ms"),
        ({"reclaim_stale_seconds": float("nan")}, "reclaim_stale_seconds"),
        ({"retention_age_seconds": 0}, "retention_age_seconds"),
        ({"prune_interval_seconds": -1}, "prune_interval_seconds"),
    ],
)
def test_shm_options_are_validated_before_adapter_construction(
    options: dict[str, object],
    option_name: str,
) -> None:
    with pytest.raises(ConfigurationError, match=option_name):
        load_configuration(
            topology="processes",
            broker="shm",
            broker_options=options,
        )


def test_shm_storage_limits_accept_documented_safe_maxima() -> None:
    cfg = load_configuration(
        topology="processes",
        broker="shm",
        broker_options={
            "max_payload_bytes": 1024**3,
            "max_store_bytes": 1024**4,
        },
    )

    assert cfg.broker_options["max_payload_bytes"] == 1024**3
    assert cfg.broker_options["max_store_bytes"] == 1024**4


@pytest.mark.parametrize("source", ["config", "environment"])
def test_shm_slot_size_is_ignored_with_one_deprecation_warning(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The removed sizing knob accepts legacy values but is never presented as active."""
    import logging

    options: dict[str, object] = {}
    if source == "config":
        options["shm_slot_size"] = "not-a-size"
    else:
        monkeypatch.setenv("MODULITH_BROKER_SHM_SLOT_SIZE", "not-a-size")

    with caplog.at_level(logging.WARNING, logger="modulith.config"):
        cfg = load_configuration(
            topology="processes",
            broker="shm",
            broker_options=options,
        )

    warnings = [
        record
        for record in caplog.records
        if "shm_slot_size is deprecated and ignored" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert cfg.broker == "shm"


def test_local_broker_artifacts_are_excluded_generically() -> None:
    root = Path(__file__).parents[1]
    patterns = {"*.db-wal", "*.db-shm", "*.db-journal", "*.hints"}
    artifacts = {
        "nested/.modulith-broker.db",
        "nested/.modulith-shm-broker.db",
        "nested/custom.db-wal",
        "nested/custom.db-shm",
        "nested/custom.db-journal",
        "nested/custom.hints",
        "nested/custom.mmap",
        "nested/.custom.hints.123.tmp",
        "nested/.custom.mmap.123.tmp",
    }
    gitignore = set((root / ".gitignore").read_text(encoding="utf-8").splitlines())
    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    sdist_excludes = set(pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]["exclude"])

    assert patterns <= gitignore
    assert {f"**/{pattern}" for pattern in patterns} <= sdist_excludes
    ignored = subprocess.run(
        ["git", "check-ignore", "--no-index", "--stdin"],
        cwd=root,
        input="\n".join(sorted(artifacts)),
        text=True,
        capture_output=True,
        check=True,
    )
    assert set(ignored.stdout.splitlines()) == artifacts
    assert all(
        any(PurePosixPath(artifact).match(pattern) for pattern in sdist_excludes)
        for artifact in artifacts
    )


def test_production_with_explicit_redis_broker_and_processes_is_allowed() -> None:
    """The production guard keys on 'broker not explicit', not on the adapter:
    any explicitly-named broker passes it."""
    cfg = load_configuration(
        topology="processes", production=True, broker="redis-streams", outbox="postgres"
    )
    assert cfg.broker == "redis-streams"


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

    Unrecognized values raise ConfigurationError instead of coercing to
    False — see test_garbage_boolean_env_var_raises.
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


# ----- loud TOML errors, subtable contract ------------------------------------


def test_scalar_and_subtable_outbox_collision_is_loud(tmp_path: Path) -> None:
    """The documented scalar+subtable dup-key collision is LOUD.

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
    """Both broker-options spellings in one file must not silently overwrite
    each other — whichever came last used to win with zero warning."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.broker]\nurl = "redis://a:6379"\n'
        '[tool.modulith.broker_options]\nconsumer_group = "grp"\n'
    )
    with pytest.raises(ConfigurationError, match="broker_options"):
        load_configuration()


def test_misspelled_subtable_raises_did_you_mean(tmp_path: Path) -> None:
    """A typo of a real subtable ([tool.modulith.worker] for workers) must
    raise loudly, not be silently dropped as forward-compat."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith.worker]\ndefault = 1\n")
    with pytest.raises(ConfigurationError, match="workers"):
        load_configuration()


def test_scalar_field_written_as_subtable_raises(tmp_path: Path) -> None:
    """A scalar option written as a table is user error, not forward-compat
    space — reject it loudly."""
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


# ----- dict-typed field validation --------------------------------------------


def test_scalar_outbox_options_in_pyproject_raises(tmp_path: Path) -> None:
    """outbox_options = "string" (forgot the table header) must be rejected
    at load time, not crash later with AttributeError on .get()."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\noutbox_options = "this-should-be-a-table-not-a-string"\n'
    )
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration()


def test_scalar_workers_kwarg_raises() -> None:
    """The dict-typed check also guards explicit kwargs."""
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration(workers=3)


def test_scalar_broker_options_kwarg_raises() -> None:
    """broker_options gets the same dict type check."""
    with pytest.raises(ConfigurationError, match="table"):
        load_configuration(broker_options="redis://x")


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("max_stream_len", 0),
        ("dlq_max_stream_len", True),
        ("poll_block_ms", 0),
        ("reclaim_min_idle_ms", -1),
        ("max_delivery_attempts", 0),
    ],
)
def test_redis_broker_delivery_options_must_be_positive_integers(
    option: str, value: object
) -> None:
    """Safety-critical Redis delivery settings fail before worker startup."""
    with pytest.raises(ConfigurationError, match=option):
        load_configuration(broker="redis-streams", broker_options={option: value})


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("sqlite_synchronous", "OFF"),
        ("completion_mode", "immediate"),
        ("no_subscriber_policy", "ignore"),
        ("orphan_replay_policy", "bogus"),
        ("busy_timeout_ms", 0),
        ("busy_timeout_ms", True),
        ("no_subscriber_wait_timeout_seconds", 0),
        ("no_subscriber_wait_poll_interval_ms", float("nan")),
        ("orphan_retention_seconds", -1),
        ("expected_consumer_groups", ["not", "a", "dict"]),
        ("expected_consumer_groups", {"orders": []}),
        ("expected_consumer_groups", {"": ["group-a"]}),
        ("schema", "bad-name"),
        ("schema", "1leading_digit"),
        ("schema", ""),
        ("schema", "valid_name\n"),
    ],
)
def test_database_broker_options_are_validated_before_adapter_construction(
    option: str, value: object
) -> None:
    """Database broker options fail at config-load time, not at bootstrap
    (parity with the shm/redis brokers' load-time validators)."""
    with pytest.raises(ConfigurationError, match=option):
        load_configuration(broker="database", broker_options={option: value})


def test_database_broker_options_accept_documented_valid_values() -> None:
    cfg = load_configuration(
        broker="database",
        broker_options={
            "sqlite_synchronous": "FULL",
            "completion_mode": "mark",
            "no_subscriber_policy": "wait",
            "orphan_replay_policy": "first_groups",
            "busy_timeout_ms": 5000,
            "no_subscriber_wait_timeout_seconds": 30.0,
            "no_subscriber_wait_poll_interval_ms": 100.0,
            "orphan_retention_seconds": 86400.0,
            "expected_consumer_groups": {"orders": ["billing", "shipping"]},
            "schema": "mod_test",
        },
    )

    assert cfg.broker == "database"
    assert cfg.broker_options["completion_mode"] == "mark"
    assert cfg.broker_options["schema"] == "mod_test"


# ----- env var handling -------------------------------------------------------


def test_empty_string_env_vars_are_documented_as_unset(monkeypatch) -> None:
    """MODULITH_X="" (e.g. from a CI template with a missing source
    variable) is treated as unset by documented contract — the field keeps
    its default and is NOT marked explicit."""
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
    """MODULITH_PRODUCTION="" = unset (documented)."""
    monkeypatch.setenv("MODULITH_PRODUCTION", "")
    cfg = load_configuration()
    assert cfg.production is False
    assert not cfg.is_explicit("production")


def test_garbage_boolean_env_var_raises(monkeypatch) -> None:
    """A non-empty unrecognized boolean value must raise ConfigurationError,
    not silently coerce to False — a typo like 'ture' would otherwise flip
    the production safety check off."""
    monkeypatch.setenv("MODULITH_PRODUCTION", "ture")
    with pytest.raises(ConfigurationError, match="MODULITH_PRODUCTION"):
        load_configuration()


def test_garbage_auto_discover_env_var_raises(monkeypatch) -> None:
    """Strict boolean parsing applies to every boolean env var."""
    monkeypatch.setenv("MODULITH_AUTO_DISCOVER", "banana")
    with pytest.raises(ConfigurationError, match="MODULITH_AUTO_DISCOVER"):
        load_configuration()


def test_boolean_env_var_accepts_explicit_false_words(monkeypatch) -> None:
    """0/false/no parse to explicit False."""
    for value in ("0", "no", "FALSE", " false "):
        monkeypatch.setenv("MODULITH_PRODUCTION", value)
        cfg = load_configuration()
        assert cfg.production is False, f"expected False for {value!r}"
        assert cfg.is_explicit("production"), f"expected explicit for {value!r}"


# ----- unshipped subinterpreters topology -------------------------------------


def test_subinterpreters_topology_is_rejected_as_unimplemented() -> None:
    """SPEC/ROADMAP declare subinterpreters unshipped, but config
    resolution accepted it and the CLI routed it through the real process
    supervisor. It must fail loudly even with a valid cross-process broker."""
    with pytest.raises(ConfigurationError, match="not yet implemented"):
        load_configuration(topology="subinterpreters", broker="redis-streams")


# ----- Validated public configuration contracts ------------------------------


def test_reads_subscription_and_actuator_configuration(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith]\nsubscription_source = "config"\nactuator_mode = "token"\n'
        "[tool.modulith.subscriptions]\n"
        'orders = ["redis-streams:orders", "amqp:events:orders"]\n'
        "[tool.modulith.broker_options]\n"
        'future_broker_key = "preserved"\n'
        "[tool.modulith.outbox_options]\n"
        "future_outbox_key = 42\n"
    )

    cfg = load_configuration()

    assert cfg.subscription_source == "config"
    assert cfg.subscriptions == {"orders": ["redis-streams:orders", "amqp:events:orders"]}
    assert cfg.actuator_mode == "token"
    assert cfg.broker_options == {"future_broker_key": "preserved"}
    assert cfg.outbox_options == {"future_outbox_key": 42}


def test_subscription_and_actuator_environment_variables(monkeypatch) -> None:
    monkeypatch.setenv("MODULITH_SUBSCRIPTION_SOURCE", "listener")
    monkeypatch.setenv("MODULITH_ACTUATOR_MODE", "disabled")

    cfg = load_configuration()

    assert cfg.subscription_source == "listener"
    assert cfg.actuator_mode == "disabled"


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("subscription_source", "database"),
        ("actuator_mode", "private"),
    ],
)
def test_enum_configuration_rejects_unknown_values(field_name: str, value: str) -> None:
    with pytest.raises(ConfigurationError, match=field_name):
        load_configuration(**{field_name: value})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("package", 1),
        ("contracts_module", False),
        ("outbox", 1),
        ("topology", True),
        ("broker", False),
        ("subscription_source", False),
        ("actuator_mode", 1),
        ("auto_discover", 1),
        ("production", 0),
        ("observability", "auto"),
        ("verify_manifests", 1),
    ],
)
def test_scalar_configuration_requires_declared_types(field_name: str, value: object) -> None:
    with pytest.raises(ConfigurationError, match=field_name):
        load_configuration(**{field_name: value})


@pytest.mark.parametrize(
    ("toml_text", "expected"),
    [
        ('tool = "not-a-table"\n', "tool"),
        ('[tool]\nmodulith = "not-a-table"\n', "tool.modulith"),
    ],
)
def test_pyproject_parent_sections_must_be_tables(
    tmp_path: Path, toml_text: str, expected: str
) -> None:
    (tmp_path / "pyproject.toml").write_text(toml_text)

    with pytest.raises(ConfigurationError, match=expected):
        load_configuration()


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("outbox_options", []),
        ("broker_options", "redis://cache"),
        ("workers", []),
        ("subscriptions", []),
    ],
)
def test_table_configuration_requires_mappings(field_name: str, value: object) -> None:
    with pytest.raises(ConfigurationError, match=field_name):
        load_configuration(**{field_name: value})


@pytest.mark.parametrize(
    "workers",
    [
        {1: 1},
        {"orders": 0},
        {"orders": -1},
        {"orders": 1.5},
        {"orders": True},
    ],
)
def test_workers_require_string_keys_and_positive_integer_counts(
    workers: dict[object, object],
) -> None:
    with pytest.raises(ConfigurationError, match="workers"):
        load_configuration(workers=workers)


@pytest.mark.parametrize(
    "subscriptions",
    [
        {1: ["redis-streams:orders"]},
        {"orders": ("redis-streams:orders",)},
        {"orders": [1]},
        {"orders": [""]},
        {"orders": ["redis-streams"]},
        {"orders": [":orders"]},
        {"orders": ["redis-streams:"]},
    ],
)
def test_subscriptions_require_string_keys_and_broker_target_lists(
    subscriptions: dict[object, object],
) -> None:
    with pytest.raises(ConfigurationError, match="subscriptions"):
        load_configuration(subscriptions=subscriptions)


def test_scalar_typo_includes_suggestion() -> None:
    with pytest.raises(ConfigurationError, match="subscription_source"):
        load_configuration(subscription_sorce="manifest")


def test_subtable_typo_includes_suggestion(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.subscription]\norders = ["redis-streams:orders"]\n'
    )

    with pytest.raises(ConfigurationError, match="subscriptions"):
        load_configuration()


def test_subtable_typo_of_scalar_includes_suggestion(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.modulith.actuator_mod]\nvalue = "disabled"\n')

    with pytest.raises(ConfigurationError, match="actuator_mode"):
        load_configuration()


# ----- outbox_options claim-strategy validation -------------------------------


def test_pyproject_outbox_options_claim_defaults(tmp_path: Path) -> None:
    """An empty (or claim-silent) [tool.modulith.outbox_options] is valid —
    claim_strategy/claim_lease_seconds/claim_batch_size are optional; the
    runtime default (lease / 60.0 / 100) applies where they consume it."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith.outbox_options]\n")
    cfg = load_configuration()
    assert cfg.outbox_options == {}


@pytest.mark.parametrize("strategy", ["lease", "advisory_lock", "none"])
def test_pyproject_outbox_options_accepts_valid_claim_strategy(
    tmp_path: Path, strategy: str
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        f'[tool.modulith.outbox_options]\nclaim_strategy = "{strategy}"\n'
    )
    cfg = load_configuration()
    assert cfg.outbox_options == {"claim_strategy": strategy}


def test_pyproject_outbox_options_rejects_invalid_claim_strategy(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.modulith.outbox_options]\nclaim_strategy = "optimistic"\n'
    )
    with pytest.raises(ConfigurationError, match="claim_strategy"):
        load_configuration()


@pytest.mark.parametrize("value", [0, -1.0, float("inf"), float("nan"), "60", True])
def test_pyproject_outbox_options_rejects_invalid_claim_lease_seconds(
    tmp_path: Path, value: object
) -> None:
    (tmp_path / "pyproject.toml").write_text("[tool.modulith.outbox_options]\n")
    with pytest.raises(ConfigurationError, match="claim_lease_seconds"):
        load_configuration(outbox_options={"claim_lease_seconds": value})


def test_pyproject_outbox_options_accepts_valid_claim_lease_seconds() -> None:
    cfg = load_configuration(outbox_options={"claim_lease_seconds": 45.5})
    assert cfg.outbox_options == {"claim_lease_seconds": 45.5}


@pytest.mark.parametrize("value", [0, -1, 1.5, "100", True])
def test_pyproject_outbox_options_rejects_invalid_claim_batch_size(value: object) -> None:
    with pytest.raises(ConfigurationError, match="claim_batch_size"):
        load_configuration(outbox_options={"claim_batch_size": value})


def test_pyproject_outbox_options_accepts_valid_claim_batch_size() -> None:
    cfg = load_configuration(outbox_options={"claim_batch_size": 250})
    assert cfg.outbox_options == {"claim_batch_size": 250}


# ----- strict_boundaries (boundary enforcement mode) --------------------------


def test_strict_boundaries_defaults_to_false() -> None:
    """strict_boundaries defaults to False (non-fatal warnings)."""
    cfg = load_configuration()
    assert cfg.strict_boundaries is False
    assert not cfg.is_explicit("strict_boundaries")


def test_strict_boundaries_via_pyproject(tmp_path: Path) -> None:
    """strict_boundaries can be set via [tool.modulith].strict_boundaries."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith]\nstrict_boundaries = true\n")
    cfg = load_configuration()
    assert cfg.strict_boundaries is True
    assert cfg.is_explicit("strict_boundaries")


def test_strict_boundaries_via_env_var(monkeypatch) -> None:
    """strict_boundaries can be set via MODULITH_STRICT_BOUNDARIES env var."""
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "true")
    cfg = load_configuration()
    assert cfg.strict_boundaries is True
    assert cfg.is_explicit("strict_boundaries")


def test_strict_boundaries_via_env_var_false(monkeypatch) -> None:
    """MODULITH_STRICT_BOUNDARIES=false sets it to False."""
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "false")
    cfg = load_configuration()
    assert cfg.strict_boundaries is False
    assert cfg.is_explicit("strict_boundaries")


def test_strict_boundaries_env_overrides_pyproject(tmp_path: Path, monkeypatch) -> None:
    """MODULITH_STRICT_BOUNDARIES env var overrides pyproject setting."""
    (tmp_path / "pyproject.toml").write_text("[tool.modulith]\nstrict_boundaries = false\n")
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "true")
    cfg = load_configuration()
    assert cfg.strict_boundaries is True


def test_strict_boundaries_explicit_override_wins(monkeypatch) -> None:
    """Explicit override to load_configuration() beats env and pyproject."""
    monkeypatch.setenv("MODULITH_STRICT_BOUNDARIES", "false")
    cfg = load_configuration(strict_boundaries=True)
    assert cfg.strict_boundaries is True
