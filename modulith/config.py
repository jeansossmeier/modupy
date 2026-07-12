"""Configuration loading and validation.

Resolution order (highest priority first):
  1. Explicit kwargs to load_configuration() / configure()
  2. MODULITH_* environment variables
  3. [tool.modulith] section in pyproject.toml
  4. Hardcoded defaults

Every setting has a sensible default, so a project with no [tool.modulith]
section and no env vars runs perfectly. Configuration is only needed when
the defaults guess wrong — and the warnings on startup tell users exactly
which defaults are active.
"""

from __future__ import annotations

import difflib
import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any


class ConfigurationError(Exception):
    """Raised when configuration is invalid.

    Always carries an actionable message — what went wrong and how to fix it.
    """


# Valid values for enum-like fields. Adapter names beyond the built-in set
# are accepted; we only validate against this list when no entry-point
# adapter is registered for the given name (checked at bootstrap, not here).
# "subinterpreters" is a *reserved* spelling: it parses as a known topology
# so the error message can say "not yet implemented" instead of the generic
# "invalid topology", but resolution rejects it (see _validate).
_VALID_TOPOLOGIES = ("single", "processes", "subinterpreters")


@dataclass(frozen=True)
class Configuration:
    """The runtime configuration for a modulith application.

    Frozen because configuration is locked once bootstrap completes.
    Mutating it after the runtime starts would create inconsistent state.
    """

    # The application's root Python package. None means "auto-detect".
    package: str | None = None

    # Name of the shared-contracts subpackage — where cross-module event types
    # live. A convention ("contracts") with a sensible default, not a hardcode:
    # override it if your app names that module differently.
    contracts_module: str = "contracts"

    # Outbox storage. "memory" = no persistence (default for dev).
    # Other values name a registered adapter (e.g. "postgres", "mongodb").
    outbox: str = "memory"

    # Process topology. "single" = one process, all modules in-memory bus.
    # "processes" = one subprocess per module, broker-based IPC.
    topology: str = "single"

    # Default broker for cross-process events. "memory" only valid when
    # topology == "single"; other values name an adapter.
    broker: str = "memory"

    # Whether to walk subpackages on bootstrap to discover modules.
    # Disable only if you want to register modules programmatically.
    auto_discover: bool = True

    # Production mode flips a few safety checks on (e.g. refuse memory
    # outbox unless explicitly opted in). Detected via env var by default.
    production: bool = False

    # Sub-config dicts read only when their feature is enabled.
    # Keeping them here means the dataclass remains the single source of
    # truth — no scattered globals or hidden config files.
    outbox_options: dict[str, Any] = field(default_factory=dict)
    # Broker connection settings from the [tool.modulith.broker] subtable
    # (url, consumer_group, stream_prefix, max_stream_len). Consumed by the
    # selected broker adapter's registration hook; env vars still override.
    broker_options: dict[str, Any] = field(default_factory=dict)
    workers: dict[str, int] = field(default_factory=dict)

    # None = auto-detect from installed packages. True/False = force.
    observability: bool | None = None

    # Run manifest verification during bootstrap. Disable for gradual adoption.
    verify_manifests: bool = True

    # Tracks which keys were explicitly set vs got their default value.
    # Used by safety checks (e.g. "production + default outbox = error").
    explicit_keys: frozenset[str] = field(default_factory=frozenset)

    def is_explicit(self, key: str) -> bool:
        """True if the user explicitly set this key (vs accepting default)."""
        return key in self.explicit_keys


def load_configuration(**overrides: Any) -> Configuration:
    """Build a Configuration from all available sources.

    Reads pyproject.toml, layers env vars on top, then applies the
    caller's explicit overrides. Validates the result and raises
    ConfigurationError with a helpful message on any problem.
    """
    # Collect explicitly-set values from every source.
    explicit: dict[str, Any] = {}
    explicit.update(_read_pyproject())
    explicit.update(_read_env_vars())
    explicit.update(overrides)

    # Validate before constructing — fail fast on typos and bad values.
    _validate(explicit)

    # Build the dataclass. Keys absent from `explicit` get the default.
    return Configuration(**explicit, explicit_keys=frozenset(explicit.keys()))


# Configuration fields that hold tables of options rather than scalars.
# They have no MODULITH_* env var (see _read_env_vars) and get a dedicated
# type check in _validate.
_DICT_FIELDS = frozenset({"outbox_options", "broker_options", "workers"})

# Mapping of subtable name -> Configuration dict-field name. The canonical
# spellings are the field names themselves; "broker" is kept as an alias for
# broker_options because TOML forbids the scalar broker *name* and a broker
# subtable sharing one key, so a file using only the subtable form is
# unambiguous. The legacy "outbox" subtable is NOT an alias — it is rejected
# loudly (see _read_pyproject).
_SUBTABLE_FIELD = {
    "outbox_options": "outbox_options",
    "broker": "broker_options",
    "broker_options": "broker_options",
    "workers": "workers",
}

# Subtables reserved for future modulith versions. These are the ONLY
# subtable names that are dropped silently — a documented no-op so a
# future-compatible pyproject still bootstraps on today's modulith.
_RESERVED_SUBTABLES = frozenset({"verify"})


def _read_pyproject() -> dict[str, Any]:
    """Read [tool.modulith] from pyproject.toml. Returns {} if absent.

    The documented pyproject.toml convention uses subtables for compound
    options (``[tool.modulith.outbox_options]`` for outbox tuning,
    ``[tool.modulith.broker_options]`` for broker connection settings,
    ``[tool.modulith.workers]`` for per-module worker counts). TOML parses
    these as nested dicts under the ``modulith`` key. We separate scalar
    keys (which map directly to Configuration fields) from subtables
    (which map to dict-typed Configuration fields like outbox_options,
    broker_options and workers) so users can write idiomatic TOML without
    hitting the "unknown config key" guard.

    Loud-error contract (user decision, W2):
      * A pyproject.toml that fails to parse — including the duplicate-key
        collision TOML raises when one key is written as both a scalar and
        a table — raises ConfigurationError. Silently skipping it would
        discard the entire [tool.modulith] table, reverting every setting
        (including ``production = true``) to defaults with zero warning.
      * ``[tool.modulith.outbox_options]`` is the ONLY outbox options
        subtable. The legacy ``[tool.modulith.outbox]`` subtable raises,
        pointing at the new spelling ("outbox" is the scalar adapter name).
      * Two subtables configuring the same field (``broker`` and
        ``broker_options``) raise instead of silently overwriting.
      * A subtable spelled close to a real one (e.g. ``worker`` for
        ``workers``) raises with a "did you mean" hint; a subtable naming
        a scalar option raises too.

    Only genuinely unknown subtables — plus the reserved ``verify`` table,
    which Phase 1 will add — are dropped silently: the bootstrap must
    succeed against a future-compatible pyproject even if the running
    modulith version doesn't yet recognize every option.
    """
    path = _find_pyproject()
    if path is None:
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(
            f"failed to parse {path}: {exc}. Note that TOML forbids one key "
            'being both a scalar and a table — e.g. `outbox = "postgres"` '
            "cannot coexist with a `[tool.modulith.outbox]` subtable "
            "(outbox tuning belongs in [tool.modulith.outbox_options])."
        ) from exc
    except OSError as exc:
        raise ConfigurationError(
            f"could not read {path}: {exc}. Refusing to continue with default "
            "configuration — the unreadable file may declare settings such as "
            "production = true."
        ) from exc

    raw = data.get("tool", {}).get("modulith", {})
    if not raw:
        return {}

    result: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            # Scalar: pass through. _validate catches unknown scalar keys.
            result[key] = value
            continue
        field_name = _resolve_subtable(key, already_mapped=set(result))
        if field_name is not None:
            result[field_name] = value
    return result


def _resolve_subtable(key: str, *, already_mapped: set[str]) -> str | None:
    """Map one [tool.modulith.<key>] subtable to its Configuration field.

    Returns the dict-typed field name to assign, None for the documented
    silent drops (reserved + genuinely unknown subtables), and raises
    ConfigurationError for everything a user plausibly meant but got wrong
    (legacy ``outbox`` spelling, colliding spellings, a scalar option
    written as a table, a typo of a real subtable).
    """
    if key == "outbox":
        raise ConfigurationError(
            "[tool.modulith.outbox] is not a valid subtable: `outbox` is "
            'the scalar adapter name (e.g. outbox = "postgres"). Put '
            "outbox tuning in [tool.modulith.outbox_options] instead."
        )

    field_name = _SUBTABLE_FIELD.get(key)
    if field_name is not None:
        if field_name in already_mapped:
            raise ConfigurationError(
                f"[tool.modulith.{key}] collides with another subtable that "
                f"also configures {field_name!r} — use only "
                f"[tool.modulith.{field_name}]."
            )
        return field_name

    if key in _RESERVED_SUBTABLES:
        # Reserved for a future modulith version — documented no-op.
        return None

    scalar_fields = {f.name for f in fields(Configuration) if f.name not in _DICT_FIELDS} - {
        "explicit_keys"
    }
    if key in scalar_fields:
        raise ConfigurationError(
            f"[tool.modulith.{key}] is not a table: {key!r} is a scalar "
            f"option — write it as `{key} = ...` under [tool.modulith]."
        )

    # A typo of a real subtable must fail as loudly as a typo'd scalar key
    # does; only genuinely unknown names stay silent forward-compat space.
    candidates = sorted(set(_SUBTABLE_FIELD) | _RESERVED_SUBTABLES)
    close = difflib.get_close_matches(key, candidates, n=1)
    if close:
        raise ConfigurationError(
            f"Unknown config subtable [tool.modulith.{key}] — did you mean "
            f"[tool.modulith.{close[0]}]? Known subtables: {candidates}"
        )
    return None


def _find_pyproject() -> Path | None:
    """Walk up from cwd looking for pyproject.toml. Returns first match."""
    current = Path.cwd()
    for parent in (current, *current.parents):
        candidate = parent / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


# Accepted spellings for boolean env vars (case-insensitive, stripped).
_ENV_TRUE = ("1", "true", "yes")
_ENV_FALSE = ("0", "false", "no")


def _env_str(name: str) -> str | None:
    """Return the env var's value, or None when it is unset OR empty.

    Empty-string-is-unset is a documented contract, not an accident of
    truthiness: templated deployments commonly render ``MODULITH_X=""``
    when the source variable is missing (e.g. ``MODULITH_PRODUCTION=
    ${DEPLOY_ENV_IS_PROD}`` with the interpolation variable unset), and
    honoring "" as an explicit value would inject nonsense config such as
    ``package=""``.
    """
    val = os.environ.get(name)
    if val is None or val == "":
        return None
    return val


def _env_bool(name: str) -> bool | None:
    """Strictly parse a boolean env var; None means unset (or empty).

    Accepts 1/true/yes for True and 0/false/no for False, case-insensitive
    with surrounding whitespace stripped. Any other non-empty value raises
    ConfigurationError: silently coercing garbage (e.g. a typo'd "ture") to
    False would flip safety-relevant settings like MODULITH_PRODUCTION off
    without a trace.
    """
    raw = _env_str(name)
    if raw is None:
        return None
    val = raw.strip().lower()
    if val == "":
        return None  # whitespace-only counts as unset, same as empty
    if val in _ENV_TRUE:
        return True
    if val in _ENV_FALSE:
        return False
    raise ConfigurationError(
        f"{name}={raw!r} is not a valid boolean. Use one of "
        f"{'/'.join(_ENV_TRUE)} for true or {'/'.join(_ENV_FALSE)} for "
        "false, or unset the variable."
    )


def _read_env_vars() -> dict[str, Any]:
    """Read MODULITH_* environment variables.

    Standard 12-factor pattern: every *scalar* Configuration field has a
    matching env var. A variable set to the empty string is treated as
    unset (see _env_str). Booleans are parsed strictly: 1/true/yes and
    0/false/no (case-insensitive, stripped); any other non-empty value
    raises ConfigurationError (see _env_bool). The dict-typed fields
    (outbox_options, broker_options, workers) have no env var — they come
    from the [tool.modulith.*] subtables in pyproject.toml.
    """
    result: dict[str, Any] = {}
    string_vars = (
        ("MODULITH_PACKAGE", "package"),
        ("MODULITH_CONTRACTS_MODULE", "contracts_module"),
        ("MODULITH_OUTBOX", "outbox"),
        ("MODULITH_TOPOLOGY", "topology"),
        ("MODULITH_BROKER", "broker"),
    )
    for env_name, field_name in string_vars:
        if (val := _env_str(env_name)) is not None:
            result[field_name] = val
    bool_vars = (
        ("MODULITH_PRODUCTION", "production"),
        ("MODULITH_AUTO_DISCOVER", "auto_discover"),
        ("MODULITH_OBSERVABILITY", "observability"),
        ("MODULITH_VERIFY_MANIFESTS", "verify_manifests"),
    )
    for env_name, field_name in bool_vars:
        if (flag := _env_bool(env_name)) is not None:
            result[field_name] = flag
    return result


def _validate(data: dict[str, Any]) -> None:
    """Validate config values, raising ConfigurationError with guidance."""
    # Catch typos — unknown keys would silently fail otherwise.
    known = {f.name for f in fields(Configuration)} - {"explicit_keys"}
    unknown = set(data.keys()) - known
    if unknown:
        raise ConfigurationError(
            f"Unknown config keys: {sorted(unknown)}. Valid keys: {sorted(known)}"
        )

    # Dict-typed fields must actually be tables. A scalar here is a natural
    # typo (forgetting the [tool.modulith.outbox_options] table header) that
    # would otherwise pass validation and crash much later with an unrelated
    # AttributeError when the value is treated as a dict.
    for dict_field in sorted(_DICT_FIELDS):
        if dict_field in data and not isinstance(data[dict_field], dict):
            raise ConfigurationError(
                f"{dict_field} must be a table of options (e.g. "
                f"[tool.modulith.{dict_field}] in pyproject.toml), got "
                f"{type(data[dict_field]).__name__}: {data[dict_field]!r}"
            )

    # Topology must be one of the known modes.
    if "topology" in data and data["topology"] not in _VALID_TOPOLOGIES:
        raise ConfigurationError(
            f"invalid topology {data['topology']!r}; "
            f"expected one of: {', '.join(_VALID_TOPOLOGIES)}"
        )

    # Cross-field: a multi-process topology needs a real cross-process broker.
    # The in-memory broker can't carry events between processes, so
    # "topology=processes, broker=memory" would validate clean and then
    # silently no-op delivery deep in the runtime. Fail fast with the
    # documented constraint (config.py field docstring on `broker`). Effective
    # values include defaults — a process topology left on the default memory
    # broker is exactly the misconfiguration this guards.
    effective_topology = data.get("topology", "single")
    effective_broker = data.get("broker", "memory")
    if effective_topology in ("processes", "subinterpreters") and effective_broker == "memory":
        raise ConfigurationError(
            f"topology={effective_topology!r} requires a cross-process broker, but "
            "broker is the in-memory default. Set [tool.modulith].broker to a real "
            "adapter (e.g. 'redis-streams'); broker='memory' is only valid for "
            "topology='single'."
        )

    # "subinterpreters" is reserved but unshipped (SPEC §9.5 option C;
    # ROADMAP Phase 4). Accepting it would route the app through the real
    # multi-process supervisor as if it were "processes" — silently the
    # wrong isolation model. Checked AFTER the cross-process-broker guard so
    # the broker misconfiguration keeps its established error message.
    if effective_topology == "subinterpreters":
        raise ConfigurationError(
            "topology='subinterpreters' is not yet implemented (reserved for "
            "a future release — see ROADMAP Phase 4). Use 'single' or "
            "'processes'."
        )

    # Production mode + default memory outbox = silent data loss on restart.
    # Refuse to start unless the user explicitly opted in.
    if data.get("production") and "outbox" not in data:
        raise ConfigurationError(
            "Cannot start in production mode with the default in-memory "
            "outbox — events would be lost on restart. Either set "
            "[tool.modulith].outbox = 'postgres' (or another durable store), "
            "or explicitly opt in by setting outbox = 'memory'."
        )
