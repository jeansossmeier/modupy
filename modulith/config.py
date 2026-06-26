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
_VALID_TOPOLOGIES = ("single", "processes", "subinterpreters")


@dataclass(frozen=True)
class Configuration:
    """The runtime configuration for a modulith application.

    Frozen because configuration is locked once bootstrap completes.
    Mutating it after the runtime starts would create inconsistent state.
    """

    # The application's root Python package. None means "auto-detect".
    package: str | None = None

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


def _read_pyproject() -> dict[str, Any]:
    """Read [tool.modulith] from pyproject.toml. Returns {} if absent.

    The documented pyproject.toml convention uses subtables for compound
    options (e.g. ``[tool.modulith.outbox]`` for outbox completion mode,
    ``[tool.modulith.workers]`` for per-module worker counts). TOML parses
    these as nested dicts under the ``modulith`` key. We separate scalar
    keys (which map directly to Configuration fields) from subtables
    (which map to dict-typed Configuration fields like outbox_options
    and workers) so users can write idiomatic TOML without hitting the
    "unknown config key" guard.

    Subtables not yet backed by a Configuration field (currently
    ``verify``, which Phase 1 will add) are dropped silently — the
    bootstrap must succeed against a future-compatible pyproject even
    if the running modulith version doesn't yet recognize every option.
    """
    path = _find_pyproject()
    if path is None:
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        # A broken pyproject.toml is the build tool's problem to surface.
        # We just skip it and use defaults.
        return {}

    raw = data.get("tool", {}).get("modulith", {})
    if not raw:
        return {}

    # Mapping of subtable name -> Configuration dict-field name. Subtables
    # not in this mapping are forward-compatibility space and dropped.
    SUBTABLE_FIELD = {
        "outbox": "outbox_options",
        "workers": "workers",
    }

    result: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            # Subtable: map to its dict-typed field if known, else drop.
            field_name = SUBTABLE_FIELD.get(key)
            if field_name is not None:
                result[field_name] = value
            # Unknown subtables (e.g. [tool.modulith.verify]) are dropped
            # silently for forward compatibility.
        else:
            # Scalar: pass through. _validate catches unknown scalar keys.
            result[key] = value
    return result


def _find_pyproject() -> Path | None:
    """Walk up from cwd looking for pyproject.toml. Returns first match."""
    current = Path.cwd()
    for parent in (current, *current.parents):
        candidate = parent / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


def _read_env_vars() -> dict[str, Any]:
    """Read MODULITH_* environment variables.

    Standard 12-factor pattern: every Configuration field has a matching
    env var. Booleans accept 1/true/yes (case-insensitive) for True.
    """
    result: dict[str, Any] = {}
    if val := os.environ.get("MODULITH_PACKAGE"):
        result["package"] = val
    if val := os.environ.get("MODULITH_OUTBOX"):
        result["outbox"] = val
    if val := os.environ.get("MODULITH_TOPOLOGY"):
        result["topology"] = val
    if val := os.environ.get("MODULITH_BROKER"):
        result["broker"] = val
    if val := os.environ.get("MODULITH_PRODUCTION"):
        result["production"] = val.lower() in ("1", "true", "yes")
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

    # Topology must be one of the known modes.
    if "topology" in data and data["topology"] not in _VALID_TOPOLOGIES:
        raise ConfigurationError(
            f"invalid topology {data['topology']!r}; "
            f"expected one of: {', '.join(_VALID_TOPOLOGIES)}"
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
