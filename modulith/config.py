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
import keyword
import logging
import math
import os
import re
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ._claims import VALID_CLAIM_STRATEGIES
from .brokers import _split_broker_target

logger = logging.getLogger(__name__)

_SHM_SYNCHRONOUS_MODES = frozenset({"NORMAL", "FULL"})
_SHM_COMPLETION_MODES = frozenset({"delete", "mark"})
_SHM_MAX_HINT_CAPACITY = 1_000_000
_SHM_MAX_CLAIM_BATCH_SIZE = 10_000
_SHM_MAX_DISPATCH_CONCURRENCY = 1_000
_SHM_MAX_DELIVERY_ATTEMPTS = 10_000
DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024**2
DEFAULT_SHM_MAX_STORE_BYTES = 1024**3
# Shorter than the database broker's 24 h: every SHM publication, acked or not,
# holds space in the max_store_bytes-bounded store for this long.
DEFAULT_SHM_ORPHAN_RETENTION_SECONDS = 3600.0
MAX_PAYLOAD_BYTES = 1024**3
# SQLite rejects a blob over its default SQLITE_LIMIT_LENGTH with a raw
# DataError, so the SHM store cannot hold a larger payload than this.
_SHM_MAX_PAYLOAD_BYTES = 1_000_000_000
_SHM_MAX_STORE_BYTES = 1024**4
_MAX_PAYLOAD_BYTES_ENV = "MODULITH_BROKER_MAX_PAYLOAD_BYTES"
# One day. claim_batch adds the lease to the clock, and a lease near the datetime
# range raises OverflowError on every sweep. outbox.configure() applies the same bound.
MAX_CLAIM_LEASE_SECONDS = 86_400


class ConfigurationError(Exception):
    """Raised when configuration is invalid.

    Always carries an actionable message — what went wrong and how to fix it.
    """


def _validate_contracts_module(value: object) -> str:
    """Validate and return an importable dotted module name."""
    if (
        type(value) is not str
        or not value
        or any(not part.isidentifier() or keyword.iskeyword(part) for part in value.split("."))
    ):
        raise ConfigurationError(
            "contracts_module must be a non-empty dot-separated Python module name "
            f"without keywords, got {value!r}"
        )
    return value


# Valid values for enum-like fields. Adapter names beyond the built-in set
# are accepted; we only validate against this list when no entry-point
# adapter is registered for the given name (checked at bootstrap, not here).
# "subinterpreters" is a *reserved* spelling: it parses as a known topology
# so the error message can say "not yet implemented" instead of the generic
# "invalid topology", but resolution rejects it (see _validate).
_VALID_TOPOLOGIES = ("single", "processes", "subinterpreters")
_VALID_SUBSCRIPTION_SOURCES = ("manifest", "config", "listener")
_VALID_ACTUATOR_MODES = ("auto", "token", "open", "disabled")

# Filenames used inside the package-namespaced per-user state directory.
# They remain centralized so configuration warnings and adapters agree.
DEFAULT_BROKER_DB_FILENAME = ".modulith-broker.db"
DEFAULT_SHM_BROKER_DB_FILENAME = ".modulith-shm-broker.db"


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

    # Async SQLAlchemy URL of the database holding the outbox table (the
    # business data's database, so a row commits atomically with it). With a
    # durable outbox, bootstrap builds and binds a PostgresPublicationStore on
    # it in every process unless the application already bound a store.
    outbox_url: str | None = None

    # Process topology. "single" = one process, all modules in-memory bus.
    # "processes" = one subprocess per module, broker-based IPC.
    topology: str = "single"

    # Default broker for cross-process events. "memory" only valid when
    # topology == "single". When topology == "processes" and this is absent,
    # load_configuration defaults to durable local SHM, or to database when
    # an effective URL/DSN preserves a legacy database-broker configuration.
    broker: str = "memory"

    # Source used to resolve cross-process listener subscriptions.
    subscription_source: str = "manifest"

    # Access policy for the optional actuator endpoints.
    actuator_mode: str = "auto"

    # First loopback port of the process topology's workers; each module takes
    # as many consecutive ports as it has replicas.
    worker_port_base: int = 9001

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
    # (url, stream_prefix, max_stream_len). Consumed by the
    # selected broker adapter's registration hook; env vars still override.
    broker_options: dict[str, Any] = field(default_factory=dict)
    workers: dict[str, int] = field(default_factory=dict)
    subscriptions: dict[str, list[str]] = field(default_factory=dict)

    # None = auto-detect from installed packages. True/False = force.
    observability: bool | None = None

    # Run manifest verification during bootstrap. Disable for gradual adoption.
    verify_manifests: bool = True

    # Enforce boundary violations at bootstrap as fatal errors.
    # When False (default), violations generate warnings only. When True,
    # the startup fails immediately, catching boundary breaches before
    # the application starts.
    strict_boundaries: bool = False

    # Rule names skipped by the built-in verifier, from
    # [tool.modulith.verify].disabled_rules. Read via getattr with a default
    # by modulith/builtin/verifier.py, so consumers written before this field
    # existed keep working unchanged.
    verify_disabled_rules: tuple[str, ...] = ()

    # Tracks which keys were explicitly set vs got their default value.
    # Used by safety checks (e.g. "production + default outbox = error").
    explicit_keys: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        _validate_contracts_module(self.contracts_module)

    def is_explicit(self, key: str) -> bool:
        """True if the user explicitly set this key (vs accepting default)."""
        return key in self.explicit_keys


# Broker defaults already announced in this process. One boot resolves the
# same configuration more than once — the CLI resolves it to place the topology
# and Runtime.ensure_bootstrapped() resolves it again — so announcing on every
# call printed one decision repeatedly before the first worker started.
# The key is the decision, not the call, so a reload that lands somewhere
# different is still announced.
_announced_broker_defaults: set[str] = set()


def _announce_broker_default(decision: str, message: str, *args: object) -> None:
    """Warn about an auto-selected broker the first time it is selected."""
    if decision in _announced_broker_defaults:
        return
    _announced_broker_defaults.add(decision)
    logger.warning(message, *args)


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
    if isinstance(explicit.get("broker"), str):
        explicit["broker"] = explicit["broker"].strip()

    # Validate before constructing — fail fast on typos and bad values.
    _validate(explicit)
    if "verify_disabled_rules" in explicit:
        explicit["verify_disabled_rules"] = tuple(explicit["verify_disabled_rules"])

    # Freeze explicit_keys BEFORE injecting the broker default so
    # cfg.is_explicit("broker") stays False for the auto-default case.
    explicit_keys = frozenset(explicit.keys())
    if explicit.get("topology") == "processes" and "broker" not in explicit:
        configured_url = _configured_broker_url(explicit.get("broker_options"))
        explicit["broker"] = "database" if configured_url is not None else "shm"
        if configured_url is not None:
            redacted = _redact_broker_url(configured_url)
            _announce_broker_default(
                f"database:{redacted}",
                "topology='processes' with a broker URL but no broker "
                "name \u2014 inferring the 'database' adapter for %s.",
                redacted,
            )
        else:
            _announce_broker_default(
                "shm",
                "topology='processes' with no broker configured \u2014 defaulting to "
                "the durable SHM broker with embedded SQLite (file '%s' in a "
                "private per-user state directory). Set "
                "[tool.modulith].broker explicitly to silence this warning.",
                DEFAULT_SHM_BROKER_DB_FILENAME,
            )
    return Configuration(**explicit, explicit_keys=explicit_keys)


def _redact_broker_url(url: object) -> str:
    """Return a parsed, fail-closed rendering that never exposes credentials."""
    if not isinstance(url, str):
        return f"<{type(url).__name__}>"
    if any(ord(character) < 32 for character in url):
        return "<redacted broker URL>"
    try:
        parsed = urlsplit(url)
        if not parsed.scheme or "://" not in url:
            return "<redacted broker URL>"
        # These properties perform urllib's bracket and port validation.
        _ = parsed.hostname, parsed.port
        netloc = parsed.netloc
        if netloc.count("@") > 1:
            return "<redacted broker URL>"
        if "@" in netloc:
            _userinfo, host = netloc.rsplit("@", 1)
            if not host:
                return "<redacted broker URL>"
            netloc = f"***@{host}"

        query = [(key, "***") for key, _value in parse_qsl(parsed.query, keep_blank_values=True)]
        fragment = "***" if parsed.fragment else ""
        return urlunsplit((parsed.scheme, netloc, parsed.path, urlencode(query), fragment))
    except (TypeError, ValueError, UnicodeError):
        return "<redacted broker URL>"


def _configured_broker_url(broker_options: object) -> str | None:
    """Return the effective connection URL while preserving URL/DSN precedence."""
    env_url = _env_str("MODULITH_BROKER_URL")
    if env_url is not None:
        return env_url if _looks_like_connection_url(env_url) else None
    env_dsn = _env_str("MODULITH_BROKER_DSN")
    if env_dsn is not None:
        return env_dsn
    if isinstance(broker_options, dict):
        url = broker_options.get("url")
        if isinstance(url, str) and url:
            return url if _looks_like_connection_url(url) else None
        dsn = broker_options.get("dsn")
        if isinstance(dsn, str) and dsn:
            return dsn
    return None


def _looks_like_connection_url(value: str) -> bool:
    """Distinguish URL syntax from the SHM adapter's plain path compatibility alias."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return True
    return bool(parsed.scheme and "://" in value)


def _validate_shm_broker_options(options: dict[str, Any]) -> None:
    """Validate every built-in SHM option before adapter construction."""
    for name in ("state_dir", "sqlite_path", "hint_path", "shm_name", "url"):
        if name in options and (not isinstance(options[name], str) or not options[name].strip()):
            raise ConfigurationError(f"broker_options.{name} must be a non-empty filesystem path")
    for name in ("sqlite_path", "hint_path"):
        if name in options and "://" in options[name]:
            raise ConfigurationError(
                f"broker_options.{name} must be a filesystem path, not a URL. "
                "Use state_dir to choose the directory, or broker='database' with "
                "broker_options.url for a database URL."
            )

    env_url = _env_str("MODULITH_BROKER_URL")
    option_url = options.get("url")
    configured_url = (
        env_url
        if env_url is not None
        else option_url
        if isinstance(option_url, str) and option_url
        else None
    )
    if env_url is None and (
        _env_str("MODULITH_BROKER_DSN") is not None
        or (configured_url is None and options.get("dsn"))
    ):
        raise ConfigurationError(
            "broker='shm' does not accept a DSN; use state_dir, sqlite_path, and hint_path instead."
        )

    if configured_url is not None and _looks_like_connection_url(configured_url):
        raise ConfigurationError(
            "broker='shm' accepts only filesystem paths, not SQLAlchemy or "
            "network URLs. Use state_dir/sqlite_path/hint_path, or select "
            "broker='database' for a database URL."
        )

    _validate_shm_int_option(
        options,
        "shm_capacity",
        maximum=_SHM_MAX_HINT_CAPACITY,
    )
    _validate_max_payload_bytes(options.get("max_payload_bytes"), maximum=_SHM_MAX_PAYLOAD_BYTES)
    _validate_shm_int_option(
        options,
        "max_store_bytes",
        maximum=_SHM_MAX_STORE_BYTES,
    )
    _validate_shm_int_option(
        options,
        "batch_size",
        maximum=_SHM_MAX_CLAIM_BATCH_SIZE,
    )
    _validate_shm_int_option(
        options,
        "dispatch_concurrency",
        maximum=_SHM_MAX_DISPATCH_CONCURRENCY,
    )
    _validate_shm_int_option(
        options,
        "max_delivery_attempts",
        maximum=_SHM_MAX_DELIVERY_ATTEMPTS,
    )
    for name in (
        "poll_interval_ms",
        "reclaim_stale_seconds",
        "retention_age_seconds",
        "orphan_retention_seconds",
    ):
        _validate_shm_float_option(options, name, allow_zero=False)
    _validate_shm_float_option(options, "prune_interval_seconds", allow_zero=True)

    if "sqlite_synchronous" in options:
        value = options["sqlite_synchronous"]
        if type(value) is not str or value.upper() not in _SHM_SYNCHRONOUS_MODES:
            raise ConfigurationError(
                f"broker_options.sqlite_synchronous must be NORMAL or FULL, got {value!r}"
            )
    if "completion_mode" in options:
        value = options["completion_mode"]
        if type(value) is not str or value not in _SHM_COMPLETION_MODES:
            raise ConfigurationError(
                "broker_options.completion_mode must be one of "
                f"{sorted(_SHM_COMPLETION_MODES)}, got {value!r}"
            )


def _validate_max_payload_bytes(
    value: object,
    *,
    source: str = "broker_options",
    maximum: int = MAX_PAYLOAD_BYTES,
) -> int | None:
    """Return the payload cap held in ``value``, or None when it is unset or blank.

    The one range check every reader of ``max_payload_bytes`` shares: the
    config loader for every broker, the SHM adapter and the consume-side
    resolver in ``serializers.py``. Only an int or a numeric string counts: a
    bool or float would otherwise truncate (``True`` to 1, ``2.9`` to 2).
    """
    number: int | None = None
    if type(value) is int:
        number = value
    elif type(value) is str:
        if not value.strip():
            return None
        try:
            number = int(value)
        except ValueError:
            pass
    elif value is None:
        return None
    if number is None or not 1 <= number <= maximum:
        raise ConfigurationError(
            f"max_payload_bytes must be an integer from 1 to {maximum}, "
            f"got {value!r} (set by {source})"
        )
    return number


def _validate_shm_int_option(
    options: dict[str, Any],
    name: str,
    *,
    maximum: int,
) -> None:
    if name not in options:
        return
    value = options[name]
    if type(value) is int:
        number = value
    elif type(value) is str:
        try:
            number = int(value)
        except ValueError as exc:
            raise ConfigurationError(
                f"broker_options.{name} must be an integer from 1 to {maximum}, got {value!r}"
            ) from exc
    else:
        raise ConfigurationError(
            f"broker_options.{name} must be an integer from 1 to {maximum}, got {value!r}"
        )
    if not 1 <= number <= maximum:
        raise ConfigurationError(
            f"broker_options.{name} must be an integer from 1 to {maximum}, got {value!r}"
        )


def _validate_shm_float_option(
    options: dict[str, Any],
    name: str,
    *,
    allow_zero: bool,
) -> None:
    if name not in options:
        return
    value = options[name]
    if isinstance(value, bool):
        number = math.nan
    else:
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = math.nan
    minimum_valid = number >= 0 if allow_zero else number > 0
    if not math.isfinite(number) or not minimum_valid:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ConfigurationError(
            f"broker_options.{name} must be a finite {qualifier} number, got {value!r}"
        )


# Configuration fields that hold tables of options rather than scalars.
# They have no MODULITH_* env var (see _read_env_vars) and get a dedicated
# type check in _validate.
_DICT_FIELDS = frozenset({"outbox_options", "broker_options", "workers", "subscriptions"})

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
    "subscriptions": "subscriptions",
}

# Subtables reserved for future modulith versions. These are the ONLY
# subtable names that are dropped silently — a documented no-op so a
# future-compatible pyproject still bootstraps on today's modulith.
# "verify" is a partial exception: its disabled_rules key is already wired
# to Configuration.verify_disabled_rules (see _read_pyproject); every other
# key in that subtable (mode, baseline, ...) remains a silent no-op.
_RESERVED_SUBTABLES = frozenset({"verify"})


def _read_pyproject() -> dict[str, Any]:
    """Read [tool.modulith] from pyproject.toml. Returns {} if absent.

    The documented pyproject.toml convention uses subtables for compound
    options (``[tool.modulith.outbox_options]`` for outbox tuning,
    ``[tool.modulith.broker_options]`` for broker connection settings,
    ``[tool.modulith.workers]`` for per-module worker counts, and
    ``[tool.modulith.subscriptions]`` for broker targets). TOML parses these
    as nested dicts under the ``modulith`` key. We separate scalar keys (which
    map directly to Configuration fields) from subtables (which map to
    dict-typed fields like outbox_options, broker_options, workers, and
    subscriptions) so users can write idiomatic TOML without hitting the
    "unknown config key" guard.

    Loud-error contract:
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

    Only genuinely unknown subtables — plus the reserved ``verify`` table
    (see ``_RESERVED_SUBTABLES``) — are dropped silently: the bootstrap must
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

    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        raise ConfigurationError(f"[tool] must be a table, got {type(tool).__name__}: {tool!r}")
    raw = tool.get("modulith", {})
    if not isinstance(raw, dict):
        raise ConfigurationError(
            f"[tool.modulith] must be a table, got {type(raw).__name__}: {raw!r}"
        )
    if not raw:
        return {}

    result: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            # Scalar: pass through. _validate catches unknown scalar keys.
            result[key] = value
            continue
        if key == "verify":
            # Reserved subtable (see _RESERVED_SUBTABLES), but disabled_rules
            # is the one key already wired to a Configuration field — everything
            # else in [tool.modulith.verify] stays a silent forward-compat no-op.
            if "disabled_rules" in value:
                result["verify_disabled_rules"] = value["disabled_rules"]
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

    close_scalar = difflib.get_close_matches(key, sorted(scalar_fields), n=1)
    if close_scalar:
        raise ConfigurationError(
            f"Unknown config subtable [tool.modulith.{key}] — did you mean the "
            f"scalar option {close_scalar[0]!r} under [tool.modulith]?"
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


def _is_broker_target(value: object) -> bool:
    """Return whether value is a non-empty ``scheme:destination`` string."""
    if type(value) is not str:
        return False
    scheme, destination = _split_broker_target(value)
    return bool(scheme and destination)


def _env_str(name: str) -> str | None:
    """Return the env var's value, or None when it is unset, empty, or whitespace-only.

    Empty-string-is-unset is a documented contract, not an accident of
    truthiness: templated deployments commonly render ``MODULITH_X=""``
    when the source variable is missing (e.g. ``MODULITH_PRODUCTION=
    ${DEPLOY_ENV_IS_PROD}`` with the interpolation variable unset), and
    honoring "" as an explicit value would inject nonsense config such as
    ``package=""``. Whitespace-only values (``MODULITH_X="   "``) get the
    same treatment, matching ``_env_bool``'s existing stripped comparison —
    a value is never returned verbatim without first checking it holds
    something other than whitespace.
    """
    val = os.environ.get(name)
    if val is None or val.strip() == "":
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
    (outbox_options, broker_options, workers, subscriptions) have no env
    var — they come from the [tool.modulith.*] subtables in pyproject.toml.
    """
    result: dict[str, Any] = {}
    string_vars = (
        ("MODULITH_PACKAGE", "package"),
        ("MODULITH_CONTRACTS_MODULE", "contracts_module"),
        ("MODULITH_OUTBOX", "outbox"),
        ("MODULITH_OUTBOX_URL", "outbox_url"),
        ("MODULITH_TOPOLOGY", "topology"),
        ("MODULITH_BROKER", "broker"),
        ("MODULITH_SUBSCRIPTION_SOURCE", "subscription_source"),
        ("MODULITH_ACTUATOR_MODE", "actuator_mode"),
    )
    for env_name, field_name in string_vars:
        if (val := _env_str(env_name)) is not None:
            result[field_name] = val
    bool_vars = (
        ("MODULITH_PRODUCTION", "production"),
        ("MODULITH_AUTO_DISCOVER", "auto_discover"),
        ("MODULITH_OBSERVABILITY", "observability"),
        ("MODULITH_VERIFY_MANIFESTS", "verify_manifests"),
        ("MODULITH_STRICT_BOUNDARIES", "strict_boundaries"),
    )
    for env_name, field_name in bool_vars:
        if (flag := _env_bool(env_name)) is not None:
            result[field_name] = flag
    if (port_base := _env_str("MODULITH_WORKER_PORT_BASE")) is not None:
        try:
            result["worker_port_base"] = int(port_base)
        except ValueError:
            raise ConfigurationError(
                f"MODULITH_WORKER_PORT_BASE must be an integer (worker_port_base), "
                f"got {port_base!r}"
            ) from None
    return result


_COMPLETION_MODES = ("update", "delete", "archive")
_POSITIVE_SECONDS_KEYS = (
    "claim_lease_seconds",
    "retry_interval_seconds",
    "retry_stale_seconds",
    "max_retry_backoff_seconds",
)
_POSITIVE_INTEGER_KEYS = ("claim_batch_size", "dead_letter_after_attempts")


def _validate_outbox_options(options: dict[str, Any]) -> None:
    """Validate the keys of [tool.modulith.outbox_options] that
    ``Runtime.bind_configured_outbox`` forwards to ``outbox.configure()``
    (claim, retry, dead-letter and completion settings), plus the
    ``sqlite_wal`` flag it applies to a SQLite engine, when present.
    ``sqlite_wal`` is not checked against ``outbox_url``: any other database
    ignores it, so one pyproject can serve a SQLite development setup and a
    Postgres deployment. Other keys in that table are intentionally NOT
    validated here — outbox_options is a forward-compatible passthrough (see
    _read_pyproject).
    """
    if "claim_strategy" in options and options["claim_strategy"] not in VALID_CLAIM_STRATEGIES:
        raise ConfigurationError(
            "outbox_options.claim_strategy must be one of "
            f"{VALID_CLAIM_STRATEGIES}, got {options['claim_strategy']!r}"
        )
    if "completion_mode" in options and options["completion_mode"] not in _COMPLETION_MODES:
        raise ConfigurationError(
            "outbox_options.completion_mode must be one of "
            f"{_COMPLETION_MODES}, got {options['completion_mode']!r}"
        )
    for key in _POSITIVE_SECONDS_KEYS:
        if key in options:
            value = options[key]
            # bool is an int subclass; isinstance(True, (int, float)) is True, so
            # it must be excluded explicitly or `claim_lease_seconds = true` would
            # silently pass as 1.0.
            valid = isinstance(value, (int, float)) and not isinstance(value, bool)
            if not valid or not (0 < value < float("inf")):
                raise ConfigurationError(
                    f"outbox_options.{key} must be a finite number greater than 0, got {value!r}"
                )
    if (
        "claim_lease_seconds" in options
        and options["claim_lease_seconds"] > MAX_CLAIM_LEASE_SECONDS
    ):
        raise ConfigurationError(
            f"outbox_options.claim_lease_seconds must be at most {MAX_CLAIM_LEASE_SECONDS} "
            f"seconds (one day), got {options['claim_lease_seconds']!r}"
        )
    for key in _POSITIVE_INTEGER_KEYS:
        if key in options:
            value = options[key]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigurationError(
                    f"outbox_options.{key} must be a positive integer, got {value!r}"
                )
    if "sqlite_wal" in options and type(options["sqlite_wal"]) is not bool:
        raise ConfigurationError(
            f"outbox_options.sqlite_wal must be true or false, got {options['sqlite_wal']!r}"
        )


def _validate_redis_broker_options(options: dict[str, Any]) -> None:
    """Reject Redis settings that would disable delivery safety guarantees."""
    for name in (
        "max_stream_len",
        "dlq_max_stream_len",
        "poll_block_ms",
        "reclaim_min_idle_ms",
        "max_delivery_attempts",
    ):
        if name not in options:
            continue
        value = options[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigurationError(
                f"broker_options.{name} must be a positive integer, got {value!r}"
            )


# Mirror modulith/adapters/db_broker.py's _COMPLETION_MODES /
# _NO_SUBSCRIBER_POLICIES / _ORPHAN_REPLAY_POLICIES. Duplicated (not
# imported) because that adapter already imports from this module
# (DEFAULT_BROKER_DB_FILENAME) — importing back here would cycle.
_DATABASE_COMPLETION_MODES = frozenset({"delete", "mark"})
_DATABASE_NO_SUBSCRIBER_POLICIES = frozenset({"error", "store", "wait"})
_DATABASE_ORPHAN_REPLAY_POLICIES = frozenset({"ttl_all_groups", "first_groups", "expected_groups"})
_SQL_SCHEMA_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_sql_schema(value: object, *, option_name: str) -> str:
    if type(value) is not str or _SQL_SCHEMA_RE.fullmatch(value) is None:
        raise ConfigurationError(
            f"{option_name} must be a valid unquoted SQL identifier "
            f"(letters, digits, underscore, not starting with a digit), got {value!r}"
        )
    return value


def _validate_database_broker_options(options: dict[str, Any]) -> None:
    """Validate the 'database' broker's options at config-load time.

    Without this, these same shapes are only checked later, at
    ``DatabaseBroker`` construction during plugin bootstrap (see
    ``modulith/adapters/db_broker.py``'s ``_validate_choice``,
    ``_positive_finite_float``, and ``_validate_expected_consumer_groups``)
    — a validation-timing asymmetry against the shm/redis brokers, which
    both fail fast here in ``_validate``.
    """
    if "sqlite_synchronous" in options:
        value = options["sqlite_synchronous"]
        if type(value) is not str or value.upper() not in _SHM_SYNCHRONOUS_MODES:
            raise ConfigurationError(
                f"broker_options.sqlite_synchronous must be NORMAL or FULL, got {value!r}"
            )
    if "completion_mode" in options:
        value = options["completion_mode"]
        if type(value) is not str or value not in _DATABASE_COMPLETION_MODES:
            raise ConfigurationError(
                "broker_options.completion_mode must be one of "
                f"{sorted(_DATABASE_COMPLETION_MODES)}, got {value!r}"
            )
    if "no_subscriber_policy" in options:
        value = options["no_subscriber_policy"]
        if type(value) is not str or value not in _DATABASE_NO_SUBSCRIBER_POLICIES:
            raise ConfigurationError(
                "broker_options.no_subscriber_policy must be one of "
                f"{sorted(_DATABASE_NO_SUBSCRIBER_POLICIES)}, got {value!r}"
            )
    if "orphan_replay_policy" in options:
        value = options["orphan_replay_policy"]
        if type(value) is not str or value not in _DATABASE_ORPHAN_REPLAY_POLICIES:
            raise ConfigurationError(
                "broker_options.orphan_replay_policy must be one of "
                f"{sorted(_DATABASE_ORPHAN_REPLAY_POLICIES)}, got {value!r}"
            )
    if "busy_timeout_ms" in options:
        value = options["busy_timeout_ms"]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigurationError(
                f"broker_options.busy_timeout_ms must be a positive integer, got {value!r}"
            )
    if "schema" in options:
        _validate_sql_schema(options["schema"], option_name="broker_options.schema")
    for name in (
        "no_subscriber_wait_timeout_seconds",
        "no_subscriber_wait_poll_interval_ms",
        "orphan_retention_seconds",
    ):
        _validate_shm_float_option(options, name, allow_zero=False)
    if "expected_consumer_groups" in options:
        value = options["expected_consumer_groups"]
        valid = type(value) is dict and all(
            type(target) is str
            and target.strip()
            and type(groups) is list
            and groups
            and all(type(group) is str and group.strip() for group in groups)
            for target, groups in value.items()
        )
        if not valid:
            raise ConfigurationError(
                "broker_options.expected_consumer_groups must map non-empty targets "
                f"to non-empty lists of group strings, got {value!r}"
            )


def _validate(data: dict[str, Any]) -> None:
    """Validate config values, raising ConfigurationError with guidance."""
    # Catch typos — unknown keys would silently fail otherwise.
    known = {f.name for f in fields(Configuration)} - {"explicit_keys"}
    unknown = set(data.keys()) - known
    if unknown:
        suggestions = []
        for key in sorted(unknown):
            close = difflib.get_close_matches(key, sorted(known), n=1)
            if close:
                suggestions.append(f"{key!r} -> {close[0]!r}")
        guidance = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
        raise ConfigurationError(
            f"Unknown config keys: {sorted(unknown)}. Valid keys: {sorted(known)}.{guidance}"
        )

    scalar_types: dict[str, tuple[type, ...]] = {
        "package": (str, type(None)),
        "contracts_module": (str,),
        "outbox": (str,),
        "outbox_url": (str, type(None)),
        "topology": (str,),
        "broker": (str,),
        "subscription_source": (str,),
        "actuator_mode": (str,),
        "worker_port_base": (int,),
        "auto_discover": (bool,),
        "production": (bool,),
        "observability": (bool, type(None)),
        "verify_manifests": (bool,),
        "strict_boundaries": (bool,),
    }
    for field_name, expected_types in scalar_types.items():
        if field_name in data and type(data[field_name]) not in expected_types:
            expected = " or ".join(expected_type.__name__ for expected_type in expected_types)
            raise ConfigurationError(
                f"{field_name} must be {expected}, got "
                f"{type(data[field_name]).__name__}: {data[field_name]!r}"
            )
    if "contracts_module" in data:
        _validate_contracts_module(data["contracts_module"])
    if "worker_port_base" in data and not 1 <= data["worker_port_base"] <= 65535:
        raise ConfigurationError(
            f"worker_port_base must be a TCP port (1-65535), got {data['worker_port_base']}"
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

    outbox_options = data.get("outbox_options")
    if outbox_options is not None:
        _validate_outbox_options(outbox_options)

    broker_options = data.get("broker_options")
    if data.get("broker", "memory") == "redis-streams" and broker_options is not None:
        _validate_redis_broker_options(broker_options)
    selected_broker = data.get("broker")
    if selected_broker is None and data.get("topology") == "processes":
        selected_broker = (
            "database" if _configured_broker_url(broker_options) is not None else "shm"
        )
    if selected_broker == "shm":
        _validate_shm_broker_options(broker_options or {})
    if selected_broker == "database" and broker_options is not None:
        _validate_database_broker_options(broker_options)
    payload_cap_maximum = _SHM_MAX_PAYLOAD_BYTES if selected_broker == "shm" else MAX_PAYLOAD_BYTES
    _validate_max_payload_bytes(
        os.environ.get(_MAX_PAYLOAD_BYTES_ENV),
        source=_MAX_PAYLOAD_BYTES_ENV,
        maximum=payload_cap_maximum,
    )
    if broker_options is not None:
        _validate_max_payload_bytes(
            broker_options.get("max_payload_bytes"), maximum=payload_cap_maximum
        )

    workers = data.get("workers")
    if workers is not None:
        for module_name, count in workers.items():
            if type(module_name) is not str or type(count) is not int or count <= 0:
                raise ConfigurationError(
                    "workers must map string module names to positive integer "
                    f"counts; got {module_name!r}: {count!r}"
                )

    if "verify_disabled_rules" in data:
        disabled_rules = data["verify_disabled_rules"]
        if not isinstance(disabled_rules, (list, tuple)) or any(
            type(rule) is not str for rule in disabled_rules
        ):
            raise ConfigurationError(
                "[tool.modulith.verify].disabled_rules must be a list of rule-name "
                f"strings, got {disabled_rules!r}"
            )

    subscriptions = data.get("subscriptions")
    if subscriptions is not None:
        for module_name, targets in subscriptions.items():
            if type(module_name) is not str or type(targets) is not list:
                raise ConfigurationError(
                    "subscriptions must map string module names to lists of "
                    f"broker targets; got {module_name!r}: {targets!r}"
                )
            if any(not _is_broker_target(target) for target in targets):
                raise ConfigurationError(
                    "subscriptions targets must be non-empty "
                    f"'scheme:destination' strings; got {module_name!r}: {targets!r}"
                )

    # Topology must be one of the known modes.
    if "topology" in data and data["topology"] not in _VALID_TOPOLOGIES:
        raise ConfigurationError(
            f"invalid topology {data['topology']!r}; "
            f"expected one of: {', '.join(_VALID_TOPOLOGIES)}"
        )

    if (
        "subscription_source" in data
        and data["subscription_source"] not in _VALID_SUBSCRIPTION_SOURCES
    ):
        raise ConfigurationError(
            f"invalid subscription_source {data['subscription_source']!r}; "
            f"expected one of: {', '.join(_VALID_SUBSCRIPTION_SOURCES)}"
        )

    if "actuator_mode" in data and data["actuator_mode"] not in _VALID_ACTUATOR_MODES:
        raise ConfigurationError(
            f"invalid actuator_mode {data['actuator_mode']!r}; "
            f"expected one of: {', '.join(_VALID_ACTUATOR_MODES)}"
        )

    # Cross-field: a multi-process topology needs a real cross-process broker.
    # An absent broker is fine — load_configuration will default it to "shm".
    # An *explicit* memory broker is always wrong for cross-process topologies.
    effective_topology = data.get("topology", "single")
    if effective_topology in ("processes", "subinterpreters") and data.get("broker") == "memory":
        raise ConfigurationError(
            f"topology={effective_topology!r} requires a cross-process broker, but "
            "broker='memory' was explicitly configured. Set [tool.modulith].broker "
            "to a real adapter (e.g. 'shm', 'database', or 'redis-streams'); "
            "broker='memory' is only valid for topology='single'."
        )

    # Production + explicit database broker still needs a URL — otherwise
    # registration invents a per-host SQLite file and cross-host delivery splits.
    if (
        data.get("production")
        and effective_topology == "processes"
        and data.get("broker") == "database"
        and _configured_broker_url(data.get("broker_options")) is None
    ):
        raise ConfigurationError(
            "Cannot start in production with broker='database' and no URL — "
            "an implicit embedded SQLite file is not durable across hosts. "
            "Set [tool.modulith.broker_options].url or MODULITH_BROKER_URL."
        )

    # Blank / whitespace-only broker names are never valid schemes.
    if "broker" in data and isinstance(data["broker"], str) and not data["broker"].strip():
        raise ConfigurationError(f"broker must be a non-empty adapter name, got {data['broker']!r}")

    # "subinterpreters" is reserved but unshipped (SPEC §9.1 option C;
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
