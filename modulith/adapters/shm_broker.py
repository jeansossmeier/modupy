"""Durable local-process broker backed by SQLite and mmap notifications.

Opt-in alternative to ``broker = "database"`` for ``topology = "processes"``.
SQLite is authoritative for publications, subscriptions, claims, retries, and
completion. The file-backed mmap ring only hints that a committed sequence is
available; consumers always recover work from SQLite.

Zero external dependencies -- stdlib only (``mmap``, ``sqlite3``, ``struct``).
Registers as scheme ``"shm"`` via the same pluggy hookimpl pattern as
``db_broker`` and ``redis_broker``.

Configuration resolves ``MODULITH_BROKER_<KEY>`` env var > broker_options, per
key (see ``modulith_register_brokers`` at the bottom of this file for the
exhaustive list).

Fan-out and retained replay are created transactionally from persisted
subscriptions. Producers therefore do not need a process-local subscription
cache.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any, cast

from modulith import (
    BrokerRegistry,
    ConfigurationError,
    Consumer,
    ConsumerRegistry,
    ConsumerSpec,
    hookimpl,
)

from ..config import (
    _SHM_MAX_CLAIM_BATCH_SIZE,
    _SHM_MAX_DELIVERY_ATTEMPTS,
    _SHM_MAX_DISPATCH_CONCURRENCY,
    _SHM_MAX_HINT_CAPACITY,
    _SHM_MAX_STORE_BYTES,
    DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_SHM_BROKER_DB_FILENAME,
    DEFAULT_SHM_MAX_STORE_BYTES,
    MAX_PAYLOAD_BYTES,
    _validate_shm_broker_options,
)
from ._polling_consumer import PollingConsumer
from ._shm_coldstore import ShmColdStore
from ._shm_ring import _HEADER_SIZE, _SLOT_SIZE, ShmRing
from ._shm_types import ClaimToken
from ._state_path import default_state_directory, resolve_state_directory, resolve_state_file

logger = logging.getLogger("modulith.adapters.shm")

_SHM_SCHEME = "shm"

# -- Defaults (same as the database broker where applicable) ---------------
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_DISPATCH_CONCURRENCY = 10
_DEFAULT_POLL_INTERVAL_S = 0.02
_DEFAULT_RECLAIM_STALE_S = 60.0
_MAX_DELIVERY_ATTEMPTS = 5
_DEFAULT_RETENTION_AGE_S = 3 * 86400.0
_DEFAULT_ORPHAN_RETENTION_S = 86400.0
_DEFAULT_COMPLETION_MODE = "delete"
_COMPLETION_MODES = frozenset({"delete", "mark"})
_DEFAULT_SHM_CAPACITY = 8192
_DEFAULT_SHM_SLOT_SIZE = 4096
_DEFAULT_SHM_NAME = "modulith-shm"
_DEFAULT_HINT_FILENAME = ".modulith-shm-broker.hints"
# NORMAL fsyncs only at WAL checkpoints, not per-commit: an application or
# process crash loses nothing (the WAL survives), but an OS crash or power
# loss can still drop the last commits. Set synchronous="FULL" for
# power-loss durability.
_DEFAULT_SQLITE_SYNCHRONOUS = "NORMAL"
_PRUNE_BATCH_LIMIT = 1000
_HINT_CHECK_INTERVAL_S = 0.01


def _positive_finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ConfigurationError(f"{name} must be a finite number > 0")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a finite number > 0, got {value!r}") from exc
    if not math.isfinite(number) or number <= 0:
        raise ConfigurationError(f"{name} must be a finite number > 0, got {value!r}")
    return number


def _positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ConfigurationError(f"{name} must be an integer >= 1, got {value!r}")
    return value


def _bounded_positive_int(value: Any, name: str, maximum: int) -> int:
    number = _positive_int(value, name)
    if number > maximum:
        raise ConfigurationError(f"{name} must be <= {maximum}, got {value!r}")
    return number


def _validated_hint_capacity(value: Any) -> int:
    capacity = _bounded_positive_int(value, "shm_capacity", _SHM_MAX_HINT_CAPACITY)
    if capacity > (sys.maxsize - _HEADER_SIZE) // _SLOT_SIZE:
        raise ConfigurationError(f"shm_capacity {capacity} is too large to allocate safely")
    return capacity


def _validated_synchronous(value: Any) -> str:
    if type(value) is not str or value.upper() not in {"NORMAL", "FULL"}:
        raise ConfigurationError(f"sqlite_synchronous must be NORMAL or FULL, got {value!r}")
    return value.upper()


def _non_negative_finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ConfigurationError(f"{name} must be a finite number >= 0")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a finite number >= 0, got {value!r}") from exc
    if not math.isfinite(number) or number < 0:
        raise ConfigurationError(f"{name} must be a finite number >= 0, got {value!r}")
    return number


def _resolve_notifier_path(shm_name: str, db_path: str) -> Path:
    """Make simple names converge beside SQLite while preserving explicit paths."""
    candidate = Path(shm_name).expanduser()
    has_separator = os.sep in shm_name or (os.altsep is not None and os.altsep in shm_name)
    if candidate.is_absolute() or has_separator:
        return candidate
    return Path(os.path.abspath(os.path.expanduser(db_path))).parent / candidate


# ---------------------------------------------------------------------------
# ShmBroker (producer + consumer-side operations)
# ---------------------------------------------------------------------------


class ShmBroker:
    """SQLite-authoritative broker with best-effort sequence notifications.

    Satisfies the ``modulith.protocols.Broker`` protocol and provides
    consumer-side methods driven by ``ShmConsumer``. The ``slot_size`` keyword
    is retained as an ignored compatibility argument because hints now store
    fixed-size sequence numbers.
    """

    def __init__(
        self,
        *,
        shm_name: str = _DEFAULT_SHM_NAME,
        capacity: int = _DEFAULT_SHM_CAPACITY,
        slot_size: int = _DEFAULT_SHM_SLOT_SIZE,
        db_path: str | None = None,
        completion_mode: str = _DEFAULT_COMPLETION_MODE,
        create: bool = True,
        synchronous: str = _DEFAULT_SQLITE_SYNCHRONOUS,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_store_bytes: int = DEFAULT_SHM_MAX_STORE_BYTES,
        orphan_retention_seconds: float = _DEFAULT_ORPHAN_RETENTION_S,
    ) -> None:
        capacity = _validated_hint_capacity(capacity)
        orphan_retention_seconds = _positive_finite_float(
            orphan_retention_seconds,
            "orphan_retention_seconds",
        )
        synchronous = _validated_synchronous(synchronous)
        max_payload_bytes = _bounded_positive_int(
            max_payload_bytes,
            "max_payload_bytes",
            MAX_PAYLOAD_BYTES,
        )
        max_store_bytes = _bounded_positive_int(
            max_store_bytes,
            "max_store_bytes",
            _SHM_MAX_STORE_BYTES,
        )
        if type(completion_mode) is not str or completion_mode not in _COMPLETION_MODES:
            raise ConfigurationError(
                f"completion_mode must be one of {sorted(_COMPLETION_MODES)}, "
                f"got {completion_mode!r}"
            )
        self._completion_mode = completion_mode
        if db_path is None:
            resolved_db_path = resolve_state_file(
                None,
                filename=DEFAULT_SHM_BROKER_DB_FILENAME,
                label="SHM SQLite database",
            )
        else:
            unresolved_db_path = Path(os.path.abspath(os.path.expanduser(db_path)))
            resolved_db_path = resolve_state_file(
                None,
                filename=unresolved_db_path.name,
                state_dir=unresolved_db_path.parent,
                path=unresolved_db_path,
                label="SHM SQLite database",
            )
        notifier_path = _resolve_notifier_path(shm_name, str(resolved_db_path))
        notifier_path = resolve_state_file(
            None,
            filename=notifier_path.name,
            state_dir=notifier_path.parent,
            path=notifier_path,
            create=False,
            label="SHM hint file",
        )
        self._ring = ShmRing(
            notifier_path,
            capacity,
            create=create,
        )
        if not self._ring.available:
            # An existing hint file is never re-created, and attaching requires
            # its header to match `capacity` exactly, so changing shm_capacity
            # leaves the notifier permanently dead for this process. Delivery
            # is unaffected (SQLite stays authoritative) but every consumer
            # falls back to its safety poll, which is invisible without this.
            logger.warning(
                "shm hint file %s could not be attached at capacity=%d — "
                "publishes will not notify and consumers fall back to the "
                "safety poll. Stop every worker and delete the file to have it "
                "re-created at the configured capacity.",
                notifier_path,
                capacity,
            )
        # Re-validate post-attach: raises ConfigurationError if the hint file
        # was swapped for a symlink or an insecure file between the earlier
        # check and ShmRing's own open/create above, so construction fails
        # loudly instead of silently using a hijacked path.
        resolve_state_file(
            None,
            filename=notifier_path.name,
            state_dir=notifier_path.parent,
            path=notifier_path,
            create=False,
            label="SHM hint file",
        )
        self._cold = ShmColdStore(
            str(resolved_db_path),
            synchronous=synchronous,
            completion_mode=completion_mode,
            max_payload_bytes=max_payload_bytes,
            max_store_bytes=max_store_bytes,
            orphan_retention_seconds=orphan_retention_seconds,
        )
        self._closed = False

    # -- Broker protocol ---------------------------------------------------

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Commit one publication before emitting its advisory sequence hint.

        A ``"publication_id"`` header (e.g. the outbox's stable row id) is
        threaded through as the SQLite dedup key: re-publishing the same id
        (a cross-retry re-dispatch) hits the ``INSERT OR IGNORE`` and returns
        the existing row instead of delivering a second time.
        """
        publication_id = (headers or {}).get("publication_id")
        publication = await self._cold.publish(
            target, payload, headers, publication_id=publication_id
        )
        try:
            notified = self._ring.notify(publication.sequence)
        except Exception:
            logger.debug(
                "notifier failed for committed publication %s",
                publication.publication_id,
                exc_info=True,
            )
        else:
            if not notified:
                logger.debug(
                    "notifier unavailable for committed publication %s",
                    publication.publication_id,
                )

    async def close(self) -> None:
        """Release ring + cold store resources. Idempotent."""
        if self._closed:
            return
        self._ring.close()
        await self._cold.close()
        self._closed = True

    # -- Consumer-side operations ------------------------------------------

    async def wait_for_hint(
        self,
        after_sequence: int,
        safety_timeout: float,
    ) -> int | None:
        """Return a newer intact hint, or return at the safety-poll deadline."""
        timeout = max(0.0, safety_timeout)
        if not self._ring.available:
            await asyncio.sleep(timeout)
            return None

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            hints = self._ring.read_hints(after_sequence=after_sequence)
            if hints:
                return hints[-1]
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(_HINT_CHECK_INTERVAL_S, remaining))

    async def subscribe(self, targets: list[str], group: str) -> None:
        """Persist exact subscriptions and replay retained publications."""
        await self._cold.subscribe(targets, group)

    async def group_backlog(self) -> dict[str, int]:
        """Map every subscribed group to its pending and claimed delivery count."""
        return await self._cold.group_backlog()

    async def stale_targets(
        self, group: str, targets: list[str] | tuple[str, ...]
    ) -> dict[str, int]:
        """Map each target ``group`` holds but ``targets`` omits to its backlog.

        A held target is one the group subscribes to or has pending or
        claimed deliveries for. Nothing is removed.
        """
        return await self._cold.stale_targets(group, list(targets))

    async def drop_group(self, group: str) -> tuple[int, int]:
        """Unsubscribe a retired group and delete its undelivered work.

        Returns ``(subscriptions, deliveries)`` removed. Publish fans out to
        every subscribed group and prune keeps any publication with an
        undelivered row, so a group whose module is gone for good pins every
        later publication until this runs (``modulith broker drop-group``).
        """
        return await self._cold.drop_group(group)

    async def claim_batch(
        self,
        group: str,
        *,
        batch_size: int,
        consumer_name: str,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
        max_attempts: int | None = None,
        targets: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically claim durable work, including abandoned stale claims.

        ``targets``, when given, restricts the claim to those targets, so
        deliveries for a target the consumer no longer consumes stay pending.

        A stale-claim reclaim bumps ``attempts`` only for a row whose
        dispatch had started (``renew_claims(..., start_dispatch=True)``);
        rows claimed alongside it that never reached a listener are
        reclaimed free. When ``max_attempts`` is given, a charged reclaim
        that meets or exceeds the cap dead-letters the row instead of
        redelivering it -- the same accounting
        the database broker applies, needed because a consumer that crashes
        mid-dispatch never reaches ``fail()`` to run the cap itself.
        """
        batch_size = _bounded_positive_int(
            batch_size,
            "batch_size",
            _SHM_MAX_CLAIM_BATCH_SIZE,
        )
        return await self._cold.claim(
            group,
            limit=batch_size,
            consumer_name=consumer_name,
            reclaim_stale_seconds=reclaim_stale_seconds,
            max_attempts=max_attempts,
            targets=targets,
        )

    async def renew_claims(
        self,
        row_ids: list[str],
        *,
        consumer_name: str,
        start_dispatch: bool = False,
    ) -> int:
        """Renew only claims whose owner and generation still match.

        ``start_dispatch`` also marks the claims' dispatch as started, so a
        later stale reclaim charges them an attempt.
        """
        claim_tokens: list[ClaimToken | str] = list(row_ids)
        return await self._cold.renew_claims(claim_tokens, consumer_name, start_dispatch)

    async def ack(self, row_id: str, *, consumer_name: str) -> None:
        """Complete one currently owned claim using its fencing token."""
        await self._cold.ack(
            row_id,
            consumer_name=consumer_name,
            completion_mode=self._completion_mode,
        )

    async def fail(
        self,
        row_id: str,
        error: str,
        *,
        consumer_name: str,
        max_attempts: int,
    ) -> None:
        """Retry or dead-letter one currently owned claim."""
        await self._cold.fail(
            row_id,
            error,
            max_attempts,
            consumer_name=consumer_name,
        )

    async def dead_letter(self, row_id: str, error: str, *, consumer_name: str) -> None:
        """Dead-letter one currently owned claim."""
        await self._cold.dead_letter(
            row_id,
            error,
            consumer_name=consumer_name,
        )

    async def prune(
        self,
        *,
        retention_age_seconds: float | None = None,
        retention_count: int | None = None,
    ) -> int:
        """Prune terminal cold-store rows past the retention window."""
        del retention_count  # SHM retention is age-based; count retention is DB-only.
        if retention_age_seconds is None:
            return 0
        return await self._cold.prune(
            retention_age_seconds,
            limit=_PRUNE_BATCH_LIMIT,
        )


# ---------------------------------------------------------------------------
# ShmConsumer (poll/claim/ack loop)
# ---------------------------------------------------------------------------


class ShmConsumer(PollingConsumer):
    """Compatibility wrapper over the shared durable polling lifecycle."""

    def __init__(
        self,
        *,
        broker: ShmBroker,
        bus: Any,
        serializer: Any,
        consumer_name: str,
        group: str,
        targets: list[str] | tuple[str, ...],
        poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        dispatch_concurrency: int = _DEFAULT_DISPATCH_CONCURRENCY,
        max_attempts: int = _MAX_DELIVERY_ATTEMPTS,
        reclaim_stale_seconds: float = _DEFAULT_RECLAIM_STALE_S,
        prune_interval_s: float | None = None,
        retention_age_seconds: float | None = None,
    ) -> None:
        self._hint_broker = broker
        self._last_observed_sequence = -1
        super().__init__(
            broker=broker,
            bus=bus,
            serializer=serializer,
            consumer_name=consumer_name,
            group=group,
            targets=targets,
            poll_interval_s=_positive_finite_float(poll_interval_s, "poll_interval_s"),
            batch_size=_bounded_positive_int(
                batch_size,
                "batch_size",
                _SHM_MAX_CLAIM_BATCH_SIZE,
            ),
            dispatch_concurrency=_bounded_positive_int(
                dispatch_concurrency,
                "dispatch_concurrency",
                _SHM_MAX_DISPATCH_CONCURRENCY,
            ),
            max_attempts=_bounded_positive_int(
                max_attempts,
                "max_attempts",
                _SHM_MAX_DELIVERY_ATTEMPTS,
            ),
            reclaim_stale_seconds=_positive_finite_float(
                reclaim_stale_seconds,
                "reclaim_stale_seconds",
            ),
            prune_interval_s=(
                None
                if prune_interval_s is None
                else _non_negative_finite_float(prune_interval_s, "prune_interval_s")
            ),
            retention_age_seconds=(
                None
                if retention_age_seconds is None
                else _positive_finite_float(retention_age_seconds, "retention_age_seconds")
            ),
            retention_count=None,
            logger=logger,
            scheme=_SHM_SCHEME,
            idle_backoff=True,
            idle_wait=self._wait_for_hint,
            subscribe_when_empty=True,
        )

    async def _wait_for_hint(self, safety_timeout: float) -> None:
        """Use hints only to shorten the next authoritative SQLite poll."""
        sequence = await self._hint_broker.wait_for_hint(
            self._last_observed_sequence,
            safety_timeout,
        )
        if sequence is not None:
            self._last_observed_sequence = max(self._last_observed_sequence, sequence)


# ---------------------------------------------------------------------------
# Config helpers (same pattern as db_broker)
# ---------------------------------------------------------------------------


def _broker_opt(opts: dict[str, Any], key: str, env_suffix: str) -> Any:
    env_value = os.environ.get(f"MODULITH_BROKER_{env_suffix}")
    if env_value:
        return env_value
    return opts.get(key)


_SHM_OPTION_ENV_SUFFIXES = {
    "state_dir": "STATE_DIR",
    "sqlite_path": "SQLITE_PATH",
    "hint_path": "HINT_PATH",
    "url": "URL",
    "shm_name": "SHM_NAME",
    "shm_capacity": "SHM_CAPACITY",
    "completion_mode": "COMPLETION_MODE",
    "sqlite_synchronous": "SQLITE_SYNCHRONOUS",
    "max_payload_bytes": "MAX_PAYLOAD_BYTES",
    "max_store_bytes": "MAX_STORE_BYTES",
    "orphan_retention_seconds": "ORPHAN_RETENTION_SECONDS",
    "poll_interval_ms": "POLL_INTERVAL_MS",
    "batch_size": "BATCH_SIZE",
    "dispatch_concurrency": "DISPATCH_CONCURRENCY",
    "reclaim_stale_seconds": "RECLAIM_STALE_SECONDS",
    "max_delivery_attempts": "MAX_DELIVERY_ATTEMPTS",
    "retention_age_seconds": "RETENTION_AGE_SECONDS",
    "prune_interval_seconds": "PRUNE_INTERVAL_SECONDS",
}


def _effective_shm_options(opts: dict[str, Any]) -> dict[str, Any]:
    effective = {key: value for key, value in opts.items() if key != "shm_slot_size"}
    for key, env_suffix in _SHM_OPTION_ENV_SUFFIXES.items():
        value = _broker_opt(opts, key, env_suffix)
        if value is not None:
            effective[key] = value
    _validate_shm_broker_options(effective)
    return effective


def _option_or_default(value: Any, default: Any) -> Any:
    return default if value is None else value


def _resolve_shm_paths(
    package: str | None,
    opts: dict[str, Any],
) -> tuple[Path, Path, Path]:
    """Resolve the canonical state directory, SQLite file, and hint file."""
    state_dir_value = _broker_opt(opts, "state_dir", "STATE_DIR")
    state_dir = resolve_state_directory(package, state_dir=state_dir_value)

    sqlite_value = _broker_opt(opts, "sqlite_path", "SQLITE_PATH")
    if sqlite_value is None:
        sqlite_value = _broker_opt(opts, "url", "URL")
    sqlite_path = resolve_state_file(
        package,
        filename=DEFAULT_SHM_BROKER_DB_FILENAME,
        state_dir=state_dir,
        path=sqlite_value,
        label="SHM SQLite database",
    )

    hint_value = _broker_opt(opts, "hint_path", "HINT_PATH")
    if hint_value is None:
        legacy_name = _broker_opt(opts, "shm_name", "SHM_NAME")
        if legacy_name and legacy_name != _DEFAULT_SHM_NAME:
            candidate = Path(str(legacy_name)).expanduser()
            has_separator = os.sep in str(legacy_name) or (
                os.altsep is not None and os.altsep in str(legacy_name)
            )
            hint_value = (
                candidate if candidate.is_absolute() or has_separator else state_dir / candidate
            )
    hint_path = resolve_state_file(
        package,
        filename=_DEFAULT_HINT_FILENAME,
        state_dir=state_dir,
        path=hint_value,
        create=False,
        label="SHM hint file",
    )
    return state_dir, sqlite_path, hint_path


def _opt_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConfigurationError(f"broker option expected a number, got {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"broker option expected a number, got {value!r}") from exc


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is int:
        return value
    if type(value) is not str:
        raise ConfigurationError(f"broker option expected an integer, got {value!r}")
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"broker option expected an integer, got {value!r}") from exc


# ---------------------------------------------------------------------------
# Consumer factory
# ---------------------------------------------------------------------------


def _make_shm_consumer(spec: ConsumerSpec) -> Consumer:
    """Build an SHM consumer for one worker module from ``spec``."""
    from ..runtime import _runtime

    broker = cast(ShmBroker, spec.broker_registry.get(spec.scheme))
    cfg = _runtime.config
    opts = _effective_shm_options((cfg.broker_options if cfg is not None else None) or {})

    poll_interval_ms = _opt_float(_broker_opt(opts, "poll_interval_ms", "POLL_INTERVAL_MS"))
    batch_size = _opt_int(_broker_opt(opts, "batch_size", "BATCH_SIZE"))
    dispatch_concurrency = _opt_int(
        _broker_opt(opts, "dispatch_concurrency", "DISPATCH_CONCURRENCY")
    )
    reclaim_stale_seconds = _opt_float(
        _broker_opt(opts, "reclaim_stale_seconds", "RECLAIM_STALE_SECONDS")
    )
    max_delivery_attempts = _opt_int(
        _broker_opt(opts, "max_delivery_attempts", "MAX_DELIVERY_ATTEMPTS")
    )
    retention_age_seconds = _opt_float(
        _broker_opt(opts, "retention_age_seconds", "RETENTION_AGE_SECONDS")
    )

    return ShmConsumer(
        broker=broker,
        bus=spec.bus,
        serializer=spec.serializer,
        consumer_name=spec.consumer_name,
        group=spec.group,
        targets=list(spec.targets),
        poll_interval_s=(
            poll_interval_ms / 1000.0 if poll_interval_ms is not None else _DEFAULT_POLL_INTERVAL_S
        ),
        batch_size=(batch_size if batch_size is not None else _DEFAULT_BATCH_SIZE),
        dispatch_concurrency=(
            dispatch_concurrency
            if dispatch_concurrency is not None
            else _DEFAULT_DISPATCH_CONCURRENCY
        ),
        reclaim_stale_seconds=(
            reclaim_stale_seconds if reclaim_stale_seconds is not None else _DEFAULT_RECLAIM_STALE_S
        ),
        max_attempts=(
            max_delivery_attempts if max_delivery_attempts is not None else _MAX_DELIVERY_ATTEMPTS
        ),
        prune_interval_s=_opt_float(
            _broker_opt(opts, "prune_interval_seconds", "PRUNE_INTERVAL_SECONDS")
        ),
        retention_age_seconds=(
            retention_age_seconds if retention_age_seconds is not None else _DEFAULT_RETENTION_AGE_S
        ),
    )


# ---------------------------------------------------------------------------
# Plugin registration hooks
# ---------------------------------------------------------------------------


@hookimpl
def modulith_register_brokers(registry: BrokerRegistry) -> None:
    """Register the ``shm`` scheme when the app selects it."""
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _SHM_SCHEME:
        return

    opts = _effective_shm_options(cfg.broker_options or {})
    state_dir, db_path, hint_path = _resolve_shm_paths(cfg.package, opts)
    capacity = _validated_hint_capacity(
        _option_or_default(
            _opt_int(_broker_opt(opts, "shm_capacity", "SHM_CAPACITY")),
            _DEFAULT_SHM_CAPACITY,
        )
    )
    completion_mode = _option_or_default(
        _broker_opt(opts, "completion_mode", "COMPLETION_MODE"),
        _DEFAULT_COMPLETION_MODE,
    )
    synchronous = _option_or_default(
        _broker_opt(opts, "sqlite_synchronous", "SQLITE_SYNCHRONOUS"),
        _DEFAULT_SQLITE_SYNCHRONOUS,
    )
    max_payload_bytes = _bounded_positive_int(
        _option_or_default(
            _opt_int(_broker_opt(opts, "max_payload_bytes", "MAX_PAYLOAD_BYTES")),
            DEFAULT_MAX_PAYLOAD_BYTES,
        ),
        "max_payload_bytes",
        MAX_PAYLOAD_BYTES,
    )
    max_store_bytes = _bounded_positive_int(
        _option_or_default(
            _opt_int(_broker_opt(opts, "max_store_bytes", "MAX_STORE_BYTES")),
            DEFAULT_SHM_MAX_STORE_BYTES,
        ),
        "max_store_bytes",
        _SHM_MAX_STORE_BYTES,
    )
    orphan_retention_seconds = _option_or_default(
        _opt_float(_broker_opt(opts, "orphan_retention_seconds", "ORPHAN_RETENTION_SECONDS")),
        _DEFAULT_ORPHAN_RETENTION_S,
    )

    broker = ShmBroker(
        shm_name=str(hint_path),
        capacity=capacity,
        db_path=str(db_path),
        completion_mode=completion_mode,
        synchronous=str(synchronous),
        max_payload_bytes=max_payload_bytes,
        max_store_bytes=max_store_bytes,
        orphan_retention_seconds=orphan_retention_seconds,
    )
    registry.register(_SHM_SCHEME, broker)
    logger.info(
        "registered shm broker (name=%s, capacity=%d)",
        hint_path,
        capacity,
    )
    # A worker receives the supervisor's resolution as MODULITH_BROKER_STATE_DIR,
    # so "was state_dir set" cannot tell a defaulted store apart; compare paths.
    if state_dir == default_state_directory(cfg.package):
        logger.info(
            "shm broker state directory: %s (default location, keyed on the "
            "package's install path; set state_dir in production)",
            state_dir,
        )
    else:
        logger.info("shm broker state directory: %s (explicit state_dir)", state_dir)


@hookimpl
def modulith_register_consumers(registry: ConsumerRegistry) -> None:
    """Register the ``shm`` consumer factory when the app selects it."""
    from ..runtime import _runtime

    cfg = _runtime.config
    if cfg is None or cfg.broker != _SHM_SCHEME:
        return
    registry.register(_SHM_SCHEME, _make_shm_consumer)


__all__ = [
    "ShmBroker",
    "ShmConsumer",
    "modulith_register_brokers",
    "modulith_register_consumers",
]
