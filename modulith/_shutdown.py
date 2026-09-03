"""Bounded cancellation for consumer shutdown."""

from __future__ import annotations

import asyncio
import logging

DEFAULT_STOP_TIMEOUT_S = 10.0


async def cancel_and_wait(
    task: asyncio.Task[None],
    *,
    timeout_s: float,
    logger: logging.Logger,
    what: str,
) -> None:
    """Cancel ``task`` and wait for it, never longer than ``2 * timeout_s``.

    SQLAlchemy shields the graceful close of a connection whose operation was
    cancelled, so a driver that never finishes closing absorbs the first
    ``CancelledError``; a second cancel breaks that shield. A task that
    survives both is logged and abandoned rather than wedging shutdown.

    Never raises for the task's own outcome — a task that already died with a
    real exception is logged. ``CancelledError`` still propagates when the
    caller itself is being cancelled, and reaches ``task`` first, exactly as a
    bare ``await task`` would have forwarded it.
    """
    task.cancel()
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout_s)
    except asyncio.CancelledError:
        task.cancel()
        await asyncio.wait({task}, timeout=timeout_s)
        raise
    if not done:
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=timeout_s)
    if not done:
        logger.error(
            "%s ignored two cancellations over %.1fs; abandoning it",
            what,
            2 * timeout_s,
        )
        return
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("%s had already died with an unexpected error", what, exc_info=exc)
