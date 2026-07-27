"""Tests for what a supervised worker's output looks like to an operator.

The process-per-module topology puts every worker's output behind a pipe, so
the supervisor decides what survives. Re-emitting all of it at one fixed level
made the supervisor's own level filter — not the worker's — the thing that
decided visibility, which silently swallowed every warning a worker raised.
These tests pin the surviving contract: a worker's severity is preserved
across the pipe, and nothing is promoted past what its stream can claim.

They drive real subprocesses through the ``command_builder`` seam (no Docker,
no external service) because the behaviour under test *is* the pipe.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

import pytest

from modulith.supervisor import Supervisor, WorkerSpec, _line_level

# A worker writing one line to the given stream, then idling so the supervisor
# does not race a restart while the assertions run.
_EMIT = (
    "import sys, time; sys.{stream}.write({text!r} + chr(10)); sys.{stream}.flush(); time.sleep(30)"
)


def _emitter(stream: str, text: str) -> list[str]:
    return [sys.executable, "-c", _EMIT.format(stream=stream, text=text)]


async def _run_until_logged(sup: Supervisor, needle: str, caplog) -> None:
    """Start ``sup``, wait until ``needle`` shows up in the captured log, stop."""
    try:
        await sup.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if any(needle in record.getMessage() for record in caplog.records):
                break
            await asyncio.sleep(0.05)
    finally:
        await sup.stop()


def _levels_for(caplog, needle: str) -> set[int]:
    return {r.levelno for r in caplog.records if needle in r.getMessage()}


# ---------------------------------------------------------------------------
# Severity preservation across the pipe
# ---------------------------------------------------------------------------


@pytest.mark.real_process
async def test_worker_warning_survives_a_warning_level_supervisor(caplog) -> None:
    """A worker's warning must not be demoted to INFO on the way through.

    modulith's own worker diagnostics are emitted at WARNING precisely so they
    survive a deployment that never configures logging. Re-logging them at a
    fixed INFO threw that away: the line explaining that a module exposes no
    ``router`` — the whole reason its every request 404s — was filtered out by
    the supervisor and reached no terminal at all.
    """
    warning = "module 'reports' exposes no 'router' attribute"
    sup = Supervisor(
        [WorkerSpec("reports", "fakeapp", 9001)],
        command_builder=lambda spec, port: _emitter("stderr", warning),
    )
    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await _run_until_logged(sup, warning, caplog)

    assert _levels_for(caplog, warning) == {logging.WARNING}


@pytest.mark.real_process
async def test_worker_error_line_is_forwarded_as_an_error(caplog) -> None:
    """uvicorn tags its records with a level name, and that tag is authoritative."""
    line = "ERROR:    [Errno 98] error while attempting to bind on address"
    sup = Supervisor(
        [WorkerSpec("billing", "fakeapp", 9001)],
        command_builder=lambda spec, port: _emitter("stderr", line),
    )
    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await _run_until_logged(sup, line, caplog)

    assert _levels_for(caplog, line) == {logging.ERROR}


@pytest.mark.real_process
async def test_uvicorn_startup_chatter_is_not_promoted_to_a_warning(caplog) -> None:
    """The other half of the contract: routine worker chatter stays routine.

    Forwarding every stderr line at WARNING would surface the diagnostics
    above at the cost of turning each worker's four uvicorn startup lines into
    warnings on every boot — an operator watching for real problems would be
    trained to ignore the level within one restart.
    """
    line = "INFO:     Started server process [4242]"
    sup = Supervisor(
        [WorkerSpec("orders", "fakeapp", 9001)],
        command_builder=lambda spec, port: _emitter("stderr", line),
    )
    with caplog.at_level(logging.INFO, logger="modulith.supervisor"):
        await _run_until_logged(sup, line, caplog)

    assert _levels_for(caplog, line) == {logging.INFO}


@pytest.mark.real_process
async def test_worker_stdout_stays_informational(caplog) -> None:
    """stdout is not a logging stream — a print() there claims no severity."""
    line = "serving 42 cached reports"
    sup = Supervisor(
        [WorkerSpec("reports", "fakeapp", 9001)],
        command_builder=lambda spec, port: _emitter("stdout", line),
    )
    with caplog.at_level(logging.INFO, logger="modulith.supervisor"):
        await _run_until_logged(sup, line, caplog)

    assert _levels_for(caplog, line) == {logging.INFO}


@pytest.mark.real_process
async def test_crash_traceback_reaches_the_operator_with_the_exit_message(caplog) -> None:
    """A crash-looping worker must show WHY it crashed, not only that it did.

    The restart message is a warning, so it survived on its own; the traceback
    explaining it was forwarded at INFO and dropped, leaving an operator with
    "exited with code 1; restarting" repeating and nothing to act on.
    """
    sup = Supervisor(
        [WorkerSpec("billing", "fakeapp", 9001)],
        command_builder=lambda spec, port: [
            sys.executable,
            "-c",
            "raise RuntimeError('billing module blew up')",
        ],
        restart_initial_delay=0.05,
    )
    with caplog.at_level(logging.WARNING, logger="modulith.supervisor"):
        await _run_until_logged(sup, "exited with code 1", caplog)

    messages = [r.getMessage() for r in caplog.records]
    assert any("billing module blew up" in m for m in messages), messages
    assert any("exited with code 1" in m for m in messages), messages


# ---------------------------------------------------------------------------
# _line_level (unit)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("INFO:     Application startup complete.", logging.INFO),
        ("warning:  low disk", logging.WARNING),
        ("CRITICAL: out of file descriptors", logging.CRITICAL),
        # Prose whose first colon is not a level must not be misread as one.
        ("Traceback (most recent call last):", logging.ERROR),
        ("module 'reports' exposes no 'router' attribute", logging.ERROR),
        ("NOTICE: something a level name does not cover", logging.ERROR),
    ],
)
def test_line_level_reads_only_standard_level_tokens(text: str, expected: int) -> None:
    assert _line_level(text, logging.ERROR) == expected
