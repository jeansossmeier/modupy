"""Faults on demand for the stand-in models, so one AI module can misbehave at a time.

``STUDIO_<MODULE>_FAULT`` set to ``hang``, ``block`` or ``crash`` makes every
call to that module's model fail that way, for example
``STUDIO_MODERATION_FAULT=crash``. Any other value that is not empty stops the
module from importing.
"""

import asyncio
import os
import signal
import time

FAULTS = ("hang", "block", "crash")


def fault_for(module: str) -> str:
    name = f"STUDIO_{module.upper()}_FAULT"
    fault = os.environ.get(name, "")
    if fault not in ("", *FAULTS):
        raise ValueError(f"{name} must be empty or one of {', '.join(FAULTS)}, got {fault!r}")
    return fault


def fail_now(fault: str) -> None:
    """Fail as ``fault`` names on the calling thread; return at once when it is empty."""
    if fault == "crash":
        # What the OOM killer, or a crash in native code, does to a process.
        os.kill(os.getpid(), signal.SIGKILL)
    elif fault:
        # A sync call that never returns holds its thread, whether it hangs or blocks.
        time.sleep(3600)


async def fail(fault: str) -> None:
    """Fail as ``fault`` names, the way an awaited model call does."""
    if fault == "hang":
        # A request the provider accepts and never answers.
        await asyncio.Event().wait()
    # block and crash fail as on any thread, here the event loop's: a blocking
    # SDK call inside async code holds it.
    fail_now(fault)
