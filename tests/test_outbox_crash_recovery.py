"""The outbox crash-recovery forcing function (IMPLEMENTATION_PLAN T1.4.5).

A publish process commits N events to the outbox and is hard-killed before
delivery; a fresh recover process must deliver all N via the retry loop's
crash sweep. Uses a file-based SQLite DB across real OS processes — the same
at-least-once guarantee that matters on Postgres, without needing Docker.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from crash_recovery_app import N_EVENTS  # type: ignore[import-not-found]

APP = Path(__file__).parent / "crash_recovery_app.py"


def _run(mode: str, db: Path, out: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(APP), mode, str(db), str(out)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_events_committed_before_crash_are_delivered_after_restart(tmp_path: Path) -> None:
    db = tmp_path / "crash.db"
    out = tmp_path / "delivered.txt"

    # Process 1: publish + commit, then hard-crash before delivery.
    pub = _run("publish", db, out)
    assert pub.returncode == 0, f"publish failed: {pub.stderr}"
    # Nothing delivered yet — the crash beat the dispatch.
    delivered_before = out.read_text().splitlines() if out.exists() else []
    assert delivered_before == []

    # Process 2: recover. The crash sweep delivers every committed publication.
    rec = _run("recover", db, out)
    assert rec.returncode == 0, f"recover failed: {rec.stderr}"

    delivered = sorted(int(line) for line in out.read_text().splitlines())
    # At-least-once: every event delivered, none lost.
    assert set(delivered) == set(range(N_EVENTS))
    # ...but at-least-once is not a license for unbounded duplication. A single
    # sweeper that marks rows complete must not re-deliver them in a storm
    # (e.g. a bug racing the sweep against itself, or never marking complete,
    # would deliver each event many times over). Bound it: well under 2x the
    # event count catches gross duplication while tolerating the rare legitimate
    # repeat. (Without this, a 10x-delivery regression stays green.)
    assert len(delivered) <= 2 * N_EVENTS, (
        f"excessive duplication: {len(delivered)} deliveries for {N_EVENTS} events"
    )
