"""The `modulith doctor` command implementation.

Reports operational and architectural health of a running modulith
application. Useful for both first-run sanity checks and ongoing
monitoring.

Implementation status: SKELETON. ~120 lines when complete.

Output sections:

  1. Boundary health — violation count, baseline drift over recent commits
  2. Process-split readiness — % of cross-module interactions via events
  3. Schema drift — events whose definitions changed without bump
  4. Outbox health — dead-letter count, oldest incomplete age
  5. Listener registration — declared vs actually registered

Each section either prints OK with summary stats or lists problems with
file:line references.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("modulith.doctor")


# ---------------------------------------------------------------------------
# Health report aggregation
# ---------------------------------------------------------------------------


@dataclass
class HealthCheck:
    """One named check with its status and findings."""

    name: str
    status: str  # "ok" | "warn" | "error"
    summary: str  # one-line summary
    details: list[str] = field(default_factory=list)  # multi-line if findings


@dataclass
class HealthReport:
    """Aggregated output of all checks."""

    checks: list[HealthCheck]

    @property
    def overall_status(self) -> str:
        """Worst status across all checks."""
        if any(c.status == "error" for c in self.checks):
            return "error"
        if any(c.status == "warn" for c in self.checks):
            return "warn"
        return "ok"


# ---------------------------------------------------------------------------
# Top-level doctor command
# ---------------------------------------------------------------------------


def run_doctor() -> HealthReport:
    """Run all health checks and return the aggregated report.

    IMPLEMENTATION TODO:
    Each check below contributes one HealthCheck. Run them all (errors
    in one don't prevent others from running). Return the aggregate.
    """
    checks: list[HealthCheck] = []
    checks.append(_check_boundary_health())
    checks.append(_check_split_readiness())
    checks.append(_check_schema_drift())
    checks.append(_check_outbox_health())
    checks.append(_check_listener_registration())
    return HealthReport(checks=checks)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _check_boundary_health() -> HealthCheck:
    """Run the verifier; report violation count and trend.

    IMPLEMENTATION TODO:
    1. Bootstrap modulith, collect modules.
    2. Run pm.hook.modulith_verify_module for each, aggregate violations.
    3. If a baseline file exists, compare to it.
    4. Status:
       - ok if no violations
       - warn if violations are all in the baseline (grandfathered)
       - error if there are new violations not in baseline
    5. Summary: e.g. "0 violations" or "12 violations (3 new)".
    """
    raise NotImplementedError("Phase 2 — see TODO above")


def _check_split_readiness() -> HealthCheck:
    """Compute the percentage of cross-module interactions via events.

    IMPLEMENTATION TODO:
    1. Walk all modules' source files with AST.
    2. Find calls that cross module boundaries:
       a. Direct function calls (foo.bar() where foo is another module)
       b. publish() calls (event-shaped)
    3. Score = 100 * publish_count / (publish_count + direct_call_count)
    4. Status:
       - ok if score > 80
       - warn if 50-80
       - error if < 50
    5. Details: top 5 modules with the most direct calls.

    This metric is the "are you ready to split this module into its own
    process?" number. A team that started with the right defaults sees
    near 100% from the start.
    """
    raise NotImplementedError("Phase 2")


def _check_schema_drift() -> HealthCheck:
    """Detect events whose definitions changed without a schema_version bump.

    IMPLEMENTATION TODO:
    Maintain a schema fingerprint cache (.modulith-schemas.json):
      - Key: event class fully-qualified name
      - Value: hash of class fields + types

    On run:
    1. For each @event-decorated class, compute the current hash.
    2. Compare to cached value.
    3. If changed and no schema_version attribute changed: warn.

    This is a v1.1 nice-to-have. Skip if pressed for time in v1.
    """
    raise NotImplementedError("Phase 2")


def _check_outbox_health() -> HealthCheck:
    """Query the outbox for stuck or dead-lettered events.

    IMPLEMENTATION TODO:
    1. If outbox is "memory": status=ok, summary="in-memory bus, no outbox".
    2. Otherwise: call modulith.builtin.outbox.status() to get counts.
    3. Status:
       - ok if 0 dead-lettered and oldest incomplete < 1 minute
       - warn if dead-lettered > 0 OR oldest incomplete > 5 minutes
       - error if dead-lettered > 100 OR oldest incomplete > 1 hour
    4. Details: top dead-lettered events with last_error.
    """
    raise NotImplementedError("Phase 2")


def _check_listener_registration() -> HealthCheck:
    """Compare manifest-declared listeners to actually-registered ones.

    IMPLEMENTATION TODO:
    1. For each module with a manifest:
       a. Get declared listeners from manifest.
       b. Check each is actually registered against the event bus.
       c. Find listeners that exist but aren't in any manifest (warn,
          not error — could be intentional).
    2. Status:
       - ok if all declared = all registered
       - error if any declared listener missing from bus

    Catches "module silently failed to import" — the worst kind of bug.
    See Gap 4 / §5.4 of SPEC.md.
    """
    raise NotImplementedError("Phase 2")


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


def render_report(report: HealthReport) -> str:
    """Render a HealthReport as colorful terminal output.

    IMPLEMENTATION TODO:
    For each check:
        ✓ name — summary    (green for ok)
        ⚠ name — summary    (yellow for warn)
        ✗ name — summary    (red for error)
    Followed by indented details if any.

    Use rich library if available, fall back to plain text.
    """
    raise NotImplementedError("Phase 2")


__all__ = [
    "HealthCheck",
    "HealthReport",
    "render_report",
    "run_doctor",
]
