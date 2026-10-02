"""The shape every broker adapter reports a dead-lettered delivery in.

``modulith broker dead-letter`` reads these from any adapter that offers

- ``list_dead_letters(*, after: tuple[datetime, str] | None = None, limit: int = 100)``
  returning ``list[DeadLetter]`` ordered by ``(created_at, id)``; ``after`` is
  the ``cursor`` of the last entry of the previous page.
- ``retry_dead_letters()`` returning how many deliveries it resubmitted, each
  one only to the consumer group whose delivery died.

Neither method is part of the public ``Broker`` protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class DeadLetterRetryRefused(Exception):
    """A broker declined to resubmit some dead letters, and says why in its message."""


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """One delivery that exhausted its attempts, addressed to one consumer group."""

    id: str
    target: str
    consumer_group: str
    event_type: str | None
    attempts: int
    last_error: str | None
    created_at: datetime

    @property
    def cursor(self) -> tuple[datetime, str]:
        """The ``after`` value that resumes a listing just past this entry."""
        return (self.created_at, self.id)
