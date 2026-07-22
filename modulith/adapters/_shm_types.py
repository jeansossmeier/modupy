"""Small value types shared by the durable SHM store modules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Identity and SQLite-allocated order of a durable publication."""

    publication_id: str
    sequence: int


@dataclass(frozen=True, slots=True)
class ClaimToken:
    """Opaque fencing token for one stable delivery and claim generation."""

    delivery_id: int
    generation: int

    def __str__(self) -> str:
        return f"cold:{self.delivery_id}:{self.generation}"

    @classmethod
    def decode(cls, value: ClaimToken | str) -> ClaimToken:
        """Decode only a complete delivery-generation token."""
        if isinstance(value, ClaimToken):
            return value
        parts = value.split(":")
        if len(parts) != 3 or parts[0] != "cold":
            raise ValueError("a valid claim token is required")
        try:
            delivery_id = int(parts[1])
            generation = int(parts[2])
        except ValueError as exc:
            raise ValueError("a valid claim token is required") from exc
        if delivery_id < 1 or generation < 1:
            raise ValueError("a valid claim token is required")
        return cls(delivery_id=delivery_id, generation=generation)


def require_consumer_name(value: object) -> str:
    """Return a nonempty owner name or reject an unusable fencing identity."""
    if not isinstance(value, str):
        raise TypeError("consumer_name must be a string")
    if not value:
        raise ValueError("consumer_name must not be empty")
    return value
