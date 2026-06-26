"""Scheme-based broker dispatch registry.

Brokers register under URI schemes ("kafka", "sqs", "rabbitmq") and
publish() routes to the right one by parsing the target string. This
mirrors the URL-scheme pattern from stdlib (``urllib.parse``) and
SQLAlchemy dialects — a familiar shape for Python developers.

Plugin authors register their brokers in the modulith_register_brokers
hook; user code triggers dispatch via @externalized annotations on
event classes. The registry itself stays config-free: every adapter
manages its own configuration.
"""

from __future__ import annotations

import logging

from .protocols import Broker

logger = logging.getLogger(__name__)


class UnknownBrokerError(KeyError):
    """Raised when an event targets a scheme with no registered broker.

    Subclass of KeyError so existing ``except KeyError`` handlers catch
    it, but with a clearer name when raised explicitly.
    """


class DuplicateBrokerError(ValueError):
    """Raised when two plugins try to register the same scheme.

    Silent overwrites would mask plugin conflicts and make debugging
    miserable, so we fail loudly. Plugins that genuinely want to
    replace an existing broker call ``unregister`` first.
    """


class BrokerRegistry:
    """Routes outbound events to registered brokers by URI scheme.

    Targets follow ``scheme:destination`` format. The scheme selects the
    broker; the destination is opaque to the registry — it's whatever
    the broker needs (topic, queue, exchange + routing key).
    """

    def __init__(self) -> None:
        # Plain dict, not defaultdict — we want explicit registration
        # so duplicates fail loudly rather than silently overwrite.
        self._brokers: dict[str, Broker] = {}

    def register(self, scheme: str, broker: Broker) -> None:
        """Register a broker for a URI scheme.

        Raises DuplicateBrokerError if the scheme is already taken.
        """
        if scheme in self._brokers:
            existing = type(self._brokers[scheme]).__name__
            raise DuplicateBrokerError(
                f"scheme {scheme!r} is already registered to {existing}; "
                f"call unregister({scheme!r}) first to replace it"
            )
        self._brokers[scheme] = broker
        logger.debug(
            "registered broker %s for scheme %r",
            type(broker).__name__,
            scheme,
        )

    def unregister(self, scheme: str) -> None:
        """Remove a registered broker. No-op if scheme isn't registered."""
        self._brokers.pop(scheme, None)

    def get(self, scheme: str) -> Broker:
        """Look up a broker by scheme.

        Raises UnknownBrokerError with a helpful list of registered
        schemes if the lookup fails.
        """
        try:
            return self._brokers[scheme]
        except KeyError:
            raise UnknownBrokerError(
                f"no broker registered for scheme {scheme!r}; "
                f"known schemes: {sorted(self._brokers)}"
            ) from None

    async def publish(
        self,
        target: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Dispatch a payload via the broker selected by the target's scheme.

        ``target`` must be in ``scheme:destination`` form. Splits on the
        first colon, so destinations may themselves contain colons
        (e.g. AMQP ``exchange:routing.key``).
        """
        scheme, _, destination = target.partition(":")
        if not destination:
            raise ValueError(f"invalid broker target {target!r}; expected 'scheme:destination'")
        broker = self.get(scheme)
        await broker.publish(destination, payload, headers)

    async def close_all(self) -> None:
        """Release resources for every registered broker on shutdown.

        Closes every broker even if some raise — partial cleanup is
        better than aborting on the first failure during shutdown.
        Errors are logged, not propagated.
        """
        for scheme, broker in self._brokers.items():
            try:
                await broker.close()
            except Exception:
                logger.exception(
                    "broker %s (scheme %r) failed to close cleanly",
                    type(broker).__name__,
                    scheme,
                )

    def schemes(self) -> list[str]:
        """Return sorted list of registered schemes — useful for diagnostics."""
        return sorted(self._brokers)
