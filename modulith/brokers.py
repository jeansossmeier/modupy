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

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .protocols import Broker, Consumer

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
        Errors are logged, not propagated. ``asyncio.CancelledError`` is
        caught too (it subclasses BaseException since 3.8): one broker's
        close() being cancelled — e.g. a shutdown wrapped in
        ``asyncio.wait_for`` timing out on a hanging client — must not
        skip the close() of every broker registered after it.
        """
        for scheme, broker in self._brokers.items():
            try:
                await broker.close()
            except (Exception, asyncio.CancelledError):
                logger.exception(
                    "broker %s (scheme %r) failed to close cleanly",
                    type(broker).__name__,
                    scheme,
                )

    def schemes(self) -> list[str]:
        """Return sorted list of registered schemes — useful for diagnostics."""
        return sorted(self._brokers)


# ---------------------------------------------------------------------------
# Consumer side (process-per-module cross-process delivery)
# ---------------------------------------------------------------------------


class UnknownConsumerError(KeyError):
    """Raised when a scheme has no registered consumer factory.

    Subclass of KeyError so existing ``except KeyError`` handlers catch it,
    but with a clearer name when raised explicitly. Mirrors
    ``UnknownBrokerError`` on the producer side.
    """


class DuplicateConsumerError(ValueError):
    """Raised when two plugins register a consumer factory for one scheme.

    Silent overwrites would mask plugin conflicts; we fail loudly. Mirrors
    ``DuplicateBrokerError``. Plugins that genuinely want to replace an
    existing factory call ``unregister`` first.
    """


@dataclass(frozen=True)
class ConsumerSpec:
    """Everything a consumer factory needs to build a worker's consumer.

    The worker assembles this per module at startup and hands it to
    ``ConsumerRegistry.build``. The factory pulls its concrete backend object
    (the one holding the connection/engine/pool) out of ``broker_registry`` by
    ``scheme`` — the same object registered on the producer side by
    ``modulith_register_brokers`` — so one backend serves both halves.
    """

    scheme: str
    module_name: str
    group: str
    consumer_name: str
    targets: tuple[str, ...]
    bus: Any
    serializer: Any
    broker_registry: BrokerRegistry


# A consumer factory turns a ConsumerSpec into a ready (not-yet-started)
# Consumer. Adapters register one per scheme via modulith_register_consumers.
ConsumerFactory = Callable[[ConsumerSpec], Consumer]


class ConsumerRegistry:
    """Routes per-module consumer construction to registered factories by scheme.

    The consumer-side mirror of ``BrokerRegistry``. Brokers publish; consumers
    subscribe. A worker in process-per-module topology looks up the factory for
    the configured scheme and calls ``build`` to get a Consumer for its module.
    Config-free, exactly like BrokerRegistry — each adapter reads its own config
    when its factory runs.
    """

    def __init__(self) -> None:
        # Plain dict, not defaultdict — explicit registration so duplicates
        # fail loudly rather than silently overwrite.
        self._factories: dict[str, ConsumerFactory] = {}

    def register(self, scheme: str, factory: ConsumerFactory) -> None:
        """Register a consumer factory for a URI scheme.

        Raises DuplicateConsumerError if the scheme is already taken.
        """
        if scheme in self._factories:
            raise DuplicateConsumerError(
                f"scheme {scheme!r} already has a consumer factory; "
                f"call unregister({scheme!r}) first to replace it"
            )
        self._factories[scheme] = factory
        logger.debug("registered consumer factory for scheme %r", scheme)

    def unregister(self, scheme: str) -> None:
        """Remove a registered factory. No-op if scheme isn't registered."""
        self._factories.pop(scheme, None)

    def get(self, scheme: str) -> ConsumerFactory:
        """Look up a consumer factory by scheme.

        Raises UnknownConsumerError with the registered schemes if the lookup
        fails.
        """
        try:
            return self._factories[scheme]
        except KeyError:
            raise UnknownConsumerError(
                f"no consumer registered for scheme {scheme!r}; "
                f"known schemes: {sorted(self._factories)}"
            ) from None

    def build(self, scheme: str, spec: ConsumerSpec) -> Consumer:
        """Build a Consumer for ``scheme`` from ``spec`` via its factory."""
        if scheme != spec.scheme:
            raise ValueError(
                f"requested consumer scheme {scheme!r} does not match spec.scheme {spec.scheme!r}"
            )
        return self.get(scheme)(spec)

    def schemes(self) -> list[str]:
        """Return sorted list of registered schemes — useful for diagnostics."""
        return sorted(self._factories)
