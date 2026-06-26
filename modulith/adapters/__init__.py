"""Storage and broker adapters.

Each adapter is a self-contained module conforming to one of the driver
protocols (PublicationStore, EventSerializer, Broker) or registering as
a hook implementation (modulith_register_brokers).

Adapters are technically optional — they import their backend libraries
lazily so users who don't need them don't pay the dependency cost.

Built-in adapters:
  - postgres_outbox.py  — PublicationStore for Postgres + SQLAlchemy
  - redis_broker.py     — Broker for Redis Streams (Phase 2)
  - kafka_broker.py     — Broker for Kafka (Phase 4)

Each ships as an extra: pip install modulith[postgres], etc.
"""
