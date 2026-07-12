"""Storage and broker adapters.

Each adapter is a self-contained module conforming to one of the driver
protocols (PublicationStore, EventSerializer, Broker) or registering as
a hook implementation (modulith_register_brokers).

Adapters are technically optional — users who don't need one don't pay its
dependency cost. The broker adapters (redis, kafka) import their backend
libraries lazily inside ``__init__``; the Postgres outbox adapter imports
SQLAlchemy at module import time (its ORM schema classes need it) but raises
an ImportError pointing at the ``modulith[postgres]`` extra when it is
missing.

Built-in adapters:
  - postgres_outbox.py  — PublicationStore for Postgres + SQLAlchemy
  - redis_broker.py     — Broker for Redis Streams (Phase 2)
  - kafka_broker.py     — Broker for Kafka (Phase 4)

Each ships as an extra: pip install modulith[postgres], etc.
"""
