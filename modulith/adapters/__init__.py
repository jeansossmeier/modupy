"""Storage and broker adapters.

Each adapter is a self-contained module conforming to one of the driver
protocols (PublicationStore, EventSerializer, Broker) or registering as
a hook implementation (modulith_register_brokers).

Adapters are technically optional — users who don't need one don't pay its
dependency cost. The Redis broker adapter imports its backend library
lazily inside ``__init__``; the Postgres outbox adapter imports SQLAlchemy
at module import time (its ORM schema classes need it) but raises an
ImportError pointing at the ``modupy[postgres]`` extra when it is
missing.

Shipped adapters:
  - postgres_outbox.py  — PublicationStore for Postgres + SQLAlchemy
  - redis_broker.py     — Broker for Redis Streams

Each ships as an extra: pip install modupy[postgres], etc.

Planned (NOT shipped — no module, no extra): a Kafka Broker adapter
(roadmap Phase 4; see SPEC §10.3). It is deliberately not advertised in
pyproject.toml until the adapter lands.
"""
