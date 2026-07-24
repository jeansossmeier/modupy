# Deployment Guide

This guide covers scaling modulith from a single-process monolith to a distributed topology of separate worker processes.

---

## Single-Process Monolith (Default)

The simplest deployment: all modules run in one process with an in-memory event bus.

```bash
pip install 'modulith[fastapi,cli]'
MODULITH_BROKER=memory uvicorn myapp.main:app --workers 1
```

**Characteristics:**
- No infrastructure required
- Events are in-memory; lost on crash
- Listeners are synchronous (or wrapped async in an event loop)
- Ideal for: development, testing, non-critical background work

**Trade-off:** No durability. A crash between publishing an event and dispatching it to listeners loses the event.

---

## Durable Single-Process (Outbox Pattern)

Add persistence without splitting processes: publish events atomically with your domain transaction.

In `myapp/main.py`:
```python
from modulith import configure
from modulith.builtin import outbox
from modulith.adapters.postgres_outbox import PostgresPublicationStore
from modulith.serializers import JsonEventSerializer
from sqlalchemy.ext.asyncio import create_async_engine

async_engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
store = PostgresPublicationStore(engine=async_engine)
outbox.configure(store=store, serializer=JsonEventSerializer())
configure(outbox="postgres")

app = ... # your FastAPI or ASGI app
```

Then run:
```bash
pip install 'modulith[fastapi,cli,postgres]'
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 1
```

**What changes:**
- Event publication is deferred until your domain transaction commits (the outbox pattern).
- The event row and your domain row are persisted in the same transaction.
- After commit, the outbox dispatches the event to listeners in a background loop.

**Characteristics:**
- Events persist in the database; recoverable after crash
- Listeners must be idempotent (at-least-once delivery)
- Single point of failure: the database
- Ideal for: critical transactional workflows where losing an event is unacceptable

**Listeners and durability:**
- The outbox persists only the **first hop** of events (e.g., `orders` → `inventory`).
- If `inventory` publishes a downstream event (e.g., `StockReserved` → `notifications`), that hop is **not durable by default**—it rides the in-memory bus.
- For a durable cascade, listeners must bind their own session and publish inside it:
  ```python
  @listener(external=True)
  async def on_order_placed(event: OrderPlaced, session: AsyncSession) -> None:
      await session.execute(insert(Reservation).values(...))
      # Publish inside the session to make it durable:
      await publish(StockReserved(...), session=session)
      await session.commit()
  ```

**Outbox operations:**

```bash
# CLI requires the app to be running (stores are app-scoped):
modulith outbox status              # pending events
modulith outbox retry <event-id>    # retry a failed event
modulith outbox purge               # remove delivered events
modulith outbox dead-letter         # inspect stuck events
```

---

## Process-Per-Module Topology

Split modules across separate worker processes for independent scaling, deployment, and lifecycle. Events flow through a broker (database, Redis, or other transports).

### A. SQLite Database Broker (Zero Infrastructure)

```bash
pip install 'modulith[fastapi,cli,database]' aiosqlite
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=sqlite+aiosqlite:////path/to/broker.db \
  modulith run myapp.main:app --topology processes
```

> **Single-host only.** All workers must access the same SQLite file, so this mode works only on a single machine (or a shared filesystem volume). For multi-host deployments, use Postgres or Redis instead.

**What changes:**
- Each module runs in its own worker process.
- A reverse proxy (`modulith/proxy.py`) routes HTTP requests to the correct worker.
- Events published by one module are persisted to the broker database and fanned out to subscribed workers.
- Listeners in other modules run in their own processes and are invoked via async message dispatch.

**Characteristics:**
- No Redis or other external infrastructure needed
- Single database (SQLite or Postgres) is the inter-process broker
- Each module is independently restartable
- Ideal for: single-host deployments where process isolation improves fault tolerance and independent restartability without requiring external infrastructure

**Configuration:**

In `pyproject.toml`:
```toml
[tool.modulith]
broker = "database"
topology = "processes"

[tool.modulith.broker_options]
url = "sqlite+aiosqlite:////path/to/broker.db"  # or postgres://
```

Or via environment:
```bash
export MODULITH_BROKER=database
export MODULITH_BROKER_URL=sqlite+aiosqlite:////path/to/broker.db
modulith run myapp.main:app --topology processes
```

**Module assignment:**

By default, `modulith run --topology processes` detects subpackages and assigns one worker per submodule.

To customize worker counts per module, configure in `pyproject.toml`:
```toml
[tool.modulith.workers]
orders = 2         # 2 workers for orders module
inventory = 3      # 3 workers for inventory module
notifications = 1  # 1 worker for notifications (default)
```

Or override entirely via `--workers` JSON flag:
```bash
modulith run myapp.main:app --topology processes \
  --workers '{"orders": 2, "inventory": 3, "notifications": 1}'
```

### B. Redis Streams Broker

```bash
pip install 'modulith[fastapi,cli]'
docker run -d -p 6379:6379 redis:latest

MODULITH_BROKER=redis-streams \
  REDIS_URL=redis://localhost:6379 \
  modulith run myapp.main:app --topology processes
```

**Characteristics:**
- Scales to high throughput
- Automatic consumer group management
- Built-in dead-letter handling
- Ideal for: high-volume deployments with Redis infrastructure already in place

**⚠️ Redis Durability Caveat:** XADD MAXLEN `~` (approximate trimming) is blind to consumer-group PEL state. An undersized `max_stream_len` can permanently drop unacked entries, violating at-least-once delivery. Size `max_stream_len` well above worst-case backlog: **publish_rate × (consumer_downtime + processing_latency + reclaim_min_idle_ms)**. Default SHM and database brokers are NOT affected.

**Tuning:**

```bash
# Control batch size and dispatch concurrency:
export MODULITH_REDIS_BATCH_SIZE=100           # events per dispatch
export MODULITH_REDIS_CONCURRENCY=10           # parallel listeners
export MODULITH_REDIS_CLAIM_TIMEOUT_MS=30000   # pending claim timeout
```

### C. Postgres Broker (Advanced)

For deployments where Postgres is the primary data store and you want a single database:

```bash
pip install 'modulith[fastapi,cli,database]'
MODULITH_BROKER=database \
  MODULITH_BROKER_URL=postgresql+asyncpg://user:pass@localhost/mydb \
  modulith run myapp.main:app --topology processes
```

Uses the `broker_message` and `broker_subscription` tables with `FOR UPDATE SKIP LOCKED` claims for lock-free fan-out. Supports multi-host deployments.

**Tuning:**

```bash
export MODULITH_BROKER_BATCH_SIZE=50           # events per poll
export MODULITH_BROKER_CONCURRENCY=5           # parallel listeners
export MODULITH_BROKER_POLL_INTERVAL_MS=1000   # how often to check for new events
```

---

## Docker Deployment

### Single-Process

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install -e '.[fastapi,cli,postgres]'
COPY . .
EXPOSE 8000
CMD ["uvicorn", "myapp.main:app", "--host", "0.0.0.0"]
```

### Process-Per-Module

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install -e '.[fastapi,cli,database]'
COPY . .
EXPOSE 8000-8100
ENV MODULITH_BROKER=database
ENV MODULITH_BROKER_URL=postgresql+asyncpg://...
CMD ["modulith", "run", "myapp.main:app", "--topology", "processes"]
```

With `docker-compose.yml`:

```yaml
version: "3.9"
services:
  app:
    build: .
    ports:
      - "8000-8100:8000-8100"
    environment:
      MODULITH_BROKER: database
      MODULITH_BROKER_URL: postgresql+asyncpg://user:pass@postgres/mydb
    depends_on:
      - postgres
      - redis  # if using redis broker

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: modulith
      POSTGRES_PASSWORD: modulith
      POSTGRES_DB: mydb
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:  # optional
    image: redis:7

volumes:
  postgres_data:
```

---

## Kubernetes Deployment

### Single-Process Service

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: app
          image: myapp:1.0.0
          ports:
            - containerPort: 8000
          env:
            - name: MODULITH_OUTBOX
              value: "postgres"
            - name: MODULITH_DB_URL
              valueFrom:
                secretKeyRef:
                  name: db-creds
                  key: url
```

### Process-Per-Module StatefulSet

For deployments where workers need persistent local state or a stable identity:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: myapp-workers
spec:
  serviceName: myapp-workers
  replicas: 3
  template:
    spec:
      containers:
        - name: worker
          image: myapp:1.0.0
          ports:
            - containerPort: 8000
          env:
            - name: MODULITH_BROKER
              value: "database"
            - name: MODULITH_BROKER_URL
              valueFrom:
                secretKeyRef:
                  name: broker-creds
                  key: url
          # Each pod gets a stable hostname for assignment:
          # myapp-workers-0.myapp-workers.default.svc.cluster.local
```

Or with a custom controller that assigns modules per pod (more advanced—document as a separate guide if needed).

---

## Scaling Strategies

### Horizontal Scaling (Add More Workers)

For **single-process**, add replicas with a load balancer:

```bash
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
MODULITH_OUTBOX=postgres uvicorn myapp.main:app --workers 4 &
```

For **process-per-module**, increase worker counts via `--workers` JSON flag or `pyproject.toml` configuration:

```bash
# Run more inventory workers:
MODULITH_BROKER=database modulith run myapp.main:app --topology processes \
  --workers '{"inventory": 4}'
```

Or in `pyproject.toml`:
```toml
[tool.modulith.workers]
inventory = 4  # more workers for high-traffic module
```

### Vertical Scaling (Increase Worker Resources)

Adjust worker memory and CPU in Kubernetes/Docker:

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

Or scale the broker database (connection pooling, read replicas, etc.).

### Module Isolation (Fault Domain Separation)

Run fault-prone or high-load modules on separate infrastructure by adjusting worker counts and deployments:

**Approach 1: Separate instances with different worker configurations**

Configuration file 1 (`prod.toml` - for production nodes):
```toml
[tool.modulith.workers]
orders = 4
inventory = 4
notifications = 0    # not running on prod nodes
reporting = 0
```

Configuration file 2 (`batch.toml` - for batch nodes):
```toml
[tool.modulith.workers]
orders = 0           # not running on batch nodes
inventory = 0
notifications = 2
reporting = 2
```

Then deploy each with appropriate configuration (via environment or config override).

**Approach 2: All modules in one deployment with specific worker counts**

```toml
[tool.modulith.workers]
orders = 4      # high-load module
inventory = 4
notifications = 1
reporting = 1
```

---

## Health Checks and Monitoring

### Liveness Probe (Is the Worker Running?)

```bash
curl http://localhost:8000/health
# Returns 200 if the worker is ready.
```

### Readiness Probe (Is the Broker Connected?)

```bash
curl http://localhost:8000/ready
# Returns 200 if the worker is connected to the broker and accepting events.
```

### Event Metrics

If OpenTelemetry is enabled (`modulith[otel]`), spans are emitted for:
- `modulith.publish` — an event was published
- `modulith.listen` — an event was dispatched to a listener
- `modulith.outbox.dispatch` — the outbox dispatched a batch

Export spans to Prometheus, Jaeger, or your observability stack.

---

## Operational Playbooks

### Adding a New Module

1. Create the subpackage: `myapp/my_module/__init__.py`
2. Add manifest: `myapp/my_module/_manifest.py`
3. Restart the supervisor (in single-process, restart the app; in topology mode, restart the appropriate worker)

### Retiring a Module

1. Remove listeners via `@listener` markers (existing events are no-longer-delivered)
2. Stop publishing events to that module
3. Run outstanding events via `modulith outbox retry` if needed
4. Delete the subpackage
5. Restart

### Recovering from Broker Failure

**Database broker:**
- If the database is down, workers buffer events in memory and retry on reconnect
- Outbox events persist to the database; the background loop retries

**Redis broker:**
- Consumer groups are created by the first consumer; if all are down, events accumulate in Redis
- On restart, the supervisor reads pending entries and rebalances

### Graceful Shutdown

In Kubernetes:

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 5"]  # wait for in-flight requests
```

Modulith's supervisor handles SIGTERM and drains listeners before exit.

---

## Migration Path: Monolith → Processes → Microservices

1. **Start monolithic** — `MODULITH_BROKER=memory` (default)
   - Fast to iterate
   - All modules in one process

2. **Add durability** — `MODULITH_OUTBOX=postgres`
   - Same single-process deployment
   - Events now persist; listeners are at-least-once

3. **Split processes** — `MODULITH_BROKER=database --topology processes`
   - Modules now run in separate workers
   - Code doesn't change; listeners stay `@listener` decorated

4. **Extract microservice** — Move one module to a separate FastAPI app
   - Other modules send events via the broker
   - The extracted service subscribes and acts
   - Code is nearly identical; just remove the `@listener` decorator and hook the broker SDK directly

This path is why modulith exists: **every module is a potential microservice, but you pay that cost only when it's profitable.**

---

## Troubleshooting

### Events Not Delivered

1. Check that the listener is registered: `modulith info` → inspect `Listeners`
2. Verify the manifest declares the event: `_manifest.py` → check `consumes`
3. Inspect broker state:
   - Database: `SELECT * FROM broker_message WHERE status != 'delivered'`
   - Redis: `xinfo groups myapp-events`

### Worker Crash Loop

1. Check logs: `modulith run --topology processes 2>&1 | grep ERROR`
2. Verify broker connectivity: `modulith doctor`
3. Check disk space (SQLite needs it)

### High Latency

1. Tune batch size: `MODULITH_BROKER_BATCH_SIZE=200`
2. Increase concurrency: `MODULITH_REDIS_CONCURRENCY=20`
3. Profile with `modulith[otel]` and check span duration

---

## Reference: Broker Comparison

| Broker | Setup | Durability | Scale | Ideal For |
|---|---|---|---|---|
| `memory` | None | No | Single-process | Dev/test |
| `database` (SQLite) | Local file | Yes | Single-host process-per-module (shared filesystem required for multi-process) | Dev, single-host staging |
| `database` (Postgres) | Existing DB | Yes | Multi-host process-per-module | Production monolith and distributed |
| `redis-streams` | Docker/Cloud | Yes | High throughput, multi-host | High-load production |

---

## Next Steps

- For **detailed internal architecture**, see [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- For **API reference**, see [docs/API_REFERENCE.md](API_REFERENCE.md)
- For **working examples**, see [examples/demo_app](../examples/demo_app)
- For **testing**, see [Cookbook §Testing](COOKBOOK.md#testing)
