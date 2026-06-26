"""Per-module worker: a FastAPI app containing only one module's surface.

Used by uvicorn in process-per-module mode. The supervisor spawns one
of these per module; uvicorn calls create_app() to get the ASGI app.

Implementation status: SKELETON. ~80 lines when complete.

Invoked as:
    uvicorn modulith._worker:create_app --factory \
        --host 127.0.0.1 --port 9001 \
        --env MODULITH_MODULE=orders \
        --env MODULITH_APP_PACKAGE=myapp

Critical correctness: the worker imports ONLY the configured module
(plus the contracts module). Other modules are NOT imported. This is
what gives each worker its own GIL — it has its own process, its own
import graph, its own event loop, its own memory.

Cross-module events flow through the broker, not in-memory dispatch.
The runtime's event bus is replaced (or wrapped) to publish to the
broker for any event whose listeners aren't local to this worker.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("modulith.worker")


def create_app():
    """Build a FastAPI app exposing only this worker's module.

    Called by uvicorn via --factory. Reads configuration from env vars
    so each worker process has its own context.

    IMPLEMENTATION TODO:

    1. Read env vars:
       - MODULITH_MODULE: which module this worker hosts (required)
       - MODULITH_APP_PACKAGE: the application package (required)
       - MODULITH_BROKER: broker URL for cross-module events
       - All standard MODULITH_* config

    2. Configure modulith for this worker:
       from modulith import configure
       configure(
           package=os.environ["MODULITH_APP_PACKAGE"],
           # Disable auto-discovery — we'll register one module manually.
           auto_discover=False,
           # Topology is processes here; the runtime knows to route
           # cross-module events through the broker.
           topology="processes",
       )

    3. Import the contracts module (always, for event types):
       importlib.import_module(f"{app_package}.contracts")

    4. Import only this worker's module:
       module = importlib.import_module(f"{app_package}.{module_name}")

    5. Build the FastAPI app:
       from fastapi import FastAPI
       app = FastAPI(title=f"modulith-{module_name}")

    6. Mount the module's router if it has one:
       router = getattr(module, "router", None)
       if router is not None:
           app.include_router(router, prefix=f"/{module_name}")

    7. Add the standard health endpoint:
       @app.get("/health")
       async def health():
           return {"status": "ok", "module": module_name}

    8. Return app.

    The runtime, plugin manager, and event bus are constructed by
    modulith's normal bootstrap path. The differences from single-process:
    - auto_discover=False so we don't import other modules
    - The event bus's publish() routes through the broker for events
      whose listeners are not local

    ROUTING DETAILS:
    Each worker subscribes to its consumed-event topics on the broker
    at startup (via the manifest's `consumes` list, or via runtime
    inspection of registered listeners). When a cross-module event is
    published from this worker, the runtime sees no local listeners,
    falls through to broker publish on topic
    f"modulith.events.{event_type_qualname}".

    Subscriptions deliver events back through the broker's consumer
    callback, which then runs through the local event bus to dispatch
    to listeners in this worker's module.
    """
    raise NotImplementedError("Phase 3 — see TODO above")


__all__ = ["create_app"]
