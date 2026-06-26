"""FastAPI entrypoint for the demo shop.

Run from ``examples/demo_app``::

    uvicorn shop.main:app --reload
    # or, with the CLI:
    modulith dev shop.main:app

modulith auto-detects the ``shop`` package, discovers the modules, registers
their listeners, and prints a startup banner. No bootstrap call is needed — the
runtime initializes lazily on the first publish.
"""

from __future__ import annotations

from fastapi import FastAPI

from shop.orders.api import router as orders_router

app = FastAPI(title="modulith demo shop")
app.include_router(orders_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
