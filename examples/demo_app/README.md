# modulith demo — the shop

A minimal but complete modular monolith built with modulith. Three modules
collaborate purely through events:

```
POST /orders
   │
   ▼
┌──────────┐  OrderPlaced   ┌────────────┐  StockReserved  ┌────────────────┐
│  orders  │ ─────────────▶ │ inventory  │ ──────────────▶ │ notifications  │
└──────────┘                └────────────┘                 └────────────────┘
```

No module imports another module's code. They share the event vocabulary in
`shop/contracts/events.py` and communicate through the in-memory event bus.
Adding, removing, or splitting a module out to its own process changes none of
the others.

## Layout

```
shop/
├── contracts/
│   └── events.py            # OrderPlaced, StockReserved  (shared vocabulary)
├── orders/
│   ├── __init__.py          # place_order() → publishes OrderPlaced
│   ├── _manifest.py         # declared contract (verified at startup)
│   └── api.py               # FastAPI router: POST /orders
├── inventory/
│   ├── __init__.py          # @listener reserve_stock → publishes StockReserved
│   └── _manifest.py
├── notifications/
│   ├── __init__.py          # @listener notify_customer
│   └── _manifest.py
└── main.py                  # FastAPI app (includes the orders router)
```

## Run it

From this directory (`examples/demo_app`):

```bash
pip install 'modulith[fastapi,cli]'

# Run the app (cwd is on the import path, so `shop` resolves):
uvicorn shop.main:app --reload
#   …or via the CLI, which adds the modulith banner:
modulith dev shop.main:app

# In another terminal:
curl -X POST localhost:8000/orders \
     -H 'content-type: application/json' \
     -d '{"customer_id": "c-1", "total": 19.99}'
# → {"order_id": "…"}  — and the event chain fans out across the modules.
```

## Inspect it with the CLI

Run these from this directory — the `[tool.modulith]` section in
`pyproject.toml` lets the CLI auto-detect the `shop` package (set
`PYTHONPATH=.` if `shop` isn't installed):

```bash
modulith info        # detected package, modules, manifests, plugins
modulith verify      # boundary checks — this demo passes clean
modulith docs        # Mermaid architecture + event-flow diagrams + module canvases
```

## What it demonstrates

- **Auto-discovery** — modules are subpackages of `shop`; listeners register
  with no wiring code.
- **Event-driven boundaries** — cross-module communication is `publish()` +
  `@listener`, never a direct import.
- **The contracts module** — shared event types live in `shop/contracts`, so
  producers and consumers depend on a schema, not on each other.
- **Manifests** — each module declares what it publishes/consumes; modulith
  verifies that against reality at startup (a listener that fails to register
  aborts the boot instead of silently dropping events).
- **Going multi-process** — flip to `topology = "processes"` in
  `pyproject.toml` (and configure a broker) to run each module in its own
  worker behind the reverse proxy, with the same module code.
