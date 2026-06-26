"""The demo shop — a modular monolith built with modulith.

Three modules talk only through events:

    orders ──OrderPlaced──▶ inventory ──StockReserved──▶ notifications

No module imports another module's code; they share a vocabulary via the
``contracts`` module and communicate through the event bus. Run it with
``uvicorn shop.main:app`` from ``examples/demo_app`` and watch the modulith
banner auto-discover the modules and wire the listeners.
"""
