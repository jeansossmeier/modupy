import logging

from fastapi import FastAPI

from myapp.inventory import router as inventory_router
from myapp.orders import router as orders_router

logging.basicConfig(level=logging.INFO)  # so the banner below is visible

app = FastAPI()
app.include_router(orders_router, prefix="/orders")
app.include_router(inventory_router, prefix="/inventory")

# That's it. Modules auto-discovered. Listeners auto-registered.
# Transactional outbox available with two config lines (outbox, outbox_url).
