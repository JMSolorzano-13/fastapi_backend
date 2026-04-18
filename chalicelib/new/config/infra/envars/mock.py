import os

STRIPE = bool(int(os.environ.get("MOCK_STRIPE", 0)))
ODOO = bool(int(os.environ.get("MOCK_ODOO", 0)))
