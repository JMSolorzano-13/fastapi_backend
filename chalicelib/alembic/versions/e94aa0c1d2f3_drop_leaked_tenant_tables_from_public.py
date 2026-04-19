"""Drop per-tenant business tables accidentally created in public.

Revision ID: e94aa0c1d2f3
Revises: f3a9c1e2b4d8
Create Date: 2026-04-19

Tenant DDL belongs in each company UUID schema (see ``alembic_tenant``).
Legacy public revisions (e.g. reboot) may have created these in ``public``;
they must be removed so only shared tables remain there.
"""

from alembic import op
from sqlalchemy import text

revision = "e94aa0c1d2f3"
down_revision = "f3a9c1e2b4d8"
branch_labels = None
depends_on = None

# Tables that must exist only inside tenant schemas (not exhaustive of history,
# but matches current TenantBase models + common leak list from cloud audits).
_TENANT_TABLES_IN_PUBLIC = (
    "add_sync_request",
    "attachment",
    "cfdi",
    "cfdi_export",
    "cfdi_relation",
    "nomina",
    "payment",
    "payment_relation",
    "poliza",
    "poliza_cfdi",
    "poliza_movimiento",
    "sat_query",
    "user_config",
)


def upgrade():
    for name in _TENANT_TABLES_IN_PUBLIC:
        op.execute(text(f'DROP TABLE IF EXISTS public."{name}" CASCADE'))


def downgrade():
    pass
