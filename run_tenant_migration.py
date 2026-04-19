#!/usr/bin/env python3
"""
Apply Alembic migrations for one tenant (UUID) schema in the same DB as ``DB_*``.

Run after ``scripts/init_public_database.py`` when repairing a company schema, or when the
schema was created without running migrations. Normally company creation runs this via
``chalicelib/controllers/tenant/db.py``.

Usage (from ``fastapi_backend/``)::

    poetry run python run_tenant_migration.py <company_identifier_uuid>
"""
import sys
from alembic import command
from alembic.config import Config

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 run_tenant_migration.py <schema_name>", file=sys.stderr)
        sys.exit(1)
    
    schema_name = sys.argv[1]
    
    # Load Alembic config
    cfg = Config("chalicelib/alembic_tenant.ini")
    cfg.set_main_option("TENANT_SCHEMAS", schema_name)
    
    # Run migrations
    command.upgrade(cfg, "head")
    print(f"✅ Tenant schema '{schema_name}' migrated successfully")
