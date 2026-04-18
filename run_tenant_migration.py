#!/usr/bin/env python3
"""
Helper script to run tenant migrations from the Go backend.
Usage: python3 run_tenant_migration.py <schema_name>
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
