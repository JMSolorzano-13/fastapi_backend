#!/bin/bash
# Helper script to run tenant migrations from the Go backend
# Usage: ./run_tenant_migration.sh <schema_name>

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: ./run_tenant_migration.sh <schema_name>" >&2
    exit 1
fi

SCHEMA_NAME="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"

# Use poetry to run the migration in the correct venv
exec poetry run python3 -c "
from alembic import command
from alembic.config import Config

cfg = Config('chalicelib/alembic_tenant.ini')
cfg.set_main_option('TENANT_SCHEMAS', '$SCHEMA_NAME')
command.upgrade(cfg, 'head')
print('✅ Tenant schema \\'$SCHEMA_NAME\\' migrated successfully')
"
