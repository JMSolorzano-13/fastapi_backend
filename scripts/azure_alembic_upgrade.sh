#!/usr/bin/env bash
# Run public-schema Alembic migrations against the DB in the current environment.
# Usage (from repo root in container or CI): ./scripts/azure_alembic_upgrade.sh
# Requires: DB_* env vars (or .env) and network path to PostgreSQL.
set -euo pipefail
cd "$(dirname "$0")/.."
exec alembic -c chalicelib/alembic.ini upgrade head
