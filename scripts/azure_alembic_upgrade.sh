#!/usr/bin/env bash
# Run public-schema Alembic migrations against the DB in the current environment.
# Same logic as local ``start-local-fastapi.sh`` (hybrid Go stamp + ``upgrade head``).
# Usage: from ``fastapi_backend/``: ``./scripts/azure_alembic_upgrade.sh``
# Requires: ``DB_*`` in the environment (or ``.env``) and a full app ``.env`` so Alembic can import settings.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"
exec poetry run python scripts/init_public_database.py
