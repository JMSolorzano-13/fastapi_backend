#!/usr/bin/env bash
# Initialize public PostgreSQL schema (Alembic) for FastAPI.
# Run from repo root or anywhere; delegates to Python (hybrid stamp + upgrade head).
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="${PWD}${PYTHONPATH:+:${PYTHONPATH}}"
exec poetry run python scripts/init_public_database.py
