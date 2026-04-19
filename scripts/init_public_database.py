#!/usr/bin/env python3
"""
Apply public-schema DDL and reference data via Alembic (same chain as local FastAPI start).

Use when PostgreSQL is empty or behind (new Azure Flexible Server, new Docker volume, etc.).
Requires ``fastapi_backend/.env`` with at least ``DB_*`` and the same keys as a normal API boot
(SQS, S3, …) because ``alembic/env.py`` imports application settings.

Hybrid DB: if the database was initialized only by the **Go** API (``schema_migrations`` has
``001`` but ``alembic_version`` is missing), this script runs ``alembic stamp head`` once so
Alembic does not re-apply catalog migrations that already exist.

Tenant UUID schemas are **not** handled here; they are created when a company is registered
(``chalicelib/controllers/tenant/db.py``) or manually::

    poetry run python run_tenant_migration.py <company_identifier_uuid>

Go SQL reference (embedded migrations + CSV seeds): ``go_backend/internal/db/``.
FastAPI public DDL lives in ``chalicelib/alembic/versions/``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    root = _repo_root()
    for name in (".env", ".env.local"):
        p = root / name
        if p.is_file():
            load_dotenv(p, override=False)
    load_dotenv(override=False)


def _db_connect_kwargs() -> dict[str, object]:
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "connect_timeout": 30,
    }


def _needs_go_hybrid_stamp() -> bool:
    """True only when Go applied 001 but Alembic never stamped (empty Azure DB must not touch this).

    PostgreSQL does not short-circuit ``EXISTS (SELECT … FROM schema_migrations)`` when the
    table is missing—the planner still resolves the relation and errors. So check in steps.
    """
    with psycopg2.connect(**_db_connect_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'schema_migrations'
                """
            )
            if not cur.fetchone():
                return False
            cur.execute("SELECT 1 FROM schema_migrations WHERE version = '001'")
            if not cur.fetchone():
                return False
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'alembic_version'
                """
            )
            if cur.fetchone():
                return False
    return True


def _run_poetry_alembic(args: list[str]) -> None:
    root = _repo_root()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    cmd = ["poetry", "run", "alembic", "-c", "chalicelib/alembic.ini", *args]
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


def main() -> int:
    _load_env()
    os.chdir(_repo_root())

    missing = [k for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if not os.environ.get(k)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        print("Set them in fastapi_backend/.env (see .env.local template).", file=sys.stderr)
        return 1

    try:
        if _needs_go_hybrid_stamp():
            print("Database has Go migrations without Alembic stamp; running: alembic stamp head", flush=True)
            _run_poetry_alembic(["stamp", "head"])
    except psycopg2.Error as e:
        print(f"PostgreSQL connection failed: {e}", file=sys.stderr)
        return 1

    print("Running: alembic upgrade head (public schema)", flush=True)
    try:
        _run_poetry_alembic(["upgrade", "head"])
    except subprocess.CalledProcessError:
        return 1

    print("Public schema migrations finished.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
