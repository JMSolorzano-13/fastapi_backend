#!/usr/bin/env python3
"""
One-shot SAT enqueue for all active companies (parity with Go ``cmd/cron -job all``).

Publishes the same JSON bodies as ``go_backend/cmd/cron`` for:

- ``SAT_METADATA_REQUESTED`` → ``SQS_SEND_QUERY_METADATA``
- ``SAT_COMPLETE_CFDIS_NEEDED`` (ISSUED + RECEIVED) → ``SQS_COMPLETE_CFDIS``

Transport matches ``scripts/sat/_runtime.py`` (Azure Service Bus when connection strings
and ``CLOUD_PROVIDER=azure``, else SQS / LocalStack).

Intended entrypoint for ACA Job when ``terraform_azure_siigofiscal`` uses
``compute.sat_sync_entrypoint = "fastapi_python"`` (image = FastAPI API image).

Usage::

    cd fastapi_backend && poetry run python -m scripts.sat.aca_daily_sat_sync
    poetry run python -m scripts.sat.aca_daily_sat_sync --dry-run
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import create_engine, text

from scripts.sat._runtime import configure_path_and_env, send_queue_json, transport_kind

configure_path_and_env()

from chalicelib.new.config.infra import envars
from chalicelib.schema import connection_uri

_ACTIVE_COMPANIES_SQL = """
SELECT c.id, c.identifier, c.rfc, c.workspace_id
FROM public.company AS c
JOIN public.workspace AS w ON w.id = c.workspace_id
WHERE c.active = true
  AND c.have_certificates = true
  AND w.valid_until IS NOT NULL
  AND w.valid_until > NOW()
  AND c.rfc IS NOT NULL
  AND TRIM(c.rfc) <> ''
  AND c.workspace_id IS NOT NULL
ORDER BY c.id
"""


def _list_active_companies() -> list[dict]:
    engine = create_engine(connection_uri, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(text(_ACTIVE_COMPANIES_SQL)).fetchall()
    engine.dispose()
    return [
        {
            "id": int(r[0]),
            "identifier": str(r[1]),
            "rfc": str(r[2]) if r[2] is not None else "",
            "workspace_id": int(r[3]),
        }
        for r in rows
    ]


def _metadata_payload(row: dict) -> dict:
    return {
        "identifier": str(uuid.uuid4()),
        "company_identifier": row["identifier"],
        "company_rfc": row["rfc"],
        "manually_triggered": False,
        "wid": row["workspace_id"],
        "cid": row["id"],
    }


def _complete_payload(row: dict, download_type: str) -> dict:
    return {
        "identifier": str(uuid.uuid4()),
        "company_identifier": row["identifier"],
        "company_rfc": row["rfc"],
        "download_type": download_type,
        "is_manual": False,
        "start": None,
        "end": None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue daily SAT metadata + complete-CFDI work.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List companies and counts only; do not send queue messages.",
    )
    args = parser.parse_args()

    print(f"aca_daily_sat_sync: transport={transport_kind()} dry_run={args.dry_run}")

    companies = _list_active_companies()
    print(f"active companies (with certs + valid workspace): {len(companies)}")
    if args.dry_run:
        for c in companies[:20]:
            print(f"  id={c['id']} identifier={c['identifier'][:8]}… rfc={c['rfc']!r}")
        if len(companies) > 20:
            print(f"  … {len(companies) - 20} more")
        return

    meta_q = envars.SQS_SEND_QUERY_METADATA
    complete_q = envars.SQS_COMPLETE_CFDIS
    sent_meta = 0
    sent_complete = 0
    for row in companies:
        send_queue_json(meta_q, _metadata_payload(row))
        sent_meta += 1
        send_queue_json(complete_q, _complete_payload(row, "ISSUED"))
        send_queue_json(complete_q, _complete_payload(row, "RECEIVED"))
        sent_complete += 2
    print(f"done: metadata_messages={sent_meta} complete_cfdi_messages={sent_complete}")


if __name__ == "__main__":
    main()
