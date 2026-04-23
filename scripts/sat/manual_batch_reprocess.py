#!/usr/bin/env python3
"""
Re-enqueue SENT / DOWNLOADED ``sat_query`` rows for verify or package processing.

Same JSON contract as ``backend/manual_batch_reprocess.py``; configuration from
``fastapi_backend/.env`` via ``envars`` and ``connection_uri``.

Tenant: pass ``--company-identifier`` (``public.company.identifier``) or set
``COMPANY_IDENTIFIER`` / ``TENANT_ID`` in the environment.

Transport: SQS (boto3) vs Azure Service Bus — same rules as ``scripts/sat/_runtime.py``.

**Postgres:** uses ``chalicelib.schema.connection_uri`` (``DB_HOST``, ``DB_PORT``, ``DB_NAME``, ``DB_USER``, ``DB_PASSWORD`` from the environment after ``load_dotenv``). ``Company not found`` almost always means **this process is not connected to the same DB as Azure** (e.g. ``.env`` has ``DB_HOST=localhost`` for a local Postgres without that ``public.company`` row).

**Auth / JWT:** este script usa ``configure_path_and_env(operator_script=True)``; no hace falta ``JWT_SECRET`` en laptop aunque ``LOCAL_INFRA=0`` y ``AUTH_BACKEND=local_jwt`` en ``.env``.

**Azure desde laptop (túnel SSH):** levanta el túnel (ej. ``-L 5433:<azure_fqdn>:5432``) y **sobrescribe** ``DB_*`` en la misma línea del comando (las variables ya definidas en el shell tienen prioridad sobre ``.env`` con ``python-dotenv`` por defecto):

    cd fastapi_backend
    DB_HOST=127.0.0.1 DB_PORT=5433 DB_NAME=ezaudita_db DB_USER=solcpuser \\
      PGSSLMODE=require DB_PASSWORD='<misma que ACA/Key Vault; caracteres como + se codifican en URI vía chalicelib.schema>' \\
      LOCAL_INFRA=0 CLOUD_PROVIDER=azure \\
      poetry run python scripts/sat/manual_batch_reprocess.py --company-identifier '<tenant_uuid>'

Usage:
    cd fastapi_backend && poetry run python scripts/sat/manual_batch_reprocess.py
    poetry run python scripts/sat/manual_batch_reprocess.py --company-identifier <uuid> \\
        --batch-size 3 --timeout 600
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import create_engine, text

from scripts.sat._runtime import configure_path_and_env, send_queue_raw, transport_kind

configure_path_and_env(operator_script=True)

from chalicelib.new.config.infra import envars
from chalicelib.schema import connection_uri

MAX_AGE_HOURS = 72
POLL_INTERVAL_S = 5

INTERMEDIATE_STATES = frozenset(
    {"SENT", "DOWNLOADED", "PROCESSING", "TO_DOWNLOAD", "DELAYED", "DRAFT"}
)

TERMINAL_STATES = frozenset(
    {
        "PROCESSED",
        "ERROR",
        "ERROR_TOO_BIG",
        "ERROR_IN_CERTS",
        "ERROR_SAT_WS_INTERNAL",
        "ERROR_SAT_WS_UNKNOWN",
        "TIME_LIMIT_REACHED",
        "SUBSTITUTED",
        "SPLITTED",
        "MANUALLY_CANCELLED",
        "INFORMATION_NOT_FOUND",
    }
)

SUCCESS_STATES = frozenset({"PROCESSED", "TO_DOWNLOAD", "DOWNLOADED"})


class BatchReprocessor:
    def __init__(self, tenant_id: str, batch_size: int = 5, batch_timeout: int = 1800):
        self.tenant_id = tenant_id
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.processed_ids: set[str] = set()
        self.engine = create_engine(connection_uri, pool_pre_ping=True)
        self.queue_urls = {
            "VERIFY": envars.SQS_VERIFY_QUERY,
            "PROCESS_META": envars.SQS_PROCESS_PACKAGE_METADATA,
            "PROCESS_XML": envars.SQS_PROCESS_PACKAGE_XML,
        }

    def _get_company_info(self) -> dict | None:
        cid = self.tenant_id.strip().lower()
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, identifier, workspace_id, rfc FROM public.company "
                    "WHERE lower(identifier::text) = :cid"
                ),
                {"cid": cid},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "identifier": str(row[1]),
                "workspace_id": row[2],
                "rfc": row[3],
            }

    def get_pending_queries(self) -> list[dict]:
        with self.engine.connect() as conn:
            conn.execute(text(f'SET search_path TO "{self.tenant_id}"'))
            rows = conn.execute(
                text(
                    f"""
                SELECT identifier, state, request_type, download_type,
                       name, start, "end", is_manual, cfdis_qty, packages, created_at,
                       sent_date, technology::text AS technology, origin_identifier
                FROM sat_query
                WHERE state IN ('SENT', 'DOWNLOADED')
                  AND created_at > NOW() - INTERVAL '{MAX_AGE_HOURS} hours'
                ORDER BY created_at DESC
            """
                )
            ).fetchall()

            queries = []
            for r in rows:
                qid = str(r[0])
                if qid in self.processed_ids:
                    continue
                queries.append(
                    {
                        "identifier": qid,
                        "state": r[1],
                        "request_type": r[2],
                        "download_type": r[3],
                        "name": r[4],
                        "start": r[5],
                        "end": r[6],
                        "is_manual": r[7],
                        "cfdis_qty": r[8],
                        "packages": r[9] or [],
                        "created_at": r[10],
                        "sent_date": r[11],
                        "technology": r[12] or "WebService",
                        "origin_identifier": str(r[13]) if r[13] is not None else None,
                    }
                )
            return queries

    def get_query_states(self, identifiers: list[str]) -> dict[str, str]:
        if not identifiers:
            return {}
        with self.engine.connect() as conn:
            conn.execute(text(f'SET search_path TO "{self.tenant_id}"'))
            placeholders = ", ".join(f"'{qid}'" for qid in identifiers)
            rows = conn.execute(
                text(f"SELECT identifier, state FROM sat_query WHERE identifier::text IN ({placeholders})")
            ).fetchall()
            return {str(r[0]): r[1] for r in rows}

    def _route_query(self, query: dict) -> str:
        state = query["state"]
        if state == "SENT":
            return self.queue_urls["VERIFY"]
        req_type = query["request_type"]
        if req_type == "METADATA":
            return self.queue_urls["PROCESS_META"]
        return self.queue_urls["PROCESS_XML"]

    def send_to_queue(self, query: dict, company: dict) -> str:
        queue_target = self._route_query(query)
        sd = query.get("sent_date")
        sd_iso = sd.isoformat() if sd else None
        # ``QueryVerifierWS.do_check_pending`` uses ``sent_date`` / ``origin_sent_date``; omitting them breaks retries.
        origin_sent = sd_iso
        message = {
            "company_identifier": self.tenant_id,
            "identifier": query["identifier"],
            # Go/Python handlers expect ``query_identifier`` (sat_query PK); omitting breaks verify/PROCESSED updates.
            "query_identifier": query["identifier"],
            "download_type": query["download_type"],
            "request_type": query["request_type"],
            "state": query["state"],
            "name": query.get("name"),
            "start": query["start"].isoformat() if query.get("start") else None,
            "end": query["end"].isoformat() if query.get("end") else None,
            "is_manual": query.get("is_manual") or False,
            "packages": list(query.get("packages") or []),
            "cfdis_qty": query.get("cfdis_qty"),
            "wid": company["workspace_id"],
            "cid": company["id"],
            "sent_date": sd_iso,
            "origin_sent_date": origin_sent,
            "technology": query.get("technology") or "WebService",
            "origin_identifier": query.get("origin_identifier"),
            "ws_verify_retries": 0,
        }
        send_queue_raw(queue_target, json.dumps(message, default=str))
        return queue_target.split("/")[-1] if "/" in queue_target else queue_target

    def wait_for_batch(self, batch: list[dict]) -> dict[str, str]:
        ids = [q["identifier"] for q in batch]
        original_states = {q["identifier"]: q["state"] for q in batch}
        start = time.time()
        last_states: dict[str, str] = {}

        while time.time() - start < self.batch_timeout:
            last_states = self.get_query_states(ids)
            still_pending = [
                qid
                for qid in ids
                if last_states.get(qid, "UNKNOWN") in INTERMEDIATE_STATES
                and last_states.get(qid) == original_states.get(qid)
            ]
            if not still_pending:
                return last_states
            time.sleep(POLL_INTERVAL_S)

        return last_states

    def run(self) -> None:
        company = self._get_company_info()
        if not company:
            print(f"[ERROR] Company not found in this database: {self.tenant_id!r}")
            print(
                f"  DB target: host={envars.DB_HOST!r} port={envars.DB_PORT!r} "
                f"name={envars.DB_NAME!r} user={envars.DB_USER!r}"
            )
            print(
                "  Si la empresa está en Azure: este host/puerto no es el Flexible Server "
                "(o no es el túnel). Ej. con SSH -L 5433:…postgres.database.azure.com:5432 use "
                "DB_HOST=127.0.0.1 DB_PORT=5433 PGSSLMODE=require y DB_PASSWORD alineado con Azure "
                "(Key Vault: _TEMP_DB/connect_dababase_azure.py). Ver docstring de este script."
            )
            return

        print("=" * 70)
        print("[REPROCESS] Manual Batch Reprocessor")
        print(f"  Transport:  {transport_kind()}")
        print(f"  Tenant:     {company['rfc']} ({self.tenant_id[:12]}...)")
        print(f"  Batch size: {self.batch_size}")
        print(f"  Max age:    {MAX_AGE_HOURS}h")
        print(f"  Timeout:    {self.batch_timeout}s per batch")
        print("=" * 70)

        queries = self.get_pending_queries()
        if not queries:
            print("[REPROCESS] No pending queries found.")
            return

        sent_count = sum(1 for q in queries if q["state"] == "SENT")
        dl_count = sum(1 for q in queries if q["state"] == "DOWNLOADED")
        print(f"[REPROCESS] Found {len(queries)} queries (SENT={sent_count}, DOWNLOADED={dl_count})\n")

        total_batches = (len(queries) + self.batch_size - 1) // self.batch_size
        total_ok = 0
        total_fail = 0
        batches = [queries[i : i + self.batch_size] for i in range(0, len(queries), self.batch_size)]

        for batch_num, batch in enumerate(batches, start=1):
            print(f"[BATCH {batch_num}/{total_batches}] Sending {len(batch)} queries...")

            for q in batch:
                queue_name = self.send_to_queue(q, company)
                print(
                    f"  -> {q['identifier'][:12]}... {q['state']:12} {q['request_type']:10} -> {queue_name}"
                )

            print(f"  Waiting for completion (timeout={self.batch_timeout}s)...")
            t0 = time.time()
            final_states = self.wait_for_batch(batch)
            elapsed = time.time() - t0

            for q in batch:
                qid = q["identifier"]
                original = q["state"]
                final = final_states.get(qid, "UNKNOWN")
                self.processed_ids.add(qid)

                if final in SUCCESS_STATES and final != original:
                    total_ok += 1
                    print(f"  OK  {qid[:12]}... {original} -> {final} ({elapsed:.0f}s)")
                elif final in TERMINAL_STATES and final != original:
                    total_fail += 1
                    print(f"  ERR {qid[:12]}... {original} -> {final} ({elapsed:.0f}s)")
                elif final == original:
                    total_fail += 1
                    print(f"  STUCK {qid[:12]}... still {final} after {elapsed:.0f}s")
                else:
                    total_ok += 1
                    print(f"  OK  {qid[:12]}... {original} -> {final} ({elapsed:.0f}s)")

            print()

        print("=" * 70)
        print("[SUMMARY]")
        print(f"  Total queried: {len(queries)}")
        print(f"  Success:       {total_ok}")
        print(f"  Failed/Stuck:  {total_fail}")
        print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual SAT query batch reprocessor")
    parser.add_argument("--batch-size", type=int, default=5, help="Queries per batch (default: 5)")
    parser.add_argument("--timeout", type=int, default=1800, help="Timeout per batch in seconds (default: 1800)")
    parser.add_argument(
        "--company-identifier",
        default=os.environ.get("COMPANY_IDENTIFIER") or os.environ.get("TENANT_ID"),
        help="Tenant UUID (public.company.identifier). Default: COMPANY_IDENTIFIER or TENANT_ID.",
    )
    args = parser.parse_args()
    tenant = (args.company_identifier or "").strip()
    if not tenant:
        print(
            "[ERROR] Missing tenant. Pass --company-identifier or set COMPANY_IDENTIFIER / TENANT_ID."
        )
        sys.exit(1)

    reprocessor = BatchReprocessor(
        tenant_id=tenant,
        batch_size=args.batch_size,
        batch_timeout=args.timeout,
    )
    reprocessor.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[REPROCESS] Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[REPROCESS] Fatal error: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
