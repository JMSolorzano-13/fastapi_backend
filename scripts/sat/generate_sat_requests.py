#!/usr/bin/env python3
"""
Generate SAT WebService requests by enqueueing create-query payloads.

Uses ``chalicelib.schema.connection_uri`` and ``envars`` (same DB/SQS/S3 as the API).
Transport: LocalStack SQS (boto3) when ``AWS_ENDPOINT_URL`` / ``LOCAL_INFRA`` indicates local,
or Azure Service Bus when ``SAT_SCRIPT_TRANSPORT=azure`` (or auto-detect from SB connection strings).

Usage:
    cd fastapi_backend && poetry run python -m scripts.sat.generate_sat_requests --help
    poetry run python -m scripts.sat.generate_sat_requests --company-identifier <uuid> \\
        --start 2024-01-01 --end 2024-03-31 --yes --cfdi-only
    # Azure from laptop: set ``CLOUD_PROVIDER=azure``, Service Bus send string, DB ``DATABASE_URL``/URI
    # aligned with the deployed API; optional ``SAT_SCRIPT_TRANSPORT=azure``.
    # Single-message enqueue via Azure CLI: see ``SAT_WEBSERVICE_PIPELINE_FASTAPI.md`` (section *Azure CLI — encolar WebService*).
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import create_engine, text

from scripts.sat._runtime import (
    configure_path_and_env,
    s3_client_for_scripts,
    send_queue_json,
    transport_kind,
)

configure_path_and_env()

from chalicelib.new.config.infra import envars
from chalicelib.schema import connection_uri

CFDI_CHUNK_DAYS = 60
METADATA_CHUNK_DAYS = 180
VERIFY_POLL_INTERVAL = 5
VERIFY_TIMEOUT = 120

TERMINAL_STATES = {
    "SENT",
    "DOWNLOADED",
    "PROCESSED",
    "TO_DOWNLOAD",
    "ERROR_IN_CERTS",
    "ERROR_SAT_WS_INTERNAL",
    "ERROR_SAT_WS_UNKNOWN",
    "TIME_LIMIT_REACHED",
    "INFORMATION_NOT_FOUND",
    "SPLITTED",
}
OK_STATES = {"SENT", "DOWNLOADED", "PROCESSED", "TO_DOWNLOAD", "SPLITTED"}


def _engine():
    return create_engine(connection_uri, pool_pre_ping=True)


def get_company(identifier: str) -> dict | None:
    engine = _engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, workspace_id, rfc, identifier FROM public.company WHERE identifier = :id"),
            {"id": identifier},
        ).fetchone()
    engine.dispose()
    if not row:
        return None
    return {"cid": int(row[0]), "wid": int(row[1]), "rfc": str(row[2]), "identifier": str(row[3])}


def list_companies() -> list[dict]:
    engine = _engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, identifier, rfc, workspace_id FROM public.company ORDER BY id")
        ).fetchall()
    engine.dispose()
    return [{"cid": int(r[0]), "identifier": str(r[1]), "rfc": str(r[2]), "wid": int(r[3])} for r in rows]


def check_s3_certs(wid: int, cid: int) -> bool:
    s3 = s3_client_for_scripts()
    bucket = envars.S3_CERTS
    for ext in ("cer", "key", "txt"):
        try:
            s3.head_object(Bucket=bucket, Key=f"ws_{wid}/c_{cid}.{ext}")
        except Exception:
            return False
    return True


def chunk_dates(start: datetime, end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=days), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def build_create_query_body(company: dict, request_type: str, download_type: str, start: datetime, end: datetime):
    return {
        "company_identifier": company["identifier"],
        "company_rfc": company["rfc"],
        "download_type": download_type,
        "request_type": request_type,
        "is_manual": True,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "query_origin": None,
        "origin_sent_date": None,
        "wid": company["wid"],
        "cid": company["cid"],
    }


def send_message(company: dict, request_type: str, download_type: str, start: datetime, end: datetime) -> None:
    body = build_create_query_body(company, request_type, download_type, start, end)
    send_queue_json(envars.SQS_CREATE_QUERY, body)


def prompt_date(label: str) -> datetime:
    while True:
        raw = input(f"  {label} (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            print("    Invalid format. Use YYYY-MM-DD.")


def verify_results(tenant_schema: str, expected: int, timeout: int = VERIFY_TIMEOUT) -> list[dict]:
    engine = _engine()
    start_time = time.time()
    last_count = 0

    while time.time() - start_time < timeout:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f'SELECT identifier, state, request_type, download_type, created_at '
                    f'FROM "{tenant_schema}".sat_query '
                    f"ORDER BY created_at DESC LIMIT :lim"
                ),
                {"lim": expected + 10},
            ).fetchall()

        recent = [
            {"id": str(r[0])[:12], "state": str(r[1]), "type": str(r[2]), "dl": str(r[3]), "at": str(r[4])}
            for r in rows[:expected]
        ]

        terminal_count = sum(1 for r in recent if r["state"] in TERMINAL_STATES)
        if terminal_count != last_count:
            last_count = terminal_count
            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed}s] {terminal_count}/{expected} queries in terminal state...")

        if terminal_count >= expected:
            engine.dispose()
            return recent

        time.sleep(VERIFY_POLL_INTERVAL)

    engine.dispose()
    return recent


def parse_args():
    p = argparse.ArgumentParser(description="Enqueue SAT create-query requests (CFDI + METADATA chunks).")
    p.add_argument(
        "--company-identifier",
        help="public.company.identifier (UUID). If omitted, prompts after listing companies.",
    )
    p.add_argument("--start", help="Range start YYYY-MM-DD (non-interactive).")
    p.add_argument("--end", help="Range end YYYY-MM-DD (non-interactive).")
    p.add_argument("--yes", action="store_true", help="Skip confirmation before sending.")
    p.add_argument(
        "--no-verify",
        action="store_true",
        help="Do not poll tenant sat_query for terminal states after send.",
    )
    scope = p.add_mutually_exclusive_group()
    scope.add_argument(
        "--cfdi-only",
        action="store_true",
        help="Enqueue CFDI (ISSUED+RECEIVED) chunks only; skip METADATA.",
    )
    scope.add_argument(
        "--metadata-only",
        action="store_true",
        help="Enqueue METADATA (ISSUED+RECEIVED) chunks only; skip CFDI.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan and exit without sending messages (no SB/SQS calls).",
    )
    return p.parse_args()


def resolve_company(args: argparse.Namespace) -> dict:
    companies = list_companies()
    if not companies:
        print("No companies found in the database.")
        sys.exit(1)

    cid_input = (args.company_identifier or "").strip()
    if not cid_input:
        print("\n=== SAT WebService Request Generator ===\n")
        print("Available companies:")
        for c in companies:
            print(f"  [{c['cid']}] {c['rfc']}  ({c['identifier']})")
        cid_input = input("\nCompany ID or UUID: ").strip()

    company = next((c for c in companies if str(c["cid"]) == cid_input), None)
    if not company:
        company = next((c for c in companies if c["identifier"] == cid_input), None)
    if not company:
        if cid_input.isdigit():
            print(f"Company not found: no row with public.company.id = {cid_input}.")
            sys.exit(1)
        try:
            UUID(cid_input)
        except ValueError:
            print(
                f"Invalid --company-identifier: {cid_input!r} "
                f"(use a UUID or numeric id from public.company; omit the flag for an interactive list)."
            )
            sys.exit(1)
        full = get_company(cid_input)
        if full:
            company = full
        else:
            print(f"Company not found: {cid_input}")
            sys.exit(1)
    return company


def resolve_dates(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.start and args.end:
        try:
            start = datetime.strptime(args.start, "%Y-%m-%d")
            end = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            print("Invalid --start/--end; use YYYY-MM-DD.")
            sys.exit(1)
        return start, end
    start = prompt_date("Start date")
    end = prompt_date("End date  ")
    return start, end


def main() -> None:
    args = parse_args()
    print(f"Queue transport: {transport_kind()}  (override with SAT_SCRIPT_TRANSPORT=sqs|azure)")

    company = resolve_company(args)
    print(f"\nSelected: [{company['cid']}] {company['rfc']} ({company['identifier'][:12]}...)")

    if not check_s3_certs(company["wid"], company["cid"]):
        print(
            f"\n  ERROR: Certificates not found in S3 (ws_{company['wid']}/c_{company['cid']}.*)\n"
            f"  Bucket: {envars.S3_CERTS}\n"
            f"  Upload certs before running this script."
        )
        sys.exit(1)
    print("  S3 certs: OK")

    start, end = resolve_dates(args)
    if start >= end:
        print("Start must be before end.")
        sys.exit(1)

    include_cfdi = not args.metadata_only
    include_metadata = not args.cfdi_only

    cfdi_chunks = chunk_dates(start, end, CFDI_CHUNK_DAYS) if include_cfdi else []
    meta_chunks = chunk_dates(start, end, METADATA_CHUNK_DAYS) if include_metadata else []
    total = len(cfdi_chunks) * 2 + len(meta_chunks) * 2

    print("\n--- Plan ---")
    if include_cfdi:
        print(f"  CFDI     : {len(cfdi_chunks)} chunks x 2 (ISSUED+RECEIVED) = {len(cfdi_chunks) * 2} requests")
    else:
        print("  CFDI     : (skipped)")
    if include_metadata:
        print(f"  METADATA : {len(meta_chunks)} chunks x 2 (ISSUED+RECEIVED) = {len(meta_chunks) * 2} requests")
    else:
        print("  METADATA : (skipped)")
    print(f"  Total    : {total} messages -> SQS_CREATE_QUERY / Service Bus peer")
    print(f"  Target   : {envars.SQS_CREATE_QUERY!r}")
    if args.dry_run:
        print("\n  --dry-run: no messages sent.")
    print()

    for i, (s, e) in enumerate(cfdi_chunks, 1):
        print(f"  CFDI     {i:>3}/{len(cfdi_chunks)}  {s.date()} -> {e.date()}")
    for i, (s, e) in enumerate(meta_chunks, 1):
        print(f"  METADATA {i:>3}/{len(meta_chunks)}  {s.date()} -> {e.date()}")

    if args.dry_run:
        sys.exit(0)

    if total == 0:
        print("Nothing to send (empty range or filters removed all chunks).")
        sys.exit(0)

    if not args.yes:
        confirm = input(f"\nSend {total} messages? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Cancelled.")
            sys.exit(0)

    sent = 0
    for s, e in cfdi_chunks:
        for dt in ("ISSUED", "RECEIVED"):
            send_message(company, "CFDI", dt, s, e)
            sent += 1
    for s, e in meta_chunks:
        for dt in ("ISSUED", "RECEIVED"):
            send_message(company, "METADATA", dt, s, e)
            sent += 1

    print(f"\n{sent} messages sent.")
    if args.no_verify:
        return

    print("Waiting for worker to process...\n")
    results = verify_results(company["identifier"], total)
    ok = [r for r in results if r["state"] in OK_STATES]
    err = [r for r in results if r["state"] not in OK_STATES]
    print(f"\n{'=' * 60}")
    print(f"  RESULTS: {len(ok)} OK  /  {len(err)} ERRORS  /  {total} expected")
    print(f"{'=' * 60}")
    for r in results:
        status = "OK" if r["state"] in OK_STATES else "!!"
        print(f"  [{status}] {r['id']}... {r['type']:>8} {r['dl']:>8}  -> {r['state']}")
    if err:
        print(f"\n  WARNING: {len(err)} queries failed. Check worker logs.")
        sys.exit(1)
    print(f"\n  All {len(ok)} queries created successfully.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)
