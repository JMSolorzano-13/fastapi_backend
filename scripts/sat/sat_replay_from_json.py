#!/usr/bin/env python3
"""
Replay ``sat_query`` rows to Azure Service Bus **without Postgres**: read a JSON file
under ``scripts/sat/``, prompt for company context, map ``state`` → queue, POST each body.

Queues (hyphen names, HTTP API on the namespace):

- **SENT** → ``data-queue-verify-request`` (VerificaSolicitud loop)
- **TO_DOWNLOAD** → ``data-queue-download-zips-s3`` (descarga ZIP → blob)
- **DOWNLOADED** + ``CFDI`` → ``queue-process-xml-query``
- **DOWNLOADED** + ``METADATA`` → ``data-queue-metadata``
- **PROCESSED** / **COMPLETED** → omitido (éxito terminal; no hay trabajo que encolar)

Otros estados del dominio (``ERROR*``, ``TIME_LIMIT_REACHED``, ``DELAYED``, etc.) **no**
se encolan aquí: este script es solo para re-disparar el pipeline WebService de descarga/proceso.

Requisitos: ``az login``, rol **Azure Service Bus Data Sender**; ``SB_NAMESPACE_HOST``.

Uso:
    cd fastapi_backend
    export SB_NAMESPACE_HOST="sb-….servicebus.windows.net"
    poetry run python scripts/sat/sat_replay_from_json.py
    poetry run python scripts/sat/sat_replay_from_json.py --json scripts/sat/sat_queries_replay.json --dry-run
    poetry run python scripts/sat/sat_replay_from_json.py --company-identifier '<uuid>' --wid 14 --cid 26 --no-input
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SCRIPT_DIR = Path(__file__).resolve().parent
FASTAPI_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_JSON = SCRIPT_DIR / "sat_queries_replay.json"

QUEUE_VERIFY = "data-queue-verify-request"
QUEUE_DOWNLOAD = "data-queue-download-zips-s3"
QUEUE_XML = "queue-process-xml-query"
QUEUE_META = "data-queue-metadata"


def _iso_z(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        return s
    if " " in s and "T" not in s[:11]:
        s = s.replace(" ", "T", 1)
    if len(s) >= 19 and "+" not in s[19:] and not s.endswith("Z"):
        return s + "Z"
    return s


def _parse_packages(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        t = raw.strip()
        if not t:
            return []
        try:
            out = json.loads(t)
            if isinstance(out, list):
                return [str(x) for x in out]
        except json.JSONDecodeError:
            pass
    return []


def _truthy_manual(raw: object) -> bool:
    if raw is None:
        return False
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    return s in ("1", "true", "yes", "t")


def _int_qty(raw: object) -> int:
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def load_queries(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "queries" in data:
        q = data["queries"]
        if isinstance(q, list):
            return q
    raise ValueError("JSON must be a list of queries or {\"queries\": [...]}")


def route_message(
    row: dict[str, Any],
    company_identifier: str,
    wid: int,
    cid: int,
    *,
    strict: bool,
) -> tuple[str | None, dict[str, Any] | None, str | None]:
    """Return (queue_name, body, skip_reason). skip_reason set when no POST."""
    state = str(row.get("state") or "").strip().upper()
    req_type = str(row.get("request_type") or "").strip().upper()
    qid = str(row.get("identifier") or "").strip()
    name = str(row.get("name") or "").strip()
    dl = str(row.get("download_type") or "").strip().upper()
    if not qid:
        raise ValueError("row missing identifier")

    start = _iso_z(row.get("start"))
    end = _iso_z(row.get("end"))
    if not start or not end:
        raise ValueError(f"{qid}: start/end required")

    pkgs = _parse_packages(row.get("packages"))
    qty = _int_qty(row.get("cfdis_qty"))
    is_man = _truthy_manual(row.get("is_manual"))
    tech = str(row.get("technology") or "WebService").strip() or "WebService"
    origin = row.get("origin_identifier")
    if origin is not None and origin != "":
        origin = str(origin).strip()
    else:
        origin = None

    sent = _iso_z(row.get("sent_date")) or _iso_z(row.get("created_at"))

    if state in ("PROCESSED", "COMPLETED"):
        return None, None, "terminal_ok"

    if req_type in ("BOTH", "CANCELLATION"):
        if strict:
            raise ValueError(f"{qid}: request_type {req_type!r} not supported for replay")
        return None, None, f"unsupported_request_type:{req_type}"

    if state == "SENT":
        if not sent:
            raise ValueError(f"{qid}: SENT requires sent_date or created_at")
        if not name:
            raise ValueError(f"{qid}: SENT requires name (SAT solicitud id)")
        body = {
            "company_identifier": company_identifier,
            "identifier": qid,
            "query_identifier": qid,
            "download_type": dl,
            "request_type": req_type,
            "state": state,
            "name": name,
            "start": start,
            "end": end,
            "is_manual": is_man,
            "packages": pkgs,
            "cfdis_qty": qty if qty else None,
            "wid": wid,
            "cid": cid,
            "sent_date": sent,
            "origin_sent_date": sent,
            "technology": tech,
            "origin_identifier": origin,
            "ws_verify_retries": 0,
        }
        return QUEUE_VERIFY, body, None

    if state == "TO_DOWNLOAD":
        if not pkgs:
            raise ValueError(f"{qid}: TO_DOWNLOAD requires packages")
        if not name:
            raise ValueError(f"{qid}: TO_DOWNLOAD requires name")
        # ``Query.identifier`` must be the ``sat_query`` PK (same contract as bus / manual_batch).
        body = {
            "identifier": qid,
            "company_identifier": company_identifier,
            "query_identifier": qid,
            "download_type": dl,
            "request_type": req_type,
            "start": start,
            "end": end,
            "state": state,
            "name": name,
            "is_manual": is_man,
            "cfdis_qty": qty,
            "packages": pkgs,
            "wid": wid,
            "cid": cid,
        }
        return QUEUE_DOWNLOAD, body, None

    if state == "DOWNLOADED":
        if not pkgs:
            raise ValueError(f"{qid}: DOWNLOADED requires packages")
        if not name:
            raise ValueError(f"{qid}: DOWNLOADED requires name")
        body = {
            "identifier": qid,
            "company_identifier": company_identifier,
            "query_identifier": qid,
            "download_type": dl,
            "request_type": req_type,
            "state": state,
            "start": start,
            "end": end,
            "name": name,
            "is_manual": is_man,
            "cfdis_qty": qty,
            "packages": pkgs,
            "wid": wid,
            "cid": cid,
        }
        if sent:
            body["sent_date"] = sent
        if req_type == "CFDI":
            return QUEUE_XML, body, None
        if req_type == "METADATA":
            return QUEUE_META, body, None
        if strict:
            raise ValueError(f"{qid}: DOWNLOADED with request_type {req_type!r}")
        return None, None, f"unsupported_request_type:{req_type}"

    if strict:
        raise ValueError(
            f"{qid}: state {state!r} not mapped (use SENT, TO_DOWNLOAD, DOWNLOADED; "
            "PROCESSED/COMPLETED skip)"
        )
    return None, None, f"unmapped_state:{state or '?'}"


def az_service_bus_token() -> str:
    r = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://servicebus.azure.net",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        raise RuntimeError(
            "az get-access-token failed. Run `az login` and ensure Service Bus scope.\n"
            + (r.stderr or r.stdout or "")
        )
    tok = (r.stdout or "").strip()
    if not tok:
        raise RuntimeError("empty token from az")
    return tok


def post_message(namespace_host: str, queue: str, body: dict[str, Any], token: str) -> None:
    url = f"https://{namespace_host.strip().rstrip('/')}/{queue}/messages"
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urlopen(req, timeout=120) as resp:
            if resp.status != 201:
                raise RuntimeError(f"HTTP {resp.status} expected 201")
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {detail}") from e
    except URLError as e:
        raise RuntimeError(str(e.reason)) from e


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay sat_query JSON to Azure Service Bus")
    parser.add_argument(
        "--json",
        type=Path,
        default=DEFAULT_JSON,
        help=f"Path to JSON (default: {DEFAULT_JSON})",
    )
    parser.add_argument("--company-identifier", default="", help="Tenant UUID (prompt if empty and not --no-input)")
    parser.add_argument("--wid", type=int, default=0, help="Workspace id (prompt if 0)")
    parser.add_argument("--cid", type=int, default=0, help="Company id in public.company (prompt if 0)")
    parser.add_argument(
        "--namespace-host",
        default="",
        help="Service Bus host (default: env SB_NAMESPACE_HOST)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print routing only, no HTTP")
    parser.add_argument(
        "--no-input",
        action="store_true",
        help="Fail if company/wid/cid missing (non-interactive)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on unmapped state or BOTH/CANCELLATION; default is skip with warning",
    )
    args = parser.parse_args()

    ns = (args.namespace_host or "").strip() or os.environ.get("SB_NAMESPACE_HOST", "").strip()
    if not args.dry_run and not ns:
        print("Set SB_NAMESPACE_HOST or pass --namespace-host.", file=sys.stderr)
        sys.exit(1)

    company = (args.company_identifier or "").strip()
    wid = int(args.wid)
    cid = int(args.cid)

    if not args.no_input:
        if not company:
            company = input("Company identifier (UUID): ").strip()
        if not wid:
            wid = int(input("Workspace id (wid): ").strip() or "0")
        if not cid:
            cid = int(input("Company id (cid, public.company.id): ").strip() or "0")
    if not company or not wid or not cid:
        print("company-identifier, wid and cid are required.", file=sys.stderr)
        sys.exit(1)

    path = args.json
    if not path.is_file():
        print(
            f"File not found: {path}\nCopy scripts/sat/sat_queries_replay.example.json → sat_queries_replay.json",
            file=sys.stderr,
        )
        sys.exit(1)

    rows = load_queries(path)
    token = ""
    if not args.dry_run:
        token = az_service_bus_token()

    sent: list[tuple[str, str, str]] = []
    skipped: list[tuple[str, str, str]] = []
    errors: list[tuple[str, str]] = []

    for row in rows:
        qid = str(row.get("identifier") or "?")
        st = str(row.get("state") or "")
        try:
            queue, body, skip_reason = route_message(row, company, wid, cid, strict=args.strict)
            if queue is None:
                skipped.append((qid, st, skip_reason or "?"))
                if skip_reason and skip_reason != "terminal_ok":
                    print(f"SKIP {qid[:8]}… {st:12} — {skip_reason}", file=sys.stderr)
                continue
            if args.dry_run:
                print(f"[dry-run] {qid[:8]}… {st:12} → {queue}")
                sent.append((qid, st, queue))
                continue
            assert body is not None
            post_message(ns, queue, body, token)
            print(f"OK {qid[:8]}… {st:12} → {queue}")
            sent.append((qid, st, queue))
        except Exception as e:  # noqa: BLE001 — CLI tool
            err = f"{type(e).__name__}: {e}"
            print(f"ERR {qid[:8]}… {st:12} — {err}", file=sys.stderr)
            errors.append((qid, err))

    print()
    print("— Summary —")
    print(f"  Enqueued / dry-run: {len(sent)}")
    n_term = sum(1 for _, _, r in skipped if r == "terminal_ok")
    n_other = len(skipped) - n_term
    print(f"  Skipped (PROCESSED/COMPLETED): {n_term}")
    print(f"  Skipped (other): {n_other}")
    print(f"  Errors: {len(errors)}")
    if skipped:
        for qid, st, reason in skipped:
            print(f"    skip {qid} ({st}) {reason}")
    if errors:
        for qid, err in errors:
            print(f"    error {qid}: {err}")
        sys.exit(1)

    print()
    print(
        "Estados en este flujo WebService (descarga SAT → blob → parseo):\n"
        "  • SENT — solicitud enviada al SAT; worker verifica hasta EstadoSolicitud 3.\n"
        "  • TO_DOWNLOAD — paquetes listos; worker descarga ZIPs.\n"
        "  • DOWNLOADED — ZIP en blob; worker encola METADATA o CFDI a process.\n"
        "  • PROCESSED — CFDI/metadata aplicados a Postgres; no re-encolar salvo corrección puntual.\n"
        "  • COMPLETED — (Go/legado) tratado como terminal; omitido igual que PROCESSED.\n"
        "Otros (ERROR*, TIME_LIMIT_REACHED, DELAYED, …) son del dominio pero no los re-envía este script."
    )


if __name__ == "__main__":
    main()
