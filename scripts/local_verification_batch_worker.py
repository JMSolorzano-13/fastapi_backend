#!/usr/bin/env python3
"""Local SAT query batch worker: enqueue SENT/DOWNLOADED rows to LocalStack SQS (no Chalice).

Mirrors ``backend/local_verification_batch_worker.py`` for FastAPI-only trees.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any

import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("LOCAL_INFRA", "1")
load_dotenv(_ROOT / ".env")
load_dotenv(_ROOT / ".env.local", override=False)

from chalicelib.new.config.infra import envars


def _database_url() -> str:
    direct = os.environ.get("DATABASE_URL")
    if direct:
        return direct
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "ezaudita_db")
    user = os.environ.get("DB_USER", "solcpuser")
    password = os.environ.get("DB_PASSWORD", "local_dev_password")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


class LocalSATBatchWorker:
    def __init__(self) -> None:
        self.running = True
        self.batch_size = 5
        self.wait_between_batches = 30
        self.cycle_interval = 120

        self.engine: Engine = create_engine(_database_url())

        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
        region_name = os.environ.get("REGION_NAME", "us-east-1")
        self.sqs = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )

        self.verify_queue_url = envars.SQS_VERIFY_QUERY
        self.process_xml_queue_url = envars.SQS_PROCESS_PACKAGE_XML
        self.process_metadata_queue_url = envars.SQS_PROCESS_PACKAGE_METADATA

        self._all_queues = [
            self.verify_queue_url,
            self.process_xml_queue_url,
            self.process_metadata_queue_url,
        ]

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        print("=" * 80)
        print("Local SAT Query Batch Worker (fastapi_backend)")
        print("=" * 80)
        print(f"\n   Batch Size:          {self.batch_size}")
        print(f"   Cycle Interval:      {self.cycle_interval}s")
        print("   Queues:")
        print(f"     SENT       -> {self.verify_queue_url.split('/')[-1]}")
        print(f"     DL + CFDI  -> {self.process_xml_queue_url.split('/')[-1]}")
        print(f"     DL + META  -> {self.process_metadata_queue_url.split('/')[-1]}")

        self._wait_for_queues()
        print("=" * 80)

    def _wait_for_queues(self, max_retries: int = 30, interval: int = 2) -> None:
        print("\n   Waiting for queues...")
        for attempt in range(1, max_retries + 1):
            missing: list[str] = []
            for url in self._all_queues:
                try:
                    self.sqs.get_queue_attributes(
                        QueueUrl=url,
                        AttributeNames=["ApproximateNumberOfMessages"],
                    )
                except Exception:
                    missing.append(url.split("/")[-1])
            if not missing:
                print("   All queues ready.\n")
                return
            print(f"   [{attempt}/{max_retries}] Waiting for: {', '.join(missing)}")
            time.sleep(interval)
        print(
            f"   WARNING: some queues not available after {max_retries * interval}s, "
            "starting anyway.\n"
        )

    def _signal_handler(self, signum: int, _frame: FrameType | None) -> None:
        print("\n\nShutting down...")
        self.running = False

    def get_all_tenants(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT id, identifier, rfc, workspace_id
                FROM public.company
                WHERE active = true
                ORDER BY created_at DESC
            """)
            )
            return [
                {"cid": row[0], "identifier": str(row[1]), "rfc": row[2], "wid": row[3]}
                for row in result.fetchall()
            ]

    def get_queries_by_state(
        self, tenant_id: str, state: str, limit: int = 5
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            conn.execute(text(f'SET search_path TO "{tenant_id}"'))
            result = conn.execute(
                text("""
                SELECT identifier, download_type, request_type, name,
                       start, "end", state, is_manual, cfdis_qty, packages,
                       sent_date, created_at, technology
                FROM sat_query
                WHERE state = :state
                  AND request_type IN ('CFDI', 'METADATA')
                ORDER BY created_at ASC
                LIMIT :limit
            """),
                {"state": state, "limit": limit},
            )
            rows: list[dict[str, Any]] = []
            for r in result.fetchall():
                rows.append(
                    {
                        "identifier": str(r[0]),
                        "download_type": r[1],
                        "request_type": r[2],
                        "name": str(r[3]) if r[3] else None,
                        "start": r[4].isoformat() if r[4] else None,
                        "end": r[5].isoformat() if r[5] else None,
                        "state": r[6],
                        "is_manual": r[7],
                        "cfdis_qty": r[8],
                        "packages": list(r[9]) if r[9] else [],
                        "sent_date": r[10].isoformat() if r[10] else None,
                        "created_at": r[11].isoformat() if r[11] else None,
                        "technology": r[12] if r[12] else "WebService",
                    }
                )
            return rows

    def count_by_state(self, tenant_id: str, state: str) -> int:
        with self.engine.connect() as conn:
            conn.execute(text(f'SET search_path TO "{tenant_id}"'))
            result = conn.execute(
                text("SELECT COUNT(*) FROM sat_query WHERE state = :state"),
                {"state": state},
            )
            row = result.fetchone()
            return int(row[0]) if row else 0

    def send_to_queue(
        self, queue_url: str, queries: list[dict[str, Any]], tenant: dict[str, Any]
    ) -> int:
        sent = 0
        for q in queries:
            msg = {
                **q,
                "company_identifier": tenant["identifier"],
                "company_rfc": tenant["rfc"],
                "wid": tenant["wid"],
                "cid": tenant["cid"],
                "execute_at": None,
            }
            try:
                self.sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(msg, default=str),
                )
                sent += 1
            except Exception as e:
                ident = str(q.get("identifier", ""))[:8]
                print(f"      ERR sending {ident}: {e}")
        return sent

    def get_queue_depth(self, queue_url: str) -> tuple[int, int]:
        try:
            r = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )
            return (
                int(r["Attributes"].get("ApproximateNumberOfMessages", 0)),
                int(r["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0)),
            )
        except Exception:
            return 0, 0

    def run_cycle(self) -> None:
        cycle_start = datetime.now()
        print(f"\n{'─' * 80}")
        print(f"Cycle {cycle_start.strftime('%H:%M:%S')}")
        print(f"{'─' * 80}")

        tenants = self.get_all_tenants()
        if not tenants:
            print("  No active tenants")
            return

        print(f"  {len(tenants)} tenant(s)")

        total_actions = 0

        for tenant in tenants:
            tid = tenant["identifier"]
            rfc = tenant["rfc"]

            sent_count = self.count_by_state(tid, "SENT")
            dl_count = self.count_by_state(tid, "DOWNLOADED")

            if sent_count == 0 and dl_count == 0:
                continue

            print(f"\n  {rfc} (Tenant: {tid[:8]}...)")

            if sent_count > 0:
                print(f"    SENT: {sent_count} total")
                queries = self.get_queries_by_state(tid, "SENT", self.batch_size)
                sent = self.send_to_queue(self.verify_queue_url, queries, tenant)
                total_actions += sent

                meta = sum(1 for q in queries if q["request_type"] == "METADATA")
                cfdi = sum(1 for q in queries if q["request_type"] == "CFDI")
                print(f"    -> Sent {sent}/{len(queries)} to verify (CFDI:{cfdi} META:{meta})")

            if dl_count > 0:
                print(f"    DOWNLOADED: {dl_count} total")
                queries = self.get_queries_by_state(tid, "DOWNLOADED", self.batch_size)

                cfdi_queries = [q for q in queries if q["request_type"] == "CFDI"]
                meta_queries = [q for q in queries if q["request_type"] == "METADATA"]

                if cfdi_queries:
                    sent = self.send_to_queue(self.process_xml_queue_url, cfdi_queries, tenant)
                    total_actions += sent
                    print(f"    -> Sent {sent} CFDI to process_xml queue")

                if meta_queries:
                    sent = self.send_to_queue(
                        self.process_metadata_queue_url, meta_queries, tenant
                    )
                    total_actions += sent
                    print(f"    -> Sent {sent} METADATA to process_metadata queue")

        v_avail, v_flight = self.get_queue_depth(self.verify_queue_url)
        x_avail, x_flight = self.get_queue_depth(self.process_xml_queue_url)
        m_avail, m_flight = self.get_queue_depth(self.process_metadata_queue_url)

        dur = (datetime.now() - cycle_start).total_seconds()
        print(f"\n{'─' * 80}")
        print(f"  Summary ({dur:.1f}s)")
        print(f"    verify:   {v_avail} avail / {v_flight} in-flight")
        print(f"    xml:      {x_avail} avail / {x_flight} in-flight")
        print(f"    metadata: {m_avail} avail / {m_flight} in-flight")
        print(f"{'─' * 80}")

        if total_actions > 0 and self.running:
            print(f"\n  Waiting {self.wait_between_batches}s...")
            time.sleep(self.wait_between_batches)

    def run(self) -> None:
        print("\nRunning (Ctrl+C to stop)\n")

        last_cycle = datetime.now() - timedelta(seconds=self.cycle_interval)

        while self.running:
            try:
                now = datetime.now()
                if (now - last_cycle).total_seconds() >= self.cycle_interval:
                    self.run_cycle()
                    last_cycle = now
                else:
                    remaining = self.cycle_interval - (now - last_cycle).total_seconds()
                    v_a, v_f = self.get_queue_depth(self.verify_queue_url)
                    x_a, x_f = self.get_queue_depth(self.process_xml_queue_url)
                    m_a, m_f = self.get_queue_depth(self.process_metadata_queue_url)
                    print(
                        f"[{now.strftime('%H:%M:%S')}] next={remaining:.0f}s "
                        f"| verify:{v_a}/{v_f} xml:{x_a}/{x_f} meta:{m_a}/{m_f}",
                        end="\r",
                    )
                time.sleep(5)
            except Exception as e:
                print(f"\nError: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(5)

        print("\n\nStopped.")


def main() -> None:
    try:
        worker = LocalSATBatchWorker()
        worker.run()
    except KeyboardInterrupt:
        print("\n\nInterrupted")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
