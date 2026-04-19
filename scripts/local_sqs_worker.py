#!/usr/bin/env python3
"""Poll LocalStack SQS and run SAT pipeline handlers without Chalice (dict Records shape)."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

for _name in ("boto3", "botocore", "urllib3", "s3transfer"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("LOCAL_INFRA", "1")
load_dotenv(_ROOT / ".env")
load_dotenv(_ROOT / ".env.local", override=False)

from chalicelib.infra.localstack_boto_clients import make_ephemeral_sqs_client  # noqa: E402
from chalicelib.workers.sat_sqs_pipeline import get_sat_local_poll_dispatchers  # noqa: E402
from chalicelib.workers.sqs_lambda_shim import build_lambda_sqs_event_dict  # noqa: E402


class LocalSqsPoller:
    SUMMARY_INTERVAL = 300
    HEARTBEAT_INTERVAL = 60
    POLL_SLEEP = 2

    def __init__(self) -> None:
        self.running = True
        self.stats: dict = {
            "total_processed": 0,
            "total_errors": 0,
            "by_handler": {},
            "last_summary": datetime.now(),
        }

        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
        region_name = os.environ.get("REGION_NAME", "us-east-1")

        self.sqs_client = make_ephemeral_sqs_client(
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )

        self.queue_handlers: dict[str, tuple[Callable, str]] = dict(
            get_sat_local_poll_dispatchers()
        )

        for _, handler_name in self.queue_handlers.values():
            self.stats["by_handler"][handler_name] = {"ok": 0, "err": 0}

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, _frame) -> None:
        print(f"\n[WORKER] Received signal {signum}, shutting down...")
        self.running = False

    def _ts(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _queue_name(self, url: str) -> str:
        return url.split("/")[-1]

    def poll_queue(self, queue_url: str, handler: Callable, handler_name: str) -> bool:
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
                MessageAttributeNames=["All"],
            )
        except Exception:
            return False

        messages = response.get("Messages", [])
        if not messages:
            return False

        message = messages[0]
        receipt_handle = message["ReceiptHandle"]

        try:
            event_dict = build_lambda_sqs_event_dict(message)
            handler(event_dict, {})

            self.stats["total_processed"] += 1
            self.stats["by_handler"][handler_name]["ok"] += 1

            self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            return True

        except Exception as e:
            self.stats["total_errors"] += 1
            self.stats["by_handler"][handler_name]["err"] += 1
            print(
                f"[{self._ts()}] ERROR {handler_name} "
                f"queue={self._queue_name(queue_url)} "
                f"{type(e).__name__}: {e!s:.200}"
            )
            return False

    def _print_summary(self) -> None:
        print(
            f"\n[{self._ts()}] SUMMARY  processed={self.stats['total_processed']}  "
            f"errors={self.stats['total_errors']}"
        )
        active = {
            name: s for name, s in self.stats["by_handler"].items() if s["ok"] or s["err"]
        }
        if active:
            for name, s in sorted(active.items()):
                print(f"  {name:20} ok={s['ok']}  err={s['err']}")
        print()
        self.stats["last_summary"] = datetime.now()

    def run(self) -> None:
        print("=" * 70)
        print(f"[WORKER] FastAPI local SQS poller — {len(self.queue_handlers)} queues")
        print("=" * 70)
        for url, (_, name) in self.queue_handlers.items():
            print(f"  {name:20} -> {self._queue_name(url)}")
        print("=" * 70 + "\n")

        last_heartbeat = time.time()

        while self.running:
            for queue_url, (handler, handler_name) in self.queue_handlers.items():
                try:
                    attrs = self.sqs_client.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=["ApproximateNumberOfMessages"],
                    )
                    msg_count = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
                    if msg_count > 0:
                        self.poll_queue(queue_url, handler, handler_name)
                except Exception:
                    pass

            now = time.time()
            if now - last_heartbeat > self.HEARTBEAT_INTERVAL:
                print(
                    f"[{self._ts()}] heartbeat  "
                    f"processed={self.stats['total_processed']}  "
                    f"errors={self.stats['total_errors']}"
                )
                last_heartbeat = now

            elapsed_since_summary = (datetime.now() - self.stats["last_summary"]).total_seconds()
            if elapsed_since_summary > self.SUMMARY_INTERVAL:
                self._print_summary()

            time.sleep(self.POLL_SLEEP)

        self._print_summary()
        print("[WORKER] Shutdown complete\n")


def main() -> None:
    print("\n[WORKER] Initializing (fastapi_backend, no Chalice)...\n")

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

    try:
        sqs = make_ephemeral_sqs_client(
            endpoint_url=endpoint_url,
            region_name=os.environ.get("REGION_NAME", "us-east-1"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        queues = sqs.list_queues().get("QueueUrls", [])
        print(f"[WORKER] LocalStack OK ({endpoint_url}) — {len(queues)} queues\n")
    except Exception as e:
        print(f"[WORKER] Cannot connect to LocalStack: {e}")
        sys.exit(1)

    LocalSqsPoller().run()


if __name__ == "__main__":
    main()
