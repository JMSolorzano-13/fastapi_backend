"""Load env and send queue payloads via SQS (local) or Azure Service Bus."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

FASTAPI_ROOT = Path(__file__).resolve().parents[2]


def configure_path_and_env() -> None:
    """Ensure imports resolve and load ``.env`` / ``.env.local`` from ``fastapi_backend``."""
    root_s = str(FASTAPI_ROOT)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)
    os.chdir(FASTAPI_ROOT)
    from dotenv import load_dotenv

    load_dotenv(FASTAPI_ROOT / ".env")
    load_dotenv(FASTAPI_ROOT / ".env.local", override=True)


def transport_kind() -> str:
    """Re-exported from ``queue_transport`` (single source for scripts + ``SQSHandler``)."""
    from chalicelib.new.shared.infra.queue_transport import transport_kind as _transport_kind

    return _transport_kind()


def azure_send_connection_string() -> str:
    from chalicelib.new.shared.infra.queue_transport import (
        azure_send_connection_string as _azure_send_connection_string,
    )

    return _azure_send_connection_string()


def send_queue_raw(queue_target: str, body: str) -> None:
    """Send ``body`` (JSON string) to the queue identified by URL or queue name."""
    from chalicelib.new.shared.infra.queue_transport import send_queue_raw as _send_queue_raw

    _send_queue_raw(queue_target, body)


def send_queue_json(queue_target: str, payload: object) -> None:
    send_queue_raw(queue_target, json.dumps(payload, default=str))


def s3_client_for_scripts():
    """S3 client aligned with ``chalicelib.boto3_clients`` (LocalStack keys or Azure Blob shim)."""
    from chalicelib.boto3_clients import s3_client

    return s3_client()
