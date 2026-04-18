"""Load env and send queue payloads via SQS (boto3) or Azure Service Bus."""

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
    """``SAT_SCRIPT_TRANSPORT`` overrides; else infer from endpoint / Service Bus strings."""
    raw = (os.environ.get("SAT_SCRIPT_TRANSPORT") or "auto").strip().lower()
    if raw in ("azure", "servicebus", "sb"):
        return "azure"
    if raw in ("sqs", "localstack", "aws"):
        return "sqs"
    if os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("LOCAL_INFRA") == "1":
        return "sqs"
    send = (os.environ.get("AZURE_SERVICEBUS_SEND_CONNECTION_STRING") or "").strip()
    primary = (os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING") or "").strip()
    if send or primary:
        return "azure"
    return "sqs"


def azure_send_connection_string() -> str:
    send = (os.environ.get("AZURE_SERVICEBUS_SEND_CONNECTION_STRING") or "").strip()
    if send:
        return send
    return (os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING") or "").strip()


def _sqs_queue_url(sqs, queue_target: str) -> str:
    if queue_target.startswith("http://") or queue_target.startswith("https://"):
        return queue_target
    out = sqs.get_queue_url(QueueName=queue_target)
    return str(out["QueueUrl"])


def send_queue_raw(queue_target: str, body: str) -> None:
    """Send ``body`` (JSON string) to the queue identified by URL or queue name."""
    if transport_kind() == "azure":
        from azure.servicebus import ServiceBusClient, ServiceBusMessage

        from worker.service_bus_worker import queue_name_from_sqs_url

        conn = azure_send_connection_string()
        if not conn:
            raise RuntimeError(
                "Azure Service Bus selected but set AZURE_SERVICEBUS_SEND_CONNECTION_STRING "
                "or AZURE_SERVICEBUS_CONNECTION_STRING."
            )
        qname = queue_name_from_sqs_url(queue_target)
        if not qname:
            raise RuntimeError(f"Could not resolve Service Bus queue name from {queue_target!r}")
        with (
            ServiceBusClient.from_connection_string(conn) as client,
            client.get_queue_sender(qname) as sender,
        ):
            sender.send_messages(ServiceBusMessage(body))
        return

    import boto3

    from chalicelib.new.config.infra import envars

    endpoint: str | None = None
    if os.environ.get("LOCAL_INFRA") == "1":
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    elif os.environ.get("AWS_ENDPOINT_URL"):
        endpoint = os.environ["AWS_ENDPOINT_URL"]

    sqs_kwargs: dict[str, str] = {"region_name": envars.REGION_NAME}
    if endpoint:
        sqs_kwargs["endpoint_url"] = endpoint
    ak = os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        sqs_kwargs["aws_access_key_id"] = ak
        sqs_kwargs["aws_secret_access_key"] = sk

    sqs = boto3.client("sqs", **sqs_kwargs)
    queue_url = _sqs_queue_url(sqs, queue_target)
    sqs.send_message(QueueUrl=queue_url, MessageBody=body)


def send_queue_json(queue_target: str, payload: object) -> None:
    send_queue_raw(queue_target, json.dumps(payload, default=str))


def s3_client_for_scripts():
    """S3 client aligned with ``chalicelib.boto3_clients`` LocalStack / keys."""
    import boto3

    from chalicelib.new.config.infra import envars

    kwargs: dict[str, str] = {"region_name": envars.REGION_NAME}
    if os.environ.get("LOCAL_INFRA") == "1":
        kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    elif os.environ.get("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["AWS_ENDPOINT_URL"]
    kwargs["aws_access_key_id"] = envars.S3_ACCESS_KEY
    kwargs["aws_secret_access_key"] = envars.S3_SECRET_KEY
    return boto3.client("s3", **kwargs)
