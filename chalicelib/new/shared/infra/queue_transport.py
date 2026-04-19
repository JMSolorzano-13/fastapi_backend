"""Publish to LocalStack/AWS SQS or Azure Service Bus from ``SQS_*`` targets.

``scripts/sat/_runtime.py`` and ``SQSHandler`` share this so ACA (queue names + SB
connection string) matches operator scripts and avoids ``SEND_FAILED`` from passing
a queue *name* where an HTTPS ``QueueUrl`` is required.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse


def queue_name_from_sqs_url(queue_url: str) -> str:
    """Mirror ``go_backend/internal/infra/azsbpub.QueueNameFromSQSURL`` (underscores → hyphens)."""
    raw = (queue_url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.path or parsed.path == "/":
        name = raw.strip("/")
    else:
        segments = [s for s in parsed.path.split("/") if s]
        name = segments[-1] if segments else ""
    return name.replace("_", "-")


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


def resolve_sqs_queue_url(client: Any, queue_target: str) -> str:
    if queue_target.startswith("http://") or queue_target.startswith("https://"):
        return queue_target
    out = client.get_queue_url(QueueName=queue_target)
    return str(out["QueueUrl"])


def _boto_sqs_client_for_transport() -> Any:
    """Ephemeral SQS client aligned with ``scripts/sat/_runtime`` (keys + endpoint)."""
    from chalicelib.infra import localstack_boto_clients as _ls

    return _ls.make_ephemeral_sqs_client_from_transport_env()


def _resolve_default_boto_sqs_client() -> Any:
    """Prefer cached app client unless scripts set explicit AWS keys (per-send client)."""
    ak = os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        return _boto_sqs_client_for_transport()
    from chalicelib.boto3_clients import sqs_client

    return sqs_client()


def send_queue_raw(
    queue_target: str,
    body: str,
    *,
    delay_seconds: int | None = None,
    message_group_id: str | None = None,
    message_deduplication_id: str | None = None,
    boto_sqs_client: Any | None = None,
) -> None:
    """Send ``body`` to the queue identified by HTTPS URL, queue name, or path fragment."""
    if transport_kind() == "azure":
        from azure.servicebus import ServiceBusClient, ServiceBusMessage

        conn = azure_send_connection_string()
        if not conn:
            raise RuntimeError(
                "Azure Service Bus selected but set AZURE_SERVICEBUS_SEND_CONNECTION_STRING "
                "or AZURE_SERVICEBUS_CONNECTION_STRING."
            )
        qname = queue_name_from_sqs_url(queue_target)
        if not qname:
            raise RuntimeError(f"Could not resolve Service Bus queue name from {queue_target!r}")
        msg = ServiceBusMessage(body)
        if delay_seconds is not None and int(delay_seconds) > 0:
            msg.scheduled_enqueue_time_utc = datetime.now(UTC) + timedelta(
                seconds=int(delay_seconds)
            )
        # FIFO / sessions: Azure SAT queues are non-session in Terraform; AWS FIFO extras ignored.
        with (
            ServiceBusClient.from_connection_string(conn) as client,
            client.get_queue_sender(qname) as sender,
        ):
            sender.send_messages(msg)
        return

    client = boto_sqs_client if boto_sqs_client is not None else _resolve_default_boto_sqs_client()
    queue_url = resolve_sqs_queue_url(client, queue_target)
    kwargs: dict[str, Any] = {"QueueUrl": queue_url, "MessageBody": body}
    if delay_seconds is not None:
        kwargs["DelaySeconds"] = int(delay_seconds)
    if message_group_id:
        kwargs["MessageGroupId"] = message_group_id
    if message_deduplication_id:
        kwargs["MessageDeduplicationId"] = message_deduplication_id
    client.send_message(**kwargs)
