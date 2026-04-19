"""Long-running Service Bus consumer for the FastAPI ACA worker image.

Reads the same ``SQS_*`` environment variables as the API (queue URL or plain name).
Resolves Azure queue names with underscore→hyphen parity vs Go ``QueueNameFromSQSURL``.

Default (``FASTAPI_SB_WORKER_ECHO=1`` or unset): receive, log preview, complete.
Set ``FASTAPI_SB_WORKER_ECHO=0`` to run ``dispatch_payload`` → ``worker.sat_sb_dispatch``:
create-query, send-metadata, verify, download, process metadata/xml, complete CFDIs, updater
(same handlers as ``sat_sqs_pipeline`` / local poller). Use ``FASTAPI_SB_WORKER_QUEUE_NAMES`` /
``FASTAPI_SB_WORKER_EXCLUDE_QUEUES`` to limit which SB queues this process subscribes to.

Env:
  ``AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING`` — preferred listen SAS.
  ``AZURE_SERVICEBUS_CONNECTION_STRING`` — fallback.
  ``FASTAPI_SB_WORKER_ECHO`` — ``1`` echo+complete; ``0`` calls ``dispatch_payload`` (stub).
  ``FASTAPI_SB_WORKER_EXCLUDE_QUEUES`` — optional comma-separated queue names **not** to subscribe
  (hyphen or underscore); use e.g. ``queue-create-query`` so echo mode does not **complete** SAT
  create-query messages before ``go-worker`` processes them.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import signal
import sys
import time

from azure.servicebus import ServiceBusClient
from azure.servicebus.amqp import AmqpMessageBodyType

from chalicelib.new.shared.infra.queue_transport import queue_name_from_sqs_url
from worker.sat_sb_dispatch import dispatch_sat_queue_message

logger = logging.getLogger(__name__)

# SAT-focused queues aligned with ``go_backend/cmd/worker`` routes (extend as needed).
SAT_QUEUE_ENV_KEYS: tuple[str, ...] = (
    "SQS_SEND_QUERY_METADATA",
    "SQS_CREATE_QUERY",
    "SQS_VERIFY_QUERY",
    "SQS_DOWNLOAD_QUERY",
    "SQS_PROCESS_PACKAGE_METADATA",
    "SQS_PROCESS_PACKAGE_XML",
    "SQS_COMPLETE_CFDIS",
    "SQS_UPDATER_QUERY",
)


def _connection_strings() -> tuple[str, str]:
    listen = (os.environ.get("AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING") or "").strip()
    primary = (os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING") or "").strip()
    if listen:
        return listen, "AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING"
    if primary:
        return primary, "AZURE_SERVICEBUS_CONNECTION_STRING"
    logger.error(
        "Set AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING or AZURE_SERVICEBUS_CONNECTION_STRING"
    )
    sys.exit(1)


def _explicit_queue_names() -> list[str]:
    raw = (os.environ.get("FASTAPI_SB_WORKER_QUEUE_NAMES") or "").strip()
    if not raw:
        return []
    return [q.strip().replace("_", "-") for q in raw.split(",") if q.strip()]


def _queues_from_sqs_env() -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for key in SAT_QUEUE_ENV_KEYS:
        val = (os.environ.get(key) or "").strip()
        if not val:
            continue
        qn = queue_name_from_sqs_url(val)
        if qn and qn not in seen:
            seen.add(qn)
            names.append(qn)
    return names


def _resolve_queue_names() -> list[str]:
    explicit = _explicit_queue_names()
    if explicit:
        names = explicit
    else:
        names = _queues_from_sqs_env()
    return _apply_exclude_queues(names)


def _apply_exclude_queues(names: list[str]) -> list[str]:
    raw = (os.environ.get("FASTAPI_SB_WORKER_EXCLUDE_QUEUES") or "").strip()
    if not raw:
        return names
    deny = {q.strip().replace("_", "-") for q in raw.split(",") if q.strip()}
    return [n for n in names if n not in deny]


def decode_service_bus_received_body(msg: object) -> str:
    """Turn ``ServiceBusReceivedMessage.body`` into UTF-8 text for SQS-shaped JSON handlers.

    ``azure-servicebus`` 7.12+ returns an **iterator of byte chunks** for DATA bodies (not ``bytes``).
    Using ``str(msg.body)`` stringifies the generator and breaks ``QueryCreateEvent`` parsing.
    """
    try:
        body_type = msg.body_type  # type: ignore[attr-defined]
        raw = msg.body  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return "<read_error>"

    def _join_byte_parts(parts: list[object]) -> str:
        out = bytearray()
        for p in parts:
            if isinstance(p, (bytes, bytearray)):
                out.extend(p)
            elif isinstance(p, memoryview):
                out.extend(p.tobytes())
            else:
                out.extend(str(p).encode("utf-8", errors="replace"))
        return bytes(out).decode("utf-8", errors="replace")

    if body_type == AmqpMessageBodyType.VALUE:
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw).decode("utf-8", errors="replace")
        if isinstance(raw, memoryview):
            return raw.tobytes().decode("utf-8", errors="replace")
        if isinstance(raw, str):
            return raw
        return str(raw)

    if body_type == AmqpMessageBodyType.DATA:
        if isinstance(raw, memoryview):
            return raw.tobytes().decode("utf-8", errors="replace")
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw).decode("utf-8", errors="replace")
        if isinstance(raw, (list, tuple)):
            return _join_byte_parts(list(raw))
        # Iterable[bytes] (often a generator from AmqpAnnotatedMessage.body)
        try:
            return _join_byte_parts(list(raw))
        except TypeError:
            return str(raw)

    if body_type == AmqpMessageBodyType.SEQUENCE:
        try:
            materialized = list(raw)
        except TypeError:
            return str(raw)
        return json.dumps(materialized, default=str)

    return str(raw)


def dispatch_payload(queue_name: str, body: str) -> None:
    """Route Service Bus message body to SAT handlers (extend in ``worker.sat_sb_dispatch``)."""
    dispatch_sat_queue_message(queue_name, body)


def _run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    echo = os.environ.get("FASTAPI_SB_WORKER_ECHO", "1").strip().lower() in ("1", "true", "yes", "")

    conn_str, conn_src = _connection_strings()
    queues = _resolve_queue_names()
    ex = (os.environ.get("FASTAPI_SB_WORKER_EXCLUDE_QUEUES") or "").strip()
    if ex:
        logger.warning("service_bus_worker: FASTAPI_SB_WORKER_EXCLUDE_QUEUES=%r applied", ex)
    if not queues:
        logger.error(
            "No queues to listen: set FASTAPI_SB_WORKER_QUEUE_NAMES (comma-separated) "
            "or populate SQS_* queue env vars (see worker/service_bus_worker.py)."
        )
        sys.exit(1)

    logger.warning(
        "service_bus_worker: starting (%s), echo=%s, queues=%s",
        conn_src,
        echo,
        queues,
    )

    running = True

    def _stop(_signum: int, _frame: object | None) -> None:
        nonlocal running
        logger.warning("service_bus_worker: signal %s, draining…", _signum)
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    with ServiceBusClient.from_connection_string(conn_str) as client:
        receivers = {}
        try:
            for q in queues:
                receivers[q] = client.get_queue_receiver(q, max_wait_time=30)

            while running:
                had_message = False
                for qname, receiver in receivers.items():
                    if not running:
                        break
                    batch = receiver.receive_messages(max_message_count=1, max_wait_time=5)
                    if not batch:
                        continue
                    had_message = True
                    msg = batch[0]
                    try:
                        body = decode_service_bus_received_body(msg)
                    except Exception:  # noqa: BLE001
                        body = "<decode_error>"

                    preview = body[:500] + ("..." if len(body) > 500 else "")
                    logger.info("queue=%s message_id=%s preview=%s", qname, msg.message_id, preview)
                    try:
                        if echo:
                            pass
                        else:
                            dispatch_payload(qname, body)
                        receiver.complete_message(msg)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("handler failed queue=%s: %s", qname, exc)
                        with contextlib.suppress(Exception):
                            receiver.abandon_message(msg)
                if not had_message and running:
                    time.sleep(0.2)
        finally:
            for r in receivers.values():
                with contextlib.suppress(Exception):
                    r.close()


if __name__ == "__main__":
    _run()
