"""Long-running Service Bus consumer for the FastAPI ACA worker image.

Reads the same ``SQS_*`` environment variables as the API (queue URL or plain name).
Resolves Azure queue names with underscore→hyphen parity vs Go ``QueueNameFromSQSURL``.

Default (``FASTAPI_SB_WORKER_ECHO=1`` or unset): receive, log preview, complete.
Chalice ``@app.on_sqs_message`` parity is not in this tree; use ``go-worker`` for SAT,
or ``FASTAPI_SB_WORKER_ECHO=0`` and extend ``dispatch_payload`` when ported.

Env:
  ``AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING`` — preferred listen SAS.
  ``AZURE_SERVICEBUS_CONNECTION_STRING`` — fallback.
  ``FASTAPI_SB_WORKER_ECHO`` — ``1`` echo+complete; ``0`` calls ``dispatch_payload`` (stub).
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import sys
import time
from urllib.parse import urlparse

from azure.servicebus import ServiceBusClient

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


def _connection_strings() -> tuple[str, str]:
    listen = (os.environ.get("AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING") or "").strip()
    primary = (os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING") or "").strip()
    if listen:
        return listen, "AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING"
    if primary:
        return primary, "AZURE_SERVICEBUS_CONNECTION_STRING"
    logger.error(
        "Set AZURE_SERVICEBUS_LISTEN_CONNECTION_STRING "
        "or AZURE_SERVICEBUS_CONNECTION_STRING"
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
        return explicit
    return _queues_from_sqs_env()


def dispatch_payload(queue_name: str, body: str) -> None:
    """Hook for real processing; raises if ``FASTAPI_SB_WORKER_ECHO=0`` and not implemented."""
    raise NotImplementedError(
        f"No Python handler for queue={queue_name!r}; "
        "use FASTAPI_SB_WORKER_ECHO=1 or go-worker."
    )


def _run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    echo = os.environ.get("FASTAPI_SB_WORKER_ECHO", "1").strip().lower() in ("1", "true", "yes", "")

    conn_str, conn_src = _connection_strings()
    queues = _resolve_queue_names()
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
                    body = ""
                    try:
                        raw = msg.body
                        if isinstance(raw, (list, tuple)):
                            raw = b"".join(
                                x if isinstance(x, (bytes, bytearray)) else str(x).encode()
                                for x in raw
                            )
                        if isinstance(raw, memoryview):
                            body = raw.tobytes().decode("utf-8", errors="replace")
                        elif isinstance(raw, (bytes, bytearray)):
                            body = bytes(raw).decode("utf-8", errors="replace")
                        else:
                            body = str(raw)
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
