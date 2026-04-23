"""SQS message dispatch without Chalice — shared by Chalice app and local pollers."""

import os
from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any, Protocol

from chalicelib.logger import DEBUG, EXCEPTION, INFO, log
from chalicelib.modules import Modules
from chalicelib.new.shared.infra.message.sqs_message import SQSMessage
from chalicelib.new.shared.infra.sqs_handler import SQSHandler


class SupportsSQSRecordBody(Protocol):
    """Minimal shape required by SQSMessage.from_event (body JSON string)."""

    body: str


def dict_to_sqs_event_records(event_dict: dict[str, Any]) -> Iterable[SupportsSQSRecordBody]:
    """Yield record-like objects from a Lambda SQS event dict (Records[].body)."""
    for record in event_dict.get("Records", []):
        yield SimpleNamespace(body=record["body"])


def sqs_handle_events(
    events: Iterable[SupportsSQSRecordBody],
    message_type: type[SQSMessage],
    function: Callable[..., None],
    sqs_handler: SQSHandler | None = None,
    not_ready_function: Callable[[SQSMessage], None] | None = None,
    log_event_level=DEBUG,
    strict_parse: bool = False,
) -> None:
    """Try to execute `function` for each SQS record (same semantics as legacy app.py)."""
    event_qty = 0
    for event in events:
        event_qty += 1
        log(
            Modules.SQS_HANDLER,
            log_event_level,
            "MESSAGE",
            {
                "message": event.body,
            },
        )
        context = {
            "message": event.body,
            "message_type": message_type,
            "function": function,
            "sqs_handler": sqs_handler,
            "not_ready_function": not_ready_function,
        }
        try:
            message = message_type.from_event(event)
        except Exception as e:
            log(
                Modules.SQS_HANDLER,
                EXCEPTION,
                "PARSING_FAILED",
                context=context | {"exception": str(e)},
            )
            if strict_parse:
                raise
            continue

        if not message.is_ready():
            if not_ready_function:
                try:
                    not_ready_function(message)
                except Exception as e:
                    log(
                        Modules.SQS_HANDLER,
                        EXCEPTION,
                        "NOT_READY_FUNCTION_FAILED",
                        context=context | {"exception": str(e)},
                    )
            elif sqs_handler:
                try:
                    sqs_handler.handle(message)
                except Exception as e:
                    log(
                        Modules.SQS_HANDLER,
                        EXCEPTION,
                        "RESEND_FAILED",
                        context=context | {"exception": str(e)},
                    )
            continue

        try:
            log(
                Modules.SQS_HANDLER,
                DEBUG,
                "PROCESSING",
                {
                    "message": message,
                    "function": function,
                },
            )
            function(message=message)
        except Exception as e:
            log(
                Modules.SQS_HANDLER,
                EXCEPTION,
                "PROCESS_FAILED",
                context=context | {"exception": str(e)},
            )
            if os.environ.get("FASTAPI_SB_WORKER_STRICT_ERRORS", "").strip().lower() in (
                "1",
                "true",
                "yes",
            ):
                raise
            continue
    log(
        Modules.SQS_HANDLER,
        INFO,
        "MESSAGE_QTY",
        {
            "sqs_handler": sqs_handler,
            "qty": event_qty,
        },
    )


def build_lambda_sqs_event_dict(raw_message: dict[str, Any]) -> dict[str, Any]:
    """Build an SQS event dict from boto3 receive_message output (Lambda shape)."""
    return {
        "Records": [
            {
                "messageId": raw_message.get("MessageId", ""),
                "receiptHandle": raw_message.get("ReceiptHandle", ""),
                "body": raw_message["Body"],
                "attributes": raw_message.get("Attributes", {}),
                "messageAttributes": raw_message.get("MessageAttributes", {}),
                "eventSource": "aws:sqs",
            }
        ]
    }
