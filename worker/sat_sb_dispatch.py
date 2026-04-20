"""Service Bus → SAT pipeline: route message bodies to ``sat_sqs_pipeline`` handlers.

Same contract as ``local_sqs_worker`` / Chalice: one SB message body = one SQS ``Records[0].body``
JSON string. Queue names are matched to ``envars.SQS_*`` via ``queue_name_from_sqs_url`` (hyphen /
underscore tolerant).

Covered queues align with ``worker.service_bus_worker.SAT_QUEUE_ENV_KEYS`` (create → metadata
send → verify → download → process metadata/xml → complete CFDIs → updater).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from chalicelib.new.config.infra import envars
from chalicelib.new.shared.infra.queue_transport import queue_name_from_sqs_url
from chalicelib.new.utils.session import new_session
from chalicelib.workers import sat_sqs_pipeline as sat_pipe
from chalicelib.workers.sqs_lambda_shim import dict_to_sqs_event_records


def _normalize_queue_name(name: str) -> str:
    return (name or "").strip().replace("_", "-")


def _events_from_body(body: str) -> Iterable:
    return dict_to_sqs_event_records({"Records": [{"body": body}]})


def _runner_with_session(comment: str, fn: Callable[..., None]) -> Callable[[str], None]:
    def _run(body: str) -> None:
        with new_session(comment=comment, read_only=False) as session:
            fn(_events_from_body(body), session)

    return _run


def _runner_no_session(fn: Callable[..., None]) -> Callable[[str], None]:
    def _run(body: str) -> None:
        fn(_events_from_body(body))

    return _run


def _sat_dispatch_specs() -> list[tuple[str, str, Callable[..., None], bool]]:
    """(envars attribute name, new_session comment, pipeline fn, needs_db_session)."""
    return [
        ("SQS_CREATE_QUERY", "service_bus_create_query", sat_pipe.process_sqs_create_query, True),
        (
            "SQS_SEND_QUERY_METADATA",
            "service_bus_send_query_metadata",
            sat_pipe.process_sqs_send_query_metadata_listener,
            True,
        ),
        ("SQS_VERIFY_QUERY", "service_bus_verify_query", sat_pipe.process_sqs_verify_query, True),
        (
            "SQS_DOWNLOAD_QUERY",
            "service_bus_download_query",
            sat_pipe.process_sqs_download_query,
            False,
        ),
        ("SQS_UPDATER_QUERY", "service_bus_updater_query", sat_pipe.process_sqs_updater_query, True),
        (
            "SQS_PROCESS_PACKAGE_METADATA",
            "service_bus_process_metadata",
            sat_pipe.process_sqs_process_query_metadata,
            True,
        ),
        (
            "SQS_COMPLETE_CFDIS",
            "service_bus_complete_cfdis",
            sat_pipe.process_sqs_complete_cfdis,
            True,
        ),
        (
            "SQS_PROCESS_PACKAGE_XML",
            "service_bus_process_xml",
            sat_pipe.process_sqs_process_query_xml,
            True,
        ),
    ]


def sat_dispatch_registry() -> dict[str, Callable[[str], None]]:
    """Map canonical Service Bus queue name → ``body`` runner (rebuilt each call — cheap)."""
    reg: dict[str, Callable[[str], None]] = {}
    for attr, comment, fn, needs_session in _sat_dispatch_specs():
        raw = (getattr(envars, attr, None) or "").strip()
        if not raw:
            continue
        qn = queue_name_from_sqs_url(raw)
        if not qn:
            continue
        key = _normalize_queue_name(qn)
        if needs_session:
            reg[key] = _runner_with_session(comment, fn)
        else:
            reg[key] = _runner_no_session(fn)
    return reg


def canonical_create_query_queue_name() -> str:
    """Logical Azure queue name for ``SQS_CREATE_QUERY`` (for tests, patch this symbol)."""
    return queue_name_from_sqs_url(envars.SQS_CREATE_QUERY)


def is_create_query_queue(queue_name: str) -> bool:
    return _normalize_queue_name(queue_name) == canonical_create_query_queue_name()


def run_create_query_from_service_bus_body(body: str) -> None:
    """Parse SB message body and run the same handler as SQS ``Create Query``."""
    dispatch_sat_queue_message(canonical_create_query_queue_name(), body)


def dispatch_sat_queue_message(queue_name: str, body: str) -> None:
    """Route a single Service Bus message to the SAT handler for ``queue_name``."""
    key = _normalize_queue_name(queue_name)
    handler = sat_dispatch_registry().get(key)
    if handler is None:
        known = ", ".join(sorted(sat_dispatch_registry().keys())) or "(no SQS_* env configured)"
        raise NotImplementedError(
            f"No SAT Service Bus dispatch for queue={queue_name!r} (normalized={key!r}). "
            f"Configured SAT queues: {known}"
        )
    handler(body)
