"""Unit tests for Service Bus → ``sat_sqs_pipeline`` dispatch (minimal contest)."""

from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from worker import sat_sb_dispatch


@contextmanager
def _fake_new_session(*_a, **_kw):
    yield MagicMock()


def test_is_create_query_queue_matches_configured_name() -> None:
    with patch.object(sat_sb_dispatch, "canonical_create_query_queue_name", return_value="queue-create-query"):
        assert sat_sb_dispatch.is_create_query_queue("queue-create-query") is True
        assert sat_sb_dispatch.is_create_query_queue("queue_create_query") is True
        assert sat_sb_dispatch.is_create_query_queue("data-queue-verify-request") is False


def test_dispatch_unknown_queue_raises() -> None:
    with patch.object(sat_sb_dispatch, "sat_dispatch_registry", return_value={"queue-create-query": lambda b: None}):
        with pytest.raises(NotImplementedError, match="No SAT Service Bus dispatch"):
            sat_sb_dispatch.dispatch_sat_queue_message("unknown-queue", "{}")


def test_run_create_query_from_service_bus_body_invokes_pipeline() -> None:
    body = json.dumps(
        {
            "company_identifier": "00000000-0000-4000-8000-000000000001",
            "company_rfc": "XAXX010101000",
            "download_type": "ISSUED",
            "request_type": "CFDI",
            "is_manual": True,
            "start": "2024-01-01T00:00:00",
            "end": "2024-01-31T00:00:00",
            "query_origin": None,
            "origin_sent_date": None,
            "wid": 1,
            "cid": 2,
        }
    )

    with patch.object(sat_sb_dispatch.sat_pipe, "process_sqs_create_query") as mock_pcq, patch.object(
        sat_sb_dispatch, "new_session", _fake_new_session
    ):
        sat_sb_dispatch.run_create_query_from_service_bus_body(body)

    mock_pcq.assert_called_once()
    (events_arg, session_arg), _kwargs = mock_pcq.call_args
    records = list(events_arg)
    assert len(records) == 1
    assert records[0].body == body
    assert session_arg is not None


def test_dispatch_routes_create_query_via_registry() -> None:
    mock_run = MagicMock()
    with patch.object(sat_sb_dispatch, "sat_dispatch_registry", return_value={"queue-create-query": mock_run}):
        sat_sb_dispatch.dispatch_sat_queue_message("queue-create-query", '{"ok":true}')
    mock_run.assert_called_once_with('{"ok":true}')


def test_dispatch_verify_routes_to_pipeline() -> None:
    body = '{"identifier":"00000000-0000-4000-8000-000000000002"}'
    with patch.object(sat_sb_dispatch.sat_pipe, "process_sqs_verify_query") as mock_pv, patch.object(
        sat_sb_dispatch, "sat_dispatch_registry",
        return_value={"data-queue-verify-request": sat_sb_dispatch._runner_no_session(mock_pv)},
    ):
        sat_sb_dispatch.dispatch_sat_queue_message("data-queue-verify-request", body)
    mock_pv.assert_called_once()
    (events_arg,) = mock_pv.call_args[0]
    assert list(events_arg)[0].body == body


def test_service_bus_worker_dispatch_payload_delegates() -> None:
    from worker import service_bus_worker

    with patch("worker.service_bus_worker.dispatch_sat_queue_message") as mock_dispatch:
        service_bus_worker.dispatch_payload("queue-create-query", '{"x":1}')
    mock_dispatch.assert_called_once_with("queue-create-query", '{"x":1}')


def test_sat_dispatch_specs_count() -> None:
    assert len(sat_sb_dispatch._sat_dispatch_specs()) == 8
