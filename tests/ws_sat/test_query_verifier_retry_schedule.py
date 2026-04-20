"""Incremental verify re-queue delays (``ws_verify_retries`` + ``execute_at``)."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.query import Query
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.infra.query_verifier_ws import QueryVerifierWS


def _minimal_query(**kwargs) -> Query:
    base = dict(
        company_identifier="00000000-0000-0000-0000-000000000001",
        download_type=DownloadType.ISSUED,
        request_type=RequestType.CFDI,
        state=QueryState.SENT,
        name="sat-uuid-test",
        sent_date=utc_now() - timedelta(minutes=1),
        origin_sent_date=utc_now() - timedelta(minutes=1),
        wid=1,
        cid=1,
    )
    base.update(kwargs)
    return Query(**base)


def test_retry_increments_retries_and_execute_at(monkeypatch):
    monkeypatch.setattr(
        envars,
        "WS_VERIFY_RETRY_INTERVALS_MINUTES",
        (5, 15, 30, 60),
    )
    v = QueryVerifierWS(bus=MagicMock())
    q = _minimal_query(ws_verify_retries=0)
    before = utc_now()
    events = list(v.retry(q))
    assert len(events) == 1
    assert events[0][0] == EventType.SAT_WS_QUERY_VERIFY_NEEDED
    assert q.ws_verify_retries == 1
    assert q.execute_at is not None
    delta = q.execute_at - before
    assert timedelta(minutes=4, seconds=59) < delta < timedelta(minutes=5, seconds=2)


def test_retry_fourth_uses_last_interval(monkeypatch):
    monkeypatch.setattr(
        envars,
        "WS_VERIFY_RETRY_INTERVALS_MINUTES",
        (5, 15, 30, 60),
    )
    v = QueryVerifierWS(bus=MagicMock())
    q = _minimal_query(ws_verify_retries=3)
    before = utc_now()
    list(v.retry(q))
    assert q.ws_verify_retries == 4
    delta = q.execute_at - before
    assert timedelta(minutes=59, seconds=59) < delta < timedelta(minutes=60, seconds=2)


@pytest.mark.parametrize(
    ("retries_before", "expected_minutes"),
    [(0, 5), (1, 15), (2, 30)],
)
def test_retry_schedule_indices(monkeypatch, retries_before, expected_minutes):
    monkeypatch.setattr(
        envars,
        "WS_VERIFY_RETRY_INTERVALS_MINUTES",
        (5, 15, 30, 60),
    )
    v = QueryVerifierWS(bus=MagicMock())
    q = _minimal_query(ws_verify_retries=retries_before)
    before = utc_now()
    list(v.retry(q))
    delta = q.execute_at - before
    assert (
        timedelta(minutes=expected_minutes, seconds=-2)
        < delta
        < timedelta(minutes=expected_minutes, seconds=2)
    )
