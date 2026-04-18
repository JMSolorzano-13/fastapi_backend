"""Golden JSON for QueryCreateEvent.model_dump_json() — parity with SQSHandler wire format."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from chalicelib.new.query.domain.enums import DownloadType, RequestType
from chalicelib.new.query.domain.events.query_sent_event import QueryCreateEvent

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "query_create_event_golden.json"


@pytest.fixture
def golden_query_create_event() -> QueryCreateEvent:
    ev = QueryCreateEvent(
        company_identifier="00000000-0000-0000-0000-00000000ca01",
        download_type=DownloadType.ISSUED,
        request_type=RequestType.CFDI,
        is_manual=True,
        start=datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
        end=datetime(2026, 1, 16, 12, 0, 0, tzinfo=UTC),
        wid=7,
        cid=42,
    )
    ev.identifier = "11111111-1111-1111-1111-111111111111"
    return ev


def test_query_create_event_json_matches_fixture(golden_query_create_event: QueryCreateEvent) -> None:
    expected = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    actual = json.loads(golden_query_create_event.model_dump_json())
    assert actual == expected
