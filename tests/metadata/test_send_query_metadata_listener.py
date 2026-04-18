from unittest.mock import Mock, patch

import pytest
from sqlalchemy.orm import Session

from chalicelib.controllers.tenant.session import with_company_session_from_message_reuse_connection
from chalicelib.new.query.domain.enums import DownloadType, RequestType
from chalicelib.new.query.domain.query_creator import QueryCreator
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message.sqs_company import SQSCompanySendMetadata
from chalicelib.new.ws_sat.domain.events import QueryNeedToBeSplittedEvent
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.sat_query import SATQuery


@pytest.mark.skip(reason="wip")
def test_sqs_send_query_metadata_listener_normal_company(
    session: Session, company: Company, company_session: Session
):
    company.exceed_metadata_limit = False
    session.commit()

    sqs_message = SQSCompanySendMetadata(
        company_identifier=company.identifier,
        wid=company["workspace_id"],
        cid=company.id,
        manually_triggered=False,
    )

    published_events = []
    queries = []

    def mock_publish(event_type, event_data):
        published_events.append((event_type, event_data))

    with patch("chalicelib.bus.get_global_bus") as mock_bus_factory:
        mock_bus = Mock()
        mock_bus.publish = mock_publish
        mock_bus_factory.return_value = mock_bus

        @with_company_session_from_message_reuse_connection(session=session, read_only=False)
        def _sqs_send_query_metadata_listener(message: SQSCompanySendMetadata, company_session):
            query_repo = QueryRepositorySA(session=company_session)
            creator = QueryCreator(query_repo=query_repo, session=session)
            issued_query = creator.create(
                company_identifier=message.company_identifier,
                download_type=DownloadType.ISSUED,
                request_type=RequestType.METADATA,
                is_manual=message.manually_triggered,
                wid=message.wid,
                cid=message.cid,
            )
            received_query = creator.create(
                company_identifier=message.company_identifier,
                download_type=DownloadType.RECEIVED,
                request_type=RequestType.METADATA,
                is_manual=message.manually_triggered,
                wid=message.wid,
                cid=message.cid,
            )
            queries.extend([issued_query, received_query])

        _sqs_send_query_metadata_listener(message=sqs_message)

    sat_queries = company_session.query(SATQuery).all()

    assert len(sat_queries) == 2
    assert len(queries) == 2


@pytest.mark.skip(reason="wip")
def test_sqs_send_query_metadata_listener_high_volume_company(
    session: Session, company: Company, company_session: Session
):
    company.exceed_metadata_limit = True
    session.commit()

    sqs_message = SQSCompanySendMetadata(
        company_identifier=company.identifier,
        wid=company["workspace_id"],
        cid=company.id,
        manually_triggered=False,
    )

    published_events = []

    def mock_publish(event_type, event_data):
        published_events.append((event_type, event_data))

    with patch("chalicelib.bus.get_global_bus") as mock_bus_factory:
        mock_bus = Mock()
        mock_bus.publish = mock_publish
        mock_bus_factory.return_value = mock_bus

        @with_company_session_from_message_reuse_connection(session=session, read_only=False)
        def _sqs_send_query_metadata_listener(message: SQSCompanySendMetadata, company_session):
            query_repo = QueryRepositorySA(session=company_session)
            creator = QueryCreator(query_repo=query_repo, session=session)

            issued_query = creator.create(
                company_identifier=message.company_identifier,
                download_type=DownloadType.ISSUED,
                request_type=RequestType.METADATA,
                is_manual=message.manually_triggered,
                wid=message.wid,
                cid=message.cid,
            )
            received_query = creator.create(
                company_identifier=message.company_identifier,
                download_type=DownloadType.RECEIVED,
                request_type=RequestType.METADATA,
                is_manual=message.manually_triggered,
                wid=message.wid,
                cid=message.cid,
            )

            for query in (issued_query, received_query):
                mock_bus.publish(
                    EventType.SAT_SPLIT_NEEDED,
                    QueryNeedToBeSplittedEvent(
                        query=query,
                        wid=query.wid,
                        cid=query.cid,
                        company_identifier=query.company_identifier,
                    ),
                )

        _sqs_send_query_metadata_listener(message=sqs_message)

    sat_split_events = [e for e in published_events if e[0] == EventType.SAT_SPLIT_NEEDED]

    sat_queries = company_session.query(SATQuery).all()

    assert len(sat_queries) == 2
    assert len(sat_split_events) == 2
