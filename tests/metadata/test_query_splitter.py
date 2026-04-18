from datetime import datetime

from sqlalchemy.orm import Session

from chalicelib.new.config.infra.envars.control import NUM_QUERY_SPLITS
from chalicelib.new.query.domain.enums import DownloadType, RequestType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.query_creator import QueryCreator
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.ws_sat.domain.events.query_need_to_be_splited import (
    QueryNeedToBeSplittedEvent,
)
from chalicelib.new.ws_sat.domain.query_splitter_binary_dates import (
    QuerySplitterBinaryDates,
)
from chalicelib.new.ws_sat.infra.query_verifier_ws import QueryVerifierWS
from chalicelib.schema.models.company import Company


def test_verify_split(company_session: Session, company: Company, session: Session):
    query = Query(
        download_type=DownloadType.ISSUED,
        request_type=RequestType.METADATA,
        start=datetime(2025, 1, 1),
        end=datetime(2025, 3, 5),
        company_identifier=company.identifier,
    )

    event = QueryNeedToBeSplittedEvent(
        query=query,
        wid=company.workspace_id,
        cid=company.id,
        company_identifier=company.identifier,
    )
    query_repo = QueryRepositorySA(session=company_session)
    query = event.query
    query_creator = QueryCreator(query_repo=query_repo, session=session)
    splitter = QuerySplitterBinaryDates(query_creator=query_creator)

    query_verifier = QueryVerifierWS(bus=None)
    query_verifier.mark_need_to_be_splitted(query)
    assert query.state == QueryState.SPLITTED

    result = splitter.split(query)
    assert len(result) == NUM_QUERY_SPLITS
