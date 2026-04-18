import uuid
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy.orm import Session

from chalicelib.mx_edi.connectors.sat.enums import DownloadType as WSDownloadType
from chalicelib.mx_edi.connectors.sat.enums import RequestType as WSRequestType
from chalicelib.mx_edi.connectors.sat.query import Query as WSQuery
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.domain.query_creator import QueryCreator
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.ws_sat.domain.enums.send import SendStatusCode
from chalicelib.new.ws_sat.infra.query_sender_ws import QuerySenderWS
from chalicelib.schema.models.company import Company


def make_parallel_send_with_status(status: SendStatusCode):
    def _parallel_send(self, queries: list[Query]):
        res: list[tuple[Query, WSQuery | None, Exception | None]] = []
        for query in queries:
            ws_query = WSQuery(
                download_type=WSDownloadType[query.download_type.name],
                request_type=WSRequestType[query.request_type.name],
                start=query.start,
                end=query.end,
            )
            ws_query.status = status
            ws_query.identifier = str(uuid.uuid4())
            ws_query.sent_date = datetime.now()
            res.append((query, ws_query, None))
        return iter(res)

    return _parallel_send


@pytest.mark.parametrize(
    "send_status",
    [
        SendStatusCode.REVOKED_OR_EXPIRED_CERTIFICATE,
        SendStatusCode.INVALID_CERTIFICATE,
    ],
)
def test_mark_company_with_no_has_valid_certs_if_send_fail_by_certificate(
    send_status,
    company_session: Session,
    company: Company,
    session: Session,
    bus: EventBus,
):
    with patch(
        "chalicelib.new.ws_sat.infra.query_sender_ws.QuerySenderWS._parallel_send",
        make_parallel_send_with_status(send_status),
    ):
        sender = QuerySenderWS(
            bus=bus,
            company_session=company_session,
            session=session,
        )
        query_repo = QueryRepositorySA(session=company_session)
        creator = QueryCreator(query_repo=query_repo, session=session)

        issued = creator.create(
            company_identifier=company.identifier,
            download_type=DownloadType.ISSUED,
            request_type=RequestType.METADATA,
            is_manual=False,
            wid=company.workspace_id,
            cid=company.id,
        )

        session.refresh(company)
        assert company.has_valid_certs

        sender.parallel_send([issued])

        session.refresh(company)
        assert not company.has_valid_certs
