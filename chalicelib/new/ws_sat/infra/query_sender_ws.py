import concurrent.futures
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from logging import DEBUG, ERROR, INFO

import boto3
from sqlalchemy import update
from sqlalchemy.orm import Session

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.mx_edi import connectors
from chalicelib.mx_edi.connectors.sat.enums import DownloadType as WSDownloadType
from chalicelib.mx_edi.connectors.sat.enums import RequestType as WSRequestType
from chalicelib.mx_edi.connectors.sat.query import Query as WSQuery
from chalicelib.mx_edi.connectors.sat.sat_connector import SATConnector
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums import QueryState
from chalicelib.new.query.domain.query import Query
from chalicelib.new.shared.domain.event import EventType
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.domain.enums.send import SendStatusCode
from chalicelib.new.ws_sat.fiel_repository_s3 import _check_certs_exist
from chalicelib.new.ws_sat.infra.ws import WSRepo
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import SATQuery as SATQuery


@dataclass
class QuerySenderWS(WSRepo):
    bus: EventBus
    company_session: Session
    session: Session

    def actions_from_status(self) -> dict[int, Callable[[Query, WSQuery], None]]:
        return {
            SendStatusCode.DOWNLOAD_REQUEST_RECEIVED_SUCCESSFULLY: self.mark_as_sent,
            SendStatusCode.REVOKED_OR_EXPIRED_CERTIFICATE: self.mark_as_error_in_certs,
            SendStatusCode.INVALID_CERTIFICATE: self.mark_as_error_in_certs,
            SendStatusCode.UNKNOWN: self.mark_as_error_unknown,
        }

    def _parallel_send(
        self, queries: list[Query]
    ) -> Iterator[tuple[Query, WSQuery | None, Exception | None]]:
        """Returns a list of queries properly sent"""
        companies_with_cert_errors = set()
        connector_by_company = {}

        unique_companies = {}
        for query in queries:
            company_identifier = query.company_identifier
            if company_identifier not in unique_companies:
                unique_companies[company_identifier] = (query.wid, query.cid)

        for company_identifier, (wid, cid) in unique_companies.items():
            if not _check_certs_exist(
                s3_client=boto3.client("s3"), bucket_url=envars.S3_CERTS, wid=wid, cid=cid
            ):
                companies_with_cert_errors.add(company_identifier)
            else:
                connector_by_company[company_identifier] = self.get_sat_connector(wid, cid)

        valid_queries = []
        for query in queries:
            if query.company_identifier in companies_with_cert_errors:
                self.mark_as_error_in_certs(query, None)
            else:
                valid_queries.append(query)

        queries = valid_queries
        queries_with_connector = [
            (query, connector_by_company[query.company_identifier]) for query in queries
        ]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=envars.control.PARALLEL_SENDS
        ) as executor:
            # MAX_SAT_WS_REQUEST_SEND_TIMEOUT
            return executor.map(
                self._parallel_send_ws,
                queries_with_connector,
            )

    def _parallel_send_ws(
        self, query_with_connector: tuple[Query, SATConnector]
    ) -> tuple[Query, WSQuery | None, Exception | None]:
        query, connector = query_with_connector
        if not connector:
            return query, None, Exception("No connector found")
        try:
            return query, self._send(query, connector), None
        except Exception as e:  # pylint: disable=broad-except
            return query, None, e

    def parallel_send(self, queries: list[Query]) -> None:
        sent = self._parallel_send(queries)

        for query, ws_query, error in sent:
            if error:
                self.mark_as_error(query, error)
                continue
            assert ws_query
            action = self.actions_from_status().get(ws_query.status)
            if not action:
                self.mark_as_error(
                    query,
                    Exception(
                        f"Unhandled WS Query status {ws_query.status} for {query.identifier} "
                        f"in company {query.company_identifier}"
                    ),
                )
                continue
            action(query, ws_query)

    def mock_send(self, query: Query, ws_query: WSQuery) -> WSQuery | None:
        if not query.is_mocked:
            return None

        ws_query.status = SendStatusCode.DOWNLOAD_REQUEST_RECEIVED_SUCCESSFULLY
        ws_query.identifier = query.name
        ws_query.sent_date = datetime.now()
        return ws_query

    def _send(self, query: Query, connector) -> WSQuery:
        log(
            Modules.SAT_WS_SEND,
            DEBUG,
            "SENDING",
            {
                "query_execute_at": query.execute_at,
                "query_identifier": query.identifier,
                "company_identifier": query.company_identifier,
            },
        )
        ws_query = WSQuery(
            download_type=WSDownloadType[query.download_type.name],
            request_type=WSRequestType[query.request_type.name],
            start=query.start,
            end=query.end,
        )

        mocked = self.mock_send(query, ws_query)
        if mocked:
            return mocked

        log(
            Modules.SAT_WS_SEND,
            INFO,
            "CONNECTOR_SET",
            {
                "company_identifier": query.company_identifier,
                "query_identifier": query.identifier,
                "rfc": connector.rfc,
            },
        )
        log(
            Modules.SAT_WS_SEND,
            DEBUG,
            "sending_query",
            {
                "company_identifier": query.company_identifier,
                "query_identifier": query.identifier,
            },
        )
        connectors.sat.utils.REQUEST_TIMEOUT = envars.control.MAX_SAT_WS_REQUEST_SEND_TIMEOUT
        ws_query.send(connector)
        log(
            Modules.SAT_WS_SEND,
            INFO,
            "SENT",
            {
                "query_execute_at": query.execute_at,
                "query_identifier": query.identifier,
                "company_identifier": query.company_identifier,
                "ws_query_status": ws_query.status,
                "ws_query_identifier": ws_query.identifier,
            },
        )
        return ws_query

    def mark_as_sent(self, query: Query, ws_query: WSQuery) -> None:
        query.name = ws_query.identifier
        query.state = QueryState.SENT
        query.sent_date = utc_now()
        query.origin_sent_date = query.origin_sent_date or query.sent_date
        if query.is_manual:
            query.execute_at = utc_now()

        self.bus.publish(EventType.SAT_WS_QUERY_SENT, query)

        log(
            Modules.SAT_WS_SEND,
            DEBUG,
            "marking_as_sent",
            {
                "company_identifier": query.company_identifier,
                "query_identifier": query.identifier,
            },
        )
        self.company_session.execute(
            update(SATQuery)
            .where(SATQuery.identifier == query.identifier)
            .values(
                name=query.name,
                state=query.state,
                sent_date=query.sent_date,
            )
        )

    def mark_as_error_in_certs(self, query: Query, ws_query: WSQuery | None) -> None:
        self.company_session.query(SATQuery).filter(SATQuery.identifier == query.identifier).update(
            {
                "state": QueryState.ERROR_IN_CERTS,
            },
            synchronize_session=False,
        )
        self.session.query(Company).filter(Company.identifier == query.company_identifier).update(
            {
                "has_valid_certs": False,
            },
            synchronize_session=False,
        )

    def mark_as_error_unknown(self, query: Query, ws_query: WSQuery) -> None:
        self.company_session.query(SATQuery).filter(SATQuery.identifier == query.identifier).update(
            {
                "state": QueryState.ERROR_SAT_WS_UNKNOWN,
            },
            synchronize_session=False,
        )

    def mark_as_error(
        self,
        query: Query,
        error: Exception,
    ) -> None:
        log(
            Modules.SAT_WS_SEND,
            ERROR,
            "FAILED_SAT_WS_CAUSE",
            {
                "query_execute_at": query.execute_at,
                "query_identifier": query.identifier,
                "company_identifier": query.company_identifier,
                "error": error,
                "body query": query.model_dump(),
            },
        )
        self.company_session.query(SATQuery).filter(SATQuery.identifier == query.identifier).update(
            {
                "state": QueryState.ERROR_SAT_WS_INTERNAL,
            },
            synchronize_session=False,
        )
