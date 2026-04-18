import concurrent.futures
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from logging import DEBUG, ERROR, INFO, WARNING

import requests

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.mx_edi import connectors
from chalicelib.mx_edi.connectors.sat.query import Query as WSQuery
from chalicelib.mx_edi.connectors.sat.sat_connector import SATConnector
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums import RequestType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.events.query_sent_event import QueryCreateEvent
from chalicelib.new.query.domain.query import Query
from chalicelib.new.shared.domain.event.event import DomainEvent
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message.sqs_company import SQSUpdaterQuery
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.domain.enums.verify import (
    VerifyQueryStatus,
    VerifyQueryStatusCode,
    VerifyStatusCode,
)
from chalicelib.new.ws_sat.domain.events import (
    QueryNeedToBeSplittedEvent,
)
from chalicelib.new.ws_sat.domain.query_verify_response import QueryVerifyResponse
from chalicelib.new.ws_sat.infra.ws import WSRepo

QueryActionResponse = Iterable[tuple[EventType, DomainEvent]]


MAX_PACKAGES = 200
MAX_LINK_DEPTH_LENGTH = 100


@dataclass
class QueryVerifierWS(WSRepo):
    bus: EventBus

    def get_actions(self) -> dict[VerifyQueryStatus, Callable]:
        return {
            VerifyQueryStatus.UNKNOWN: self.do_check_pending,
            VerifyQueryStatus.ACCEPTED: self.do_check_pending,
            VerifyQueryStatus.IN_PROCESS: self.do_check_pending,
            VerifyQueryStatus.FINISHED: self.do_mark_as_ready_to_download,
            VerifyQueryStatus.ERROR: self.do_mark_as_error,
            VerifyQueryStatus.REJECTED: self.do_mark_as_error,
            VerifyQueryStatus.EXPIRED: self.do_mark_as_error,
        }

    def do_mark_as_ready_to_download(
        self, verify_response: QueryVerifyResponse, query: Query
    ) -> QueryActionResponse:
        query.state = QueryState.TO_DOWNLOAD
        query.cfdis_qty = verify_response.cfdi_qty
        query.packages = verify_response.package_ids
        if not self.has_razonable_packages(query) or not self.has_razonable_cfdi_qty(query):
            query.state = QueryState.MANUALLY_CANCELLED
            return []
        return [
            (
                EventType.SAT_WS_QUERY_DOWNLOAD_READY,
                query,
            )
        ]

    def has_razonable_packages(self, query: Query) -> bool:
        # TODO log message in query
        is_ok = len(query.packages) <= MAX_PACKAGES
        if not is_ok:
            query_body_j = query.model_dump_json()
            log(
                Modules.SAT_WS_VERIFY,
                WARNING,
                "TOO_MANY_PACKAGES",
                {
                    "query_execute_at": query.execute_at,
                    "query_identifier": query.identifier,
                    "company_identifier": query.company_identifier,
                    "max-size": MAX_PACKAGES,
                    "body": query_body_j,
                },
            )
        return is_ok

    def has_razonable_cfdi_qty(self, query: Query) -> bool:
        if query.request_type == RequestType.METADATA:
            return True
        is_ok = query.cfdis_qty <= envars.control.MAX_CFDI_QTY_IN_QUERY
        if not is_ok:
            query_body_j = query.model_dump_json()
            log(
                Modules.SAT_WS_VERIFY,
                WARNING,
                "TOO_MANY_CFDI_TO_BE_PROCESSED_INTERNALLY",
                {
                    "query_execute_at": query.execute_at,
                    "query_identifier": query.identifier,
                    "company_identifier": query.company_identifier,
                    "max-size": envars.control.MAX_CFDI_QTY_IN_QUERY,
                    "body": query_body_j,
                },
            )
        return is_ok

    def do_check_pending(
        self, verify_response: QueryVerifyResponse | None, query: Query
    ) -> QueryActionResponse:
        now = utc_now()
        time_lapsed = now - query.sent_date

        if time_lapsed <= envars.WS_MAX_WAITING_MINUTES:
            return self.retry(query)

        if (now - query.origin_sent_date) < envars.WS_MAX_WAITING_MINUTES_TO_RECREATE:
            return self.recreate(query)

        return self.do_mark_as_time_limit_reached(query)

    def do_mark_as_time_limit_reached(self, query: Query) -> QueryActionResponse:
        query.state = QueryState.TIME_LIMIT_REACHED
        return []

    def recreate(self, query: Query) -> QueryActionResponse:
        query.state = QueryState.SUBSTITUTED
        return [
            (
                EventType.SAT_WS_REQUEST_CREATE_QUERY,
                QueryCreateEvent(
                    company_identifier=query.company_identifier,
                    download_type=query.download_type,
                    request_type=query.request_type,
                    is_manual=query.is_manual,
                    start=query.start,
                    end=query.end,
                    query_origin=query.identifier,
                    wid=query.wid,
                    cid=query.cid,
                    origin_sent_date=query.origin_sent_date,
                ),
            )
        ]

    def retry(self, query: Query) -> QueryActionResponse:
        return [
            (
                EventType.SAT_WS_QUERY_VERIFY_NEEDED,
                query,
            )
        ]

    def mark_information_not_found(self, query: Query) -> QueryActionResponse:
        query.state = QueryState.INFORMATION_NOT_FOUND
        return []

    def mark_need_to_be_splitted(self, query: Query) -> QueryActionResponse:
        query.state = QueryState.SPLITTED
        return [
            (
                EventType.SAT_SPLIT_NEEDED,
                QueryNeedToBeSplittedEvent(
                    company_identifier=query.company_identifier,
                    query=query,
                ),
            )
        ]

    def do_mark_as_error(
        self, verify_response: QueryVerifyResponse, query: Query
    ) -> QueryActionResponse:
        if verify_response.status_code == VerifyQueryStatusCode.INFORMATION_NOT_FOUND:
            self.mark_information_not_found(query)
            return []
        if verify_response.status_code == VerifyQueryStatusCode.MAXIMUM_LIMIT:
            return self._error_to_big(query)
        query.state = QueryState.ERROR
        return []

    def _error_to_big(self, query):
        query.state = QueryState.ERROR_TOO_BIG
        query_body_j = query.model_dump_json()
        if query.request_type == RequestType.CFDI:
            log(
                Modules.SAT_WS_VERIFY,
                WARNING,
                "TOO_MANY_CFDI_IN_QUERY",
                {
                    "query_execute_at": query.execute_at,
                    "query_identifier": query.identifier,
                    "company_identifier": query.company_identifier,
                    "max-size": envars.control.MAX_CFDI_QTY_IN_QUERY,
                    "body": query_body_j,
                },
            )
            return []

        return self.mark_need_to_be_splitted(query)

    def _parallel_verify(
        self, queries: Iterable[Query]
    ) -> list[tuple[Query, QueryActionResponse | None, Exception | None]]:
        # Diccionario para cachear conectores por (wid, cid)
        connector_by_wc: dict[tuple[int, int], SATConnector] = {}
        queries_with_connector: list[tuple[Query, SATConnector | None]] = []

        for query in queries:
            # TODO restructuracion
            if query.is_mocked:
                queries_with_connector.append((query, None))
                continue
            wid_cid: tuple[int, int] = (query.wid, query.cid)
            if wid_cid not in connector_by_wc:
                connector_by_wc[wid_cid] = self.get_sat_connector(wid=query.wid, cid=query.cid)
            connector = connector_by_wc[wid_cid]
            queries_with_connector.append((query, connector))

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=envars.control.PARALLEL_VERIFICATIONS
        ) as executor:
            return executor.map(
                self._verify_no_exception,
                queries_with_connector,
            )

    def _verify_no_exception(
        self, query_with_connector
    ) -> tuple[Query, QueryVerifyResponse | None, Exception | None]:
        query, connector = query_with_connector
        try:
            return query, self._verify(query, connector), None
        except Exception as e:  # pylint: disable=broad-except
            return query, None, e

    def is_something_wrong(
        self, query: Query, verify_response, error: Exception | None
    ) -> tuple[str, dict]:
        query_body_j = query.model_dump_json()
        context = {
            "query_execute_at": query.execute_at,
            "query_identifier": query.identifier,
            "company_identifier": query.company_identifier,
        }
        log_code = None
        if error:
            if error.__class__ == requests.exceptions.ReadTimeout:
                log_code = "VERIFY_TIMEOUT"
            else:
                log_code = "VERIFY_CRASH"
                context |= {"error": str(error), "body": query_body_j}
        elif (
            verify_response.status_code == VerifyQueryStatusCode.SAT_ERROR
            or verify_response.status == VerifyStatusCode.SAT_ERROR
        ):
            log_code = "VERIFY_SAT_ERROR"
            context |= {
                "status_code": verify_response.status_code,
                "body": query_body_j,
                "verify_response": {
                    "sat_uuid": verify_response.sat_uuid,
                    "query_status": verify_response.query_status,
                    "message": verify_response.message,
                    "status_code": verify_response.status_code,
                    "status": verify_response.status,
                    "cfdi_qty": verify_response.cfdi_qty,
                    "package_ids": verify_response.package_ids,
                },
            }
        return log_code, context

    def parallel_verify(self, queries: Iterable[Query]) -> None:
        queries = queries if isinstance(queries, Iterable) else [queries]
        results = self._parallel_verify(queries)
        events_list_tuple: list[tuple[EventType, DomainEvent]] = []
        for query, verify_response, error in results:
            try:
                self._result_to_events(
                    query,
                    verify_response,
                    error,
                    events_list_tuple,
                )
            except Exception as e:  # pylint: disable=broad-except
                log(
                    Modules.SAT_WS_VERIFY,
                    ERROR,
                    "PROCESSING_VERIFICATION_RESULT_CRASH",
                    {
                        "query_execute_at": query.execute_at,
                        "query_identifier": query.identifier,
                        "company_identifier": query.company_identifier,
                        "error": str(e),
                    },
                )

        for event_type, event_handler in events_list_tuple:
            self.bus.publish(event_type, event_handler)

    def _result_to_events(
        self,
        query: Query,
        verify_response: QueryActionResponse | None,
        error: Exception | None,
        events_list_tuple: list[tuple[EventType, DomainEvent]],
    ) -> None:
        prev_state = query.state
        error_log_code, context = self.is_something_wrong(query, verify_response, error)
        if error_log_code:
            level = ERROR
            if (
                context.get("verify_response", {}).get("status")
                == VerifyStatusCode.REVOKED_OR_EXPIRED_CERTIFICATE
            ):
                level = INFO
            else:
                events_list_tuple.extend(self.do_check_pending(None, query))
            log(
                Modules.SAT_WS_VERIFY,
                level,
                error_log_code,
                context,
            )
            return

        action = self.get_actions()[verify_response.query_status]
        events_list_tuple.extend(action(verify_response, query))

        # SOLO enviar tick al updater en casos de éxito o error (estados finales)
        if self._is_final_state(query.state) and query.state != prev_state:
            self._send_update_tick(query, verify_response)

    def _is_final_state(self, state: QueryState) -> bool:
        """Determina si el estado es final (éxito o error) vs intermedio (retry)"""
        final_states = {
            QueryState.TO_DOWNLOAD,  # éxito
            QueryState.MANUALLY_CANCELLED,  # error/cancelado
            QueryState.TIME_LIMIT_REACHED,  # error/timeout
            QueryState.SUBSTITUTED,  # error/necesita recrear
            QueryState.INFORMATION_NOT_FOUND,  # error/no data
            QueryState.ERROR_TOO_BIG,  # error/muy grande
            QueryState.SPLITTED,  # éxito/dividido
            QueryState.ERROR,  # error genérico
        }
        return state in final_states

    def _send_update_tick(self, query: Query, verify_response: QueryVerifyResponse) -> None:
        """Envía tick al updater según el estado final"""
        extra_data = {}
        if query.state == QueryState.TO_DOWNLOAD:
            extra_data |= {
                "cfdis_qty": query.cfdis_qty,
                "packages": query.packages,
            }
        if query.state in {
            QueryState.TO_DOWNLOAD,
            QueryState.MANUALLY_CANCELLED,
            QueryState.TIME_LIMIT_REACHED,
            QueryState.SUBSTITUTED,
            QueryState.INFORMATION_NOT_FOUND,
            QueryState.ERROR_TOO_BIG,
            QueryState.SPLITTED,
            QueryState.ERROR,
        }:
            request = SQSUpdaterQuery(
                query_identifier=query.identifier,
                request_type=query.request_type,
                company_identifier=query.company_identifier,
                state_update_at=utc_now(),
                state=query.state,
                **extra_data,
            )

            self.bus.publish(
                EventType.WS_UPDATER,
                request,
            )

    def mock_verify(self, query: Query) -> QueryVerifyResponse:
        if not query.is_mocked:
            return None

        rfc = query.mocked_rfc
        return QueryVerifyResponse(
            sat_uuid=query.name,
            query_status=VerifyQueryStatus.FINISHED,
            message="MOCKED",
            status_code=VerifyQueryStatusCode.REQUEST_RECEIVED_SUCCESSFULLY,
            status=VerifyStatusCode.REQUEST_RECEIVED_SUCCESSFULLY,
            cfdi_qty=1,
            package_ids=(
                envars.MOCK_PACKAGES.get(rfc, {})
                .get(query.request_type.value, {})
                .get(query.download_type.value, ())
            ),
        )

    def _verify(self, query: Query, connector: SATConnector) -> QueryVerifyResponse:
        query_body_j = query.model_dump_json()
        ws_query = WSQuery(
            identifier=str(query.name),
        )
        if mocked := self.mock_verify(query):
            return mocked
        # connector ya viene del parallel_verify
        # TODO: This is a hack to avoid the timeout in the connector
        connectors.sat.utils.REQUEST_TIMEOUT = envars.control.MAX_SAT_WS_REQUEST_TIMEOUT
        ws_query.verify(connector)
        log(
            Modules.SAT_WS_VERIFY,
            DEBUG,
            "VERIFIED",
            {
                "query_execute_at": query.execute_at,
                "query_identifier": query.identifier,
                "company_identifier": query.company_identifier,
                "body": query_body_j,
            },
        )
        log(
            Modules.SAT_WS_VERIFY,
            DEBUG,
            "verified",
            {
                "query.identifier": query.identifier,
                "query.company_identifier": query.company_identifier,
                "ws_query.query_status": ws_query.query_status,
                "ws_query.status": ws_query.status,
                "ws_query.status_code": ws_query.status_code,
            },
        )
        return QueryVerifyResponse(
            sat_uuid=query.name,
            query_status=VerifyQueryStatus(ws_query.query_status),
            message=ws_query.message,
            status_code=VerifyQueryStatusCode(ws_query.status_code),
            status=VerifyStatusCode(ws_query.status),
            cfdi_qty=ws_query.cfdi_qty,
            package_ids=tuple(package.identifier for package in ws_query.packages),
        )
