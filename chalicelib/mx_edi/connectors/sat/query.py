import logging
import time
from datetime import datetime, timedelta

from requests import Response

from . import utils
from .enums import DownloadType, RequestType
from .package import Package
from .response_parsers import QueryParser, VerifyParser
from .sat_connector import SATConnector

_logger = logging.getLogger(__name__)
DEFAULT_TIME_WINDOW = timedelta(days=30)


class QueryException(Exception):
    """If not valid query"""


class Query:
    download_type: DownloadType | None
    request_type: RequestType | None
    start: datetime
    end: datetime

    identifier: str
    status: int

    request_status: int

    query_status: int
    message: str
    status_code: int
    cfdi_qty: int
    packages: list[Package]

    sent_date: datetime
    verified_date: datetime

    def __init__(
        self,
        download_type: DownloadType | None = None,
        request_type: RequestType | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        identifier: str | None = None,
    ):
        self.download_type = download_type
        self.request_type = request_type
        # Set start as current time in Mexico timezone
        self.end = end or datetime.now()
        self.start = start or self.end - DEFAULT_TIME_WINDOW
        self.identifier = identifier or ""

    def _get_query_xml(self, connector: SATConnector) -> str:
        """
        Returns the query XML to be sent to the SAT
        """
        data = self.soap_send()
        return connector.get_envelope_query(data)

    def send(self, connector: SATConnector):
        query_xml = self._get_query_xml(connector)
        operation = self._get_operation_name()
        response = connector.send_query(query_xml, operation)
        self._process_send_response(response)

    def _get_operation_name(self) -> str:
        return {
            DownloadType.ISSUED: "SolicitaDescargaEmitidos",
            DownloadType.RECEIVED: "SolicitaDescargaRecibidos",
            DownloadType.FOLIO: "SolicitaDescargaFolio",
        }[self.download_type]

    def soap_send(self) -> dict[str, str]:
        """Creates the SOAP body to the send request"""
        start = self.start.isoformat()
        end = self.end.isoformat()
        if not (self.download_type and self.request_type):
            raise QueryException("If query is sent, download type and request type must be set")

        # Automatically use v1.5 format for ISSUED/RECEIVED
        use_v15 = self.download_type in [DownloadType.ISSUED, DownloadType.RECEIVED]

        data = {
            "start": start,
            "end": end,
            "download_type": self.download_type.value,
            "request_type": self.request_type.value,
            "signature": "{signature}",
        }

        # Add use_v15 as string for template compatibility
        if use_v15:
            data["use_v15"] = "true"

        return data

    def _process_send_response(self, response: Response):
        response_clean = self._set_request_status_check_and_clean_response(response)
        parsed = QueryParser.parse(response_clean, self.download_type)
        self.status = int(parsed["CodEstatus"])

        self.identifier = parsed["IdSolicitud"]
        self.sent_date = datetime.now()

    def verify(self, connector: SATConnector):
        data = self.soap_verify()
        response = connector.verify_query(data)
        self._process_verify_response(response)

    def soap_verify(self) -> dict[str, str]:
        """Creates the SOAP body to the verify request"""
        return {
            "identifier": self.identifier,
            "signature": "{signature}",
        }

    def _process_verify_response(self, response: Response):
        response_clean = self._set_request_status_check_and_clean_response(response)
        try:
            parsed = VerifyParser.parse(response_clean)
        except KeyError as e:
            _logger.error("Missing key %s in query ID %s", e, self.identifier)
            raise
        self.status = int(parsed["CodEstatus"])

        self.query_status = int(parsed["EstadoSolicitud"])
        self.message = parsed["Mensaje"]
        self.status_code = int(parsed["CodigoEstadoSolicitud"])
        self.cfdi_qty = int(parsed["NumeroCFDIs"])
        self.packages = Package.from_ids(
            parsed["IdsPaquetes"], self.request_type or RequestType.CFDI
        )
        self.verified_date = datetime.now()

    def _set_request_status_check_and_clean_response(self, response):
        self.request_status = response.status_code
        utils.check_response(response)
        return utils.remove_namespaces(response.content.decode("UTF-8"))

    def download(self, connector: SATConnector):
        for package in self.packages:
            package.download(connector)

    def get_packages(
        self, connector: SATConnector, retries: int = 10, wait_seconds: int = 2
    ) -> list[Package]:
        for _ in range(retries):
            self.verify(connector)
            if self.query_status > 3:
                raise QueryException(f"EstadoSolicitud({self.status_code})")
            if self.query_status == 3:
                return self.packages
            time.sleep(wait_seconds)
        raise TimeoutError("The query is not yet resolved")
