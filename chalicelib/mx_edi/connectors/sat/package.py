import base64
import logging
from sys import getsizeof

from ...core import CFDI
from . import utils
from .enums import RequestType
from .package_parsers import XML2CFDI, Metadata2CFDI
from .response_parsers import DownloadParser
from .sat_connector import SATConnector

_logger = logging.getLogger(__name__)
# _logger.setLevel(logging.DEBUG)


class TooMuchDownloadsError(Exception):
    """No content downloaded, this can be caused by already dowload twice the same package"""


class Package:
    identifier: str
    request_type: RequestType | None

    binary: bytes
    cfdis: list[CFDI]

    request_status: int
    raw: bytes

    def __init__(self, package_id: str, request_type: RequestType | None = None):
        self.identifier = package_id
        self.request_type = request_type

    @classmethod
    def from_ids(cls, package_ids: list[str], request_type: RequestType) -> list["Package"]:
        return [cls(package_id, request_type) for package_id in package_ids]

    def download(self, connector: SATConnector, process: bool = True):
        data = self.soap_download()
        _logger.debug("Downloading package %s", self.identifier)
        response = connector.download_package(data)
        self.raw = response.content
        self._compute_binary()
        sizeof_binary = getsizeof(self.binary)
        _logger.debug("Downloaded package %s (%s bytes)MB", self.identifier, sizeof_binary / 1024)
        if process:
            self._process_download_response()

    def soap_download(self) -> dict[str, str]:
        """Creates the SOAP body to the verify request"""
        return {
            "package_id": self.identifier,
            "signature": "{signature}",
        }

    def _compute_binary(self):
        response_clean = utils.remove_namespaces(self.raw.decode("UTF-8"))
        parsed = DownloadParser.parse(response_clean)
        if parsed["CodEstatus"] == 5008:
            raise TooMuchDownloadsError(
                "No content downloaded, "
                "this can be caused by already download twice the same package"
            )
        if not parsed["Content"]:
            raise ValueError("No content downloaded")
        self.binary = base64.b64decode(parsed["Content"])

    def _process_download_response(self):
        _logger.debug("Creating CFDI's from package %s", self.identifier)
        if self.request_type == RequestType.CFDI:
            self.cfdis = XML2CFDI.from_binary(self.binary)
        elif self.request_type == RequestType.METADATA:
            self.cfdis = Metadata2CFDI.from_binary(self.binary)
        else:
            raise ValueError("Unkown request type")
