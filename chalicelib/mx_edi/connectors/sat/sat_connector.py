import logging
from xml.sax.saxutils import escape

from requests import Response

from . import templates, utils
from .certificate_handler import CertificateHandler
from .enums import DownloadType, RequestType
from .envelope_signer import EnvelopeSigner
from .sat_login_handler import SATLoginHandler

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class SATConnector:
    """Class to make a connection to the SAT"""

    login_handler: SATLoginHandler
    envelope_signer: EnvelopeSigner
    rfc: str

    def __init__(self, cert: bytes, key: bytes, password: bytes) -> None:
        """Loads the certificate, key file and password to stablish the connection to the SAT

        Creates a object to manage the SAT connection.

        Args:
            cert (bytes): DER Certificate in raw binary
            key (bytes): DER Key Certificate in raw binary
            password (bytes): Key password in binary
        """
        certificate_handler = CertificateHandler(cert, key, password)
        self.rfc = utils.handle_special_characters_in_rfc(
            escape(certificate_handler.unique_identifier)
        )
        self.login_handler = SATLoginHandler(certificate_handler)
        self.envelope_signer = EnvelopeSigner(certificate_handler)
        _logger.info("Data correctly loaded")

    def _get_rfc_issued_field(self, download_type: str) -> tuple[str, str]:
        issued = f' RfcEmisor="{self.rfc}"' if download_type == "RfcEmisor" else ""
        received = (
            f"<des:RfcReceptores><des:RfcReceptor>{self.rfc}</des:RfcReceptor></des:RfcReceptores>"
            if download_type == "RfcReceptor"
            else ""
        )
        return issued, received

    def _prepare_v15_query_data(self, data: dict[str, str]) -> dict[str, str]:
        """Prepare data for v1.5 query operations with simplified parameters"""
        result = data.copy()
        result["rfc"] = self.rfc

        # CFDIs Recibidos solo se pueden descargar si están Vigentes
        can_cancelled = not (
            data["download_type"] == DownloadType.RECEIVED.value
            and data["request_type"] == RequestType.CFDI.value
        )
        estado_comprobante = "Todos" if can_cancelled else "Vigente"
        # Set empty values for optional v1.5 parameters
        result["complemento"] = ""
        result["estado_comprobante"] = estado_comprobante
        result["tipo_comprobante"] = ""
        result["rfc_a_cuenta_terceros"] = ""
        result["rfc_solicitante"] = self.rfc

        if data["download_type"] == DownloadType.ISSUED.value:
            # For SolicitaDescargaEmitidos - no RfcReceptores by default
            result["rfc_receptor"] = ""
        elif data["download_type"] == DownloadType.RECEIVED.value:
            # For SolicitaDescargaRecibidos - include RfcReceptor attribute
            result["rfc_receptor"] = f' RfcReceptor="{self.rfc}"'

        return result

    def get_envelope_query(self, data: dict[str, str]) -> str:
        use_v15 = data.get("use_v15") == "true"

        if use_v15:
            return self._get_envelope_query_v15(data)
        else:
            return self._get_envelope_query_v14(data)

    def _get_envelope_query_v14(self, data: dict[str, str]) -> str:
        """Legacy v1.4 query format for backward compatibility"""
        download_type = data["download_type"]
        rfc_issued, rfc_received = self._get_rfc_issued_field(download_type)
        data["rfc_issued"] = rfc_issued
        data["rfc_received"] = rfc_received
        data["rfc"] = self.rfc
        return self.envelope_signer.create_common_envelope(
            templates.SolicitaDescarga,
            data,
        )

    def _get_envelope_query_v15(self, data: dict[str, str]) -> str:
        """New v1.5 query format with separate operations"""
        download_type = data["download_type"]
        template = {
            DownloadType.ISSUED.value: templates.SolicitaDescargaEmitidos,
            DownloadType.RECEIVED.value: templates.SolicitaDescargaRecibidos,
        }[download_type]

        query_data = self._prepare_v15_query_data(data)
        return self.envelope_signer.create_common_envelope(
            template,
            query_data,
        )

    def send_query(self, envelope: str, operation: str = "SolicitaDescarga") -> Response:
        """Send query with configurable operation for v1.4/v1.5 compatibility"""
        soap_action = (
            f"http://DescargaMasivaTerceros.sat.gob.mx/ISolicitaDescargaService/{operation}"
        )
        return utils.consume(
            soap_action,
            "https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/SolicitaDescargaService.svc",
            envelope,
            token=self.login_handler.token,
        )

    def verify_query(self, data: dict[str, str]) -> Response:
        data["rfc"] = self.rfc
        envelope = self.envelope_signer.create_common_envelope(
            templates.VerificaSolicitudDescarga,
            data,
        )
        return utils.consume(
            "http://DescargaMasivaTerceros.sat.gob.mx/IVerificaSolicitudDescargaService/VerificaSolicitudDescarga",
            "https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/VerificaSolicitudDescargaService.svc",
            envelope,
            token=self.login_handler.token,
        )

    def download_package(self, data: dict[str, str]) -> Response:
        """Get the binary response for a package"""
        data["rfc"] = self.rfc
        envelope = self.envelope_signer.create_common_envelope(
            templates.PeticionDescargaMasivaTercerosEntrada,
            data,
        )
        return utils.consume(
            "http://DescargaMasivaTerceros.sat.gob.mx/IDescargaMasivaTercerosService/Descargar",
            "https://cfdidescargamasiva.clouda.sat.gob.mx/DescargaMasivaService.svc",
            envelope,
            token=self.login_handler.token,
        )
