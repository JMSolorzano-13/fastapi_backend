import xmltodict

from chalicelib.mx_edi.connectors.sat.enums import DownloadType

from .response_parser import ResponseParser


class QueryParser(ResponseParser):
    @staticmethod
    def parse(response: str, download_type: DownloadType) -> dict[str, str]:
        """Gets the Query ID from the raw response"""
        response_dict = xmltodict.parse(response)
        keys = {
            DownloadType.ISSUED: (
                "SolicitaDescargaEmitidosResponse",
                "SolicitaDescargaEmitidosResult",
            ),
            DownloadType.RECEIVED: (
                "SolicitaDescargaRecibidosResponse",
                "SolicitaDescargaRecibidosResult",
            ),
            DownloadType.FOLIO: (
                "SolicitaDescargaFolioResponse",
                "SolicitaDescargaFolioResult",
            ),
        }[download_type]
        result = response_dict["Envelope"]["Body"][keys[0]][keys[1]]
        return {
            "CodEstatus": result["@CodEstatus"],
            "IdSolicitud": result.get("@IdSolicitud"),
        }
