from typing import Any

import xmltodict

from .response_parser import ResponseParser


class VerifyParser(ResponseParser):
    @staticmethod
    def parse(response: str) -> dict[str, Any]:
        """Gets the Query ID from the raw response"""
        response_dict = xmltodict.parse(response)
        result = response_dict["Envelope"]["Body"]["VerificaSolicitudDescargaResponse"][
            "VerificaSolicitudDescargaResult"
        ]
        ids_paquetes = result["IdsPaquetes"] if result["@EstadoSolicitud"] == "3" else []
        if isinstance(ids_paquetes, str):
            ids_paquetes = [ids_paquetes]
        elif not isinstance(ids_paquetes, list):
            raise ValueError("IdsPaquetes is not a list or string")

        return {
            "EstadoSolicitud": result["@EstadoSolicitud"],
            "CodEstatus": result["@CodEstatus"],
            "Mensaje": result["@Mensaje"],
            "CodigoEstadoSolicitud": result.get("@CodigoEstadoSolicitud", 0),
            "NumeroCFDIs": result.get("@NumeroCFDIs", 0),
            "IdsPaquetes": ids_paquetes,
        }
