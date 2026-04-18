import xmltodict

from .response_parser import ResponseParser


class DownloadParser(ResponseParser):
    @staticmethod
    def parse(response: str) -> dict[str, str]:
        """Gets the Download data from the raw response"""
        response_dict = xmltodict.parse(response)
        package = response_dict["Envelope"]["Body"]["RespuestaDescargaMasivaTercerosSalida"][
            "Paquete"
        ]
        return {
            "Content": package,
            "CodEstatus": int(response_dict["Envelope"]["Header"]["respuesta"]["@CodEstatus"]),
        }
