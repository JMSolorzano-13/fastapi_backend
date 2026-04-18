import csv
from typing import Any

import requests

from ..core import CFDI
from .base import BaseProcessor

PRESUMED_LINK = "http://omawww.sat.gob.mx/cifras_sat/Documents/Presuntos.csv"
DEFINITIVE_LINK = "http://omawww.sat.gob.mx/cifras_sat/Documents/Definitivos.csv"


class EFOSProcessor(BaseProcessor):
    _black_list_definitive: dict[str, dict[str, Any]] = {}
    _black_list_presumed: dict[str, dict[str, Any]] = {}

    def __init__(self, definitive=None, presumed=None):
        self._black_list_definitive = definitive or self._download_file(DEFINITIVE_LINK) or {}
        self._black_list_presumed = presumed or self._download_file(PRESUMED_LINK) or {}

    def _process(self, cfdi: CFDI):
        if cfdi.RfcEmisor in self._black_list_definitive:
            status = "Definitive"
        elif cfdi.RfcEmisor in self._black_list_presumed:
            status = "Presumed"
        else:
            status = "Ok"
        cfdi.add_extra("efos", {"status": status})

    @staticmethod
    def _download_file(file):
        download = requests.get(file)
        decoded_content = str(download.content, "cp1252")
        data = csv.reader(decoded_content.splitlines())
        for _ in range(3):  # Skip header lines
            next(data, None)
        return {
            line[1]: {
                "no": line[0],
                "rfc": line[1],
                "contributor_name": line[2],
            }
            for line in data
        }
