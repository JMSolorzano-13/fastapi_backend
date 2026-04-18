import csv
from collections.abc import Callable
from datetime import datetime
from typing import Any
from zipfile import ZipFile

from ....core.cfdi import CFDI
from .cfdi_parser import CFDIParser


def optional_date(date_string: str) -> datetime | None:
    return datetime.fromisoformat(date_string) if date_string else None


class Metadata2CFDI(CFDIParser):
    csv_to_attribs: dict[str, tuple[str, Callable]] = {
        "Uuid": ("UUID", str),
        "RfcEmisor": ("RfcEmisor", str),
        "NombreEmisor": ("NombreEmisor", str),
        "RfcReceptor": ("RfcReceptor", str),
        "NombreReceptor": ("NombreReceptor", str),
        "RfcPac": ("RfcPac", str),
        "FechaEmision": ("Fecha", datetime.fromisoformat),
        "FechaCertificacionSat": ("FechaCertificacionSat", datetime.fromisoformat),
        "Monto": ("Total", float),
        "EfectoComprobante": ("EfectoComprobante", str),
        "Estatus": ("Estatus", str),
        "FechaCancelacion": ("FechaCancelacion", optional_date),
    }

    @classmethod
    def _get_data(cls, metadata: dict[str, str]) -> dict[str, Any]:
        return {
            attrib[0]: attrib[1](metadata[field]) if metadata[field] else None
            for field, attrib in cls.csv_to_attribs.items()
        }

    @classmethod
    def parse(cls, metadata: dict[str, str]) -> CFDI:
        data = cls._get_data(metadata)
        return CFDI(**data)

    @classmethod
    def _get_metadatas(cls, files: list[str]) -> list[dict[str, str]]:
        return [
            row
            for metadata in files
            for row in csv.DictReader(metadata.splitlines(), delimiter="~")
        ]

    @classmethod
    def parse_zip(cls, zipfile: ZipFile) -> list["CFDI"]:
        metadata_files = cls._get_files(zipfile)
        metadatas = cls._get_metadatas(metadata_files)
        return [
            cls.parse(metadata)
            for metadata in metadatas
            if metadata.get("FechaCancelacion") is not None
        ]
