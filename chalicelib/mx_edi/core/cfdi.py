import logging
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime
from typing import Any

from .concepto import Concepto


@dataclass
class CFDI:
    UUID: str
    Fecha: datetime
    Total: float
    # XML fields
    Version: str | None = None
    Sello: str | None = None
    UsoCFDIReceptor: str | None = None
    RegimenFiscalEmisor: str | None = None
    CondicionesDePago: str | None = None
    CfdiRelacionados: set[str] = field(default_factory=set)
    Folio: str | None = None
    Serie: str | None = None
    NoCertificado: str | None = None
    Certificado: str | None = None
    TipoDeComprobante: str | None = None
    LugarExpedicion: str | None = None
    FormaPago: str | None = None
    MetodoPago: str | None = None
    Moneda: str | None = None
    TipoCambio: float | None = None
    SubTotal: float | None = None
    Conceptos: list[Concepto] = field(default_factory=list)
    xml: str | None = None
    Exportacion: str = ""
    Periodicidad: str = ""
    Meses: str = ""
    # CSV Fields
    RfcEmisor: str | None = None
    NombreEmisor: str | None = None
    RfcReceptor: str | None = None
    NombreReceptor: str | None = None
    RfcPac: str | None = None
    FechaCertificacionSat: datetime | None = None
    EfectoComprobante: str | None = None
    Estatus: str | None = None
    FechaCancelacion: datetime | None = None
    # Extras
    _extras: dict[str, Any] = field(default_factory=dict)
    cfdis_related: set["CFDI"] = field(default_factory=set)

    @property
    def extras(self) -> dict[str, Any]:
        return self._extras or {}

    def add_extra(self, key: str, value: Any):
        self._extras[key] = value

    def clean_extras(self):
        self._extras = {}

    def __post_init__(self):
        self.CfdiRelacionados = set(self.CfdiRelacionados or {})
        self.cfdis_related = set()
        self._extras = dict(self._extras or {})
        self.UUID = self.UUID.upper()

    def __bool__(self):
        return bool(self.UUID)

    def merge(self, other: "CFDI"):
        for attrib, value in self.__dict__.items():
            other_value = getattr(other, attrib)
            if not other:
                return
            if value and value != other_value:
                logging.debug("Inconsistent Information '%s' != '%s'", value, other_value)
            setattr(self, attrib, other_value)

    def to_dict(self) -> dict[str, Any]:
        dict_repr: dict[str, Any] = {}

        def _to_dict(dict_repr, obj):
            for f in obj.__dataclass_fields__.values():
                if not f.init:
                    continue
                value = getattr(obj, f.name)
                if not value:
                    continue
                if isinstance(value, list):
                    dict_repr[f.name] = [_to_dict({}, item) for item in value]
                elif is_dataclass(value):
                    dict_repr[f.name] = _to_dict({}, value)
                else:
                    dict_repr[f.name] = value
            return dict_repr

        _to_dict(dict_repr, self)
        return dict_repr

    @classmethod
    def reduce(cls, cfdis: list["CFDI"]) -> list["CFDI"]:
        by_uuid: dict[str, list[CFDI]] = {}
        for cfdi in cfdis:
            if cfdi.UUID not in by_uuid:
                by_uuid[cfdi.UUID] = []
            by_uuid[cfdi.UUID].append(cfdi)
        for cfdis_by_uuid in by_uuid.values():
            while len(cfdis_by_uuid) > 1:
                cfdi = cfdis_by_uuid.pop()
                cfdis_by_uuid[0].merge(cfdi)
        return [cfdi for cfdi_g in by_uuid.values() for cfdi in cfdi_g]

    def __hash__(self):
        return hash(self.UUID)

    def __eq__(self, other):
        return self.UUID == other.UUID

    def add_related(self, cfdfis: dict[str, "CFDI"]):
        self.__post_init__()
        for uuid in self.CfdiRelacionados:
            if uuid in cfdfis:
                self.cfdis_related.add(cfdfis[uuid])
