import enum
from dataclasses import dataclass
from datetime import datetime


class TipoComprobante(enum.Enum):
    pass


class FormaPago(enum.Enum):
    pass


class MetodoPago(enum.Enum):
    pass


class Estatus(enum.Enum):
    pass


class UsoCFDIReceptor(enum.Enum):
    pass


class Exportacion(enum.Enum):
    pass


class Periodicidad(enum.Enum):
    pass


class Meses(enum.Enum):
    pass


class RegimenFiscal(enum.Enum):
    pass


class Moneda(enum.Enum):
    pass


@dataclass
class CFDI:
    fiscal_uuid: str
    fecha: datetime
    total: float
    folio: str
    serie: str
    no_certificado: str
    certificado: str
    tipo_de_comprobante: TipoComprobante
    lugar_expedicion: str
    forma_pago: FormaPago
    metodo_pago: MetodoPago
    moneda: Moneda
    subtotal: float
    rfc_emisor: str
    nombre_emisor: str
    rfc_receptor: str
    nombre_receptor: str
    rfc_pac: str
    fecha_certificacion_sat: datetime
    estatus: Estatus
    fecha_cancelacion: datetime | None
    tipo_cambio: float | None
    conceptos: str
    version: str
    sello: str
    uso_cfdi_receptor: UsoCFDIReceptor
    regimen_fiscal_emisor: RegimenFiscal
    condiciones_de_pago: str
    cfdi_relacionados: str
    neto: float
    exportacion: Exportacion
    periodicidad: Periodicidad
    meses: Meses
    year: int
    domicilio_fiscal_receptor: str
    regimen_fiscal_receptor: RegimenFiscal
    no_certificado_sat: str
    sello_sat: str
    descuento: float
    xml_content: str
    balance: float
