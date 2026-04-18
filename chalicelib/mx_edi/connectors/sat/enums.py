import enum


class DownloadType(enum.Enum):
    """Helper to select the download type"""

    ISSUED = "RfcEmisor"
    RECEIVED = "RfcReceptor"
    FOLIO = "Folio"


class RequestType(enum.Enum):
    """Helper to select the request type"""

    CFDI = "CFDI"
    METADATA = "Metadata"


class TipoComprobante(enum.Enum):
    """Helper to select the invoice type"""

    INGRESO = "I"
    EGRESO = "E"
    TRASLADO = "T"
    NOMINA = "N"
    PAGO = "P"


class EstadoComprobante(enum.Enum):
    """Helper to select the invoice state"""

    TODOS = "Todos"
    CANCELADO = "Cancelado"
    VIGENTE = "Vigente"
