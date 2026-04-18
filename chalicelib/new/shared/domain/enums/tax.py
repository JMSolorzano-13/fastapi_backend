from enum import StrEnum


class Tax(StrEnum):
    ISR = "001"
    IVA = "002"
    IEPS = "003"


class TaxFactor(StrEnum):
    TASA = "Tasa"
    CUOTA = "Cuota"
    EXENTO = "Exento"
