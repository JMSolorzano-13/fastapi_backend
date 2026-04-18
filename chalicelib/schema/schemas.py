from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import BaseModel, Field, field_validator


class CFDIResponse(BaseModel):
    BASE_FIELDS: ClassVar[set[str]] = {
        "UUID",
        "is_issued",
        "Fecha",
        "Version",
        "PaymentDate",
        "FechaPago",
        "Serie",
        "Folio",
        "RfcEmisor",
        "NombreEmisor",
        "TipoDeComprobante",
        "UsoCFDIReceptor",
        "ExcludeFromIVA",
        "MetodoPago",
        "FormaPagoCode",
        "FormaPagoName",
    }

    uuid: str = Field(..., alias="UUID")
    is_issued: bool
    fecha: datetime = Field(..., alias="Fecha")
    version: str = Field(..., alias="Version")
    payment_date: datetime | None = Field(None, alias="PaymentDate")
    serie: str | None = Field(None, alias="Serie")
    folio: str | None = Field(None, alias="Folio")
    rfc_emisor: str = Field(..., alias="RfcEmisor")
    nombre_emisor: str = Field(..., alias="NombreEmisor")
    tipo_de_comprobante: str = Field(..., alias="TipoDeComprobante")
    uso_cfdi_receptor: str = Field(..., alias="UsoCFDIReceptor")
    exclude_from_iva: bool = Field(..., alias="ExcludeFromIVA")
    metodo_pago: str = Field(..., alias="MetodoPago")
    base_iva_16: Decimal | None = Field(default=Decimal(0.0), alias="BaseIVA16")
    base_iva_8: Decimal | None = Field(default=Decimal(0.0), alias="BaseIVA8")
    base_iva_0: Decimal | None = Field(default=Decimal(0.0), alias="BaseIVA0")
    base_iva_exento: Decimal | None = Field(default=Decimal(0.0), alias="BaseIVAExento")
    iva_trasladado_16: Decimal | None = Field(default=Decimal(0.0), alias="IVATrasladado16")
    iva_trasladado_8: Decimal | None = Field(default=Decimal(0.0), alias="IVATrasladado8")
    traslados_iva: Decimal | None = Field(default=Decimal(0.0), alias="TrasladosIVA")
    retenciones_iva: Decimal | None = Field(default=Decimal(0.0), alias="RetencionesIVA")
    total: Decimal | None = Field(default=Decimal(0.0), alias="Total")
    forma_pago_code: str | None = Field(None, alias="FormaPago")
    forma_pago_name: str | None = Field(None, alias="FormaPagoName")

    dr_uuid: str | None = Field(None, alias="DR-UUID")
    dr_exclude_from_iva: bool | None = Field(None, alias="DR-ExcludeFromIVA")
    dr_forma_pago_name: str | None = Field(None, alias="DR-FormaPagoName")
    dr_base_iva_exento: Decimal | None = Field(default=Decimal(0.0), alias="DR-BaseIVAExento")
    identifier: str | None = Field(None, alias="DR-Identifier")

    @field_validator("payment_date", "fecha")
    def parse_datetime(cls, value):
        if value is None:
            return None
        return value

    def _process_value(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value) if value else 0.0
        if isinstance(value, datetime):  # Añadimos manejo de datetime
            return value.isoformat() if value else None
        return value

    @field_validator(
        "base_iva_16",
        "base_iva_8",
        "base_iva_0",
        "base_iva_exento",
        "iva_trasladado_16",
        "iva_trasladado_8",
        "traslados_iva",
        "retenciones_iva",
        mode="before",
    )
    def convert_none_to_decimal(cls, value):
        if value is None:
            return Decimal(0.0)
        return value

    def to_json_dict(self) -> dict[str, Any]:
        data = self.model_dump(by_alias=True)
        return {k: self._process_value(v) for k, v in data.items()}

    @classmethod
    def from_orm_mapping(cls, record):
        return cls(**record._mapping)
