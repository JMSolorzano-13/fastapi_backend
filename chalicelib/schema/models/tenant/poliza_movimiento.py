from decimal import Decimal

from sqlalchemy import (
    Column,
    ForeignKey,
    Numeric,
    String,
)
from sqlalchemy.orm import relationship

from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class PolizaMovimiento(TenantIdentifiedModel):
    __tablename__ = "poliza_movimiento"

    numerador = Column(String)
    cuenta_contable = Column(String)
    nombre = Column(String)
    cargo = Column(
        Numeric,
        default=0,
        nullable=False,
    )
    abono = Column(
        Numeric,
        default=0,
        nullable=False,
    )
    cargo_me = Column(
        Numeric,
        default=0,
        nullable=False,
    )
    abono_me = Column(
        Numeric,
        default=0,
        nullable=False,
    )
    concepto = Column(String)
    referencia = Column(String)

    poliza_identifier = Column(
        IdentifierORM(),
        ForeignKey("per_tenant.poliza.identifier", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    poliza = relationship("Poliza")

    @classmethod
    def from_dict(cls, data: dict) -> "PolizaMovimiento":
        tipo_de_cambio = dff(data.pop("tipo_de_cambio", 0))
        if tipo_de_cambio:
            data["cargo_me"] = data.get("cargo_me", dff(data.get("cargo", 0))) / tipo_de_cambio
            data["abono_me"] = data.get("abono_me", dff(data.get("abono", 0))) / tipo_de_cambio

        return cls(**data)


def dff(value: float | str | int | Decimal | None) -> Decimal:
    """Decimal From Float"""
    if value is None:
        return Decimal("0")
    return Decimal(str(value))
