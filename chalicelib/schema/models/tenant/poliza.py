from sqlalchemy import (
    Column,
    DateTime,
    Index,
    String,
)
from sqlalchemy.orm import relationship

from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI
from chalicelib.schema.models.tenant.poliza_movimiento import PolizaMovimiento
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class Poliza(TenantIdentifiedModel):
    __tablename__ = "poliza"

    fecha = Column(
        DateTime,
        nullable=False,
    )
    tipo = Column(
        String,
        nullable=False,
    )
    numero = Column(
        String,
        nullable=False,
    )
    concepto = Column(
        String,
    )
    sistema_origen = Column(
        String,
    )

    relaciones = relationship(
        PolizaCFDI,
        primaryjoin="foreign(PolizaCFDI.poliza_identifier)==Poliza.identifier",
        # back_populates="poliza",
        cascade="all, delete-orphan",
    )

    # Conveniencia M:N "viewonly" porque no hay FK dura
    # (no insertará filas en poliza_cfdi por ti)
    cfdis = relationship(
        "CFDI",
        secondary=PolizaCFDI.__table__,
        primaryjoin="Poliza.identifier==PolizaCFDI.poliza_identifier",
        secondaryjoin="foreign(PolizaCFDI.uuid_related)==CFDI.UUID",
        viewonly=True,
    )

    movimientos = relationship(PolizaMovimiento)


Index(
    "ix_poliza_unique",
    Poliza.fecha,
    Poliza.tipo,
    Poliza.numero,
    unique=True,
)
