from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.tenant.tenant_model import TenantCreatedUpdatedModel


class PolizaCFDI(TenantCreatedUpdatedModel):
    __tablename__ = "poliza_cfdi"

    poliza_identifier = Column(
        IdentifierORM(),
        ForeignKey("per_tenant.poliza.identifier", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    uuid_related = Column(
        IdentifierORM(),
        nullable=False,
        primary_key=True,
    )

    poliza = relationship(
        "Poliza",
        primaryjoin="foreign(PolizaCFDI.poliza_identifier)==Poliza.identifier",
        back_populates="relaciones",
    )
    cfdi_related = relationship(
        "CFDI",
        primaryjoin="foreign(PolizaCFDI.uuid_related)==CFDI.UUID",
        viewonly=True,
    )
