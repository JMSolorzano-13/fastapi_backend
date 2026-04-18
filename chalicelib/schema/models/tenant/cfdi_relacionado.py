from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    String,
    func,
)
from sqlalchemy.orm import relationship

from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class CfdiRelacionado(TenantIdentifiedModel):
    __tablename__ = "cfdi_relation"

    company_identifier = Column(
        IdentifierORM(),
        nullable=False,
        primary_key=True,
    )
    created_at = Column(
        DateTime,
        server_default=func.now(),
        index=True,
    )

    uuid_origin = Column(
        IdentifierORM(),
        nullable=False,
        index=True,
    )

    TipoDeComprobante = Column(
        String,
        index=True,
        nullable=False,
        default="",
    )
    is_issued = Column(
        Boolean,
        nullable=False,
        primary_key=True,
        default=True,
    )
    Estatus = Column(  # TODO remove; other wise, update on cancel
        Boolean,
        nullable=False,
        default=True,
    )
    uuid_related = Column(
        IdentifierORM(),
        nullable=False,
        index=True,
    )

    TipoRelacion = Column(
        String,
        nullable=False,
    )

    cfdi_related = relationship(
        "CFDI",
        primaryjoin="CfdiRelacionado.uuid_related == foreign(CFDI.UUID)",  # noqa E501
        foreign_keys=[uuid_related],
        uselist=False,
    )
    cfdi_origin = relationship(
        "CFDI",
        primaryjoin="CfdiRelacionado.uuid_origin == foreign(CFDI.UUID)",  # noqa E501
        foreign_keys=[uuid_origin],
        uselist=False,
    )

    # egresos_relacionados = relationship(
    #     "CFDI",
    #     primaryjoin="CfdiRelacionado.uuid_origin == CFDI.UUID",  # noqa E501
    #     secondary="cfdi_relation",
    #     secondaryjoin="foreign(CfdiRelacionado.uuid_origin) == CFDI.UUID",  # noqa E501
    #     foreign_keys=[uuid_origin],
    #     uselist=False,
    #     viewonly=True,
    # )

    @classmethod
    def get_specific_table(cls, company_identifier: IdentifierORM, is_issued: bool = None) -> str:
        # TODO restructuracion
        return cls.__tablename__
