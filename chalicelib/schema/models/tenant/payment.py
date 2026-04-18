from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    and_,
)
from sqlalchemy.orm import foreign, relationship

from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.catalogs import CatFormaPago
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class Payment(TenantIdentifiedModel):
    __tablename__ = "payment"

    company_identifier = Column(
        IdentifierORM(),
        nullable=False,
        primary_key=True,
    )
    is_issued = Column(
        Boolean,
        nullable=False,
        server_default="false",  # TODO remove after migration
    )

    Estatus = Column(
        Boolean,
        nullable=False,
        default=True,
    )

    uuid_origin = Column(
        IdentifierORM(),
        index=True,
        nullable=False,
    )
    index = Column(
        Integer,
        nullable=False,
        index=True,
    )
    UniqueConstraint(
        "uuid_origin",
        "index",
    )

    FechaPago = Column(
        DateTime,
        nullable=False,
    )
    FormaDePagoP = Column(
        String,
        nullable=False,
    )
    MonedaP = Column(
        String,
        nullable=False,
    )
    Monto = Column(
        Numeric,
        nullable=False,
    )
    TipoCambioP = Column(
        Numeric,
    )
    NumOperacion = Column(
        String,
    )
    RfcEmisorCtaOrd = Column(
        String,
    )
    NomBancoOrdExt = Column(
        String,
    )
    CtaOrdenante = Column(
        String,
    )
    RfcEmisorCtaBen = Column(
        String,
    )
    CtaBeneficiario = Column(
        String,
    )
    TipoCadPago = Column(
        String,
    )
    CertPago = Column(
        String,
    )
    CadPago = Column(
        String,
    )
    SelloPago = Column(
        String,
    )
    Estatus = Column(  # reference=cfdi_origin.Estatus
        Boolean,
        nullable=False,
        server_default="TRUE",
    )

    docto_relacionados = relationship(
        "DoctoRelacionado",
        viewonly=True,
        uselist=True,
        primaryjoin="foreign(Payment.identifier) == DoctoRelacionado.payment_identifier",  # noqa E501
    )
    c_forma_pago = relationship(
        CatFormaPago,
        uselist=False,
        backref="payments",
        foreign_keys=[FormaDePagoP],
        primaryjoin=CatFormaPago.code == FormaDePagoP,
        viewonly=True,
    )
    cfdi_origin = relationship(
        "CFDI",
        uselist=False,
        backref="paymentscccc",
        foreign_keys=[uuid_origin],
        primaryjoin=and_(
            foreign(CFDIORM.UUID) == uuid_origin,
            foreign(CFDIORM.is_issued) == is_issued,
        ),
    )

    @classmethod
    def from_dict(
        cls,
        pago_dict: dict[str, Any],
        cfdi: CFDIORM,  # Not full CFDIORM, only the fields needed
    ) -> "Payment":
        fields = {
            "FechaPago",
            "FormaDePagoP",
            "MonedaP",
            "Monto",
            "TipoCambioP",
            "NumOperacion",
            "RfcEmisorCtaOrd",
            "NomBancoOrdExt",
            "CtaOrdenante",
            "RfcEmisorCtaBen",
            "CtaBeneficiario",
            "TipoCadPago",
            "CertPago",
            "CadPago",
            "SelloPago",
        }
        payment = cls(**{field: pago_dict.get(f"@{field}") for field in fields})

        payment.identifier = identifier_default_factory()

        payment.company_identifier = getattr(cfdi, "company_identifier", None)
        payment.is_issued = cfdi.is_issued
        payment.uuid_origin = cfdi.UUID
        payment.Estatus = cfdi.Estatus

        return payment

    @classmethod
    def get_specific_table(cls, company_identifier: IdentifierORM, is_issued: bool = None) -> str:
        # TODO restructuracion
        return cls.__tablename__
