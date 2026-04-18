import random
import uuid
from collections import defaultdict
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql import select

from chalicelib.new.shared.domain.enums import Tax, TaxFactor
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.new.utils import dicts
from chalicelib.schema.models.model import ColumnBetween
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel
from chalicelib.schema.UserDefinedType.mx_amount import MXAmount

BASE = 0
AMOUNT = 1


class DoctoRelacionado(TenantIdentifiedModel):
    __tablename__ = "payment_relation"

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
    created_at = Column(
        DateTime,
        server_default=func.now(),
        index=True,
    )

    payment_identifier = Column(  # points towards Payment intermediary object
        IdentifierORM(),
        nullable=False,
        # TODO make it a foreign key
    )
    UUID = Column(  # points towards Pago
        IdentifierORM(),
        nullable=False,
        index=True,
        # TODO make it a foreign key
    )
    FechaPago = ColumnBetween(
        DateTime,
        nullable=False,
    )
    UUID_related = Column(  # points towards Ingreso
        IdentifierORM(),
        nullable=False,
        index=True,
    )
    Serie = Column(
        String,
    )
    Folio = Column(
        String,
    )
    MonedaDR = Column(
        String,
        nullable=False,
    )
    EquivalenciaDR = Column(
        Numeric,
    )
    MetodoDePagoDR = Column(
        String,
    )
    NumParcialidad = Column(
        Integer,
        nullable=False,
        default=0,
    )
    ImpSaldoAnt = Column(
        Numeric,
        nullable=False,
        default=0,
    )
    ImpPagado = Column(
        Numeric,
        nullable=False,
        default=0,
    )
    ImpPagadoMXN = Column(
        MXAmount,
        nullable=False,
        default=0,
    )
    ImpSaldoInsoluto = Column(
        Numeric,
        nullable=False,
        default=0,
    )
    active = Column(  # TODO remove
        Boolean,
        nullable=False,
        default=True,
    )
    applied = Column(  # TODO remove
        Boolean,
        nullable=False,
        default=False,
    )
    ObjetoImpDR = Column(
        String,
    )
    BaseIVA16 = Column(
        MXAmount,
        nullable=False,
    )
    BaseIVA8 = Column(
        MXAmount,
        nullable=False,
    )
    BaseIVA0 = Column(
        MXAmount,
        nullable=False,
    )
    BaseIVAExento = Column(
        MXAmount,
        nullable=False,
    )
    IVATrasladado16 = Column(
        MXAmount,
        nullable=False,
    )
    IVATrasladado8 = Column(
        MXAmount,
        nullable=False,
    )
    TrasladosIVAMXN = Column(
        MXAmount,
        nullable=False,
    )
    RetencionesIVAMXN = Column(
        MXAmount,
        # nullable=False, # TODO make it not nullable after migration
    )
    RetencionesDR = Column(  # Stored in MXN
        JSONB,
    )
    TrasladosDR = Column(  # Stored in MXN
        JSONB,
    )
    Estatus = Column(  # reference=cfdi_origin.Estatus
        Boolean,
        nullable=False,
        server_default="TRUE",
    )
    ExcludeFromIVA = Column(
        Boolean,
        nullable=False,
        server_default="FALSE",
    )
    ExcludeFromISR = Column(
        Boolean,
        nullable=False,
        server_default="FALSE",
    )

    cfdi_related = relationship(
        "CFDI",
        primaryjoin="DoctoRelacionado.UUID_related == CFDI.UUID",
        foreign_keys=[UUID_related],
        uselist=False,
    )
    cfdi_origin = relationship(
        "CFDI",
        primaryjoin="DoctoRelacionado.UUID == CFDI.UUID",
        foreign_keys=[UUID],
        uselist=False,
    )

    payment_related = relationship(
        "Payment",
        primaryjoin="DoctoRelacionado.payment_identifier == Payment.identifier",  # noqa E501
        foreign_keys=[payment_identifier],
        uselist=False,
    )

    @hybrid_property
    def Neto(self) -> float:
        return self.BaseIVA16 + self.BaseIVA8 + self.BaseIVA0 + self.BaseIVAExento

    @Neto.expression
    def Neto(cls):
        return (
            func.coalesce(cls.BaseIVA16, 0)
            + func.coalesce(cls.BaseIVA8, 0)
            + func.coalesce(cls.BaseIVA0, 0)
            + func.coalesce(cls.BaseIVAExento, 0)
        )

    @hybrid_property
    def BaseIEPS(self):  # type: ignore
        return sum(
            item.get("@BaseDR", 0)
            for item in self.TrasladosDR or []  # TODO limpiar y llenar en BD
            if item.get("@ImpuestoDR") == Tax.IEPS
        )

    @BaseIEPS.expression
    def BaseIEPS(cls) -> Numeric:
        # Debe llamarse `value`, cualquier otro nombre falla
        elem = func.jsonb_array_elements(cls.TrasladosDR).table_valued("value")

        return (
            func.coalesce(
                select([func.sum(elem.c.value.op("->>")("@BaseDR").cast(Numeric))])
                .where(elem.c.value.op("->>")("@ImpuestoDR") == Tax.IEPS.value)  # noqa: E501
                .as_scalar(),
                0,
            )
        ).label("BaseIEPS")

    @hybrid_property
    def FactorIEPS(self):  # type: ignore
        return next(
            item.get("@TipoFactorP")
            for item in self.TrasladosDR or []  # TODO limpiar y llenar en BD
            if item.get("@ImpuestoDR") == Tax.IEPS
        )

    @FactorIEPS.expression
    def FactorIEPS(cls) -> Text:
        # Debe llamarse `value`, cualquier otro nombre falla
        elem = func.jsonb_array_elements(cls.TrasladosDR).table_valued("value")

        return (
            select(elem.c.value.op("->>")("@TipoFactorDR"))
            .where(elem.c.value.op("->>")("@ImpuestoDR") == Tax.IEPS.value)  # noqa: E501
            .limit(1)
            .as_scalar()
        ).label("FactorIEPS")

    @hybrid_property
    def TasaOCuotaIEPS(self):  # type: ignore
        return next(
            item.get("@TasaOCuotaDR")
            for item in self.TrasladosDR or []  # TODO limpiar y llenar en BD
            if item.get("@ImpuestoDR") == Tax.IEPS
        )

    @TasaOCuotaIEPS.expression
    def TasaOCuotaIEPS(cls) -> Text:
        # Debe llamarse `value`, cualquier otro nombre falla
        elem = func.jsonb_array_elements(cls.TrasladosDR).table_valued("value")

        return (
            select(elem.c.value.op("->>")("@TasaOCuotaDR").cast(Numeric))
            .where(elem.c.value.op("->>")("@ImpuestoDR") == Tax.IEPS.value)  # noqa: E501
            .limit(1)
            .as_scalar()
        ).label("TasaOCuotaIEPS")

    @hybrid_property
    def ImporteIEPS(self):  # type: ignore
        return sum(
            item.get("@ImporteDR")
            for item in self.TrasladosDR or []  # TODO limpiar y llenar en BD
            if item.get("@ImpuestoDR") == Tax.IEPS
        )

    @ImporteIEPS.expression
    def ImporteIEPS(cls) -> Text:
        # Debe llamarse `value`, cualquier otro nombre falla
        elem = func.jsonb_array_elements(cls.TrasladosDR).table_valued("value")

        return (
            func.coalesce(
                select([func.sum(elem.c.value.op("->>")("@ImporteDR").cast(Numeric))])
                .where(elem.c.value.op("->>")("@ImpuestoDR") == Tax.IEPS.value)  # noqa: E501
                .as_scalar(),
                0,
            )
        ).label("ImporteIEPS")

    @hybrid_property
    def RetencionesISR(self):  # type: ignore
        return sum(
            item.get("@ImporteDR", 0)
            for item in self.RetencionesDR or []  # TODO limpiar y llenar en BD
            if item.get("@ImpuestoDR") == Tax.ISR
        )

    @RetencionesISR.expression
    def RetencionesISR(cls) -> Numeric:
        # Debe llamarse `value`, cualquier otro nombre falla
        elem = func.jsonb_array_elements(cls.RetencionesDR).table_valued("value")

        return (
            func.coalesce(
                select([func.sum(elem.c.value.op("->>")("@ImporteDR").cast(Numeric))])
                .where(elem.c.value.op("->>")("@ImpuestoDR") == Tax.ISR.value)  # noqa: E501
                .as_scalar(),
                0,
            )
        ).label("RetencionesISR")

    @classmethod
    def _demo_basic(cls):
        return DoctoRelacionado(
            company_identifier=str(uuid.uuid4()),
            FechaPago=datetime.now(),
            payment_identifier=str(uuid.uuid4()),
            UUID=str(uuid.uuid4()),
            UUID_related=str(uuid.uuid4()),
            Folio="Folio",
            MonedaDR="MonedaDR",
            NumParcialidad=random.randint(0, 10),
            ImpSaldoAnt=random.randint(0, 1_000 * 100) / 100,
            ImpPagado=random.randint(0, 1_000 * 100) / 100,
            ImpSaldoInsoluto=random.randint(0, 1_000 * 100) / 100,
            active=True,
            applied=False,
            BaseIVA16=random.randint(0, 1_000 * 100) / 100,
            BaseIVA8=random.randint(0, 1_000 * 100) / 100,
            BaseIVA0=random.randint(0, 1_000 * 100) / 100,
            BaseIVAExento=random.randint(0, 1_000 * 100) / 100,
            IVATrasladado16=random.randint(0, 1_000 * 100) / 100,
            IVATrasladado8=random.randint(0, 1_000 * 100) / 100,
            TrasladosIVAMXN=random.randint(0, 1_000 * 100) / 100,
            RetencionesIVAMXN=random.randint(0, 1_000 * 100) / 100,
            RetencionesDR=[
                {
                    "@ImpuestoDR": random.choice(list(Tax)),
                    "@TipoFactorDR": random.choice(list(TaxFactor)),
                    "@TasaOCuotaDR": random.choice((0.16, 0.08, 0)),
                    "@BaseDR": random.randint(0, 1_000 * 100) / 100,
                    "@ImporteDR": random.randint(0, 1_000 * 100) / 100,
                }
                for _ in range(random.randint(1, 3))
            ],
            TrasladosDR=[
                {
                    "@ImpuestoDR": random.choice(list(Tax)),
                    "@TipoFactorDR": random.choice(list(TaxFactor)),
                    "@TasaOCuotaDR": random.choice((0.16, 0.08, 0)),
                    "@BaseDR": random.randint(0, 1_000 * 100) / 100,
                    "@ImporteDR": random.randint(0, 1_000 * 100) / 100,
                }
                for _ in range(random.randint(1, 3))
            ],
        )

    @classmethod
    def demo(cls, **kwargs):
        """Create a demo DoctoRelacionado for testing purposes."""

        docto = cls._demo_basic()

        for key, value in kwargs.items():
            setattr(docto, key, value)
        return docto

    @classmethod
    def get_specific_table(cls, company_identifier: IdentifierORM) -> str:
        # TODO restructuracion
        return cls.__tablename__

    @classmethod
    def from_dicts(
        cls,
        docto_relacionado_list: list[dict[str, str]],
        payment: "PaymentORM",  # noqa: F821 # Avoid circular import
    ) -> list["DoctoRelacionado"]:
        res = []
        for dcto_relacionado_dict in docto_relacionado_list:
            docto_relacionado = cls.from_dict(dcto_relacionado_dict, payment)
            if docto_relacionado:
                docto_relacionado.payment = payment
                res.append(docto_relacionado)
        return res

    Impuesto = TipoFactor = TasaOCuota = str

    @classmethod
    def group_taxes_by_tax_type_rate(
        cls, tax_dicts: list[dict]
    ) -> dict[tuple[Impuesto, TipoFactor, TasaOCuota], tuple[MXAmount, MXAmount]]:
        taxes_grouped = defaultdict(lambda: (0.0, 0.0))
        for tax in tax_dicts:
            key = (
                tax["@ImpuestoDR"],
                tax["@TipoFactorDR"],
                tax.get("@TasaOCuotaDR"),
            )
            taxes_grouped[key] = (
                taxes_grouped[key][BASE] + tax["@BaseDR"],
                taxes_grouped[key][AMOUNT] + tax.get("@ImporteDR", 0),
            )
        return taxes_grouped

    @classmethod
    def taxes_to_mxn(cls, tax_dicts: list, mxn_ratio: float):
        for tax_dict in tax_dicts:
            tax_dict["@BaseDR"] /= mxn_ratio
            if "@ImporteDR" in tax_dict:
                tax_dict["@ImporteDR"] /= mxn_ratio

    @classmethod
    def from_dict(
        cls,
        dcto_relacionado: dict[str, str],
        payment: "PaymentORM",  # noqa: F821 # Avoid circular import
    ) -> "DoctoRelacionado":
        try:
            uuid.UUID(dcto_relacionado["@IdDocumento"])
        except ValueError:
            # TODO log INFO
            return None
        fields = {
            # "IdDocumento",
            "Serie",
            "Folio",
            "MonedaDR",
            "EquivalenciaDR",
            "NumParcialidad",
            "ImpSaldoAnt",
            "ImpPagado",
            "ImpSaldoInsoluto",
            "ObjetoImpDR",
        }
        # Use DoctoRelacionado value / mxn_ratio to get MXN value

        mxn_ratio = dcto_relacionado.get("@EquivalenciaDR", 1) / (payment.TipoCambioP or 1)
        if mxn_ratio == 0:  # Prevent division by zero
            mxn_ratio = 1  # TODO log warning

        traslados = dicts.get_from_dot_path(
            dcto_relacionado, "ImpuestosDR.TrasladosDR.TrasladoDR", []
        )
        retenciones = dicts.get_from_dot_path(
            dcto_relacionado, "ImpuestosDR.RetencionesDR.RetencionDR", []
        )
        cls.taxes_to_mxn(traslados, mxn_ratio)
        cls.taxes_to_mxn(retenciones, mxn_ratio)

        traslados_grouped = cls.group_taxes_by_tax_type_rate(traslados)
        retenciones_grouped = cls.group_taxes_by_tax_type_rate(retenciones)
        return cls(
            **{key: dcto_relacionado.get(f"@{key}") for key in fields},
            UUID_related=dcto_relacionado["@IdDocumento"],
            company_identifier=payment.company_identifier,
            # Payment
            is_issued=payment.is_issued,
            payment_identifier=payment.identifier,
            UUID=payment.uuid_origin,
            FechaPago=payment.FechaPago,
            Estatus=payment.Estatus,
            # Extra
            ImpPagadoMXN=dcto_relacionado.get("@ImpPagado", 0) / mxn_ratio,
            BaseIVA16=traslados_grouped[(Tax.IVA, TaxFactor.TASA, 0.16)][BASE],
            BaseIVA8=traslados_grouped[(Tax.IVA, TaxFactor.TASA, 0.08)][BASE],
            BaseIVA0=traslados_grouped[(Tax.IVA, TaxFactor.TASA, 0.00)][BASE],
            BaseIVAExento=traslados_grouped[(Tax.IVA, TaxFactor.EXENTO, None)][BASE],
            IVATrasladado16=traslados_grouped[(Tax.IVA, TaxFactor.TASA, 0.16)][AMOUNT],
            IVATrasladado8=traslados_grouped[(Tax.IVA, TaxFactor.TASA, 0.08)][AMOUNT],
            TrasladosIVAMXN=sum(
                tax[AMOUNT] for key, tax in traslados_grouped.items() if key[0] == Tax.IVA
            ),
            RetencionesIVAMXN=sum(
                tax[AMOUNT] for key, tax in retenciones_grouped.items() if key[0] == Tax.IVA
            ),
            RetencionesDR=retenciones,
            TrasladosDR=traslados,
        )


Index(
    "payment_relation_company_identifier_is_issued_fecha_pago_index",
    DoctoRelacionado.is_issued,
    DoctoRelacionado.FechaPago,
)
