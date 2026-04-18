import json
import uuid
from collections import OrderedDict
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Numeric,
    String,
    Text,
    and_,
    case,
    cast,
    func,
    or_,
    select,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property, foreign, relationship
from sqlalchemy.schema import Index

from chalicelib.controllers.enums import FormaPago, UsoCFDI
from chalicelib.new.query.domain.cfdi_to_dict import CFDIDictFromXMLParser
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.catalogs import (
    CatExportacion,
    CatFormaPago,
    CatMeses,
    CatMetodoPago,
    CatMoneda,
    CatPeriodicidad,
    CatRegimenFiscal,
    CatTipoDeComprobante,
    CatUsoCFDI,
)
from chalicelib.schema.models.efos import EFOS
from chalicelib.schema.models.model import ColumnBetween
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.docto_relacionado import (
    DoctoRelacionado as DoctoRelacionadoORM,
)
from chalicelib.schema.models.tenant.poliza import Poliza
from chalicelib.schema.models.tenant.poliza_cfdi import PolizaCFDI
from chalicelib.schema.models.tenant.tenant_model import TenantBaseModel
from chalicelib.schema.UserDefinedType.xml_type import XMLType


def demo_UUID():
    return str(uuid.uuid4())


def demo_NombreEmisor():
    return "PLATAFORMA GDL S DE RL DE CV"


def demo_RfcReceptor():
    return "XAXX010101000"


def demo_NombreReceptor():
    return "PLATAFORMA GDL S DE RL DE CV"


def demo_RfcPac():
    return "XAXX010101000"


def demo_xml():
    return """<?xml version="1.0" encoding="UTF-8"?>"""


def demo_Fecha():
    return datetime.now()


def demo_FechaFiltro():
    return datetime.now()


def demo_FechaCertificacionSat():
    return datetime.now()


def demo_Total():
    return 0.0


def demo_TipoDeComprobante():
    return "I"


def demo_Estatus():
    return True


def demo_FechaCancelacion():
    return datetime.now()


def demo_created_at():
    return datetime.now()


def demo_updated_at():
    return datetime.now()


def demo_RfcEmisor():
    return "XAXX010101000"


def demo_Serie():
    return uuid.uuid4().hex[:5].upper()


def demo_Folio():
    return uuid.uuid4().hex[:10].upper()


def demo_other_rfc():
    return "XAXX010101000"


class CFDI(TenantBaseModel):
    __tablename__ = "cfdi"

    pago_list: list[OrderedDict] = None

    company_identifier = Column(
        IdentifierORM(),
        nullable=False,
        primary_key=True,
    )
    is_issued = Column(
        Boolean,
        nullable=False,
        primary_key=True,
        default=True,
    )
    # "Official" fields
    UUID = Column(
        IdentifierORM(),
        nullable=False,
        index=True,
        primary_key=True,
    )
    Fecha = ColumnBetween(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    Total = Column(
        Numeric,
        nullable=False,
        default=0,
    )
    Folio = Column(
        String,
    )
    Serie = Column(
        String,
    )
    NoCertificado = Column(
        String,
    )
    Certificado = Column(
        String,
    )
    TipoDeComprobante = Column(  # TODO ANALYZE can be partition
        String,
        index=True,
        nullable=False,
        default="",
    )
    LugarExpedicion = Column(
        String,
    )
    FormaPago = Column(
        String,
    )
    MetodoPago = Column(
        String,
        index=True,
    )
    Moneda = Column(
        String,
    )
    SubTotal = Column(
        Numeric,
    )
    RfcEmisor = Column(
        String,
        nullable=False,
        default="",
    )
    NombreEmisor = Column(
        String,
    )
    RfcReceptor = Column(
        String,
        nullable=False,
        default="",
    )
    NombreReceptor = Column(
        String,
    )
    RfcPac = Column(
        String,
    )
    FechaCertificacionSat = ColumnBetween(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    Estatus = Column(
        Boolean,
        nullable=False,
        default=True,
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
    FechaCancelacion = ColumnBetween(
        DateTime,
        index=True,
    )
    TipoCambio = Column(
        Numeric,
    )
    Conceptos = Column(
        String,
    )
    Version = Column(
        String,
    )
    Sello = Column(
        String,
    )
    UsoCFDIReceptor = Column(
        String,
    )
    RegimenFiscalEmisor = Column(
        String,
    )
    CondicionesDePago = Column(
        String,
    )
    CfdiRelacionados = Column(
        String,
    )
    Neto = Column(
        Numeric,
    )
    TrasladosIVA = Column(
        Numeric,
    )
    TrasladosIEPS = Column(
        Numeric,
    )
    TrasladosISR = Column(
        Numeric,
    )
    RetencionesIVA = Column(
        Numeric,
    )
    RetencionesIEPS = Column(
        Numeric,
    )
    RetencionesISR = Column(
        Numeric,
    )
    FechaFiltro = ColumnBetween(
        DateTime,
        nullable=False,
        index=True,
        default=datetime.utcnow,
    )
    Impuestos = Column(
        String,
    )
    Exportacion = Column(
        String,
    )
    Periodicidad = Column(
        String,
    )
    Meses = Column(
        String,
    )
    Year = Column(
        String,
    )
    DomicilioFiscalReceptor = Column(
        String,
    )
    RegimenFiscalReceptor = Column(
        String,
    )
    TotalMXN = Column(
        Numeric,
    )
    SubTotalMXN = Column(
        Numeric,
    )
    NetoMXN = Column(
        Numeric,
    )
    DescuentoMXN = Column(
        Numeric,
    )
    TrasladosIVAMXN = Column(
        Numeric,
    )
    TrasladosIEPSMXN = Column(
        Numeric,
    )
    TrasladosISRMXN = Column(
        Numeric,
    )
    RetencionesIVAMXN = Column(
        Numeric,
    )
    RetencionesIEPSMXN = Column(
        Numeric,
    )
    RetencionesISRMXN = Column(
        Numeric,
    )
    NoCertificadoSAT = Column(
        String,
    )
    SelloSAT = Column(
        String,
    )
    Descuento = Column(
        Numeric,
    )

    PaymentDate = ColumnBetween(
        DateTime,
        nullable=False,
        index=True,
    )

    # Auxiliary fields
    TipoDeComprobante_I_MetodoPago_PPD = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    TipoDeComprobante_I_MetodoPago_PUE = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    TipoDeComprobante_E_MetodoPago_PPD = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    TipoDeComprobante_E_CfdiRelacionados_None = Column(
        Boolean,
        nullable=False,
        default=False,
    )

    cancelled_other_month = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    other_rfc = Column(
        String,
        index=True,
        default="",
    )

    # Functional fields
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )
    active = Column(
        Boolean,
        nullable=False,
        default=True,
    )
    is_too_big = Column(
        Boolean,
        server_default="FALSE",
        nullable=False,
    )
    from_xml = Column(
        Boolean,
        nullable=False,
        default=False,
    )
    xml_content = Column(
        XMLType,
    )
    add_exists = Column(
        Boolean,
        nullable=False,
        server_default="FALSE",
        # TODO Index
    )

    add_cancel_date = Column(
        DateTime,
        # Index
    )
    BaseIVA16 = Column(
        Numeric,
    )
    BaseIVA8 = Column(
        Numeric,
    )
    BaseIVA0 = Column(
        Numeric,
    )
    BaseIVAExento = Column(
        Numeric,
    )
    IVATrasladado16 = Column(
        Numeric,
    )
    IVATrasladado8 = Column(
        Numeric,
    )

    pr_count = Column(
        Numeric,
        server_default="0",
        nullable=False,
    )

    efos = relationship(
        EFOS,
        uselist=False,
        foreign_keys=[RfcEmisor],
        primaryjoin=EFOS.rfc == RfcEmisor,
    )
    c_forma_pago = relationship(
        CatFormaPago,
        uselist=False,
        backref="cfdis",
        foreign_keys=[FormaPago],
        primaryjoin=CatFormaPago.code == FormaPago,
    )
    c_metodo_pago = relationship(
        CatMetodoPago,
        uselist=False,
        backref="cfdis",
        foreign_keys=[MetodoPago],
        primaryjoin=CatMetodoPago.code == MetodoPago,
    )
    c_moneda = relationship(
        CatMoneda,
        uselist=False,
        backref="cfdis",
        foreign_keys=[Moneda],
        primaryjoin=CatMoneda.code == Moneda,
    )
    c_regimen_fiscal_emisor = relationship(
        CatRegimenFiscal,
        uselist=False,
        backref="cfdis_emisor",
        foreign_keys=[RegimenFiscalEmisor],
        primaryjoin=CatRegimenFiscal.code == RegimenFiscalEmisor,
    )
    c_regimen_fiscal_receptor = relationship(
        CatRegimenFiscal,
        uselist=False,
        viewonly=True,
        backref="cfdis_receptor",
        foreign_keys=[RegimenFiscalReceptor],
        primaryjoin=CatRegimenFiscal.code == RegimenFiscalReceptor,
    )
    c_tipo_de_comprobante = relationship(
        CatTipoDeComprobante,
        uselist=False,
        backref="cfdis",
        foreign_keys=[TipoDeComprobante],
        primaryjoin=CatTipoDeComprobante.code == TipoDeComprobante,
    )
    c_uso_cfdi = relationship(
        CatUsoCFDI,
        uselist=False,
        backref="cfdis",
        foreign_keys=[UsoCFDIReceptor],
        primaryjoin=CatUsoCFDI.code == UsoCFDIReceptor,
    )
    c_exportacion = relationship(
        CatExportacion,
        uselist=False,
        backref="cfdis",
        foreign_keys=[Exportacion],
        primaryjoin=CatExportacion.code == Exportacion,
    )
    c_meses = relationship(
        CatMeses,
        uselist=False,
        backref="cfdis",
        foreign_keys=[Meses],
        primaryjoin=CatMeses.code == Meses,
    )
    c_periodicidad = relationship(
        CatPeriodicidad,
        uselist=False,
        backref="cfdis",
        foreign_keys=[Periodicidad],
        primaryjoin=CatPeriodicidad.code == Periodicidad,
    )
    paid_by = relationship(
        "DoctoRelacionado",
        foreign_keys=[UUID],
        viewonly=True,
        primaryjoin=and_(
            foreign(DoctoRelacionadoORM.UUID_related) == UUID,
            DoctoRelacionadoORM.Estatus,
            TipoDeComprobante == "I",
        ),
    )
    pays = relationship(
        "DoctoRelacionado",
        foreign_keys=[UUID],
        viewonly=True,
        primaryjoin=foreign(DoctoRelacionadoORM.UUID) == UUID,
    )
    payments = relationship(
        "Payment",
        foreign_keys=[UUID],
        uselist=True,
        primaryjoin="foreign(CFDI.UUID) == Payment.uuid_origin",  # noqa E501
    )
    cfdi_origin = relationship(
        "CfdiRelacionado",
        foreign_keys=[UUID],
        viewonly=True,
        primaryjoin="foreign(CfdiRelacionado.uuid_origin) == CFDI.UUID",  # noqa E501
    )
    cfdi_related = relationship(
        "CfdiRelacionado",
        foreign_keys=[UUID],
        viewonly=True,
        primaryjoin="and_("
        "foreign(CfdiRelacionado.uuid_related) == CFDI.UUID,"
        "foreign(CfdiRelacionado.Estatus) == True,"
        "foreign(CfdiRelacionado.TipoDeComprobante) == 'E'"
        ")",  # noqa E501
    )
    active_payments = relationship(
        "CFDI",
        foreign_keys=[UUID],
        viewonly=True,
        secondary=DoctoRelacionadoORM.__table__,
        primaryjoin=and_(
            DoctoRelacionadoORM.UUID_related == UUID,
        ),
        secondaryjoin=and_(
            Estatus,
            DoctoRelacionadoORM.UUID == UUID,
        ),
    )
    efos = relationship(
        EFOS,
        uselist=False,
        foreign_keys=[RfcEmisor],
        primaryjoin=EFOS.rfc == RfcEmisor,
        backref="cfdis",
    )

    polizas = relationship(
        "Poliza",
        secondary=PolizaCFDI.__table__,
        primaryjoin=UUID == foreign(PolizaCFDI.uuid_related),
        secondaryjoin=Poliza.identifier == foreign(PolizaCFDI.poliza_identifier),
        viewonly=True,
    )

    @property
    def iva_acreditable(self):
        return float(self.TrasladosIVAMXN or 0) - float(self.RetencionesIVAMXN or 0)

    @property
    def xml_dict(self):
        return CFDIDictFromXMLParser().get_dict_from_xml(self.xml_content)

    @property
    def CuentaPredial(self):
        if not self.Conceptos:
            return []

        try:
            conceptos_data = json.loads(self.Conceptos)
        except (json.JSONDecodeError, TypeError):
            return []

        if "Concepto" not in conceptos_data:
            return []

        conceptos = conceptos_data["Concepto"]
        if isinstance(conceptos, dict):
            conceptos = [conceptos]

        cuenta_predial_list = []
        for concepto in conceptos:
            if "CuentaPredial" in concepto:
                cuenta_predial = concepto["CuentaPredial"]
                if isinstance(cuenta_predial, dict):
                    cuenta_predial = [cuenta_predial]

                for cuenta in cuenta_predial:
                    if isinstance(cuenta, dict):
                        numero = cuenta.get("@Numero", "")
                        if numero:
                            cuenta_predial_list.append(numero)

        return cuenta_predial_list

    @staticmethod
    def get_errors() -> dict[str, str]:
        return {
            "TipoDeComprobante_I_MetodoPago_PUE": "Ingreso con Metodo de pago PUE y forma de pago igual a 99",  # noqa E501
            "TipoDeComprobante_E_CfdiRelacionados_None": "Egreso sin CFDI Relacionados",
        }

    @property
    def errors_string(self):
        error_messages = [
            error for flag, error in self.get_errors().items() if getattr(self, flag, False)
        ]

        return ", ".join(error_messages)

    @classmethod
    def _demo_basic(cls, company_identifier: Identifier):
        return {
            "company_identifier": company_identifier,
            "UUID": demo_UUID(),
            "Serie": demo_Serie(),
            "Folio": demo_Folio(),
            "NombreEmisor": demo_NombreEmisor(),
            "RfcReceptor": demo_RfcReceptor(),
            "NombreReceptor": demo_NombreReceptor(),
            "RfcPac": demo_RfcPac(),
            "Fecha": demo_Fecha(),
            "FechaFiltro": demo_FechaFiltro(),
            "PaymentDate": demo_FechaFiltro(),
            "FechaCertificacionSat": demo_FechaCertificacionSat(),
            "Total": demo_Total(),
            "TipoDeComprobante": demo_TipoDeComprobante(),
            "Estatus": demo_Estatus(),
            "FechaCancelacion": demo_FechaCancelacion(),
            "created_at": demo_created_at(),
            "updated_at": demo_updated_at(),
            "RfcEmisor": demo_RfcEmisor(),
            "other_rfc": demo_other_rfc(),
            "from_xml": False,
            "TipoDeComprobante_I_MetodoPago_PPD": False,
            "TipoDeComprobante_I_MetodoPago_PUE": False,
            "TipoDeComprobante_E_MetodoPago_PPD": False,
            "TipoDeComprobante_E_CfdiRelacionados_None": False,
            "cancelled_other_month": False,
            "active": True,
            "add_exists": False,
            "add_cancel_date": None,
        }

    @classmethod
    def demo(cls, with_xml: bool = False, **kwargs) -> "CFDI":
        """Create a demo CFDI for testing purposes."""
        kwargs.setdefault("is_issued", True)
        cid = demo_UUID()
        cfdi = cls(**cls._demo_basic(company_identifier=cid))
        if with_xml:
            cfdi.xml_content = demo_xml()

        for key, value in kwargs.items():
            setattr(cfdi, key, value)
        return cfdi

    @classmethod
    def need_add_action(cls):
        return or_(
            and_(
                cls.from_xml,
                ~cls.add_exists,
            ),
            and_(
                ~cls.FechaCancelacion.is_(None),
                cls.add_cancel_date.is_(None),
            ),
        )

    # Relationships
    nomina = relationship(
        "Nomina",
        uselist=False,
        viewonly=True,
        foreign_keys=[UUID],
        primaryjoin="foreign(CFDI.UUID) == Nomina.cfdi_uuid",
    )

    @hybrid_property
    def attachments_count(self):
        return len(self.attachments)

    @attachments_count.expression
    def attachments_count(self):
        return (
            select([func.count(Attachment.identifier)])
            .where(self.attachments)
            .correlate(self)
            .scalar_subquery()
            .label("attachments_count")
        )

    @hybrid_property
    def attachments_size(self):
        return sum(attachment.size for attachment in self.attachments)

    @attachments_size.expression
    def attachments_size(self):
        return (
            select([func.coalesce(func.sum(Attachment.size), 0)])
            .select_from(self)
            .where(self.attachments)
            .correlate(self)
            .scalar_subquery()
        )

    @hybrid_property
    def is_moved(self):
        return self.FechaFiltro != self.PaymentDate

    balance = column_property(
        select(
            func.round(
                (
                    (
                        case(
                            [(and_(TipoDeComprobante == "I", MetodoPago == "PUE"), 0)],
                            else_=Total,
                        )
                    )
                    - func.coalesce(
                        func.sum(DoctoRelacionadoORM.ImpPagado),
                        0,
                    )
                ),
                2,
            )
        )
        .where(
            and_(
                DoctoRelacionadoORM.UUID_related == UUID,  # TODO utilizar la relationship
                DoctoRelacionadoORM.Estatus,  # TODO utilizar el campo del CFDI de origen
            )
        )
        .correlate_except(DoctoRelacionadoORM)
        .scalar_subquery()
    )

    @hybrid_property
    def base_isr(self):
        return (
            float(self.BaseIVA0 or 0)
            + float(self.BaseIVAExento or 0)
            + float(self.BaseIVA8 or 0)
            + float(self.BaseIVA16 or 0)
        )

    @base_isr.expression
    def base_isr(cls):
        return (
            func.coalesce(cls.BaseIVA0, 0)
            + func.coalesce(cls.BaseIVAExento, 0)
            + func.coalesce(cls.BaseIVA8, 0)
            + func.coalesce(cls.BaseIVA16, 0)
        )

    @hybrid_property
    def used_in_isr(self):
        return or_(
            self.TipoDeComprobante == "P",
            and_(self.TipoDeComprobante == "I", self.MetodoPago == "PUE"),
        )

    @hybrid_property
    def total_docto_relacionados(self):
        if self.Version == "4.0":
            total = sum((paid.ImpPagado for paid in self.pays), 0)
            return total
        return 0

    @hybrid_property
    def auto_exclude_iva(self):
        return (
            (self.TipoDeComprobante == "E" and self.FormaPago not in FormaPago.bancarizadas())
            or (
                not self.is_issued
                and self.TipoDeComprobante == "I"
                and self.MetodoPago == "PUE"
                and (
                    self.FormaPago not in FormaPago.bancarizadas()
                    or self.UsoCFDIReceptor not in UsoCFDI.bancarizadas()
                )
            )
            or (
                not self.is_issued
                and self.TipoDeComprobante in ["E", "P"]
                and self.UsoCFDIReceptor == UsoCFDI.SIN_EFECTOS_FISCALES
            )
        )

    @hybrid_property
    def auto_exclude_isr(self):
        """
        Regla base de auto-exclusión para ISR.
        - Para I y E reutiliza la misma lógica de IVA (nueva política PIB).
        - Para P se complementa en XMLProcessor, que marca pagos recibidos
          no bancarizados o con DR no G01/G03 directamente en BD.
        """
        return self.auto_exclude_iva

    @total_docto_relacionados.expression
    def total_docto_relacionados(cls):
        total_sum = (
            select([func.sum(DoctoRelacionadoORM.ImpPagado)])
            .where(DoctoRelacionadoORM.UUID == cls.UUID)
            .correlate(cls)
            .scalar_subquery()
        )

        return case([(cls.Version == "4.0", total_sum)], else_=0)

    @hybrid_property
    def FechaYear(self):
        return self.Fecha.year if self.Fecha else None

    @FechaYear.expression
    def FechaYear(cls):
        return func.extract("year", cls.Fecha)

    @hybrid_property
    def FechaMonth(self):
        return self.Fecha.month if self.Fecha else None

    @FechaMonth.expression
    def FechaMonth(cls):
        return func.extract("month", cls.Fecha)

    @hybrid_property
    def FechaCertificacionSatYear(self):
        return self.FechaCertificacionSat.year if self.FechaCertificacionSat else None

    @FechaCertificacionSatYear.expression
    def FechaCertificacionSatYear(cls):
        return func.extract("year", cls.FechaCertificacionSat)

    @hybrid_property
    def FechaCertificacionSatMonth(self):
        return self.FechaCertificacionSat.month if self.FechaCertificacionSat else None

    @FechaCertificacionSatMonth.expression
    def FechaCertificacionSatMonth(cls):
        return func.extract("month", cls.FechaCertificacionSat)

    @hybrid_property
    def xml_content_text(self):
        if self.xml_content:
            return str(self.xml_content)
        return None

    @xml_content_text.expression
    def xml_content_text(cls):
        return cast(cls.xml_content, Text)

    @hybrid_property
    def polizas_list(self) -> str:
        if not self.polizas:
            return ""
        return ", ".join(
            f"{p.fecha.strftime('%d/%m/%Y')} - {p.tipo} - {p.numero}" for p in self.polizas
        )

    @polizas_list.expression
    def polizas_list(cls):
        subquery = (
            select(
                func.string_agg(
                    func.format(
                        "%s - %s - %s",
                        func.to_char(Poliza.fecha, "DD/MM/YYYY"),
                        Poliza.tipo,
                        Poliza.numero,
                    ),
                    ", ",
                )
            )
            .select_from(Poliza)
            .join(PolizaCFDI, PolizaCFDI.poliza_identifier == Poliza.identifier)
            .where(PolizaCFDI.uuid_related == cls.UUID)
            .scalar_subquery()
            .label("Pólizas")
        )

        return subquery

    # Catalogue
    cat_field = {
        ("P", "c_forma_pago"): "payments",
    }

    @hybrid_property
    def diff(self):
        return self.SubTotalMXN - self.DescuentoMXN

    def get_cat(self, cat):
        cat_rel = None
        if (self.TipoDeComprobante, cat) in self.cat_field:
            model_field = self.cat_field[(self.TipoDeComprobante, cat)]
            first_node = getattr(self, model_field)[:1]
            if first_node:
                cat_rel = getattr(first_node[0], cat)
        else:
            cat_rel = getattr(self, cat)

        if not cat_rel:
            return None

        return cat_rel

    @property
    def forma_pago_name(self):
        return self.get_cat("c_forma_pago") and self.get_cat("c_forma_pago").name or ""

    @property
    def forma_pago_code(self):
        return self.get_cat("c_forma_pago") and self.get_cat("c_forma_pago").code or ""

    # Generated column to know if has errors
    has_errors = column_property(
        TipoDeComprobante_I_MetodoPago_PPD
        | TipoDeComprobante_I_MetodoPago_PUE
        | TipoDeComprobante_E_MetodoPago_PPD
        | TipoDeComprobante_E_CfdiRelacionados_None
    )

    @classmethod
    def get_specific_table(
        cls,
        company_identifier: Identifier = "",
        is_issued: bool = False,
    ) -> str:
        # TODO restructuracion
        return cls.__tablename__


Index(
    "cfdi_Fecha_Estatus_from_xml_is_too_big_idx",
    CFDI.Fecha,
    CFDI.Estatus,
    CFDI.from_xml,
    CFDI.is_too_big,
    unique=False,
    postgresql_where='("Estatus" AND (NOT from_xml) AND (NOT is_too_big))',
)
Index(
    "cfdi_add_exists_UUID_idx",
    CFDI.add_exists,
    CFDI.UUID,
    unique=False,
)
Index(
    "cfdi_add_exists_UUID_add_cancel_date_idx",
    CFDI.add_exists,
    CFDI.UUID,
    CFDI.add_cancel_date,
    unique=False,
)
