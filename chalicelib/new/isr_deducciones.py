from datetime import date, datetime
from decimal import Decimal
from enum import Enum, auto

from dateutil.relativedelta import relativedelta
from sqlalchemy import Numeric, and_, cast, func
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.controllers.enums import FormaPago, UsoCFDI
from chalicelib.modules import NameEnum
from chalicelib.new.company.domain.company import Company
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant.docto_relacionado import (
    DoctoRelacionado as DoctoRelacionadoORM,
)
from chalicelib.schema.models.tenant.nomina import Nomina as NominaORM
from chalicelib.schema.models.tenant.payment import Payment as PaymentORM

DEFAULT_ISR_PCT = Decimal("0.47")


class ConceptoDeduccionEnum(str, Enum):
    GASTOS_NOMINA_GRAVADOS = "Gastos de nomina gravada"
    GASTOS_NOMINA_EXENTOS = "Gastos de nomina exenta"
    GASTOS_NOMINA_EXENTOS_DEDUCIBLES = "Gastos de nomina exenta deducible"
    GASTOS_NOMINA_DEDUCIBLES = "Gastos de nomina deducibles"
    COMPRAS_CONTADO = "Compras y gastos facturas de contado"
    COMPRAS_PAGOS = "Compras y gastos CFDI de pagos"
    DEVOLUCIONES = "Devoluciones, descuentos y bonificaciones facturadas"
    DEV_INGRESOS = "Devoluciones, descuentos y bonificaciones en ingresos emitidos"
    DEV_EGRESOS = "Devoluciones, descuentos y bonificaciones en egresos emitidos"
    NO_CONSIDERADOS = "Compras y gastos no considerados en el pre llenado"
    NO_CONSIDERADOS_INGRESOS = "No considerados en el pre llenado Ingresos PUE"
    NO_CONSIDERADOS_PAGOS = "No considerados en el pre llenado Pagos"
    FACTURAS_E_RECIBIDAS_COMPRAS_GASTOS = "Facturas de egresos recibidas por compras y gastos"
    ADQUISICIONES_POR_CONCEPTO_DE_INVERSIONES = "Adquisiciones por concepto de Inversiones"

    # 👇 Nuevos conceptos agregados como sumas personalizadas
    COMPRAS_Y_GASTOS = "Compras y gastos"
    DEDUCCIONES_AUTORIZADAS = "Deducciones autorizadas (sin inversiones)"


class ColumnConceptEnum(str, Enum):
    CONTEO = "ConteoCFDIs"
    IMPORTE = "Importe"
    ISR = "isr_cargo"
    PORCENTAJE = "porcentaje"


class ValorKeyEnum(NameEnum):
    # Nómina
    CONTEO_NOMINA_GRAVADA = auto()
    IMPORTE_NOMINA_GRAVADA = auto()
    IMPORTE_NOMINA_EXENTA = auto()
    PORCENTAJE_ISR_NOMINA_EXENTA = auto()
    IMPORTE_NOMINA_EXENTA_DEDUCIBLE = auto()
    TOTAL_NOMINA_DEDUCIBLE = auto()

    # Compras contado
    CONTEO_COMPRAS_CONTADO = auto()
    IMPORTE_COMPRAS_CONTADO = auto()
    ISR_COMPRAS_CONTADO = auto()

    # Compras pagos
    CONTEO_COMPRAS_PAGOS = auto()
    IMPORTE_COMPRAS_PAGOS = auto()
    ISR_COMPRAS_PAGOS = auto()

    # Devoluciones y descuentos
    CONTEO_DEVOLUCIONES_FACTURADAS = auto()
    IMPORTE_DEVOLUCIONES_FACTURADAS = auto()

    CONTEO_DEVOLUCIONES_INGRESOS = auto()
    IMPORTE_DEVOLUCIONES_INGRESOS = auto()
    CONTEO_DEVOLUCIONES_EGRESOS = auto()
    IMPORTE_DEVOLUCIONES_EGRESOS = auto()

    # No considerados (prellenado)
    CONTEO_COMPRAS_NO_CONSIDERADAS = auto()
    IMPORTE_COMPRAS_NO_CONSIDERADAS = auto()
    ISR_COMPRAS_NO_CONSIDERADAS = auto()

    CONTEO_COMPRAS_NO_CONSIDERADAS_INGRESOS = auto()
    IMPORTE_COMPRAS_NO_CONSIDERADAS_INGRESOS = auto()
    ISR_COMPRAS_NO_CONSIDERADAS_INGRESOS = auto()

    CONTEO_COMPRAS_NO_CONSIDERADAS_PAGOS = auto()
    IMPORTE_COMPRAS_NO_CONSIDERADAS_PAGOS = auto()
    ISR_COMPRAS_NO_CONSIDERADAS_PAGOS = auto()

    # Facturas recibidas por compras
    CONTEO_FACTURAS_EGRESOS_COMPRAS_PAGOS = auto()
    IMPORTE_FACTURAS_EGRESOS_COMPRAS_PAGOS = auto()

    # Adquisiciones
    CONTEO_ADQUISICIONES_INVERSION = auto()
    IMPORTE_ADQUISICIONES_INVERSION = auto()

    # Totales compuestos
    IMPORTE_COMPRAS_Y_GASTOS = auto()
    IMPORTE_DEDUCCIONES_AUTORIZADAS = auto()
    ISR_DEDUCCIONES_AUTORIZADAS = auto()


MAPEO_CONCEPTOS: dict[ConceptoDeduccionEnum, dict[ColumnConceptEnum, ValorKeyEnum]] = {
    # Nómina
    ConceptoDeduccionEnum.GASTOS_NOMINA_GRAVADOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_NOMINA_GRAVADA,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_NOMINA_GRAVADA,
    },
    ConceptoDeduccionEnum.GASTOS_NOMINA_EXENTOS: {
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_NOMINA_EXENTA,
    },
    ConceptoDeduccionEnum.GASTOS_NOMINA_EXENTOS_DEDUCIBLES: {
        ColumnConceptEnum.PORCENTAJE: ValorKeyEnum.PORCENTAJE_ISR_NOMINA_EXENTA,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_NOMINA_EXENTA_DEDUCIBLE,
    },
    ConceptoDeduccionEnum.GASTOS_NOMINA_DEDUCIBLES: {
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.TOTAL_NOMINA_DEDUCIBLE,
    },
    # Compras contado
    ConceptoDeduccionEnum.COMPRAS_CONTADO: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_COMPRAS_CONTADO,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_CONTADO,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_COMPRAS_CONTADO,
    },
    # Compras pagos
    ConceptoDeduccionEnum.COMPRAS_PAGOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_COMPRAS_PAGOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_PAGOS,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_COMPRAS_PAGOS,
    },
    # Devoluciones y descuentos
    ConceptoDeduccionEnum.DEVOLUCIONES: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_DEVOLUCIONES_FACTURADAS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_DEVOLUCIONES_FACTURADAS,
    },
    ConceptoDeduccionEnum.DEV_INGRESOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_DEVOLUCIONES_INGRESOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_DEVOLUCIONES_INGRESOS,
    },
    ConceptoDeduccionEnum.DEV_EGRESOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_DEVOLUCIONES_EGRESOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_DEVOLUCIONES_EGRESOS,
    },
    # No considerados (prellenado)
    ConceptoDeduccionEnum.NO_CONSIDERADOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS,
    },
    ConceptoDeduccionEnum.NO_CONSIDERADOS_INGRESOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS_INGRESOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS_INGRESOS,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS_INGRESOS,
    },
    ConceptoDeduccionEnum.NO_CONSIDERADOS_PAGOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS_PAGOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS_PAGOS,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS_PAGOS,
    },
    # Facturas electrónicas recibidas por compras
    ConceptoDeduccionEnum.FACTURAS_E_RECIBIDAS_COMPRAS_GASTOS: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_FACTURAS_EGRESOS_COMPRAS_PAGOS,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_FACTURAS_EGRESOS_COMPRAS_PAGOS,
    },
    # Adquisiciones de inversión
    ConceptoDeduccionEnum.ADQUISICIONES_POR_CONCEPTO_DE_INVERSIONES: {
        ColumnConceptEnum.CONTEO: ValorKeyEnum.CONTEO_ADQUISICIONES_INVERSION,
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_ADQUISICIONES_INVERSION,
    },
    # Totales globales
    ConceptoDeduccionEnum.DEDUCCIONES_AUTORIZADAS: {
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_DEDUCCIONES_AUTORIZADAS,
        ColumnConceptEnum.ISR: ValorKeyEnum.ISR_DEDUCCIONES_AUTORIZADAS,
    },
    ConceptoDeduccionEnum.COMPRAS_Y_GASTOS: {
        ColumnConceptEnum.IMPORTE: ValorKeyEnum.IMPORTE_COMPRAS_Y_GASTOS,
    },
}


SUBCONCEPTOS: dict[ConceptoDeduccionEnum, list[ConceptoDeduccionEnum]] = {
    ConceptoDeduccionEnum.DEVOLUCIONES: [
        ConceptoDeduccionEnum.DEV_INGRESOS,
        ConceptoDeduccionEnum.DEV_EGRESOS,
    ],
    ConceptoDeduccionEnum.NO_CONSIDERADOS: [
        ConceptoDeduccionEnum.NO_CONSIDERADOS_INGRESOS,
        ConceptoDeduccionEnum.NO_CONSIDERADOS_PAGOS,
    ],
}

SUMAS_CUSTOM: dict[ConceptoDeduccionEnum, list[tuple[ConceptoDeduccionEnum, int]]] = {
    ConceptoDeduccionEnum.DEDUCCIONES_AUTORIZADAS: [
        (ConceptoDeduccionEnum.GASTOS_NOMINA_DEDUCIBLES, 1),
        (ConceptoDeduccionEnum.COMPRAS_CONTADO, 1),
        (ConceptoDeduccionEnum.COMPRAS_PAGOS, 1),
        (ConceptoDeduccionEnum.DEVOLUCIONES, 1),
        (ConceptoDeduccionEnum.NO_CONSIDERADOS, 1),  # 👈 este se resta
        (ConceptoDeduccionEnum.FACTURAS_E_RECIBIDAS_COMPRAS_GASTOS, -1),
    ],
    ConceptoDeduccionEnum.COMPRAS_Y_GASTOS: [
        (ConceptoDeduccionEnum.COMPRAS_CONTADO, 1),
        (ConceptoDeduccionEnum.COMPRAS_PAGOS, 1),
        (ConceptoDeduccionEnum.DEVOLUCIONES, 1),
        (ConceptoDeduccionEnum.NO_CONSIDERADOS, 1),  # 👈 este se resta
        (ConceptoDeduccionEnum.FACTURAS_E_RECIBIDAS_COMPRAS_GASTOS, -1),
    ],
}


def get_fecha_filtro_dict(period: date) -> dict[str, datetime]:
    """
    A partir de un `period` (primer día del mes), genera un diccionario con:
        {
            "start": datetime(2025, 9, 1, 0, 0, 0),
            "end": datetime(2025, 10, 1, 0, 0, 0),
        }
    """
    start = datetime.combine(period, datetime.min.time())
    end = datetime.combine(period + relativedelta(months=1), datetime.min.time())
    return {"start": start, "end": end}


def build_total_deducciones_cfdi_query(session: Session, domain: list, fields: list):
    """
    Retorna una query con SUM dinámico para CFDIORM, aún sin ejecutarse.
    """
    query_expressions = [func.count().label("ConteoCFDIs")]

    for field in fields:
        model_field = getattr(CFDIORM, field)
        query_expressions.append(func.sum(model_field).label(field))

    q = session.query(*query_expressions)

    query = CFDIController.apply_domain(q, domain, fuzzy_search="", session=session)

    return query


def build_total_deducciones_pagos_query(session: Session, domain: list, fields: list):
    """
    Retorna una query con SUM dinámico para DoctoRelacionado, aún sin ejecutarse.
    """
    query_expressions = [func.count().label("ConteoCFDIs")]

    for field in fields:
        model_field = getattr(DoctoRelacionadoORM, field)
        query_expressions.append(func.sum(model_field).label(field))

    q = session.query(*query_expressions)

    query = DoctoRelacionadoController.apply_domain(q, domain, fuzzy_search="", session=session)

    return query


def get_isr_percentage(company) -> Decimal:
    try:
        return Decimal(str(company.data["isr_percentage"]))
    except (KeyError, TypeError):
        return DEFAULT_ISR_PCT


def query_gastos_nomina_gravada(company_session: Session, period: date) -> Select:
    """
    Construye una query para obtener:
    - Conteo de CFDIs de tipo Nómina emitidos y vigentes
    - Suma de 'PercepcionesTotalGravado' desde el modelo Nomina

    Parámetros:
        - session: Sesión SQLAlchemy activa
        - domain_global: dicta de filtros

    Retorna:
        - Objeto SQLAlchemy Select listo para ejecutar
    """
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        CFDIORM.TipoDeComprobante == "N",
        CFDIORM.is_issued,
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )

    query = (
        company_session.query(
            func.count(),
            cast(func.coalesce(func.sum(NominaORM.PercepcionesTotalGravado), 0), Numeric),
        )
        .select_from(CFDIORM)
        .outerjoin(NominaORM, CFDIORM.UUID == NominaORM.cfdi_uuid)
        .filter(filters)
    )
    return query


def query_gastos_nomina_exento_total(company_session: Session, period: date) -> Select:
    """
    Construye una query para obtener la suma de '
    PercepcionesTotalExento' de CFDIs tipo Nómina vigentes.

    Parámetros:
        - session: Sesión SQLAlchemy activa
        - domain_global: dicta de filtros estilo Odoo (con fechas y compañía)

    Retorna:
        - Objeto SQLAlchemy Select listo para ejecutar
    """
    fecha_filtro = get_fecha_filtro_dict(period)

    filters = and_(
        CFDIORM.TipoDeComprobante == "N",
        CFDIORM.is_issued,
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )

    query = (
        company_session.query(
            cast(func.coalesce(func.sum(NominaORM.PercepcionesTotalExento), 0), Numeric),
        )
        .select_from(CFDIORM)
        .join(NominaORM, CFDIORM.UUID == NominaORM.cfdi_uuid)
        .filter(filters)
    )

    return query


def compras_gastos_facturas_contado(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        CFDIORM.TipoDeComprobante == "I",
        CFDIORM.is_issued.is_(False),
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.MetodoPago == "PUE",
        CFDIORM.FormaPago.in_(FormaPago.bancarizadas()),
        CFDIORM.UsoCFDIReceptor.in_(UsoCFDI.bancarizadas()),
        CFDIORM.ExcludeFromISR.is_(exclude_from_isr),
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro <= fecha_filtro["end"],
    )
    query = company_session.query(
        func.count(),
        cast(func.coalesce(func.sum(CFDIORM.NetoMXN), 0), Numeric),
        cast(func.coalesce(func.sum(CFDIORM.RetencionesISRMXN), 0), Numeric),
    )
    query = query.filter(filters)
    return query


def compras_gastos_cfdis_pagos(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)

    pr = aliased(DoctoRelacionadoORM)
    cfdi_pago = aliased(CFDIORM)
    p = aliased(PaymentORM)
    i = aliased(CFDIORM)  #

    filters = [
        cfdi_pago.TipoDeComprobante == "P",
        p.FormaDePagoP.in_(FormaPago.bancarizadas()),
        i.UsoCFDIReceptor.in_(UsoCFDI.bancarizadas()),
        cfdi_pago.ExcludeFromISR.is_(exclude_from_isr),
        cfdi_pago.Estatus.is_(True),
        cfdi_pago.is_issued.is_(False),
        pr.FechaPago >= fecha_filtro["start"],
        pr.FechaPago < fecha_filtro["end"],
    ]

    query = (
        company_session.query(
            func.count(cfdi_pago.UUID),
            cast(func.coalesce(func.sum(pr.Neto), 0), Numeric),
            cast(func.coalesce(func.sum(pr.RetencionesISR), 0), Numeric),
        )
        .select_from(pr)
        .join(cfdi_pago, pr.UUID == cfdi_pago.UUID)
        .join(p, pr.payment_identifier == p.identifier)
        .join(i, pr.UUID_related == i.UUID)
        .filter(*filters)
    )

    return query


def dev_desctos_bonif_ingresos_emitidos(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        CFDIORM.TipoDeComprobante == "I",
        CFDIORM.is_issued.is_(True),
        CFDIORM.Estatus,
        CFDIORM.MetodoPago == "PUE",
        CFDIORM.ExcludeFromISR.is_(exclude_from_isr),
        CFDIORM.Version == "4.0",
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )
    query = (
        company_session.query(
            func.count().label("ConteoCFDIs"),
            cast(func.coalesce(func.sum(CFDIORM.DescuentoMXN), 0), Numeric).label("DescuentoMXN"),
        )
        .select_from(CFDIORM)
        .filter(filters)
    )
    return query


def dev_desctos_bonif_egresos_emitidos(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        CFDIORM.TipoDeComprobante == "E",
        CFDIORM.is_issued.is_(True),
        CFDIORM.Estatus,
        CFDIORM.MetodoPago == "PUE",
        CFDIORM.ExcludeFromISR.is_(exclude_from_isr),
        CFDIORM.Version == "4.0",
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )
    query = (
        company_session.query(
            func.count().label("ConteoCFDIs"),
            cast(func.coalesce(func.sum(CFDIORM.NetoMXN), 0), Numeric).label("NetoMXN"),
        )
        .select_from(CFDIORM)
        .filter(filters)
    )
    return query


def dev_pagos_provisionales_persona_f_actividad_empresarial(
    company_session: Session, domain_global: dict
):
    """
    Devuelve un SELECT que suma ConteoCFDIs e importes de:
      - dev_desctos_bonif_ingresos_emitidos (DescuentoMXN)
      - dev_desctos_bonif_egresos_emitidos (NetoMXN)
    """
    q_ing = dev_desctos_bonif_ingresos_emitidos(company_session, domain_global).subquery()
    q_egr = dev_desctos_bonif_egresos_emitidos(company_session, domain_global).subquery()

    query = company_session.query(
        (func.coalesce(q_ing.c.ConteoCFDIs, 0) + func.coalesce(q_egr.c.ConteoCFDIs, 0)).label(
            "ConteoCFDIs"
        ),
        (func.coalesce(q_ing.c.DescuentoMXN, 0) + func.coalesce(q_egr.c.NetoMXN, 0)).label(
            "NetoMXN"
        ),
    )
    return query


def no_considerados_ingresos_pue(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        CFDIORM.TipoDeComprobante == "I",
        CFDIORM.MetodoPago == "PUE",
        ~CFDIORM.is_issued,
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.FormaPago.in_(FormaPago.no_bancarizadas()),
        CFDIORM.ExcludeFromISR.is_(exclude_from_isr),
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )
    query = (
        company_session.query(
            func.count().label("ConteoCFDIs"),
            cast(func.coalesce(func.sum(CFDIORM.NetoMXN), 0), Numeric).label("NetoMXN"),
            cast(func.coalesce(func.sum(CFDIORM.RetencionesISRMXN), 0), Numeric).label(
                "RetencionesISRMXN"
            ),
        )
        .select_from(CFDIORM)
        .filter(filters)
    )
    return query


def no_considerados_pagos(
    company_session: Session, period: date, exclude_from_isr: bool = False
) -> Select:
    fecha_filtro = get_fecha_filtro_dict(period)

    pr = aliased(DoctoRelacionadoORM)
    cfdi_pago = aliased(CFDIORM)
    p = aliased(PaymentORM)
    i = aliased(CFDIORM)

    filters = and_(
        cfdi_pago.TipoDeComprobante == "P",
        pr.ExcludeFromISR.is_(exclude_from_isr),
        cfdi_pago.Estatus.is_(True),
        cfdi_pago.is_issued.is_(True),
        p.FormaDePagoP.in_(FormaPago.no_bancarizadas()),
        i.UsoCFDIReceptor.notin_(
            [
                UsoCFDI.CONSTRUCCIONES,
                UsoCFDI.MOBILIARIO_Y_EQUIPO_DE_OFICINA_POR_INVERSIONES,
                UsoCFDI.EQUIPO_DE_TRANSPORTE,
                UsoCFDI.EQUIPO_DE_COMPUTO_Y_ACCESORIOS,
                UsoCFDI.DADOS_TROQUELES_MOLDES_MATRICES_Y_HERRAMENTAL,
                UsoCFDI.COMUNICACIONES_TELEFONICAS,
                UsoCFDI.COMUNICACIONES_SATELITALES,
                UsoCFDI.OTRA_MAQUINARIA_Y_EQUIPO,
            ]
        ),
        pr.FechaPago >= fecha_filtro["start"],
        pr.FechaPago < fecha_filtro["end"],
    )

    query = (
        company_session.query(
            func.count(cfdi_pago.UUID).label("ConteoCFDIs"),
            cast(func.coalesce(func.sum(pr.Neto), 0), Numeric).label("NetoMXN"),
            cast(func.coalesce(func.sum(pr.RetencionesISR), 0), Numeric).label("RetencionesISR"),
        )
        .select_from(pr)
        .join(cfdi_pago, pr.UUID == cfdi_pago.UUID)
        .join(p, pr.payment_identifier == p.identifier)
        .join(i, pr.UUID_related == i.UUID)
        .filter(filters)
    )

    return query


def compras_gastos_no_considerados(company_session, domain_global: dict):
    n_c_ingresos = no_considerados_ingresos_pue(company_session, domain_global).subquery()
    n_c_pagos = no_considerados_pagos(company_session, domain_global).subquery()
    query = company_session.query(
        (
            func.coalesce(n_c_ingresos.c.ConteoCFDIs, 0) + func.coalesce(n_c_pagos.c.ConteoCFDIs, 0)
        ).label("ConteoCFDIs"),
        (func.coalesce(n_c_ingresos.c.NetoMXN, 0) + func.coalesce(n_c_pagos.c.NetoMXN, 0)).label(
            "NetoMXN"
        ),
        (
            func.coalesce(n_c_ingresos.c.RetencionesISRMXN, 0)
            + func.coalesce(n_c_pagos.c.RetencionesISR, 0)
        ).label("RetencionesISR"),
    )
    return query


def facturas_de_egresos_pre_llenado_pagos(
    company_session, period: date, exclude_from_isr: bool = False
):
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        ~CFDIORM.is_issued,
        CFDIORM.TipoDeComprobante == "E",
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.ExcludeFromISR == exclude_from_isr,
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )
    query = (
        company_session.query(
            func.count(),
            cast(func.coalesce(func.sum(CFDIORM.NetoMXN), 0), Numeric),
        )
        .select_from(CFDIORM)
        .filter(filters)
    )
    return query


def construir_fila(concepto: ConceptoDeduccionEnum, valores: dict) -> dict:
    mapeo = MAPEO_CONCEPTOS.get(concepto, {})

    fila = {
        "Concepto": concepto.value,
        ColumnConceptEnum.CONTEO.value: valores.get(mapeo.get(ColumnConceptEnum.CONTEO), 0),
        ColumnConceptEnum.IMPORTE.value: valores.get(mapeo.get(ColumnConceptEnum.IMPORTE), 0),
        ColumnConceptEnum.ISR.value: valores.get(mapeo.get(ColumnConceptEnum.ISR), 0),
    }

    if ColumnConceptEnum.PORCENTAJE in mapeo:
        fila[ColumnConceptEnum.PORCENTAJE.value] = valores.get(
            mapeo[ColumnConceptEnum.PORCENTAJE], 0
        )

    return fila


def adquisiciones_de_inversiones(company_session, period: date):
    fecha_filtro = get_fecha_filtro_dict(period)
    filters = and_(
        ~CFDIORM.is_issued,
        CFDIORM.TipoDeComprobante == "I",
        CFDIORM.Estatus,
        CFDIORM.Version == "4.0",
        CFDIORM.UsoCFDIReceptor.in_(
            [
                UsoCFDI.CONSTRUCCIONES,
                UsoCFDI.MOBILIARIO_Y_EQUIPO_DE_OFICINA_POR_INVERSIONES,
                UsoCFDI.EQUIPO_DE_TRANSPORTE,
                UsoCFDI.EQUIPO_DE_COMPUTO_Y_ACCESORIOS,
                UsoCFDI.DADOS_TROQUELES_MOLDES_MATRICES_Y_HERRAMENTAL,
                UsoCFDI.COMUNICACIONES_TELEFONICAS,
                UsoCFDI.COMUNICACIONES_SATELITALES,
                UsoCFDI.OTRA_MAQUINARIA_Y_EQUIPO,
            ]
        ),
        CFDIORM.FechaFiltro >= fecha_filtro["start"],
        CFDIORM.FechaFiltro < fecha_filtro["end"],
    )
    query = (
        company_session.query(
            func.count(),
            cast(func.coalesce(func.sum(CFDIORM.NetoMXN), 0), Numeric),
        )
        .select_from(CFDIORM)
        .filter(filters)
    )
    return query


def construir_fila_sumada(
    concepto: ConceptoDeduccionEnum, items: list[tuple[ConceptoDeduccionEnum, int]], valores: dict
) -> dict:
    """
    Usar * multiplicador le da al sistema flexibilidad para componer conceptos complejos
    sin necesidad de hardcodear sumas y restas en otro lado.
    """
    fila = {"Concepto": concepto.value}

    columnas_permitidas = MAPEO_CONCEPTOS.get(concepto, {}).keys()

    for col_enum in columnas_permitidas:
        total = 0
        for sub_concepto, multiplicador in items:
            val_enum = MAPEO_CONCEPTOS.get(sub_concepto, {}).get(col_enum)
            if val_enum is None:
                continue
            valor = valores.get(val_enum, 0)
            total += valor * multiplicador

        fila[col_enum.value] = total

    return fila


def construir_sumas_custom(valores: dict) -> dict[ConceptoDeduccionEnum, dict]:
    """
    Devuelve un dict con las filas resultantes de todas las sumas definidas en SUMAS_CUSTOM.
    """
    sumas = {}
    for concepto, items in SUMAS_CUSTOM.items():
        sumas[concepto] = construir_fila_sumada(concepto, items, valores)
    return sumas


def construir_totals_table(valores: dict) -> list[dict]:
    resultado = []
    ya_agregados = set()

    # Primero agregar las sumas personalizadas
    sumas = construir_sumas_custom(valores)
    for concepto, fila in sumas.items():
        resultado.append(fila)
        ya_agregados.add(concepto)

    # Ahora recorrer los demás conceptos base
    for concepto in ConceptoDeduccionEnum:
        if concepto in ya_agregados:
            continue
        if any(concepto in subs for subs in SUBCONCEPTOS.values()):
            continue

        fila = construir_fila(concepto, valores)

        # Subconceptos visibles
        if concepto in SUBCONCEPTOS:
            fila["concepts"] = [construir_fila(sub, valores) for sub in SUBCONCEPTOS[concepto]]

        resultado.append(fila)

    return resultado


def construir_totals_table_excluded(
    company_session: Session, domain: list, company: Company
) -> list[dict]:
    CED = ConceptoDeduccionEnum

    def _q(f):
        return lambda: f(company_session, domain, exclude_from_isr=True).one()[0]

    conceptos = {
        CED.COMPRAS_CONTADO: _q(compras_gastos_facturas_contado),
        CED.COMPRAS_PAGOS: _q(compras_gastos_cfdis_pagos),
        CED.DEV_INGRESOS: _q(dev_desctos_bonif_ingresos_emitidos),
        CED.DEV_EGRESOS: _q(dev_desctos_bonif_egresos_emitidos),
        CED.NO_CONSIDERADOS_INGRESOS: _q(no_considerados_ingresos_pue),
        CED.NO_CONSIDERADOS_PAGOS: _q(no_considerados_pagos),
        CED.FACTURAS_E_RECIBIDAS_COMPRAS_GASTOS: _q(facturas_de_egresos_pre_llenado_pagos),
    }

    resultado = []

    for concepto, get_conteo in conceptos.items():
        conteo = get_conteo() or 0
        resultado.append({"Concepto": concepto.value, "ConteoCFDIs": conteo})

    return resultado


def gastos_nomina_deducibles(
    session: Session, domain: list, isr_pct: Decimal
) -> tuple[int, Decimal, Decimal]:
    conteo_gravado, total_gravado = query_gastos_nomina_gravada(session, domain).one()
    (total_exento,) = query_gastos_nomina_exento_total(session, domain).one()

    deducible_exento = total_exento * isr_pct
    total_deducible = total_gravado + deducible_exento

    return conteo_gravado, total_deducible, Decimal("0")


def calcular_totales_nomina_data(
    company_session: Session, session: Session, company: Company, domain: list
) -> dict:
    isr_pct = get_isr_percentage(company)

    conteo_nomina_gravado, importe_nomina_gravado = query_gastos_nomina_gravada(
        company_session, domain
    ).one()
    (importe_nomina_exento,) = query_gastos_nomina_exento_total(company_session, domain).one()

    conteo_compras_contado, importe_compras_contado, isr_compras_contado = (
        compras_gastos_facturas_contado(company_session, domain).one()
    )
    conteo_compras_pagos, importe_compras_pagos, isr_compras_pagos = compras_gastos_cfdis_pagos(
        company_session, domain
    ).one()

    conteo_devs_egresos, importe_devs_egresos = dev_desctos_bonif_egresos_emitidos(
        company_session, domain
    ).one()
    conteo_devs_ingresos, importe_devs_ingresos = dev_desctos_bonif_ingresos_emitidos(
        company_session, domain
    ).one()
    conteo_devs_total, importe_devs_total = dev_pagos_provisionales_persona_f_actividad_empresarial(
        company_session, domain
    ).one()

    conteo_ncp, importe_ncp, isr_ncp = no_considerados_ingresos_pue(company_session, domain).one()
    conteo_nci, importe_nci, isr_nci = no_considerados_pagos(company_session, domain).one()
    conteo_cg_nc, importe_cg_nc, isr_cg_nc = compras_gastos_no_considerados(
        company_session, domain
    ).one()

    conteo_facturas_recibidas, importe_facturas_recibidas = facturas_de_egresos_pre_llenado_pagos(
        company_session, domain
    ).one()
    conteo_adquisiciones, importe_adquisiciones = adquisiciones_de_inversiones(
        company_session, domain
    ).one()

    deducible_exento = importe_nomina_exento * isr_pct
    total_deducible = importe_nomina_gravado + deducible_exento

    valores = {
        # Nómina
        ValorKeyEnum.CONTEO_NOMINA_GRAVADA: conteo_nomina_gravado,
        ValorKeyEnum.IMPORTE_NOMINA_GRAVADA: importe_nomina_gravado,
        ValorKeyEnum.IMPORTE_NOMINA_EXENTA: importe_nomina_exento,
        ValorKeyEnum.PORCENTAJE_ISR_NOMINA_EXENTA: isr_pct,
        ValorKeyEnum.IMPORTE_NOMINA_EXENTA_DEDUCIBLE: deducible_exento,
        ValorKeyEnum.TOTAL_NOMINA_DEDUCIBLE: total_deducible,
        # Compras contado
        ValorKeyEnum.CONTEO_COMPRAS_CONTADO: conteo_compras_contado,
        ValorKeyEnum.IMPORTE_COMPRAS_CONTADO: importe_compras_contado,
        ValorKeyEnum.ISR_COMPRAS_CONTADO: isr_compras_contado,
        # Compras pagos
        ValorKeyEnum.CONTEO_COMPRAS_PAGOS: conteo_compras_pagos,
        ValorKeyEnum.IMPORTE_COMPRAS_PAGOS: importe_compras_pagos,
        ValorKeyEnum.ISR_COMPRAS_PAGOS: isr_compras_pagos,
        # Devoluciones
        ValorKeyEnum.CONTEO_DEVOLUCIONES_FACTURADAS: conteo_devs_total,
        ValorKeyEnum.IMPORTE_DEVOLUCIONES_FACTURADAS: importe_devs_total,
        ValorKeyEnum.CONTEO_DEVOLUCIONES_INGRESOS: conteo_devs_ingresos,
        ValorKeyEnum.IMPORTE_DEVOLUCIONES_INGRESOS: importe_devs_ingresos,
        ValorKeyEnum.CONTEO_DEVOLUCIONES_EGRESOS: conteo_devs_egresos,
        ValorKeyEnum.IMPORTE_DEVOLUCIONES_EGRESOS: importe_devs_egresos,
        # No considerados
        ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS: conteo_cg_nc,
        ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS: importe_cg_nc,
        ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS: isr_cg_nc,
        ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS_INGRESOS: conteo_ncp,
        ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS_INGRESOS: importe_ncp,
        ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS_INGRESOS: isr_ncp,
        ValorKeyEnum.CONTEO_COMPRAS_NO_CONSIDERADAS_PAGOS: conteo_nci,
        ValorKeyEnum.IMPORTE_COMPRAS_NO_CONSIDERADAS_PAGOS: importe_nci,
        ValorKeyEnum.ISR_COMPRAS_NO_CONSIDERADAS_PAGOS: isr_nci,
        # Facturas recibidas
        ValorKeyEnum.CONTEO_FACTURAS_EGRESOS_COMPRAS_PAGOS: conteo_facturas_recibidas,
        ValorKeyEnum.IMPORTE_FACTURAS_EGRESOS_COMPRAS_PAGOS: importe_facturas_recibidas,
        # Adquisiciones
        ValorKeyEnum.CONTEO_ADQUISICIONES_INVERSION: conteo_adquisiciones,
        ValorKeyEnum.IMPORTE_ADQUISICIONES_INVERSION: importe_adquisiciones,
    }

    totals_table = construir_totals_table(valores)
    totals_table_excluded = construir_totals_table_excluded(company_session, domain, company)

    return {
        "totals_table": totals_table,
        "totals_table_excluded": totals_table_excluded,
    }


def calcular_deducciones_autorizadas_y_compras(
    session: Session,
    period: dict,
    company: Company,
) -> dict[str, dict[str, Decimal | int]]:
    """
    Calcula el importe y el ISR cargo para:
    - Deducciones autorizadas (sin inversiones)
    """
    isr_pct = get_isr_percentage(company)

    i_nomina = gastos_nomina_deducibles(session, period, isr_pct)[1]
    _, i_contado, isr_contado = compras_gastos_facturas_contado(session, period).one()
    _, i_pagos, isr_pagos = compras_gastos_cfdis_pagos(session, period).one()
    _, i_devs = dev_pagos_provisionales_persona_f_actividad_empresarial(session, period).one()
    _, i_nc, isr_nc = compras_gastos_no_considerados(session, period).one()
    _, i_egresos = facturas_de_egresos_pre_llenado_pagos(session, period).one()

    return {
        "Importe": i_nomina + i_contado + i_pagos + i_devs + i_nc - i_egresos,
        "isr_cargo": isr_contado + isr_pagos + isr_nc,
    }


# TODO: Validar que no haya nullos en la respuesta
# Quitar ISR de "Compras y gastos"
# TODO: propuesta para la exportación CSV de totales de nómina
# def build_totales_flat_query(
#     session: Session,
#     company_session: Session,
#     domain: list,
#     company: Company,
# ) -> Select:
#     """
#     Devuelve un SELECT con columnas planas ideales para exportar CSV,
#     basado en los datos de build_totales_dict.
#     """
#     data = calcular_totales_nomina_data(company_session, session, company, domain)

#     return select(
#         literal_column(str(data["Gastos de nómina gravada"]["Conteo"])).label(
#             "Gastos de nómina gravada - Conteo de CFDIs"
#         ),
#         literal_column(str(data["Gastos de nómina gravada"]["Importe"])).label(
#             "Gastos de nómina gravada - Importe"
#         ),
#         literal_column(str(data["Gastos de nómina exenta total"]["Importe"])).label(
#             "Gastos de nómina exenta total - Importe"
#         ),
#         literal_column(str(data["Gastos de nómina exenta deducible"]["porcentaje"])).label(
#             "Gastos de nómina exenta deducible - % deducción"
#         ),
#         literal_column(str(data["Gastos de nómina exenta deducible"]["Importe"])).label(
#             "Gastos de nómina exenta deducible - Importe"
#         ),
#         literal_column(str(data["Gastos de nómina deducibles"]["Importe"])).label(
#             "Gastos de nómina deducibles - Importe"
#         ),
#     )
