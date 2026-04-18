import io
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Any
from zipfile import ZipFile

from sqlalchemy import and_, distinct, func, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.expression import Extract as SQL_EXTRACT
from sqlalchemy.sql.functions import count as SQL_COUNT
from sqlalchemy.sql.functions import sum as SQL_SUM

from chalicelib.controllers import Domain, ensure_set, get_filters
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.enums import ResumeType
from chalicelib.logger import WARNING, log
from chalicelib.modules import Modules
from chalicelib.modules.export.pdf import get_cfdi_pdf
from chalicelib.new.cfdi.domain.cfdi_resume import EXERCISE, FILTERED, CFDIResume
from chalicelib.schema.models.tenant import CFDI
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM
from chalicelib.schema.models.tenant import Nomina as NominaORM

EXPORT_EXPIRATION = 60 * 60 * 2


class CFDIController(CommonController):
    model = CFDI
    fuzzy_fields = (
        model.NombreEmisor,
        model.NombreReceptor,
        model.RfcEmisor,
        model.RfcReceptor,
        model.UUID,
    )
    _order_by = model.FechaFiltro.key
    user_editable_fields = {
        CFDIORM.ExcludeFromISR,
        CFDIORM.ExcludeFromIVA,
        CFDIORM.PaymentDate,
    }

    def to_pdf(self, query: Iterable[CFDI], fields, session: Session, context) -> bytes:
        """Return a ZIP with the XML's of the records"""

        # TODO Temporal, cuando haya solo un camino de PDF esto ya no será necesario
        # hay funciones que utilizan una Query con multiples columnas y no solo el objeto CFDI
        # Reemplaza la query original por una que solo traiga CFDI (completo)
        if isinstance(query, Query) and len(query.column_descriptions) != 1:
            query = session.query(CFDI).filter(query.whereclause)

        f = io.BytesIO()
        with ZipFile(f, "w") as zf:
            for record in query:
                pdf = get_cfdi_pdf(record)
                zf.writestr(f"{record.UUID}.pdf", pdf)
        return f.getvalue()

    @classmethod
    @ensure_set
    def get_xml(cls, records: set[CFDI]) -> list[dict[str, str]]:
        """Given an CFDI (or a set), returns a dictionary with the UUID, id and
        SE url to download the XML."""
        return [
            {
                "uuid": cfdi.UUID,
                "xml_content": cfdi.xml_content,
            }
            for cfdi in records
        ]

    @classmethod
    def custom_count(
        cls, domain: Domain, internal_domain: list[Any], fuzzy_search: str = "", *, session: Session
    ) -> int:
        query = session.query(func.count(distinct(cls.model.UUID))).select_from(cls.model)

        if internal_domain is not None:
            filters = get_filters(cls.model, domain, session)
            query = query.filter(and_(*filters), internal_domain)
        query = CFDIController.apply_domain(query, domain, fuzzy_search, session=session)
        res = query.first()
        return res[0] if res else 0

    @classmethod
    def get_by_period(
        cls, domain: Domain, *, session: Session, context=None
    ) -> dict[str, dict[str, dict[str, int | float]]]:
        query = (
            session.query(  # type: ignore
                SQL_EXTRACT("YEAR", CFDI.FechaFiltro),
                SQL_EXTRACT("MONTH", CFDI.FechaFiltro),
                SQL_COUNT(),
                SQL_SUM(CFDI.TotalMXN),
                SQL_SUM(CFDI.SubTotalMXN),
                SQL_SUM(CFDI.NetoMXN),
            )
            .group_by(
                SQL_EXTRACT("YEAR", CFDI.FechaFiltro),
                SQL_EXTRACT("MONTH", CFDI.FechaFiltro),
            )
            .order_by(SQL_EXTRACT("YEAR", CFDI.FechaFiltro), SQL_EXTRACT("MONTH", CFDI.FechaFiltro))
        )
        query = CFDIController.apply_domain(query, domain, session=session)
        query = query.filter(CFDI.TipoDeComprobante == "I", CFDI.Estatus)

        move_type_queries = {
            "incomes": query.filter(CFDI.is_issued),
            "expenses": query.filter(~CFDI.is_issued),
        }
        periods: dict[str, dict[str, dict[str, int | float]]] = defaultdict(dict)
        for move_type, sub_query in move_type_queries.items():
            for row in sub_query:
                period = datetime(int(row[0]), int(row[1]), 1).strftime("%Y-%m")
                periods[period][move_type] = {
                    "count": int(row[2] or 0),
                    "total": float(row[3] or 0),
                    "subtotal": float(row[4] or 0),
                    "neto": float(row[5] or 0),
                }
        return periods

    @classmethod
    def resume(
        cls,
        domain: Domain,
        fuzzy_search: str = "",
        *,
        session: Session,
        context=None,  # TODO Eliminar
        resume_type,
        fields: list[
            str
        ] = None,  # Requerido para compatibilidad con common.export() aunque no se use internamente
    ) -> CFDIResume:
        # TODO permissions
        payments_in_domain = has_payments_in_domain(domain)
        resume_fields_to_use = get_resume_fields(resume_type, payments_in_domain)
        join_fields = get_resume_join_fields(resume_type)
        filtered = compute_resume(
            domain,
            fuzzy_search,
            session,
            resume_fields_to_use,
            payments_in_domain,
            join_fields,
        )
        if is_historic_domain(domain):
            exercise = filtered
        else:
            exercise_domain = get_exercise_domain(
                domain, session=session, fuzzy_search=fuzzy_search
            )
            exercise = compute_resume(
                exercise_domain,
                fuzzy_search,
                session,
                resume_fields_to_use,
                payments_in_domain,
                join_fields,
            )
        return {
            FILTERED: filtered,
            EXERCISE: exercise,
        }

    @classmethod
    def count_cfdis_by_type(
        cls,
        domain: Domain,
        fuzzy_search: str = None,
        *,
        session: Session,
    ) -> dict[str, Any]:
        return _get_count_cfdis_type(domain, fuzzy_search, session)

    @staticmethod
    def need_calculate_balance(domain: Domain) -> bool:
        return any(dt[0] == "balance" for dt in domain)


def has_payments_in_domain(domain: Domain) -> bool:
    return any(filter[0] == "payments.FormaDePagoP" for filter in domain)


def compute_resume(
    domain,
    fuzzy_search,
    session: Session,
    fields: Iterable[Any],
    payments_in_domain: bool = False,
    join_fields: set[Any] = None,
) -> dict[str, Any]:
    query = session.query(*fields)

    # Apply joins if needed (e.g., for Nomina)
    if join_fields:
        for relationship in join_fields:
            query = query.join(relationship)

    query = CFDIController.apply_domain(query, domain, fuzzy_search, session=session)

    result = dict(query.one())

    result["total_docto_relacionados"] = get_total_docto_relacionados(
        domain, fuzzy_search, session, payments_in_domain
    )
    return result


def _get_count_cfdis_type(domain, fuzzy_search, session: Session) -> dict[str, str]:
    """
    Cuenta CFDIs por tipo de comprobante aplicando los filtros del domain.

    Args:
        domain: Filtros a aplicar
        fuzzy_search: Término de búsqueda difusa
        session: Sesión de SQLAlchemy

    Returns:
        Diccionario con conteo por tipo: {"E": "0", "I": "5", "N": "0", "P": "2", "T": "0"}
    """
    query = session.query(CFDI.TipoDeComprobante, SQL_COUNT(func.distinct(CFDI.UUID))).group_by(
        CFDI.TipoDeComprobante
    )

    query = CFDIController.apply_domain(query, domain, fuzzy_search, session=session)

    # Inicializar todos los tipos de comprobante con "0"
    cfdis_by_type = {"E": "0", "I": "0", "N": "0", "P": "0", "T": "0"}

    # Actualizar con los conteos reales
    for tipo_comprobante, count in query.all():
        cfdis_by_type[tipo_comprobante] = str(count)

    return cfdis_by_type


def _get_domain_with_normalized_FechaFiltro_begin_exercise(
    domain: Domain, default_exercise_date
) -> Domain:
    domain = domain.copy()
    starts = [
        datetime.fromisoformat(rule[2])
        for rule in domain
        if rule[0] == "FechaFiltro" and rule[1] in [">", ">="]
    ]
    starts.sort()
    to_remove = [
        i for i, rule in enumerate(domain) if rule[0] == "FechaFiltro" and rule[1] in [">", ">="]
    ]
    for i in reversed(to_remove):
        del domain[i]
    exercise_start = starts[0] if starts else default_exercise_date
    if not exercise_start:
        return domain
    exercise_start = exercise_start.replace(month=1, day=1)  # Set to first day of January

    domain.append(("FechaFiltro", ">=", exercise_start))
    return domain


def get_exercise_domain(domain: Domain, session: Session, fuzzy_search: str = ""):
    query = session.query(func.max(CFDI.FechaFiltro))
    query = CFDIController.apply_domain(query, domain, fuzzy_search, session=session)
    if query._group_by_clauses or query.having:
        reset_group_by_and_having(query)
    first_Fecha_Filtro = query.scalar()
    return _get_domain_with_normalized_FechaFiltro_begin_exercise(domain, first_Fecha_Filtro)


def reset_group_by_and_having(query):
    if hasattr(query, "_group_by_clauses"):
        query._group_by_clauses = ()
    if hasattr(query, "_having_criteria"):
        query._having_criteria = ()
    return query


def is_historic_domain(domain: Domain) -> bool:
    return all(rule[0] != "FechaFiltro" for rule in domain)


RESUME_FIELDS_BASIC = (
    SQL_SUM(CFDIORM.SubTotalMXN).label(CFDIORM.SubTotalMXN.name),
    SQL_SUM(CFDIORM.SubTotal).label(CFDIORM.SubTotal.name),
    SQL_SUM(CFDIORM.NetoMXN).label(CFDIORM.NetoMXN.name),
    SQL_SUM(CFDIORM.Neto).label(CFDIORM.Neto.name),
    SQL_SUM(CFDIORM.TrasladosIVAMXN).label(CFDIORM.TrasladosIVAMXN.name),
    SQL_SUM(CFDIORM.TrasladosIVA).label(CFDIORM.TrasladosIVA.name),
    SQL_SUM(CFDIORM.TrasladosIEPSMXN).label(CFDIORM.TrasladosIEPSMXN.name),
    SQL_SUM(CFDIORM.TrasladosIEPS).label(CFDIORM.TrasladosIEPS.name),
    SQL_SUM(CFDIORM.TrasladosISRMXN).label(CFDIORM.TrasladosISRMXN.name),
    SQL_SUM(CFDIORM.TrasladosISR).label(CFDIORM.TrasladosISR.name),
    SQL_SUM(CFDIORM.RetencionesIVAMXN).label(CFDIORM.RetencionesIVAMXN.name),
    SQL_SUM(CFDIORM.RetencionesIVA).label(CFDIORM.RetencionesIVA.name),
    SQL_SUM(CFDIORM.RetencionesIEPSMXN).label(CFDIORM.RetencionesIEPSMXN.name),
    SQL_SUM(CFDIORM.RetencionesIEPS).label(CFDIORM.RetencionesIEPS.name),
    SQL_SUM(CFDIORM.RetencionesISRMXN).label(CFDIORM.RetencionesISRMXN.name),
    SQL_SUM(CFDIORM.RetencionesISR).label(CFDIORM.RetencionesISR.name),
    SQL_SUM(CFDIORM.TotalMXN).label(CFDIORM.TotalMXN.name),
    SQL_SUM(CFDIORM.Total).label(CFDIORM.Total.name),
    SQL_SUM(CFDIORM.DescuentoMXN).label(CFDIORM.DescuentoMXN.name),
    SQL_SUM(CFDIORM.Descuento).label(CFDIORM.Descuento.name),
    SQL_SUM(CFDIORM.RetencionesIVA + CFDIORM.RetencionesIEPS + CFDIORM.RetencionesISR).label(
        "ImpuestosRetenidos"
    ),
    SQL_SUM(CFDIORM.pr_count).label("PaymentRelatedCount"),
    SQL_COUNT().label("count"),
)

RESUME_FIELDS_N = (
    SQL_COUNT().label("Qty"),
    SQL_COUNT(CFDIORM.RfcReceptor.distinct()).label("EmpleadosQty"),
    SQL_SUM(NominaORM.TotalPercepciones).label("TotalPercepciones"),
    SQL_SUM(NominaORM.TotalDeducciones).label("TotalDeducciones"),
    SQL_SUM(NominaORM.TotalOtrosPagos).label("TotalOtrosPagos"),
    SQL_SUM(NominaORM.PercepcionesTotalSueldos).label("PercepcionesTotalSueldos"),
    SQL_SUM(NominaORM.PercepcionesTotalGravado).label("PercepcionesTotalGravado"),
    SQL_SUM(NominaORM.PercepcionesTotalExento).label("PercepcionesTotalExento"),
    SQL_SUM(NominaORM.DeduccionesTotalImpuestosRetenidos).label(
        "DeduccionesTotalImpuestosRetenidos"
    ),
    SQL_SUM(NominaORM.DeduccionesTotalOtrasDeducciones).label("DeduccionesTotalOtrasDeducciones"),
    SQL_SUM(NominaORM.SubsidioCausado).label("SubsidioCausado"),
    SQL_SUM(NominaORM.NetoAPagar).label("NetoAPagar"),
    SQL_SUM(NominaORM.OtrasPercepciones).label("OtrasPercepciones"),
    SQL_SUM(NominaORM.AjusteISRRetenido).label("AjusteISRRetenido"),
    SQL_SUM(NominaORM.PercepcionesJubilacionPensionRetiro).label(
        "PercepcionesJubilacionPensionRetiro"
    ),
    SQL_SUM(NominaORM.PercepcionesSeparacionIndemnizacion).label(
        "PercepcionesSeparacionIndemnizacion"
    ),
)


def get_total_docto_relacionados(domain, fuzzy_search, session: Session, payments_in_domain: bool):
    pr = aliased(DoctoRelacionadoORM)

    def base(q):
        q = q.select_from(pr).join(
            CFDIORM,
            and_(
                pr.UUID == CFDIORM.UUID,
                CFDIORM.Version == "4.0",
            ),
        )
        return CFDIController.apply_domain(q, domain, fuzzy_search, session=session)

    if not payments_in_domain:
        q = base(session.query(SQL_SUM(pr.ImpPagadoMXN)))
        return q.scalar() or 0

    grouped = base(session.query(SQL_SUM(pr.ImpPagadoMXN).label("suma"))).group_by(
        pr.payment_identifier
    )
    subq = grouped.subquery()
    return session.query(SQL_SUM(subq.c.suma)).scalar() or 0


RESUME_FIELDS_P = {
    SQL_COUNT().label("count"),
    SQL_SUM(CFDIORM.BaseIVA16).label(CFDIORM.BaseIVA16.name),
    SQL_SUM(CFDIORM.IVATrasladado16).label(CFDIORM.IVATrasladado16.name),
    SQL_SUM(CFDIORM.BaseIVA8).label(CFDIORM.BaseIVA8.name),
    SQL_SUM(CFDIORM.IVATrasladado8).label(CFDIORM.IVATrasladado8.name),
    SQL_SUM(CFDIORM.BaseIVA0).label(CFDIORM.BaseIVA0.name),
    SQL_SUM(0).label("IVATrasladado0"),  # Is always 0 TODO: Rev Performance
    SQL_SUM(CFDIORM.BaseIVAExento).label(CFDIORM.BaseIVAExento.name),
    SQL_SUM(CFDIORM.TrasladosIVA).label(CFDIORM.TrasladosIVA.name),
    SQL_SUM(CFDIORM.RetencionesIVA).label(CFDIORM.RetencionesIVA.name),
    SQL_SUM(CFDIORM.RetencionesISR).label(CFDIORM.RetencionesISR.name),
    SQL_SUM(CFDIORM.RetencionesIEPS).label(CFDIORM.RetencionesIEPS.name),
    SQL_SUM(CFDIORM.Total).label(CFDIORM.Total.name),
    SQL_SUM(CFDIORM.pr_count).label("PaymentRelatedCount"),
}

RESUME_FIELDS_PAYMENT_WITH_DOCTOS = {
    SQL_COUNT().label("count"),
    SQL_SUM(CFDIORM.BaseIVA16).label(CFDIORM.BaseIVA16.name),
    SQL_SUM(CFDIORM.IVATrasladado16).label(CFDIORM.IVATrasladado16.name),
    SQL_SUM(CFDIORM.BaseIVA8).label(CFDIORM.BaseIVA8.name),
    SQL_SUM(CFDIORM.IVATrasladado8).label(CFDIORM.IVATrasladado8.name),
    SQL_SUM(CFDIORM.BaseIVA0).label(CFDIORM.BaseIVA0.name),
    SQL_SUM(0).label("IVATrasladado0"),  # Is always 0 TODO: Rev Performance
    SQL_SUM(CFDIORM.BaseIVAExento).label(CFDIORM.BaseIVAExento.name),
    SQL_SUM(CFDIORM.TrasladosIVA).label(CFDIORM.TrasladosIVA.name),
    SQL_SUM(CFDIORM.RetencionesIVA).label(CFDIORM.RetencionesIVA.name),
    SQL_SUM(CFDIORM.RetencionesISR).label(CFDIORM.RetencionesISR.name),
    SQL_SUM(CFDIORM.RetencionesIEPS).label(CFDIORM.RetencionesIEPS.name),
    SQL_SUM(CFDIORM.Total).label(CFDIORM.Total.name),
    SQL_SUM(CFDIORM.pr_count).label("PaymentRelatedCount"),
}

RESUME_JOIN = {ResumeType.N: {CFDIORM.nomina}}


RESUME_FIELDS = {
    ResumeType.BASIC: RESUME_FIELDS_BASIC,
    ResumeType.N: RESUME_FIELDS_N,
    ResumeType.P: RESUME_FIELDS_P,
    ResumeType.PAYMENT_WITH_DOCTOS: RESUME_FIELDS_PAYMENT_WITH_DOCTOS,
}


def get_resume_fields(resume_type: ResumeType, payments_in_domain: bool) -> Iterable[Any]:
    if resume_type not in RESUME_FIELDS:
        log(
            Modules.CFDI_CONTROLLER,
            WARNING,
            "INVALID_RESUME_TYPE",
            {
                "resume_type": resume_type,
            },
        )
        return ()
    if payments_in_domain:
        return RESUME_FIELDS[ResumeType.PAYMENT_WITH_DOCTOS]
    return RESUME_FIELDS[resume_type]


def get_resume_join_fields(resume_type: ResumeType) -> set[Any]:
    if resume_type not in RESUME_JOIN:
        log(
            Modules.CFDI_CONTROLLER,
            WARNING,
            "INVALID_RESUME_TYPE_JOIN",
            {
                "resume_type": resume_type,
            },
        )
        return set()
    return RESUME_JOIN[resume_type]


def get_subquery_fields(query):
    return select(
        SQL_COUNT(query.c.UUID).label("count"),
        SQL_SUM(query.c.BaseIVA16).label(CFDIORM.BaseIVA16.name),
        SQL_SUM(query.c.IVATrasladado16).label(CFDIORM.IVATrasladado16.name),
        SQL_SUM(query.c.BaseIVA8).label(CFDIORM.BaseIVA8.name),
        SQL_SUM(query.c.IVATrasladado8).label(CFDIORM.IVATrasladado8.name),
        SQL_SUM(query.c.BaseIVA0).label(CFDIORM.BaseIVA0.name),
        SQL_SUM(0).label("IVATrasladado0"),  # Is always 0 TODO: Rev Performance
        SQL_SUM(query.c.BaseIVAExento).label(CFDIORM.BaseIVAExento.name),
        SQL_SUM(query.c.TrasladosIVA).label(CFDIORM.TrasladosIVA.name),
        SQL_SUM(query.c.RetencionesIVA).label(CFDIORM.RetencionesIVA.name),
        SQL_SUM(query.c.RetencionesISR).label(CFDIORM.RetencionesISR.name),
        SQL_SUM(query.c.RetencionesIEPS).label(CFDIORM.RetencionesIEPS.name),
        SQL_SUM(query.c.Total).label(CFDIORM.Total.name),
        SQL_SUM(query.c.PaymentRelatedCount).label("PaymentRelatedCount"),
    )
