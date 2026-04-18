import enum
from datetime import date, timedelta

from sqlalchemy import (
    TIMESTAMP,
    Date,
    and_,
    case,
    cast,
    func,
    literal,
    or_,
    select,
)
from sqlalchemy.orm import Session

from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.utils.datetime import utc_to_mx
from chalicelib.schema.models.tenant.cfdi import CFDI as CFDIORM
from chalicelib.schema.models.tenant.sat_query import SATQuery as SATQueryORM


class CFDIStatusLog(enum.Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    EMPTY = "EMPTY"


# Estados válidos para considerar queries procesadas
PROCESSED_STATES = [QueryState.PROCESSED, QueryState.INFORMATION_NOT_FOUND]

# Estados de queries en progreso
IN_PROGRESS_STATES = [
    QueryState.TO_DOWNLOAD,
    QueryState.DOWNLOADED,
    QueryState.PROCESSING,
    QueryState.DELAYED,
]


def get_cfdi_status_log(session: Session, start_date: date, end_date: date) -> dict:
    """
    Obtiene el log de estado de CFDIs usando una query SQL única con range_agg.

    Args:
        session: SQLAlchemy session
        start_date: Fecha inicial del rango
        end_date: Fecha final del rango

    Returns:
        Dict con formato: {"days": [...], "historic": {...}}
    """
    daily_status = _execute_daily_status_query(session, start_date, end_date)
    days = _format_daily_results(daily_status)
    historic = _calculate_historic(session)

    return {"days": days, "historic": historic}


def _build_days_cte(start_date: date, end_date: date):
    """CTE 1: Genera serie de fechas para el rango especificado."""
    return select(
        func.generate_series(start_date, end_date, timedelta(days=1)).cast(Date).label("fecha_date")
    ).cte("days")


def _build_cfdi_stats_cte(is_issued: bool, start_date: date, end_date: date):
    """CTE 2/3: Calcula estadísticas de CFDIs (total y con XML) por día."""
    cte_name = "cfdi_stats_issued" if is_issued else "cfdi_stats_received"

    return (
        select(
            func.date(CFDIORM.Fecha).label("fecha_date"),
            func.count().label("total"),
            func.count().filter(or_(CFDIORM.from_xml, CFDIORM.is_too_big)).label("with_xml"),
        )
        .where(
            CFDIORM.Estatus == True,
            CFDIORM.is_issued == is_issued,
            CFDIORM.Fecha >= start_date,
            CFDIORM.Fecha <= end_date,
        )
        .group_by(func.date(CFDIORM.Fecha))
        .cte(cte_name)
    )


def _build_merged_ranges_cte(download_type: DownloadType):
    """
    CTE 4/5: Agrega rangos de tiempo de queries procesadas.
    Excluye CANCELLATION porque no garantiza cobertura completa.
    """
    cte_name = f"merged_ranges_{download_type.value.lower()}"

    download_type_filter = or_(
        SATQueryORM.download_type == download_type,
        SATQueryORM.download_type == DownloadType.BOTH,
    )

    return (
        select(
            func.range_agg(func.tsrange(SATQueryORM.start, SATQueryORM.end, literal("[)"))).label(
                "ranges"
            )
        )
        .where(
            SATQueryORM.state.in_(PROCESSED_STATES),
            download_type_filter,
            SATQueryORM.request_type != RequestType.CANCELLATION,
        )
        .cte(cte_name)
    )


def _build_status_case(stats_cte, ranges_cte, day_range):
    """Construye la expresión CASE para determinar el estado de un día."""
    return case(
        (
            # Si no todos tienen XML -> INCOMPLETE
            func.coalesce(stats_cte.c.total, 0) > func.coalesce(stats_cte.c.with_xml, 0),
            literal("INCOMPLETE"),
        ),
        (
            # Si no estamos seguros (día no cubierto completamente) -> INCOMPLETE
            ~func.coalesce(day_range.op("<@")(ranges_cte.c.ranges), False),
            literal("INCOMPLETE"),
        ),
        (
            # Si no tiene CFDIs -> EMPTY
            func.coalesce(stats_cte.c.total, 0) == 0,
            literal("EMPTY"),
        ),
        else_=literal("COMPLETE"),
    )


def _build_final_status_case(status_issued, status_received):
    """Combina los estados de issued y received en un estado final."""
    return case(
        (
            # Prioridad 1: INCOMPLETE (si alguno es incomplete)
            or_(
                status_issued == literal("INCOMPLETE"),
                status_received == literal("INCOMPLETE"),
            ),
            literal("INCOMPLETE"),
        ),
        (
            # Prioridad 2: COMPLETE (si ambos son complete)
            and_(
                status_issued == literal("COMPLETE"),
                status_received == literal("COMPLETE"),
            ),
            literal("COMPLETE"),
        ),
        (
            # Prioridad 3: EMPTY (si ambos son EMPTY)
            and_(
                status_issued == literal("EMPTY"),
                status_received == literal("EMPTY"),
            ),
            literal("EMPTY"),
        ),
        # Caso mixto: Si al menos uno es COMPLETE -> COMPLETE
        else_=literal("COMPLETE"),
    ).label("status")


def _execute_daily_status_query(session: Session, start_date: date, end_date: date) -> list:
    """
    Ejecuta la query principal con CTEs y range_agg para determinar el estado de cada día.
    """
    # Construir CTEs usando funciones helper
    days_cte = _build_days_cte(start_date, end_date)
    cfdi_stats_issued_cte = _build_cfdi_stats_cte(
        is_issued=True, start_date=start_date, end_date=end_date
    )
    cfdi_stats_received_cte = _build_cfdi_stats_cte(
        is_issued=False, start_date=start_date, end_date=end_date
    )
    merged_ranges_issued_cte = _build_merged_ranges_cte(DownloadType.ISSUED)
    merged_ranges_received_cte = _build_merged_ranges_cte(DownloadType.RECEIVED)

    # Definir rango del día una sola vez para ambas validaciones
    day_range = func.tsrange(
        cast(days_cte.c.fecha_date, TIMESTAMP),
        cast(days_cte.c.fecha_date + timedelta(days=1), TIMESTAMP),
        literal("[)"),
    )

    # Determinar estados usando funciones helper
    status_issued = _build_status_case(
        cfdi_stats_issued_cte, merged_ranges_issued_cte, day_range
    ).label("status_issued")
    status_received = _build_status_case(
        cfdi_stats_received_cte, merged_ranges_received_cte, day_range
    ).label("status_received")
    status_final = _build_final_status_case(status_issued, status_received)

    query = (
        select(
            days_cte.c.fecha_date,
            func.coalesce(cfdi_stats_issued_cte.c.total, 0).label("total_issued"),
            func.coalesce(cfdi_stats_issued_cte.c.with_xml, 0).label("processed_issued"),
            func.coalesce(cfdi_stats_received_cte.c.total, 0).label("total_received"),
            func.coalesce(cfdi_stats_received_cte.c.with_xml, 0).label("processed_received"),
            status_final,
        )
        .select_from(days_cte)
        .outerjoin(
            cfdi_stats_issued_cte, days_cte.c.fecha_date == cfdi_stats_issued_cte.c.fecha_date
        )
        .outerjoin(
            cfdi_stats_received_cte, days_cte.c.fecha_date == cfdi_stats_received_cte.c.fecha_date
        )
        .outerjoin(merged_ranges_issued_cte, literal(True))
        .outerjoin(merged_ranges_received_cte, literal(True))
        .order_by(days_cte.c.fecha_date.desc())
    )

    return session.execute(query).all()


def _format_daily_results(query_results: list) -> list[dict]:
    """
    Transforma resultados de la query SQL al formato de respuesta esperado.

    Returns:
        Lista de dicts con formato:
        {
            "date": "2026-02-09",
            "status": "COMPLETE",
            "issued": {"total": 10, "processed": 10},
            "received": {"total": 5, "processed": 5}
        }
    """
    days = []
    for row in query_results:
        days.append(
            {
                "date": row.fecha_date.isoformat(),
                "status": row.status,
                "issued": {"total": row.total_issued, "processed": row.processed_issued},
                "received": {"total": row.total_received, "processed": row.processed_received},
            }
        )

    return days


def _calculate_historic(session: Session) -> dict:
    """
    Calcula el histórico completo de CFDIs desde la primera metadata query
    hasta la última metadata query completada.

    Returns:
        Dict con formato:
        {
            "start": "2019-12-31",
            "end": "2025-08-21",
            "status": "COMPLETE",
            "received": {"total": 71551, "processed": 71551},
            "issued": {"total": 95681, "processed": 95681}
        }

        Retorna {} si no existe ninguna metadata query.
    """
    # Buscar la primera metadata query
    first_metadata_query = (
        session.query(SATQueryORM)
        .filter(SATQueryORM.request_type == RequestType.METADATA)
        .order_by(SATQueryORM.created_at.asc())
        .first()
    )

    if not first_metadata_query:
        return {}

    start_log = utc_to_mx(first_metadata_query.start).date()

    # Buscar la última metadata query completada
    last_metadata_query = (
        session.query(SATQueryORM)
        .filter(
            SATQueryORM.request_type == RequestType.METADATA,
            SATQueryORM.state.in_(PROCESSED_STATES),
        )
        .order_by(SATQueryORM.end.desc())
        .first()
    )

    # Usar la fecha de la última query completada, o la fecha de inicio si no hay ninguna
    end_log = utc_to_mx(last_metadata_query.end).date() if last_metadata_query else start_log

    # Contar CFDIs por tipo (issued/received)
    cfdi_processed = or_(CFDIORM.from_xml, CFDIORM.is_too_big)

    cfdis_stats = (
        session.query(
            CFDIORM.is_issued,
            func.count().label("total"),
            func.count(case((cfdi_processed, 1))).label("processed"),
        )
        .filter(
            CFDIORM.Estatus == True,  # noqa: E712
            CFDIORM.Fecha >= start_log,
            CFDIORM.Fecha <= end_log,
        )
        .group_by(CFDIORM.is_issued)
        .all()
    )

    # Inicializar resultado con valores por defecto
    result = {
        "issued": {"total": 0, "processed": 0},
        "received": {"total": 0, "processed": 0},
    }

    # Procesar resultados de la query
    for cfdi_stat in cfdis_stats:
        if cfdi_stat.is_issued:
            result["issued"]["total"] = cfdi_stat.total
            result["issued"]["processed"] = cfdi_stat.processed
        else:
            result["received"]["total"] = cfdi_stat.total
            result["received"]["processed"] = cfdi_stat.processed

    # Determinar estado
    status = (
        CFDIStatusLog.COMPLETE
        if result["issued"]["total"] == result["issued"]["processed"]
        and result["received"]["total"] == result["received"]["processed"]
        else CFDIStatusLog.INCOMPLETE
    )

    return {
        "start": start_log.isoformat(),
        "end": end_log.isoformat(),
        "status": status.value,
        **result,
    }
