from datetime import datetime, timedelta

from sqlalchemy import (
    and_,
    func,
    literal,
    select,
    true,
)
from sqlalchemy.orm import Session

from chalicelib.new.query.domain.chunk import Chunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.technology import SATDownloadTechnology
from chalicelib.schema.models.tenant.sat_query import SATQuery

COVERAGE_BUFFER = timedelta(hours=72)
CHUNK_SIZE = timedelta(days=20)


def get_chunks_no_ws_query_processed(
    company_session: Session,
    start: datetime,
    end: datetime,
    download_type: DownloadType,
    coverage_buffer: timedelta = COVERAGE_BUFFER,
    chunk_size: timedelta = CHUNK_SIZE,
) -> list[Chunk]:
    from_ts = literal(start)
    to_ts = literal(end)
    coverage_buffer = literal(coverage_buffer)
    chunk_size = literal(chunk_size)

    sat_query_filter = and_(
        # Solo considerar las que ya fueron procesadas, o en su defecto, no hay nada por procesar
        SATQuery.state.in_([QueryState.PROCESSED, QueryState.INFORMATION_NOT_FOUND]),
        SATQuery.download_type == download_type,
        SATQuery.technology == SATDownloadTechnology.WebService,
    )
    queries_to_use = (
        select(
            SATQuery.start,
            func.least(
                SATQuery.end,
                SATQuery.created_at - coverage_buffer,
            ).label("covered_end"),
        )
        .where(sat_query_filter)
        .cte("queries_to_use")
    )

    covered_gaps = func.range_agg(
        func.tsrange(queries_to_use.c.start, queries_to_use.c.covered_end, "[]"),
    )
    to_check_range = func.tsmultirange(func.tsrange(from_ts, to_ts, "[]"))
    to_check_range_at_least_one_range = func.coalesce(
        covered_gaps,
        # Este rango vacío ayuda a poder hacer la resta al no haber rangos cubiertos
        func.tsmultirange(),
    )
    gaps = (
        select(func.unnest(to_check_range - to_check_range_at_least_one_range).label("gap"))
        # Se ignoran las queries donde el covered_end resulta inferior al start
        .where(queries_to_use.c.start < queries_to_use.c.covered_end)
        .cte("gaps")
    )
    gs = (
        select(
            func.generate_series(
                func.lower(gaps.c.gap),
                func.upper(gaps.c.gap)
                - timedelta(seconds=1),  # -1 sec para evitar un gap final con ambas fechas iguales
                chunk_size,
            ).label("period_start")
        )
        .correlate(gaps)
        .lateral()
        .alias("gs")
    )

    query = (
        select(
            gs.c.period_start,
            func.least(
                gs.c.period_start + chunk_size,
                func.upper(gaps.c.gap),
            ).label("period_end"),
        )
        .select_from(gaps)
        .join(gs, true())
        .order_by(gs.c.period_start)
    )

    rows = company_session.execute(query).all()
    return [Chunk(start=row.period_start, end=row.period_end) for row in rows]
