import io
from datetime import datetime, timedelta

from boto3_type_annotations.s3 import Client as S3Client
from dateutil.relativedelta import relativedelta
from sqlalchemy import or_
from sqlalchemy.orm import Session

from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain import DownloadType, RequestType
from chalicelib.new.query.domain.chunk import Chunk, ScrapChunk
from chalicelib.new.query.domain.enums import QueryState, SATDownloadTechnology
from chalicelib.new.query.domain.get_chunks_no_cfdi_ws_query_processed import (
    get_chunks_no_ws_query_processed,
)
from chalicelib.new.query.domain.query_creator import last_X_fiscal_years
from chalicelib.new.query.infra.copy_query import copy_query
from chalicelib.new.scraper.domain.events.sqs_request_new_scrap import CompanyScrapEvent
from chalicelib.new.utils.datetime import mx_now, utc_now
from chalicelib.schema.models.company import Company as CompanyORM
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import SATQuery as SATQueryORM

WORKERS = 100
MAX_SIMULTANEOUS_INVOKES_PER_WORKER = 10
TIME_BETWEEN_INVOKES = timedelta(minutes=10)


def get_company_event_scrap(company_session: Session, company: CompanyORM) -> CompanyScrapEvent:
    subchunks = generate_subchunks(company_session)

    return CompanyScrapEvent(
        company_identifier=company.identifier,
        cid=company.id,
        wid=company.workspace_id,
        request_type=RequestType.BOTH,
        start_metadata_cancel=envars.SCRAP_START_METADATA_CANCEL,
        end_metadata_cancel=mx_now(),
        chunks=subchunks,
    )


def generate_subchunks(company_session: Session) -> list[ScrapChunk]:
    start = last_X_fiscal_years(years=5)
    end = mx_now()
    optimistic_chunks_emitidos = get_chunks_no_ws_query_processed(
        company_session,
        start,
        end,
        DownloadType.ISSUED,
    )

    optimistic_chunks_recibidos = get_chunks_no_ws_query_processed(
        company_session,
        start,
        end,
        DownloadType.RECEIVED,
    )
    chunks = [(c, DownloadType.ISSUED) for c in optimistic_chunks_emitidos] + [
        (c, DownloadType.RECEIVED) for c in optimistic_chunks_recibidos
    ]
    return chunks_to_subchunks(chunks)


def chunks_to_subchunks(chunks: list[tuple[Chunk, DownloadType]]) -> list[ScrapChunk]:
    subchunks = []
    for chunk, tipo in chunks:
        subchunks.extend(get_subchunks(chunk, tipo))

    subchunks = sorted(
        subchunks,
        # Primero mes anterior, luego los demás meses en orden descendente,
        # y dentro de cada mes los recibidos antes que los emitidos
        key=lambda x: (is_prev_month(x), get_year_month(x), x[2] == DownloadType.RECEIVED),
        reverse=True,
    )
    subchunks = [ScrapChunk(start=s, end=e, is_issued=tipo) for s, e, tipo in subchunks]
    return subchunks


def get_year_month(chunk):
    start, _end, _tipo = chunk
    return f"{start.year}-{start.month:02d}"


def is_prev_month(chunk):
    start, _end, _tipo = chunk
    prev_month = datetime.now() - relativedelta(months=1)
    return start.year == prev_month.year and start.month == prev_month.month


def get_subchunks(chunk, tipo):
    """Divide cada chunk en trozos de no más de 1 mes"""
    start, end = chunk.start, chunk.end
    subchunks = []
    current_start = start
    while current_start < end:
        current_end = min(
            (current_start + relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0)
            - timedelta(seconds=1),
            end,
        )
        subchunks.append((current_start, current_end, tipo))
        current_start = current_end + timedelta(seconds=1)
    return subchunks


def set_delay_scrap_events(scrap_events: list[CompanyScrapEvent]) -> None:
    def split_list(lista, n):
        return [lista[i : i + n] for i in range(0, len(lista), n)]

    now = utc_now()
    events_grouped = split_list(scrap_events, WORKERS)
    for i, events in enumerate(events_grouped):
        sim_invoke = i // MAX_SIMULTANEOUS_INVOKES_PER_WORKER
        for event in events:
            event.execute_at = now + sim_invoke * TIME_BETWEEN_INVOKES


def upload_uuids_already_downloaded(
    company_session: Session, s3_client: S3Client, request: CompanyScrapEvent
):
    start = (
        min(chunk.start for chunk in request.chunks)
        if request.chunks
        else envars.SCRAP_START_METADATA_CANCEL
    )
    end = max(chunk.end for chunk in request.chunks) if request.chunks else mx_now()
    # TODO revisar si vale la pena hacer la revisión por tipos (emitidos/recibidos)
    query = company_session.query(CFDIORM.UUID).filter(
        or_(
            CFDIORM.from_xml,
            CFDIORM.is_too_big,
        ),
        CFDIORM.Fecha.between(start, end),
    )
    query_str = str(query.statement.compile(compile_kwargs={"literal_binds": True}))

    with io.BytesIO() as file:
        copy_query(company_session, query_str, file)
        s3_client.upload_fileobj(
            file, Bucket=envars.S3_UUIDS_COMPARE_SCRAPER, Key=f"{request.query_identifier}.csv"
        )


def generate_queries(requests: list[CompanyScrapEvent], is_manual: bool) -> list[SATQueryORM]:
    """Crea las queries 1:1 con base en los requests, manteniendo el orden"""
    now = mx_now()
    return [
        SATQueryORM(
            start=now,
            end=now,
            download_type=DownloadType.BOTH,
            request_type=request.request_type,
            state=QueryState.TO_SCRAP,
            is_manual=is_manual,
            technology=SATDownloadTechnology.Scraper,
        )
        for request in requests
    ]


def prepare_to_scrap(
    company_session: Session,
    s3_client: S3Client,
    requests: list[CompanyScrapEvent],
    is_manual: bool = False,
):
    queries = generate_queries(requests, is_manual)
    company_session.add_all(queries)
    company_session.commit()  # Necesario para tener el identifier de las queries
    for request, query in zip(requests, queries, strict=True):
        request.query_identifier = query.identifier

    for request in requests:
        upload_uuids_already_downloaded(company_session, s3_client, request)
