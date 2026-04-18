from datetime import datetime

from sqlalchemy.orm import Session

from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.chunk import Chunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.get_chunks_no_cfdi_ws_query_processed import (
    get_chunks_no_ws_query_processed,
)
from chalicelib.new.query.infra.get_chunks_sa import get_chunks_need_xml
from chalicelib.new.shared.domain.primitives import Identifier

max_per_chunk: int = envars.MAX_CFDI_PER_CHUNK


def get_cfdi_chunks(
    company_session: Session,
    company_identifier: Identifier,
    download_type: DownloadType,
    start: datetime,
    end: datetime,
) -> tuple[Chunk, ...]:
    chunks = get_chunks_need_xml(
        company_session,
        company_identifier,
        download_type,
        max_per_chunk,
        start=start,
        end=end,
    )

    chunks_no_cover_by_sat_ws_processed = get_chunks_no_ws_query_processed(
        company_session,
        start,
        end,
        download_type,
    )

    res = chunks + chunks_no_cover_by_sat_ws_processed

    return tuple(sorted(res))
