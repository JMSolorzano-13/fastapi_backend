import csv
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import NamedTuple

from sqlalchemy import func

from chalicelib.new.query.domain.chunk import Chunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.infra.cursor_utils import cursor_with_schema
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM


class ChunkDate(NamedTuple):
    Fecha: datetime
    to_download: bool


def _get_dates_to_chunk(
    company_session,
    company_identifier: Identifier,
    download_type: DownloadType,
    start: datetime,
    end: datetime,
) -> list[ChunkDate]:
    table = CFDIORM.get_specific_table()

    to_download = '"Estatus" AND NOT from_xml AND NOT is_too_big'

    res = (
        company_session.query(func.min(CFDIORM.Fecha), func.max(CFDIORM.Fecha))
        .filter(
            CFDIORM.Fecha.between(start, end),
            CFDIORM.is_issued == download_type.to_bool(),
            CFDIORM.Estatus,
            ~CFDIORM.from_xml,
            ~CFDIORM.is_too_big,
        )
        .one_or_none()
    )
    first_date, last_date = res if res else (None, None)

    if not (first_date and last_date):
        return []

    with NamedTemporaryFile("wb", suffix=".csv") as temp_file:
        cursor = cursor_with_schema(company_session)
        cursor.copy_expert(
            f"""
            COPY (
                SELECT
                "Fecha", {to_download}
                FROM "{table}"
                WHERE
                    "Fecha" BETWEEN '{first_date.isoformat()}' AND '{last_date.isoformat()}'
                    AND is_issued = {download_type.to_bool()}
                ORDER BY "Fecha"
            ) TO STDOUT WITH (FORMAT CSV)""",
            temp_file,
        )
        temp_file.flush()  # To ensure the file is written before reading it
        with open(temp_file.name, encoding="UTF-8") as csv_file:
            reader = csv.reader(csv_file)
            all_dates = [
                ChunkDate(
                    Fecha=datetime.fromisoformat(row[0]),
                    to_download=row[1] == "t",
                )
                for row in reader
            ]
    return all_dates


def get_chunks_need_xml(
    company_session,
    company_identifier: Identifier,
    download_type: DownloadType,
    max_per_chunk: int,
    start: datetime,
    end: datetime,
) -> list[Chunk]:
    all_dates = _get_dates_to_chunk(company_session, company_identifier, download_type, start, end)

    # Asume always first and last in all_dates are to_download
    chunks = []

    MAX_IX = len(all_dates) - 1  # Subtract 1 because the indices are inclusive # noqa: N806
    ix_start = 0
    while ix_start <= MAX_IX:
        while not all_dates[ix_start].to_download and ix_start < MAX_IX:
            ix_start += 1  # Increase as much as possible

        ix_end = min(MAX_IX, ix_start + max_per_chunk - 1)  # Avoid out of bounds
        last_end = ix_end  # Save the last end index

        while not all_dates[ix_end].to_download and ix_end > ix_start:
            ix_end -= 1  # Decrease as much as possible

        chunks.append(Chunk(all_dates[ix_start].Fecha, all_dates[ix_end].Fecha))

        ix_start = last_end + 1
    return chunks
