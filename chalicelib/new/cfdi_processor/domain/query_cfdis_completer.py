from dataclasses import dataclass
from datetime import datetime
from logging import DEBUG

from sqlalchemy.orm import Session

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.query.domain.chunk import Chunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.query_cfdi_splitter import get_cfdi_chunks
from chalicelib.new.query.domain.query_creator import QueryCreator, last_X_fiscal_years
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import mx_now
from chalicelib.new.ws_sat.domain.query_sender import QuerySender


@dataclass
class QueryCFDISCompleter:
    company_session: Session
    query_creator: QueryCreator
    query_sender: QuerySender
    company_repo: CompanyRepositorySA

    def complete_cfdis(
        self,
        company_identifier: Identifier,
        download_type: DownloadType,
        is_manual: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Chunk, ...]:
        start = start or last_X_fiscal_years(years=5)
        end = end or mx_now()
        log(
            Modules.GENERATE_XML_REQUESTS,
            DEBUG,
            "COMPLETING_CFDI",
            {
                "download_type": download_type,
                "company_identifier": company_identifier,
            },
        )

        # Obtener datos de la empresa para wid, cid, rfc
        company = self.company_repo.get_by_identifier(company_identifier)

        chunks = get_cfdi_chunks(
            company_session=self.company_session,
            company_identifier=company_identifier,
            download_type=download_type,
            start=start,
            end=end,
        )
        log(
            Modules.GENERATE_XML_REQUESTS,
            DEBUG,
            "CHUNKS_TO_SEND",
            {"chunks": chunks, "chunks_count": len(chunks)},
        )
        queries = [
            self.query_creator.create(
                company_identifier=company_identifier,
                download_type=download_type,
                request_type=RequestType.CFDI,
                start=chunk.start,
                end=chunk.end,
                is_manual=is_manual,
                wid=company.workspace_id,
                cid=company.id,
            )
            for chunk in chunks
        ]
        self.query_sender.parallel_send(queries)

        log(
            Modules.GENERATE_XML_REQUESTS,
            DEBUG,
            "CHUNKS_SENT",
            {
                "company_identifier": company_identifier,
                "chunks": len(chunks),
            },
        )
        return chunks
