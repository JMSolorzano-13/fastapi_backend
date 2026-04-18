from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.logger import WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.metadata_processor import MetadataProcessor
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.scraper.domain.events import ScrapResult, ScrapState
from chalicelib.schema.models.tenant import SATQuery as SATQueryORM


@dataclass
class ScrapProcessor:
    company_session: Session
    metadata_processor: MetadataProcessor
    xml_processor: XMLProcessor

    def process_scrap_result(self, result: ScrapResult) -> None:
        if result.state == ScrapState.FAILED:
            self.set_as_failed(result)
            return
        self.process(result)

    def set_as_failed(self, result: ScrapResult) -> None:
        if not result.query_identifier:
            log(
                Modules.SCRAPER_PROCESS,
                WARNING,
                "NO_QUERY_ORIGIN_IDENTIFIER",
                {
                    "result": result,
                },
            )
            return
        self.company_session.query(SATQueryORM).filter(
            SATQueryORM.identifier == result.query_identifier,
        ).update(
            {
                SATQueryORM.state: QueryState.SCRAP_FAILED.value,
            },
        )

    def process(self, result: ScrapResult) -> None:
        self.metadata_processor.process_zip(
            result.company_identifier, result.metadata_s3_path, result.company_rfc
        )
        self.xml_processor.process_zip(
            result.company_identifier, result.xml_s3_path, result.company_rfc
        )

        if not result.query_identifier:
            log(
                Modules.SCRAPER_PROCESS,
                WARNING,
                "NO_QUERY_ORIGIN_IDENTIFIER",
                {
                    "result": result,
                },
            )
            return
        self.company_session.query(SATQueryORM).filter(
            SATQueryORM.identifier == result.query_identifier,
        ).update(
            {
                SATQueryORM.state: QueryState.PROCESSED.value,
                SATQueryORM.cfdis_qty: result.cfdis_qty,
                SATQueryORM.start: result.start,
                SATQueryORM.end: result.end,
                SATQueryORM.request_type: result.request_type,
                SATQueryORM.download_type: result.download_type,
            },
        )
