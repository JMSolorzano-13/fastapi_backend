import enum
from dataclasses import dataclass
from datetime import datetime

from pydantic import Field

from chalicelib.new.query.domain.chunk import ScrapChunk
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.shared.domain.event.event import CompanyEvent, CompanyWithSession
from chalicelib.new.shared.domain.primitives import Identifier, identifier_default_factory
from chalicelib.new.shared.infra.message.sqs_message import SQSMessage


class ScrapState(enum.Enum):
    DONE = "DONE"
    FAILED = "FAILED"


class ScrapResult(SQSMessage, CompanyEvent):
    start: datetime
    end: datetime
    cfdis_qty: int | None = None
    state: ScrapState
    query_identifier: Identifier | None = None
    metadata_s3_path: str | None = None
    xml_s3_path: str | None = None
    request_type: RequestType | None = None
    download_type: DownloadType | None = None


class CompanyScrapEvent(SQSMessage):
    identifier: Identifier = Field(default_factory=identifier_default_factory, init=False)
    company_identifier: Identifier
    wid: int
    cid: int
    query_identifier: Identifier | None = None
    start_metadata_cancel: datetime | None = None
    end_metadata_cancel: datetime | None = None
    request_type: RequestType = RequestType.BOTH
    chunks: list[ScrapChunk]


@dataclass
class ScrapRequest(CompanyWithSession):
    chunks: list[ScrapChunk]
    start_metadata_cancel: datetime | None = None
    end_metadata_cancel: datetime | None = None
    request_type: RequestType = RequestType.BOTH
