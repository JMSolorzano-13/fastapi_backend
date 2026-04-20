from datetime import datetime

from pydantic import Field

from chalicelib.new.query.domain.enums import (
    DownloadType,
    QueryState,
    RequestType,
    SATDownloadTechnology,
)
from chalicelib.new.shared.domain.primitives import Identifier, identifier_default_factory
from chalicelib.new.shared.infra.message import SQSCompany
from chalicelib.new.utils.datetime import utc_now


class Query(SQSCompany):
    identifier: Identifier = Field(default_factory=identifier_default_factory)
    download_type: DownloadType
    request_type: RequestType
    state: QueryState = QueryState.DRAFT
    start: datetime | None = None
    end: datetime | None = None
    name: Identifier | None = None
    sent_date: datetime | None = None
    cfdis_qty: int | None = None
    packages: tuple[str, ...] = Field(default_factory=tuple)
    company_id: int | None = None
    is_manual: bool = False
    origin_identifier: Identifier | None = None
    technology: SATDownloadTechnology = SATDownloadTechnology.WebService
    created_at: datetime = utc_now()
    origin_sent_date: datetime | None = None
    wid: int | None = None
    cid: int | None = None
    #: Count of SAT verify attempts after a pending (0–2) result; drives incremental re-queue delays.
    ws_verify_retries: int = 0

    @property
    def is_mocked(self) -> bool:
        return "MOCKED" in self.name

    @property
    def mocked_rfc(self) -> str:
        return self.name.split("-")[1] if self.is_mocked else ""

    @property
    def is_issued(self) -> bool:
        is_issued = None
        if self.download_type == DownloadType.ISSUED:
            is_issued = True
        elif self.download_type == DownloadType.RECEIVED:
            is_issued = False
        return is_issued

    def to_dict(self):
        return self.model_dump(exclude={"execute_at"})
