from datetime import datetime

from pydantic import Field

from chalicelib.new.query.domain import RequestType
from chalicelib.new.query.domain.enums import QueryState
from chalicelib.new.shared.domain.event.event import CompanyEvent
from chalicelib.new.shared.domain.primitives import Identifier

from .sqs_message import SQSMessage


class SQSCompany(SQSMessage, CompanyEvent):
    pass


class SQSCompanyManual(SQSCompany):
    manually_triggered: bool = False


class SQSCompanySendMetadata(SQSCompanyManual):
    wid: int
    cid: int


class SQSUpdaterQuery(SQSCompanyManual):
    query_identifier: Identifier
    state: QueryState
    request_type: RequestType
    name: Identifier | None = None
    sent_date: datetime | None = None
    packages: tuple[str, ...] = Field(default_factory=tuple)
    cfdis_qty: int = 0
    is_mocked: bool = False
    state_update_at: datetime = None
