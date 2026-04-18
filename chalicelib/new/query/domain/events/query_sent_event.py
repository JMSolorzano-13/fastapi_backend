from datetime import datetime

from chalicelib.new.query.domain.enums import DownloadType, RequestType
from chalicelib.new.shared.domain.event.event import CompanyEvent
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message.sqs_message import SQSMessage


class QueryCreateEvent(SQSMessage, CompanyEvent):
    download_type: DownloadType
    request_type: RequestType
    is_manual: bool = False
    start: datetime | None = None
    end: datetime | None = None
    query_origin: Identifier | None = None
    origin_sent_date: datetime | None = None
    wid: int
    cid: int
