from datetime import datetime

from chalicelib.new.query.domain.enums import DownloadType
from chalicelib.new.shared.infra.message.sqs_company import SQSCompany


class NeedToCompleteCFDIsEvent(SQSCompany):
    download_type: DownloadType
    is_manual: bool = False
    start: datetime | None = None
    end: datetime | None = None
