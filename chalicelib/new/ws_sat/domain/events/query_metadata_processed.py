from datetime import datetime

from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.shared.domain.event.event import CompanyEvent


class QueryMetadataProcessedEvent(CompanyEvent):
    download_type: DownloadType
    is_manual: bool = False
    start: datetime = None
    end: datetime = None
