from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel

from chalicelib.new.query.domain.enums.download_type import DownloadType


@dataclass
class Chunk:
    start: datetime
    end: datetime

    def __lt__(self, other):
        return self.start < other.start


class ScrapChunk(BaseModel):
    start: datetime
    end: datetime
    is_issued: DownloadType

    def __lt__(self, other):
        return self.start < other.start
