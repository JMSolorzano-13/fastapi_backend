from chalicelib.new.query.domain.query import Query
from chalicelib.new.shared.domain.event.event import CompanyEvent


class QueryNeedToBeSplittedEvent(CompanyEvent):
    query: Query
