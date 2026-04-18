from typing import Protocol

from chalicelib.new.query.domain.query import Query


class QuerySender(Protocol):
    def send(self, query: Query) -> None: ...
