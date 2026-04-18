from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.controllers import with_session
from chalicelib.controllers.tenant.session import new_company_session_from_company_identifier
from chalicelib.new.query.domain.query_creator import QueryCreator
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.ws_sat.domain.events.query_need_to_be_splited import (
    QueryNeedToBeSplittedEvent,
)
from chalicelib.new.ws_sat.domain.query_splitter_binary_dates import (
    QuerySplitterBinaryDates,
)
from chalicelib.new.ws_sat.infra.query_sender_ws import QuerySenderWS


@dataclass
class QueryNeedSplitHandler:  # >EventHandler
    bus: EventBus

    @with_session(read_only=False)
    def handle(self, event: QueryNeedToBeSplittedEvent, session: Session):
        with new_company_session_from_company_identifier(
            company_identifier=event.company_identifier,
            session=session,
            read_only=False,
        ) as company_session:
            query_repo = QueryRepositorySA(session=company_session)
            query = event.query
            query_creator = QueryCreator(query_repo=query_repo, session=session)
            splitter = QuerySplitterBinaryDates(query_creator)
            split_queries = splitter.split(query)
            query_sender = QuerySenderWS(
                bus=self.bus,
                company_session=company_session,
                session=session,
            )
            query_sender.parallel_send(split_queries)
