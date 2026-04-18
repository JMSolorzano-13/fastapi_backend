from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import and_, func
from sqlalchemy.orm import Query as SQLAlchemyQuery
from sqlalchemy.orm import Session

from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain import Query
from chalicelib.new.query.domain.enums import DownloadType, SATDownloadTechnology
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.new.utils.datetime import today_mx_in_utc
from chalicelib.schema.models.tenant import SATQuery as QueryORM

ToReVerifySates = {
    QueryState.SENT,
    QueryState.TIME_LIMIT_REACHED,
}


@dataclass
class QueryRepositorySA(SQLAlchemyRepo):
    session: Session  # TODO esto debería ser llamado company_session
    _model = Query
    _model_orm = QueryORM

    def queries_to_reverify_filter(self, now: datetime):
        min_created_at = now - envars.REVERIFY_CREATED_AFTER
        max_created_at = now - envars.REVERIFY_CREATED_BEFORE
        return and_(
            QueryORM.state.in_(ToReVerifySates),
            QueryORM.created_at >= min_created_at,
            QueryORM.created_at <= max_created_at,
        )

    def get_all_queries_to_re_verify(self, now: datetime) -> SQLAlchemyQuery:
        return self.session.query(QueryORM).filter(self.queries_to_reverify_filter(now))

    def scrap_requested_today_mx(self, download_type: DownloadType) -> int:
        today = today_mx_in_utc()
        return (
            self.session.query(QueryORM.identifier)
            .filter(
                QueryORM.download_type == download_type.value,
                QueryORM.technology == SATDownloadTechnology.Scraper.value,
                QueryORM.created_at >= today,
            )
            .count()
        )

    def get_delayed(self, max_queries: int) -> list[Query]:
        # Gets a random sample of queries from the DB
        return (
            self.session.query(QueryORM)
            .filter(QueryORM.state == QueryState.DELAYED.value)
            .order_by(func.random())
            .limit(max_queries)
        ).all()

    def _create_record_orm(self, model: _model) -> None:
        query_orm = QueryORM(
            download_type=model.download_type,
            request_type=model.request_type,
            start=model.start,
            end=model.end,
            state=model.state,
            packages=model.packages,
            identifier=model.identifier,
            name=model.name,
            # TODO remove when model no longer depends on DB infra
            is_manual=model.is_manual,
            origin_identifier=model.origin_identifier,
            technology=model.technology.value,
        )
        self.session.add(query_orm)

    def _model_from_orm(self, record_orm: QueryORM) -> Query:
        return Query(
            company_identifier="",  # TODO restructuracion
            identifier=record_orm.identifier,
            download_type=record_orm.download_type,
            request_type=record_orm.request_type,
            start=record_orm.start,
            end=record_orm.end,
            state=record_orm.state,
            name=record_orm.name,
            cfdis_qty=record_orm.cfdis_qty,
            packages=record_orm.packages,
            sent_date=record_orm.sent_date,
            is_manual=record_orm.is_manual,
            origin_identifier=record_orm.origin_identifier,
            technology=record_orm.technology,
        )

    def _update_orm(self, record_orm: QueryORM, model: Query) -> None:
        record_orm.name = model.name or ""
        record_orm.identifier = model.identifier
        record_orm.start = model.start
        record_orm.end = model.end
        record_orm.download_type = model.download_type
        record_orm.request_type = model.request_type
        record_orm.packages = model.packages
        record_orm.cfdis_qty = model.cfdis_qty
        record_orm.state = model.state
        record_orm.sent_date = model.sent_date
        record_orm.is_manual = model.is_manual
        record_orm.origin_identifier = model.origin_identifier
        record_orm.technology = model.technology.value

    def last_query_processed(self, download_type: DownloadType) -> QueryORM | None:
        return (
            self.session.query(QueryORM)
            .filter(
                QueryORM.download_type == download_type.value,
                QueryORM.technology == SATDownloadTechnology.WebService.value,
                QueryORM.state.in_(
                    (QueryState.PROCESSED.value, QueryState.INFORMATION_NOT_FOUND.value)
                ),
            )
            .order_by(QueryORM.end.desc())
            .first()
        )
