from dataclasses import dataclass
from datetime import datetime, timedelta
from random import randint
from typing import Literal

from sqlalchemy.orm import Session

from chalicelib.logger import DEBUG, Modules, log
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.query.domain.query import Query, QueryState
from chalicelib.new.query.infra.query_repository_sa import QueryRepositorySA
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import mx_now
from chalicelib.schema.models.company import Company as CompanyORM
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace

MOCK_MAIL_PROVIDERS = {
    "mozmail.com",
    "duck.com",
}
rfc_from_cid_cache: dict[str, str] = {}
is_cid_to_mock_cache: dict[str, bool] = {}


def randomize_milliseconds(date: datetime, sign: Literal[1, -1]) -> datetime:
    return date + timedelta(milliseconds=sign * randint(0, 1000))


@dataclass
class QueryCreator:
    query_repo: QueryRepositorySA
    session: Session

    def create(
        self,
        company_identifier: Identifier,
        download_type: DownloadType,
        request_type: RequestType,
        wid: int,
        cid: int,
        origin_sent_date: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        state: QueryState = QueryState.DRAFT,
        sat_uuid: Identifier = "Draft",
        is_manual: bool = False,
        origin_identifier: Identifier | None = None,
    ) -> Query:
        query = self._create(
            company_identifier=company_identifier,
            download_type=download_type,
            request_type=request_type,
            wid=wid,
            cid=cid,
            origin_sent_date=origin_sent_date,
            start=start,
            end=end,
            state=state,
            sat_uuid=sat_uuid,
            is_manual=is_manual,
            origin_identifier=origin_identifier,
        )
        log(
            Modules.SAT_WS_CREATE_QUERY,
            DEBUG,
            "query_created",
            {
                "company_identifier": query.company_identifier,
                "query_identifier": query.identifier,
            },
        )
        self.query_repo.create(query, auto_commit=False)
        return query

    def _create(
        self,
        company_identifier: Identifier,
        download_type: DownloadType,
        request_type: RequestType,
        wid: int,
        cid: int,
        origin_sent_date: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        state: QueryState = QueryState.DRAFT,
        sat_uuid: Identifier = "Draft",
        is_manual: bool = False,
        origin_identifier: Identifier | None = None,
    ) -> Query:
        start = start or last_X_fiscal_years(years=5)
        end = end or mx_now()
        start = randomize_milliseconds(start, sign=-1)
        end = randomize_milliseconds(end, sign=1)
        query = Query(
            company_identifier=company_identifier,
            download_type=download_type,
            request_type=request_type,
            wid=wid,
            cid=cid,
            origin_sent_date=origin_sent_date,
            start=start,
            end=end,
            state=state,
            name=sat_uuid,
            is_manual=is_manual,
            origin_identifier=origin_identifier,
        )
        if is_cid_to_mock(company_identifier, self.session):
            rfc = rfc_from_cid(company_identifier, self.session)
            query.name = f"MOCKED-{rfc}"

        return query

    def duplicate(
        self,
        query: Query,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Query:
        return self.create(
            company_identifier=query.company_identifier,
            download_type=query.download_type,
            request_type=query.request_type,
            start=start or query.start,
            end=end or query.end,
            wid=query.wid,
            cid=query.cid,
        )

    @classmethod
    def buffer_between_emissions_and_posted(cls) -> timedelta:
        # Supuestamente, el sat permite timbrar hasta con 72 horas de retraso, sin embargo,
        # en la práctica, algunos emisores tardan más en reportar los CFDI al SAT.
        # Por ello, usamos un buffer de 20 días para evitar perder CFDI
        # emitidos pero no reportados aún.
        return timedelta(days=20)


def last_X_fiscal_years(years) -> datetime:
    now = mx_now()
    return now.replace(
        year=now.year - years, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )


def rfc_from_cid(company_identifier: str, session: Session) -> str:
    if company_identifier in rfc_from_cid_cache:
        return rfc_from_cid_cache[company_identifier]

    rfc = (session.query(CompanyORM.rfc).filter_by(identifier=company_identifier).one()).rfc
    rfc_from_cid_cache[company_identifier] = rfc
    return rfc


def is_cid_to_mock(company_identifier: str, session: Session) -> bool:
    if company_identifier in is_cid_to_mock_cache:
        return is_cid_to_mock_cache[company_identifier]

    is_cid_to_mock_cache[company_identifier] = False

    rfc = rfc_from_cid(company_identifier, session)
    if rfc not in envars.SPECIAL_RFCS:
        return is_cid_to_mock_cache[company_identifier]

    owner_email = (
        session.query(User.email)
        .join(CompanyORM.workspace)
        .join(Workspace.owner)
        .filter(CompanyORM.identifier == company_identifier)
        .one()
    )

    if is_mock_user(owner_email.email):
        is_cid_to_mock_cache[company_identifier] = True

    return is_cid_to_mock_cache[company_identifier]


def is_mock_user(email):
    try:
        return email.split("@")[1] in MOCK_MAIL_PROVIDERS
    except IndexError:
        return False
