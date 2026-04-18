"""SAT Query routes — SAT web service queries, scraping, and sync management.

Ported from: backend/chalicelib/blueprints/sat_query.py
5 routes total.
"""

import json
from dataclasses import asdict
from datetime import date, datetime

from fastapi import APIRouter, Body, Depends
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, load_only, selectinload

from chalicelib.bus import get_global_bus
from chalicelib.controllers.sat_query import SATQueryController
from chalicelib.controllers.tenant.session import (
    new_company_session_from_company_identifier,
)
from chalicelib.new.cfdi_status_logger import get_cfdi_status_log
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars import control
from chalicelib.new.query.domain.enums import RequestType
from chalicelib.new.query.domain.enums.download_type import DownloadType
from chalicelib.new.query.domain.events.query_sent_event import QueryCreateEvent
from chalicelib.new.query.domain.query import Query
from chalicelib.new.query.infra.query_repository_sa import ToReVerifySates
from chalicelib.new.scraper.domain.events.sqs_request_new_scrap import ScrapRequest
from chalicelib.new.scraper.utils import generate_subchunks
from chalicelib.new.shared.domain.event import EventType
from chalicelib.new.utils.datetime import mx_now, utc_now
from chalicelib.new.ws_sat import ManualRequestVerifier
from chalicelib.schema.models import Permission, User
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.sat_query import SATQuery
from chalicelib.schema.models.workspace import Workspace
from dependencies import (
    common,
    get_company,
    get_company_rw,
    get_company_session,
    get_company_session_rw,
    get_current_user,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["SATQuery"])

ALLOWED_ROLES = (
    Permission.RoleEnum.OPERATOR,
    Permission.RoleEnum.PAYROLL,
)


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, SATQueryController, session=company_session)


@router.post("/manual")
def manual_request(
    session: Session = Depends(get_db_session_rw),
    company_session: Session = Depends(get_company_session_rw),
    user: User = Depends(get_current_user_rw),
    company: Company = Depends(get_company_rw),
):
    bus = get_global_bus()
    verifier = ManualRequestVerifier(company_session=company_session)
    response = verifier.can_request_manual_sync(
        user,
        company,
        session,
        limit_request_types={RequestType.BOTH},
    )

    response_dict = json.loads(json.dumps(asdict(response), default=datetime.isoformat))
    if response.status != "ok":
        return response_dict

    mx_now_val = mx_now()
    start = mx_now_val - control.MANUAL_REQUEST_START_DELTA
    queries = company_session.query(SATQuery).filter(
        SATQuery.state.in_(ToReVerifySates),
        SATQuery.created_at >= (utc_now() - control.MANUAL_REQUEST_START_DELTA),
    )
    response_dict["reverifications"] = queries.count()
    for query in queries:
        internal_query = Query(
            company_identifier=company.identifier,
            download_type=query.download_type,
            request_type=query.request_type,
            start=query.start,
            end=query.end,
            state=query.state,
            name=query.name,
            is_manual=query.is_manual,
            origin_identifier=query.origin_identifier,
            sent_date=query.sent_date,
            origin_sent_date=query.created_at,
            wid=company.workspace_id,
            cid=company.id,
        )
        internal_query.set_identifier(query.identifier)
        bus.publish(
            EventType.SAT_WS_QUERY_VERIFY_NEEDED,
            internal_query,
        )

    cfdi_issued = QueryCreateEvent(
        company_identifier=company.identifier,
        download_type=DownloadType.ISSUED,
        request_type=RequestType.CFDI,
        is_manual=True,
        start=start,
        end=mx_now_val,
        wid=company.workspace_id,
        cid=company.id,
    )
    bus.publish(
        EventType.SAT_WS_REQUEST_CREATE_QUERY,
        cfdi_issued,
    )
    cfdi_received = cfdi_issued.model_copy(update={"download_type": DownloadType.RECEIVED})
    bus.publish(
        EventType.SAT_WS_REQUEST_CREATE_QUERY,
        cfdi_received,
    )

    subchunks = generate_subchunks(company_session)
    bus.publish(
        EventType.REQUEST_SCRAP,
        ScrapRequest(
            company=company,
            company_session=company_session,
            start_metadata_cancel=envars.SCRAP_START_METADATA_CANCEL,
            end_metadata_cancel=mx_now(),
            chunks=subchunks,
        ),
    )
    return response_dict


@router.post("/can_manual_request")
def can_manual_request(
    session: Session = Depends(get_db_session),
    company_session: Session = Depends(get_company_session),
    user: User = Depends(get_current_user),
    company: Company = Depends(get_company),
):
    verifier = ManualRequestVerifier(company_session=company_session)
    response = verifier.can_request_manual_sync(
        user,
        company,
        session,
        limit_request_types={RequestType.BOTH},
    )
    return json.loads(json.dumps(asdict(response), default=datetime.isoformat))


@router.post("/log")
def log_endpoint(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    start = body["start"]
    end = body["end"]

    return get_cfdi_status_log(
        session=company_session,
        start_date=date.fromisoformat(start),
        end_date=date.fromisoformat(end),
    )


@router.post("/massive_scrap")
def massive_scrap(
    body: dict = Body(default={}),
    session: Session = Depends(get_db_session),
    user: User = Depends(get_current_user),
):
    start, end = body.get("start"), body.get("end", mx_now())

    companies = _list_companies_for_user(session, user, load_ws=True)

    bus = get_global_bus()
    published: set[str] = set()
    skipped: set[str] = set()

    read_only = False
    for company in companies:
        with new_company_session_from_company_identifier(
            company_identifier=company.identifier,
            session=session,
            read_only=read_only,
        ) as company_session:
            if not _can_publish_scrap(user, company, company_session, session):
                skipped.add(company.identifier)
                continue

            _publish_scrap(bus, company, company_session, start, end)
            published.add(company.identifier)

    return {
        "status": "ok",
        "companies": {
            "published": sorted(published),
            "skipped": sorted(skipped),
            "total": len(companies),
        },
    }


def _list_companies_for_user(session: Session, user: User, *, load_ws: bool = False):
    stmt = (
        select(Company)
        .join(Company.workspace)
        .outerjoin(Permission, Permission.company_id == Company.id)
        .where(
            Company.active.is_(True),
            Workspace.is_active,
            or_(
                Workspace.owner_id == user.id,
                and_(
                    Permission.user_id == user.id,
                    Permission.role.in_(ALLOWED_ROLES),
                ),
            ),
        )
        .distinct()
        .options(load_only(Company.id, Company.identifier))
    )

    if load_ws:
        stmt = stmt.options(selectinload(Company.workspace))

    return session.execute(stmt).scalars().all()


def _can_publish_scrap(
    user: User, company: Company, company_session: Session, session: Session
) -> bool:
    verifier = ManualRequestVerifier(company_session=company_session)
    is_owner = bool(company.workspace and company.workspace.owner_id == user.id)
    resp = verifier.can_request_manual_sync(
        user,
        company,
        session=session,
        limit_request_types={RequestType.BOTH},
        is_owner=is_owner,
    )
    return resp.status == "ok"


def _publish_scrap(
    bus, company: Company, company_session: Session, start: datetime | None, end: datetime | None
):
    bus.publish(
        EventType.REQUEST_SCRAP,
        ScrapRequest(
            start_metadata_cancel=start,
            end_metadata_cancel=end,
            company=company,
            company_session=company_session,
            chunks=[],
        ),
    )
