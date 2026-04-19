"""Pasto Sync routes — ADD sync request management.

Ported from: backend/chalicelib/blueprints/pasto/sync.py
4 routes total.
"""

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.controllers.add_sync_request import ADDSyncRequestController
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.pasto import ADDSyncRequester
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message import SQSCompanyManual
from chalicelib.new.workspace.infra.workspace_repository_sa import WorkspaceRepositorySA
from chalicelib.schema.models import Company
from dependencies import (
    get_company_session_rw,
    get_db_session_rw,
    get_json_body,
)
from dependencies.common import get_search_attrs

router = APIRouter(tags=["Pasto Sync"])


@router.post("", include_in_schema=False)
@router.post("/")
def create_sync_request(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    session: Session = Depends(get_db_session_rw),
):
    company_identifier = body["company_identifier"]
    start = body["start"]
    end = body["end"]

    company: Company = session.query(Company).filter_by(identifier=company_identifier).one()
    pasto_token = company.workspace.pasto_worker_token
    pasto_company_identifier = company.pasto_company_identifier

    requester = ADDSyncRequester(
        company_session=company_session,
        bus=get_global_bus(),
    )
    requester.request(
        company_identifier=company_identifier,
        pasto_company_identifier=pasto_company_identifier,
        start=start,
        end=end,
        manually_triggered=True,
        pasto_token=pasto_token,
    )
    return {"status": "ok"}


@router.post("/enable_auto_sync")
def enable_auto_sync(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
):
    company_identifier = body.get("company_identifier", "")
    add_auto_state = body.get("add_auto_state", False)
    company_repo = CompanyRepositorySA(session)
    company = company_repo._search_by_identifier(company_identifier)
    response, status = ADDSyncRequestController.enable_auto_sync(
        company, add_auto_state, session=session
    )
    return JSONResponse(content=response, status_code=status)


@router.post("/create_metadata_sync_request")
def create_metadata_sync_request(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
):
    company_identifier = body.get("company_identifier", "")
    bus = get_global_bus()

    company_repo = CompanyRepositorySA(session=session)
    company = company_repo._search_by_identifier(company_identifier)
    workspace_identifier = company.workspace_identifier
    workspace_repo = WorkspaceRepositorySA(session=session)
    workspace = workspace_repo._search_by_identifier(workspace_identifier)
    if workspace.add_permission:
        bus.publish(
            EventType.ADD_METADATA_REQUESTED,
            SQSCompanyManual(company_identifier=company_identifier, manually_triggered=True),
        )
        return {"status": "ok"}
    return JSONResponse(
        content={"status": "error", "message": "No tiene permisos para sincronizar"},
        status_code=403,
    )


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session_rw),
):
    search_attrs = get_search_attrs(json_body)

    pos, next_page, total_records = ADDSyncRequestController.search(
        **search_attrs, session=company_session
    )
    dict_repr = ADDSyncRequestController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }
