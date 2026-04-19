"""Pasto Company routes — ADD company webhook and management.

Ported from: backend/chalicelib/blueprints/pasto/company.py
3 routes total.
"""

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from chalicelib.controllers.pasto_company import PastoCompanyController
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import CompanyCreator, CompanyRequester
from chalicelib.new.workspace.infra import WorkspaceRepositorySA
from dependencies import get_db_session, get_db_session_rw, get_json_body
from dependencies.common import get_search_attrs
from exceptions import BadRequestError
from helpers.pasto_common import parse_pasto_webhook

router = APIRouter(tags=["Pasto Company"])


@router.post("", include_in_schema=False)
@router.post("/")
def company_webhook(
    body: dict = Body(...),
    request: Request = None,
    session: Session = Depends(get_db_session_rw),
):
    headers = dict(request.headers)
    error, pasto_body, hdrs = parse_pasto_webhook(body, headers, "company_webhook")
    if error:
        return {"status": "ok"}

    worker_id = hdrs["worker_id"]
    workspace_identifier = hdrs["workspace_identifier"]

    if not pasto_body:
        raise BadRequestError("Body is empty")

    workspace_repo = WorkspaceRepositorySA(session)
    workspace = workspace_repo.get_by_identifier(workspace_identifier)
    creator = CompanyCreator(
        session=session,
    )
    to_delete, to_create, to_update = creator.create(
        workspace=workspace, worker_id=worker_id, data=pasto_body
    )
    log(
        Modules.ADD,
        DEBUG,
        "COMPANY_HOOK",
        {
            "workspace_identifier": workspace_identifier,
            "to_delete": to_delete,
            "to_create": to_create,
            "to_update": to_update,
        },
    )
    return {"status": "ok"}


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    search_attrs = get_search_attrs(json_body)

    pos, next_page, total_records = PastoCompanyController.search(**search_attrs, session=session)
    dict_repr = PastoCompanyController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }


@router.post("/request_new")
def request_new(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
):
    workspace_identifier = body["workspace_identifier"]

    log(
        Modules.ADD,
        DEBUG,
        "COMPANY_REQUEST",
        {
            "workspace_identifier": workspace_identifier,
        },
    )

    workspace = WorkspaceRepositorySA(session).get_by_identifier(workspace_identifier)
    worker_id = workspace.pasto_worker_id
    worker_token = workspace.pasto_worker_token

    company_requester = CompanyRequester(
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
        authorization=None,
        endpoint=envars.SELF_ENDPOINT,
        api_route=envars.ADD_COMPANIES_WEBHOOK,
    )

    response = company_requester.request_companies(
        workspace_identifier=workspace_identifier,
        worker_id=worker_id,
        authorization=worker_token,
    )

    log(
        Modules.ADD,
        DEBUG,
        "COMPANY_REQUESTED",
        {
            "workspace_identifier": workspace_identifier,
            "response": response,
        },
    )
    return {"status": "ok"}
