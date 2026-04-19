"""Company routes — certificate management, CRUD, company data, ISR.

Ported from: backend/chalicelib/blueprints/company.py
11 routes total.
"""

import base64

from fastapi import APIRouter, BackgroundTasks, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.company import (
    CompanyController,
    get_certificate_and_validate_private_key,
    get_company_isr_percentage,
    populate_company_emails,
)
from chalicelib.controllers.user import UserController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars.control import ISR_PERCENTAGE_LIST
from chalicelib.schema.models import Company, Permission, User
from dependencies import (
    common,
    get_admin_create_user_rw,
    get_company,
    get_company_rw,
    get_current_user,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)
from exceptions import BadRequestError

router = APIRouter(tags=["Company"])


@router.post("/upload_cer")
def upload_cer(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
    company: Company = Depends(get_company_rw),
):
    cer = base64.b64decode(body["cer"])
    key = base64.b64decode(body["key"])
    password = body["pas"]
    context = {"user": user}
    return CompanyController.upload_certs(
        company, cer, key, password, context=context, session=session
    )


@router.post("/get_cer")
def get_cer(
    session: Session = Depends(get_db_session),
    user: User = Depends(get_current_user),
    company: Company = Depends(get_company),
):
    context = {"user": user}
    cert_info = CompanyController.get_cert_info(company, context=context, session=session)
    cert_info["not_before"] = cert_info["not_before"].isoformat()
    cert_info["not_after"] = cert_info["not_after"].isoformat()
    return cert_info


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session_rw),
):
    return common.search(json_body, CompanyController, session=session)


@router.post("", include_in_schema=False)
@router.post("/")
def create(
    background_tasks: BackgroundTasks,
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    cer = base64.b64decode(body["cer"])
    key = base64.b64decode(body["key"])
    password = body["pas"]
    workspace_identifier = body["workspace_identifier"]
    workspace_id = body["workspace_id"]
    context = {"user": user}
    defer_company_created = not envars.LOCAL_INFRA
    company = CompanyController.create_from_certs(
        workspace_identifier,
        workspace_id,
        cer,
        key,
        password,
        context=context,
        session=session,
        defer_company_created=defer_company_created,
    )
    populate_company_emails(company, user.email)
    if defer_company_created:
        background_tasks.add_task(
            CompanyController.publish_company_created_deferred,
            str(company.identifier),
        )
    # ``to_nested_dict`` is list-shaped (@ensure_list); SPA expects ``Company[]``.
    out = CompanyController.to_nested_dict(company)
    while len(out) == 1 and isinstance(out[0], list):
        out = out[0]
    return out


@router.post("/admin_create")
def admin_create(
    background_tasks: BackgroundTasks,
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    admin_create_user: User = Depends(get_admin_create_user_rw),
):
    cer = base64.b64decode(body["cer"])
    key = base64.b64decode(body["key"])
    password = body["pas"]

    user_id: str = body["user_id"].lower()
    user: User = UserController.get_or_create_from_email(email=user_id, session=session)
    context = {"user": user}

    certificate = get_certificate_and_validate_private_key(cer, key, password)

    company = (
        session.query(Company)
        .filter(
            Company.workspace_id == user.workspace.id,
            Company.rfc == certificate.subject.rfc,
        )
        .first()
    )
    if company:
        return {"company_identifier": company.identifier}

    defer_company_created = not envars.LOCAL_INFRA
    company = CompanyController.create_from_certs(
        user.workspace.identifier,
        user.workspace.id,
        cer,
        key,
        password,
        context=context,
        session=session,
        defer_company_created=defer_company_created,
    )
    session.add_all(
        [
            Permission(
                user_id=admin_create_user.id,
                company_id=company.id,
                role=Permission.RoleEnum.OPERATOR,
            ),
            Permission(
                user_id=admin_create_user.id,
                company_id=company.id,
                role=Permission.RoleEnum.PAYROLL,
            ),
        ]
    )
    WorkspaceController.update_license(
        user.workspace,
        product_license=envars.control.ADMIN_CREATE_DEFAULT_LICENSE,
        session=session,
        initial=True,
    )
    if defer_company_created:
        background_tasks.add_task(
            CompanyController.publish_company_created_deferred,
            str(company.identifier),
        )
    return {"company_identifier": company.identifier}


@router.put("", include_in_schema=False)
@router.put("/")
def update(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.update(body, CompanyController, session=session, user=user)


@router.delete("", include_in_schema=False)
@router.delete("/")
def delete(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.delete(body, CompanyController, session=session, user=user)


@router.get("/{company_identifier}/data/{key}")
def get_data(
    company_identifier: str,
    key: str,
    company: Company = Depends(get_company),
):
    return company.data.get(key)


@router.put("/{company_identifier}/data/{key}")
def set_data(
    company_identifier: str,
    key: str,
    body: dict = Body(...),
    company: Company = Depends(get_company_rw),
    admin_create_user: User = Depends(get_admin_create_user_rw),
):
    value = body["value"]
    company.data[key] = value
    return {"key": key, "value": value}


@router.put("/set_isr_percentage")
def set_isr_percentage(
    body: dict = Body(...),
    company: Company = Depends(get_company_rw),
):
    percentage = body["percentage"]
    if percentage not in ISR_PERCENTAGE_LIST:
        raise BadRequestError(f"ISR percentage must be one of {ISR_PERCENTAGE_LIST}")
    company.data["isr_percentage"] = percentage
    return {"message": "ISR percentage updated"}


@router.get("/get_isr_percentage")
def get_isr_percentage(
    company: Company = Depends(get_company),
):
    return {"isr_percentage": get_company_isr_percentage(company)}
