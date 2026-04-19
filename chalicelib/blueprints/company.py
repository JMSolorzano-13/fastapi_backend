import base64

from chalice import BadRequestError
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
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

bp = SuperBlueprint(__name__)


@bp.route("/upload_cer", methods=["POST"], cors=common.cors_config, read_only=False)
def upload_cer(session: Session, user: User, company: Company):
    json_body = bp.current_request.json_body or {}

    cer = base64.b64decode(json_body["cer"])
    key = base64.b64decode(json_body["key"])
    password = json_body["pas"]
    context = {"user": user}
    return CompanyController.upload_certs(
        company, cer, key, password, context=context, session=session
    )


@bp.route("/get_cer", methods=["POST"], cors=common.cors_config)
def get_cer(session: Session, user: User, company: Company):
    context = {"user": user}
    cert_info = CompanyController.get_cert_info(company, context=context, session=session)
    cert_info["not_before"] = cert_info["not_before"].isoformat()
    cert_info["not_after"] = cert_info["not_after"].isoformat()
    return cert_info


@bp.route("/search", methods=["POST"], cors=common.cors_config, read_only=False)
def search(session: Session):
    return common.search(bp, CompanyController, session=session)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def create(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    cer = base64.b64decode(json_body["cer"])
    key = base64.b64decode(json_body["key"])
    password = json_body["pas"]
    workspace_identifier = json_body["workspace_identifier"]
    workspace_id = json_body["workspace_id"]
    context = {"user": user}
    company = CompanyController.create_from_certs(
        workspace_identifier, workspace_id, cer, key, password, context=context, session=session
    )

    populate_company_emails(company, user.email)

    return [CompanyController.to_nested_dict(company)]


@bp.route("/admin_create", methods=["POST"], cors=common.cors_config, read_only=False)
def admin_create(session: Session, admin_create_user: User):
    """
    Admin create company

    This endpoint is used by the admin to create a company with certificates.
    It requires the user to provide the certificate, key, password, and user_id of the
    owner of the company.
    The user_id should be the ID of the user who will own the company.
    The invoker user will be invited to the company as an admin.
    """

    json_body = bp.current_request.json_body or {}

    cer = base64.b64decode(json_body["cer"])
    key = base64.b64decode(json_body["key"])
    password = json_body["pas"]

    user_id: str = json_body["user_id"].lower()  # Owner of the company

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

    company = CompanyController.create_from_certs(
        user.workspace.identifier,
        user.workspace.id,
        cer,
        key,
        password,
        context=context,
        session=session,
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
    return {"company_identifier": company.identifier}


@bp.route("/", methods=["PUT"], cors=common.cors_config, read_only=False)
def update(session: Session, user: User):
    return common.update(bp, CompanyController, session=session, user=user)


@bp.route("/", methods=["DELETE"], cors=common.cors_config, read_only=False)
def delete(session: Session, user: User):
    return common.delete(bp, CompanyController, session=session, user=user)


@bp.route(
    "/{company_identifier}/data/{key}",
    methods=["GET"],
    cors=common.cors_config,
)
def get_data(company_identifier: str, key: str, company: Company):
    return company.data.get(key)


@bp.route(
    "/{company_identifier}/data/{key}",
    methods=["PUT"],
    cors=common.cors_config,
    read_only=False,
)
def set_data(company_identifier: str, key: str, company: Company, admin_create_user: User):
    value = bp.current_request.json_body["value"]
    company.data[key] = value
    return {"key": key, "value": value}


@bp.route("/set_isr_percentage", methods=["PUT"], cors=common.cors_config, read_only=False)
def set_isr_percentage(company: Company):
    percentage = bp.current_request.json_body["percentage"]
    # Validate percentage is valid
    if percentage not in ISR_PERCENTAGE_LIST:
        raise BadRequestError(f"ISR percentage must be one of {ISR_PERCENTAGE_LIST}")
    company.data["isr_percentage"] = percentage
    return {"message": "ISR percentage updated"}


@bp.route("/get_isr_percentage", methods=["GET"], cors=common.cors_config)
def get_isr_percentage(company: Company):
    return {"isr_percentage": get_company_isr_percentage(company)}
