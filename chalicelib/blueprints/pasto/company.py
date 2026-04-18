from chalice import BadRequestError
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.pasto.common import bp_to_pasto_data
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.pasto_company import PastoCompanyController
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import CompanyCreator, CompanyRequester
from chalicelib.new.workspace.infra import WorkspaceRepositorySA

bp = SuperBlueprint(__name__)


# envars.ADD_COMPANIES_WEBHOOK
@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def company_webhook(session: Session):
    error, body, headers = bp_to_pasto_data(bp, "company_webhook")
    if error:
        return {"status": "ok"}

    worker_id = headers["worker_id"]
    workspace_identifier = headers["workspace_identifier"]

    if not body:
        raise BadRequestError("Body is empty")

    workspace_repo = WorkspaceRepositorySA(session)
    workspace = workspace_repo.get_by_identifier(workspace_identifier)
    creator = CompanyCreator(
        session=session,
    )
    to_delete, to_create, to_update = creator.create(
        workspace=workspace, worker_id=worker_id, data=body
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


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(session: Session):
    json_body = bp.current_request.json_body or {}

    search_attrs = common.get_search_attrs(json_body)

    pos, next_page, total_records = PastoCompanyController.search(**search_attrs, session=session)
    dict_repr = PastoCompanyController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }


@bp.route("/request_new", methods=["POST"], cors=common.cors_config, read_only=False)
def request_new(session: Session):
    json_body = bp.current_request.json_body or {}

    workspace_identifier = json_body["workspace_identifier"]

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
