import chalice
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.controllers.add_sync_request import ADDSyncRequestController
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.pasto import ADDSyncRequester
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message import SQSCompanyManual
from chalicelib.new.workspace.infra.workspace_repository_sa import WorkspaceRepositorySA
from chalicelib.schema.models import Company

bp = SuperBlueprint(__name__)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def create_sync_request(company_session: Session, session: Session):
    json_body = bp.current_request.json_body or {}

    company_identifier = json_body["company_identifier"]
    start = json_body["start"]
    end = json_body["end"]

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


@bp.route("/enable_auto_sync", methods=["POST"], cors=common.cors_config, read_only=False)
def enable_auto_sync(session: Session):
    json_body = bp.current_request.json_body or {}
    company_identifier = json_body.get("company_identifier", "")
    add_auto_state = json_body.get("add_auto_state", False)
    company_repo = CompanyRepositorySA(session)
    company = company_repo._search_by_identifier(company_identifier)
    response, status = ADDSyncRequestController.enable_auto_sync(
        company, add_auto_state, session=session
    )
    return chalice.Response(body=response, status_code=status)


@bp.route(
    "/create_metadata_sync_request", methods=["POST"], cors=common.cors_config, read_only=False
)
def create_metadata_sync_request(session: Session):
    json_body = bp.current_request.json_body or {}
    company_identifier = json_body.get("company_identifier", "")
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
    return chalice.Response(
        body={"status": "error", "message": "No tiene permisos para sincronizar"},
        status_code=403,
    )


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    json_body = bp.current_request.json_body or {}

    search_attrs = common.get_search_attrs(json_body)

    pos, next_page, total_records = ADDSyncRequestController.search(
        **search_attrs, session=company_session
    )
    dict_repr = ADDSyncRequestController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }
