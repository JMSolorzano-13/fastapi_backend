from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.logger import EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import Dashboard, WorkerCreator
from chalicelib.new.shared.domain.exceptions.not_found_exception import (
    NotFoundException,
)
from chalicelib.new.workspace.infra.workspace_repository_sa import WorkspaceRepositorySA

bp = SuperBlueprint(__name__)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def create_worker(session: Session):
    json_body = bp.current_request.json_body or {}
    workspace_identifier = json_body["workspace_identifier"]
    workspace_repo = WorkspaceRepositorySA(session)
    try:
        workspace = workspace_repo.get_by_identifier(workspace_identifier)
    except NotFoundException as e:
        log(
            Modules.ADD,
            EXCEPTION,
            "CREATE_WORKER_FAILED",
            {
                "workspace_identifier": workspace_identifier,
                "exception": e,
            },
        )
        return {
            "status": "error",
            "message": f"Workspace: {workspace_identifier} not found",
        }
    if workspace.pasto_worker_id:
        return {
            "status": "error",
            "message": f"Workspace: {workspace_identifier} already has a worker",
        }
    dashboard = Dashboard(
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
    )
    dashboard.login(email=envars.PASTO_EMAIL, password=envars.PASTO_PASSWORD)
    creator = WorkerCreator(
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
        authorization=dashboard.token,
        subscription_id=envars.PASTO_SUBSCRIPTION_ID,
        dashboard_id=envars.PASTO_DASHBOARD_ID,
        workspace_repo=workspace_repo,
        bus=get_global_bus(),
    )
    creator.create(workspace)
    return {"status": "ok"}
