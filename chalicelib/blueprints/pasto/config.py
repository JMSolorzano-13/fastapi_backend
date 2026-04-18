from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.pasto.common import bp_to_pasto_data
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import Dashboard, WorkerConfigurator
from chalicelib.new.workspace.infra import WorkspaceRepositorySA

bp = SuperBlueprint(__name__)


@bp.route(envars.ADD_CONFIG_WEBHOOK, methods=["POST"], cors=common.cors_config, read_only=False)
def config_webhook(session: Session):
    error, body, headers = bp_to_pasto_data(bp, "config_webhook")
    if error:
        return {"status": "ok"}

    worker_id = headers["worker_id"]
    workspace_identifier = headers["workspace_identifier"]

    workspace_repo = WorkspaceRepositorySA(session)
    workspace = workspace_repo.get_by_identifier(workspace_identifier)

    dashboard = Dashboard(
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
    )
    dashboard.login(email=envars.PASTO_EMAIL, password=envars.PASTO_PASSWORD)
    worker_configurator = WorkerConfigurator(
        workspace_repo=workspace_repo,
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
        authorization=dashboard.token,
        bus=get_global_bus(),
    )
    worker_configurator.set_credentials(
        workspace=workspace,
        worker_id=worker_id,
        server=body["DbServerName"],
        username=body["DbUsername"],
        password=body["DbPassword"],
    )
