"""Pasto Config webhook — worker configuration from ADD.

Ported from: backend/chalicelib/blueprints/pasto/config.py
1 route total.
"""

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import Dashboard, WorkerConfigurator
from chalicelib.new.workspace.infra import WorkspaceRepositorySA
from dependencies import get_db_session_rw
from helpers.pasto_common import parse_pasto_webhook

router = APIRouter(tags=["Pasto Config"])


@router.post("/" + envars.ADD_CONFIG_WEBHOOK)
def config_webhook(
    body: dict = Body(...),
    request: Request = None,
    session: Session = Depends(get_db_session_rw),
):
    headers = dict(request.headers)
    error, pasto_body, hdrs = parse_pasto_webhook(body, headers, "config_webhook")
    if error:
        return {"status": "ok"}

    worker_id = hdrs["worker_id"]
    workspace_identifier = hdrs["workspace_identifier"]

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
        server=pasto_body["DbServerName"],
        username=pasto_body["DbUsername"],
        password=pasto_body["DbPassword"],
    )
