"""Pasto Worker routes — ADD worker creation.

Ported from: backend/chalicelib/blueprints/pasto/worker.py
1 route total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.logger import EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto import Dashboard, WorkerCreator
from chalicelib.new.shared.domain.exceptions.not_found_exception import (
    NotFoundException,
)
from chalicelib.new.workspace.infra.workspace_repository_sa import WorkspaceRepositorySA
from dependencies import get_db_session_rw

router = APIRouter(tags=["Pasto Worker"])


@router.post("/")
def create_worker(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
):
    workspace_identifier = body["workspace_identifier"]
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
