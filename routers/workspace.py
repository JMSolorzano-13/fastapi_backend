"""Workspace routes — CRUD and license management.

Ported from: backend/chalicelib/blueprints/workspace.py
6 routes total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace
from dependencies import (
    common,
    get_admin_create_user,
    get_admin_create_user_rw,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["Workspace"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    return common.search(json_body, WorkspaceController, session=session)


@router.post("/")
def create(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.create(json_body, WorkspaceController, session=session, user=user)


@router.put("/")
def update(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.update(body, WorkspaceController, session=session, user=user)


@router.delete("/")
def delete(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.delete(body, WorkspaceController, session=session, user=user)


@router.get("/{workspace_identifier}/license/{key}")
def get_license(
    workspace_identifier: str,
    key: str,
    session: Session = Depends(get_db_session),
    admin_create_user: User = Depends(get_admin_create_user),
):
    workspace: Workspace = (
        session.query(Workspace).filter(Workspace.identifier == workspace_identifier).one()
    )
    return workspace.license.get(key)


@router.put("/{workspace_identifier}/license/{key}")
def set_license(
    workspace_identifier: str,
    key: str,
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    admin_create_user: User = Depends(get_admin_create_user_rw),
):
    value = body["value"]
    workspace: Workspace = (
        session.query(Workspace).filter(Workspace.identifier == workspace_identifier).one()
    )
    workspace.license[key] = value
    return {"key": key, "value": value}
