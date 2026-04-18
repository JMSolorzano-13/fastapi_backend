"""Notification routes — notification config search and update.

Ported from: backend/chalicelib/blueprints/notification.py
2 routes total.
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.notification_config import NotificationConfigController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)

router = APIRouter(tags=["Notification"])


@router.post("/config/search")
def search(
    json_body: dict = Depends(get_json_body),
    session: Session = Depends(get_db_session),
):
    return common.search(json_body, NotificationConfigController, session=session)


@router.put("/config")
def set_config(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    workspace_id = body["workspace_id"]
    notification_configs: dict[str, list[str]] = body["notifications"]

    context = {"user": user}
    workspace = WorkspaceController.get(workspace_id, context=context, session=session)

    notification_configs = NotificationConfigController.set_notification_types(
        notification_configs, workspace, context=context, session=session
    )
    return NotificationConfigController.to_nested_dict(notification_configs)
