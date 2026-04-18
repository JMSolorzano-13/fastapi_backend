from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.notification_config import NotificationConfigController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.schema.models.user import User

bp = SuperBlueprint(__name__)


@bp.route("/config/search", methods=["POST"], cors=common.cors_config)
def search(session: Session):
    return common.search(bp, NotificationConfigController, session=session)


@bp.route("/config", methods=["PUT"], cors=common.cors_config, read_only=False)
def set_config(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    workspace_id = json_body["workspace_id"]
    notification_configs: dict[str, list[str]] = json_body["notifications"]

    context = {"user": user}
    workspace = WorkspaceController.get(workspace_id, context=context, session=session)

    notification_configs = NotificationConfigController.set_notification_types(
        notification_configs, workspace, context=context, session=session
    )
    return NotificationConfigController.to_nested_dict(notification_configs)
