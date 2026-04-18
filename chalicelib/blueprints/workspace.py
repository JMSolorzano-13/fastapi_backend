from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace

bp = SuperBlueprint(__name__)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(session: Session):
    return common.search(bp, WorkspaceController, session=session)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def create(session: Session, user: User):
    return common.create(bp, WorkspaceController, session=session, user=user)


@bp.route("/", methods=["PUT"], cors=common.cors_config, read_only=False)
def update(session: Session, user: User):
    return common.update(bp, WorkspaceController, session=session, user=user)


@bp.route("/", methods=["DELETE"], cors=common.cors_config, read_only=False)
def delete(session: Session, user: User):
    return common.delete(bp, WorkspaceController, session=session, user=user)


@bp.route("/{workspace_identifier}/license/{key}", methods=["GET"], cors=common.cors_config)
def get_license(workspace_identifier: str, key: str, admin_create_user: User, session: Session):
    workspace: Workspace = (
        session.query(Workspace).filter(Workspace.identifier == workspace_identifier).one()
    )
    return workspace.license.get(key)


@bp.route(
    "/{workspace_identifier}/license/{key}",
    methods=["PUT"],
    cors=common.cors_config,
    read_only=False,
)
def set_license(workspace_identifier: str, key: str, admin_create_user: User, session: Session):
    value = bp.current_request.json_body["value"]
    workspace: Workspace = (
        session.query(Workspace).filter(Workspace.identifier == workspace_identifier).one()
    )
    workspace.license[key] = value
    return {"key": key, "value": value}
