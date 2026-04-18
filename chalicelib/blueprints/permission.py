from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.permission import PermissionController
from chalicelib.controllers.user import UserController
from chalicelib.schema.models.user import User

bp = SuperBlueprint(__name__)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(session: Session):
    return common.search(bp, PermissionController, session=session)


@bp.route("/", methods=["PUT"], cors=common.cors_config, read_only=False)
def set_permissions(session: Session, user: User):
    json_body = bp.current_request.json_body or {}

    emails = json_body["emails"]
    permissions_by_company: dict[str, set[str]] = json_body["permissions"]
    context = {"user": user}
    result = UserController.set_permissions(
        emails, permissions_by_company, context=context, session=session
    )

    return {
        "state": "success",
        "users_processed": len(result),
        "companies_processed": len(permissions_by_company),
    }
