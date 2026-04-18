from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers import scale_to_super_user
from chalicelib.controllers.efos import EFOSController
from chalicelib.schema.models.user import User

bp = SuperBlueprint(__name__)


@bp.route("/update", methods=["POST"], cors=common.cors_config, read_only=False)
def update(session: Session):
    context = scale_to_super_user()
    return EFOSController.update_from_sat(session=session, context=context)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, EFOSController, session=company_session)


@bp.route("/resume", methods=["POST"], cors=common.cors_config)
def resume(company_session: Session, user: User):
    return common.resume(bp, EFOSController, session=company_session, user=user)
