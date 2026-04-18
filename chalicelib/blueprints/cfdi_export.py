from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.cfdi_export import CfdiExportController

bp = SuperBlueprint(__name__)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, CfdiExportController, session=company_session)
