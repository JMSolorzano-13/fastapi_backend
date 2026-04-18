import chalice
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.dashboard import Dashboard
from chalicelib.new.query.domain.reset_license import ResetLicense
from chalicelib.new.workspace.infra.reset_license_repository_sa import (
    LicenseRepositorySA,
)

bp = SuperBlueprint(__name__)


@bp.route("/", methods=["POST"], cors=common.cors_config, read_only=False)
def reset_license(session: Session):
    body = bp.current_request.json_body
    license_key = body["license_key"]
    dashboard = Dashboard(
        url=envars.PASTO_URL,
        ocp_key=envars.PASTO_OCP_KEY,
    )
    dashboard.login(email=envars.PASTO_EMAIL, password=envars.PASTO_PASSWORD)
    log(
        Modules.ADD,
        DEBUG,
        "RESET_LICENSE",
        {
            "license_key": license_key,
            "dashboard_token": dashboard.token,
            "dashboard_ocp_key": dashboard.ocp_key,
        },
    )
    reset_repo = LicenseRepositorySA(session)
    reset = ResetLicense(
        url=envars.PASTO_RESET_LICENSE_URL,
        authorization=dashboard.token,
        ocp_key=envars.PASTO_OCP_KEY,
        reset_repo=reset_repo,
        bus=get_global_bus(),
    )
    response_status = reset._reset_license(license_key=license_key)
    if response_status.error_code == 200:
        return {"status": "ok"}
    return chalice.Response(
        body={"status": "error", "message": "Ocurrio un error al resetear la licencia"},
        status_code=response_status.error_code,
    )
