import uuid
from logging import DEBUG

from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.pasto.common import bp_to_pasto_data
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.tenant.session import (
    new_company_session_from_company_identifier,
)
from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.metadata_updater import MetadataUpdater
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import ADDSyncRequest

bp = SuperBlueprint(__name__)


@bp.route(envars.ADD_XML_WEBHOOK, methods=["POST"], cors=common.cors_config, read_only=False)
def xml_webhook(session: Session):
    error, body, headers = bp_to_pasto_data(bp, "xml_webhook")
    request_identifier = headers["request_identifier"]
    company_identifier = Identifier(uuid.UUID(headers["company_identifier"]))
    log(
        Modules.ADD,
        DEBUG,
        "WEBHOOK_PASTO_XML",
        {
            "error": error,
            "body": body,
            "headers": headers,
        },
    )
    with new_company_session_from_company_identifier(
        company_identifier=company_identifier,
        session=session,
        read_only=False,
    ) as company_session:
        request: ADDSyncRequest = company_session.query(ADDSyncRequest).get(request_identifier)
        if not body:
            request.state = ADDSyncRequest.StateEnum.ERROR
            return {"status": "ok"}
        request.xmls_to_send_pending = body["ErrorRows"]
        if body["ErrorRows"]:
            request.state = ADDSyncRequest.StateEnum.ERROR
        uuids = {report["Uuid"] for report in body["Reports"] if report["Success"]}
        MetadataUpdater(session=None, bucket=None, bus=None).update_exists(
            new=uuids, company_session=company_session
        )
        return {"status": "ok"}
