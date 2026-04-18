import uuid
from datetime import date

from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.pasto.common import bp_to_pasto_data
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.bus import get_global_bus
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message import SQSCompany
from chalicelib.schema.models.tenant.add_sync_request import ADDSyncRequest

bp = SuperBlueprint(__name__)


@bp.route(envars.ADD_METADATA_WEBHOOK, methods=["POST"], cors=common.cors_config, read_only=False)
def metadata_webhook(session: Session):
    error, body, headers = bp_to_pasto_data(bp, "metadata_webhook")
    if error:
        request = ADDSyncRequest(
            identifier=str(uuid.uuid4()),
            company_identifier=headers["company_identifier"],
            start=date.today().replace(day=1),
            end=date.today(),
            manually_triggered=False,
            state=ADDSyncRequest.StateEnum.ERROR,
        )
        session.add(request)
        return {"status": "ok"}

    company_identifier = headers["company_identifier"]

    bus = get_global_bus()
    bus.publish(
        EventType.ADD_METADATA_DOWNLOADED,
        SQSCompany(
            company_identifier=company_identifier,
        ),
    )

    return {"status": "ok"}
