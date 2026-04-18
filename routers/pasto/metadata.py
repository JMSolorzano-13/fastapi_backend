"""Pasto Metadata webhook — metadata download notifications from ADD.

Ported from: backend/chalicelib/blueprints/pasto/metadata.py
1 route total.
"""

import uuid
from datetime import date

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.infra.message import SQSCompany
from chalicelib.schema.models.tenant.add_sync_request import ADDSyncRequest
from dependencies import get_db_session_rw
from helpers.pasto_common import parse_pasto_webhook

router = APIRouter(tags=["Pasto Metadata"])


@router.post("/" + envars.ADD_METADATA_WEBHOOK)
def metadata_webhook(
    body: dict = Body(...),
    request: Request = None,
    session: Session = Depends(get_db_session_rw),
):
    headers = dict(request.headers)
    error, pasto_body, hdrs = parse_pasto_webhook(body, headers, "metadata_webhook")
    if error:
        request_obj = ADDSyncRequest(
            identifier=str(uuid.uuid4()),
            company_identifier=hdrs["company_identifier"],
            start=date.today().replace(day=1),
            end=date.today(),
            manually_triggered=False,
            state=ADDSyncRequest.StateEnum.ERROR,
        )
        session.add(request_obj)
        return {"status": "ok"}

    company_identifier = hdrs["company_identifier"]

    bus = get_global_bus()
    bus.publish(
        EventType.ADD_METADATA_DOWNLOADED,
        SQSCompany(
            company_identifier=company_identifier,
        ),
    )

    return {"status": "ok"}
