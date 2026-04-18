"""Pasto Cancel webhook — cancellation notifications from ADD.

Ported from: backend/chalicelib/blueprints/pasto/cancel.py
1 route total.
"""

import uuid
from logging import DEBUG

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from chalicelib.controllers.tenant.session import (
    new_company_session_from_company_identifier,
)
from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.metadata_updater import MetadataUpdater
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import ADDSyncRequest
from dependencies import get_db_session_rw
from helpers.pasto_common import parse_pasto_webhook

router = APIRouter(tags=["Pasto Cancel"])


@router.post("/" + envars.ADD_CANCEL_WEBHOOK)
def cancel_webhook(
    body: dict = Body(...),
    request: Request = None,
    session: Session = Depends(get_db_session_rw),
):
    headers = dict(request.headers)
    error, pasto_body, hdrs = parse_pasto_webhook(body, headers, "cancel_webhook")
    request_identifier = hdrs["request_identifier"]
    company_identifier = Identifier(uuid.UUID(hdrs["company_identifier"]))
    log(
        Modules.ADD,
        DEBUG,
        "WEBHOOK_PASTO_CANCEL",
        {
            "error": error,
            "body": pasto_body,
            "headers": hdrs,
        },
    )
    with new_company_session_from_company_identifier(
        company_identifier=company_identifier,
        session=session,
        read_only=False,
    ) as company_session:
        add_request: ADDSyncRequest = company_session.query(ADDSyncRequest).get(request_identifier)
        if error or not pasto_body:
            add_request.state = ADDSyncRequest.StateEnum.ERROR
            return {"status": "ok"}
        add_request.cfdis_to_cancel_pending = pasto_body["ErrorRows"]
        if pasto_body["ErrorRows"]:
            add_request.state = ADDSyncRequest.StateEnum.ERROR
        uuids = {report["Uuid"] for report in pasto_body["Reports"] if report["Success"]}
        MetadataUpdater(session=None, bucket=None, bus=None).update_cancel_date_optimistic(
            uuids, company_session=company_session
        )
        return {"status": "ok"}
