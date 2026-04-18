"""COI routes — ADD sync request management.

Ported from: backend/chalicelib/blueprints/coi.py
4 routes total.
"""

from datetime import datetime

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.bus import get_global_bus
from chalicelib.controllers.add_sync_request import ADDSyncRequestController
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars.control import (
    COI_DATA_SUFFIX,
    COI_METADATA_SUFFIX,
    COI_PREFIX,
)
from chalicelib.new.pasto.event.add_syn_request_created import COIMetadataUploaded
from chalicelib.new.query.domain.query_creator import last_X_fiscal_years
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.utils.datetime import mx_now
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.add_sync_request import ADDSyncRequest
from dependencies import (
    common,
    get_company_rw,
    get_company_session,
    get_company_session_rw,
    get_db_session_rw,
    get_json_body,
)
from exceptions import ChaliceViewError, NotFoundError

router = APIRouter(tags=["COI"])

COI_FLAG = "coi_enabled"


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, ADDSyncRequestController, session=company_session)


@router.get("/{company_identifier}/{identifier}")
def get(
    company_identifier: str,
    identifier: str,
    company_session: Session = Depends(get_company_session),
):
    request: ADDSyncRequest = company_session.query(ADDSyncRequest).get(identifier)
    if not request:
        raise NotFoundError("ADDSyncRequest not found")
    res = {
        "identifier": request.identifier,
        "start": request.start.isoformat(),
        "end": request.end.isoformat(),
        "state": request.state.name,
    }
    if request.state == ADDSyncRequest.StateEnum.SENT:
        xml_url = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": envars.S3_ADD,
                "Key": _get_coi_path(company_identifier, request.identifier, COI_DATA_SUFFIX),
            },
            ExpiresIn=int(envars.ADD_S3_EXPIRATION_DELTA.total_seconds()),
        )
        cancel_url = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": envars.S3_ADD,
                "Key": _get_coi_path(
                    company_identifier, request.identifier, envars.control.COI_CANCEL_SUFFIX
                ),
            },
            ExpiresIn=int(envars.ADD_S3_EXPIRATION_DELTA.total_seconds()),
        )
        result_url = s3_client().generate_presigned_url(
            "put_object",
            Params={
                "Bucket": envars.S3_ADD,
                "Key": _get_coi_path(
                    company_identifier, request.identifier, envars.control.COI_METADATA_SUFFIX
                ),
            },
            ExpiresIn=int(envars.ADD_S3_EXPIRATION_DELTA.total_seconds()),
        )
        res |= {
            "xml_url": xml_url,
            "to_cancel_url": cancel_url,
            "result_url": result_url,
        }
    return res


@router.post("/{company_identifier}/{identifier}/notify")
def notify_metadata_uploaded(
    company_identifier: str,
    identifier: str,
    body: dict = Body(default={}),
    company_session: Session = Depends(get_company_session_rw),
):
    is_result = body.get("is_result", False)

    event = COIMetadataUploaded(
        company_identifier=company_identifier,
        request_identifier=identifier,
    )
    request: ADDSyncRequest = company_session.query(ADDSyncRequest).get(identifier)
    if not request:
        raise NotFoundError("ADDSyncRequest not found")
    request.xmls_to_send_pending = 0
    request.cfdis_to_cancel_pending = 0
    if is_result:
        event.launch_sync = False

    get_global_bus().publish(
        EventType.COI_METADATA_UPLOADED,
        event,
    )
    return {
        "identifier": request.identifier,
        "is_result": is_result,
    }


@router.post("/{company_identifier}")
def new_sync(
    company_identifier: str,
    body: dict = Body(default={}),
    session: Session = Depends(get_db_session_rw),
    company_session: Session = Depends(get_company_session_rw),
    company: Company = Depends(get_company_rw),
):
    company.data[COI_FLAG] = True
    return _new_sync(body, company_session, company)


def _new_sync(json_body: dict, company_session: Session, company: Company) -> dict:
    start_json = json_body.get("start")
    end_json = json_body.get("end")

    start: datetime = (
        datetime.fromisoformat(start_json) if start_json else last_X_fiscal_years(years=5)
    )
    end: datetime = datetime.fromisoformat(end_json) if end_json else mx_now()

    request = ADDSyncRequest(
        start=start,
        end=end,
    )
    company_session.add(request)
    company_session.flush()

    url_upload_metadata: str | None = s3_client().generate_presigned_url(
        "put_object",
        Params={
            "Bucket": envars.S3_ADD,
            "Key": _get_coi_path(company.identifier, request.identifier, COI_METADATA_SUFFIX),
        },
        ExpiresIn=int(envars.ADD_S3_EXPIRATION_DELTA.total_seconds()),
    )
    if not url_upload_metadata:
        raise ChaliceViewError("Could not generate presigned URL")

    return {
        "identifier": request.identifier,
        "start": request.start.isoformat(),
        "end": request.end.isoformat(),
        "state": request.state.name,
    } | {"url_upload_metadata": url_upload_metadata}


def _get_coi_path(
    company_identifier: str,
    request_identifier: str,
    resource: str,
) -> str:
    return f"{COI_PREFIX}/{company_identifier}/{request_identifier}/{resource}"
