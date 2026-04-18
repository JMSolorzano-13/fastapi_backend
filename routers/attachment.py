"""Attachment routes — file attachment management for CFDIs.

Ported from: backend/chalicelib/blueprints/attachment.py
4 routes total.
"""

import os
import urllib.parse
from datetime import datetime, timedelta

from botocore.exceptions import ClientError
from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ByteSize, HttpUrl, field_validator
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.controllers.attachment import AttachmentController
from chalicelib.logger import WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_company_session,
    get_company_session_rw,
    get_current_user_rw,
    get_db_session_rw,
    get_json_body,
)
from exceptions import BadRequestError, NotFoundError

router = APIRouter(tags=["Attachment"])

MAX_ATTACHMENT_SIZE = ByteSize._validate("10MB", None)  # type: ignore
UPLOAD_URL_EXPIRATION = timedelta(minutes=15)
DOWNLOAD_URL_EXPIRATION = timedelta(minutes=15)


def validate_file_name(file_name: str) -> str:
    if not file_name:
        raise BadRequestError("File name cannot be empty")

    if "\x00" in file_name:
        raise BadRequestError("File name cannot contain null bytes")

    if "/" in file_name or "\\" in file_name:
        raise BadRequestError("File name cannot contain path separators")

    if file_name in (".", ".."):
        raise BadRequestError("File name cannot be '.' or '..'")

    if os.path.basename(file_name) != file_name:
        raise BadRequestError("File name contains invalid characters")

    return file_name


class CreateRequestAttachment(BaseModel):
    file_name: str
    size: int
    content_hash: str

    @field_validator("file_name")
    @classmethod
    def validate_file_name_field(cls, v: str) -> str:
        return validate_file_name(v)


class CreateRequest(BaseModel):
    items: list[CreateRequestAttachment]


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, AttachmentController, session=company_session)


@router.post("/{company_identifier}/{uuid}")
def create_many(
    company_identifier: str,
    uuid: str,
    body: CreateRequest = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    user: User = Depends(get_current_user_rw),
    session: Session = Depends(get_db_session_rw),
) -> dict[str, HttpUrl]:
    return _create_many(
        company_identifier=company_identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=uuid,
        attachments_data=body,
    )


@router.get("/{company_identifier}/{uuid}")
def get_download_urls(
    company_identifier: str,
    uuid: str,
    company_session: Session = Depends(get_company_session),
) -> dict[str, HttpUrl]:
    return _get_download_urls(
        company_session=company_session,
        cfdi_uuid=uuid,
    )


@router.delete("/{company_identifier}/{uuid}/{file_name:path}")
def delete_attachment(
    company_identifier: str,
    uuid: str,
    file_name: str,
    company_session: Session = Depends(get_company_session_rw),
    user: User = Depends(get_current_user_rw),
    session: Session = Depends(get_db_session_rw),
) -> dict[str, str]:
    file_name = urllib.parse.unquote(file_name)
    return _delete_attachment(
        company_session=company_session,
        cfdi_uuid=uuid,
        file_name=file_name,
        user=user,
    )


def _delete_attachment(
    company_session: Session,
    cfdi_uuid: str,
    file_name: str,
    user: User,
) -> dict[str, str]:
    file_name = validate_file_name(file_name)

    cfdi = company_session.query(CFDI).filter_by(UUID=cfdi_uuid).one_or_none()
    if not cfdi:
        raise NotFoundError(f"CFDI with UUID {cfdi_uuid} does not exist")

    attachment: Attachment = (
        company_session.query(Attachment)
        .filter(
            Attachment.cfdi_uuid == cfdi_uuid,
            Attachment.state != Attachment.StateEnum.DELETED,
            Attachment.file_name == file_name,
        )
        .one_or_none()
    )
    if not attachment:
        raise NotFoundError(
            f"Attachment with file_name {file_name} does not exist for CFDI {cfdi_uuid}"
        )

    try:
        s3_client().delete_object(
            Bucket=envars.S3_FILESATTACH,
            Key=attachment.s3_key,
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            log(
                Modules.ATTACHMENT,
                WARNING,
                "ATTACHMENT_S3_DELETE_FAILED_NOT_FOUND",
                {
                    "cfdi_uuid": cfdi_uuid,
                    "file_name": file_name,
                    "s3_key": attachment.s3_key,
                    "error": str(e),
                },
            )
        else:
            raise

    attachment.deleted_at = datetime.utcnow()
    attachment.deleter_identifier = user.identifier
    attachment.state = Attachment.StateEnum.DELETED
    company_session.refresh(cfdi)

    return {"message": f"Attachment {file_name} deleted successfully from CFDI {cfdi_uuid}"}


def _create_many(
    company_identifier: str,
    company_session: Session,
    user: User,
    cfdi_uuid: str,
    attachments_data: CreateRequest,
) -> dict[str, HttpUrl]:
    if len({att.file_name for att in attachments_data.items}) != len(attachments_data.items):
        raise BadRequestError("Duplicate file_name found in attachments data")

    cfdi = company_session.query(CFDI).filter_by(UUID=cfdi_uuid).one_or_none()
    if not cfdi:
        raise NotFoundError(f"CFDI with UUID {cfdi_uuid} does not exist")

    if dups := {a.file_name for a in cfdi.attachments} & {
        att.file_name for att in attachments_data.items
    }:
        raise BadRequestError(
            f"Attachments with file_name(s) {', '.join(dups)} already exist for CFDI {cfdi_uuid}"
        )

    new_attachments_size = sum(att.size for att in attachments_data.items)
    total_size = cfdi.attachments_size + new_attachments_size
    if total_size > MAX_ATTACHMENT_SIZE:
        total_size_human = ByteSize(total_size).human_readable()
        max_size_human = ByteSize(MAX_ATTACHMENT_SIZE).human_readable()
        raise BadRequestError(
            f"Total attachments size ({total_size_human}) exceeds the "
            f"maximum allowed limit ({max_size_human})"
        )

    attachments = []
    for att_data in attachments_data.items:
        attachment = Attachment(
            creator_identifier=user.identifier,
            cfdi_uuid=cfdi_uuid,
            file_name=att_data.file_name,
            size=att_data.size,
            content_hash=att_data.content_hash,
            s3_key=_get_s3_key(company_identifier, cfdi_uuid, att_data.file_name),
        )
        attachments.append(attachment)
    company_session.add_all(attachments)
    company_session.refresh(cfdi)
    return {attachment.file_name: _get_upload_s3_url(attachment) for attachment in attachments}


def _get_download_urls(
    company_session: Session,
    cfdi_uuid: str,
) -> dict[str, HttpUrl]:
    cfdi = company_session.query(CFDI).filter_by(UUID=cfdi_uuid).one_or_none()
    if not cfdi:
        raise NotFoundError(f"CFDI with UUID {cfdi_uuid} does not exist")

    return {
        attachment.file_name: _get_download_s3_url(attachment) for attachment in cfdi.attachments
    }


def _get_s3_key(company_identifier: str, cfdi_uuid: str, file_name: str) -> str:
    return f"{company_identifier}/{cfdi_uuid}/{file_name}"


def _get_upload_s3_url(attachment: Attachment) -> HttpUrl:
    return s3_client().generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": envars.S3_FILESATTACH,
            "Key": attachment.s3_key,
        },
        ExpiresIn=int(UPLOAD_URL_EXPIRATION.total_seconds()),
    )


def _get_download_s3_url(attachment: Attachment) -> HttpUrl:
    return s3_client().generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": envars.S3_FILESATTACH,
            "Key": attachment.s3_key,
        },
        ExpiresIn=int(DOWNLOAD_URL_EXPIRATION.total_seconds()),
    )
