import os
import urllib.parse
from datetime import datetime, timedelta

from botocore.exceptions import ClientError
from chalice import BadRequestError, NotFoundError
from pydantic import BaseModel, ByteSize, HttpUrl, field_validator
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.boto3_clients import s3_client
from chalicelib.controllers.attachment import AttachmentController
from chalicelib.logger import WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User

bp = SuperBlueprint(__name__)

MAX_ATTACHMENT_SIZE = ByteSize._validate("10MB", None)  # type: ignore
UPLOAD_URL_EXPIRATION = timedelta(minutes=15)
DOWNLOAD_URL_EXPIRATION = timedelta(minutes=15)


def validate_file_name(file_name: str) -> str:
    """
    Validates that a file name is safe to use.
    Prevents path traversal attacks by ensuring the file name:
    - Is not empty
    - Does not contain null bytes
    - Does not contain path separators (/ or \\)
    - Is not a relative path component (. or ..)
    - Matches its basename (no path traversal)

    Args:
        file_name: The file name to validate

    Returns:
        The validated file name

    Raises:
        BadRequestError: If the file name is invalid or potentially dangerous
    """
    if not file_name:
        raise BadRequestError("File name cannot be empty")

    if "\x00" in file_name:
        raise BadRequestError("File name cannot contain null bytes")

    if "/" in file_name or "\\" in file_name:
        raise BadRequestError("File name cannot contain path separators")

    if file_name in (".", ".."):
        raise BadRequestError("File name cannot be '.' or '..'")

    # Ensure basename matches original (additional protection against path traversal)
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


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, AttachmentController, session=company_session)


@bp.route(
    "/{company_identifier}/{uuid}", methods=["POST"], cors=common.cors_config, read_only=False
)
def create_many(
    company_session: Session,
    user: User,
    uuid: str,
    company_identifier: str,
    session: Session,  # No se utiliza explícitamente, pero se requiere para usa `user`
) -> dict[str, HttpUrl]:
    create_request = CreateRequest.model_validate(bp.current_request.json_body)
    attachment_urls = _create_many(
        company_identifier=company_identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=uuid,
        attachments_data=create_request,
    )
    return attachment_urls


@bp.route("/{company_identifier}/{uuid}", methods=["GET"], cors=common.cors_config)
def get_download_urls(
    company_session: Session,
    uuid: str,
    company_identifier: str,  # No se utiliza explícitamente, pero se requiere para la ruta
) -> dict[str, HttpUrl]:
    return _get_download_urls(
        company_session=company_session,
        cfdi_uuid=uuid,
    )


@bp.route(
    "/{company_identifier}/{uuid}/{file_name}",
    methods=["DELETE"],
    cors=common.cors_config,
    read_only=False,
)
def delete_attachment(
    company_session: Session,
    uuid: str,
    file_name: str,
    user: User,
    company_identifier: str,  # No se utiliza explícitamente, pero se requiere para la ruta
    session: Session,  # No se utiliza explícitamente, pero se requiere para usa `user`
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
    # Validate file_name to prevent path traversal attacks
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
        )  # type: ignore
        .one_or_none()
    )
    if not attachment:
        raise NotFoundError(
            f"Attachment with file_name {file_name} does not exist for CFDI {cfdi_uuid}"
        )

    try:
        # Delete the attachment from S3
        s3_client().delete_object(
            Bucket=envars.S3_FILESATTACH,
            Key=attachment.s3_key,
        )
    except ClientError as e:
        # Only catch 404/NoSuchKey errors (non-existing documents)
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
            # Re-raise if it's not a "not found" error
            raise

    # Delete the attachment from the database
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
    # Si hay duplicados en la request, se lanza error
    if len({att.file_name for att in attachments_data.items}) != len(attachments_data.items):
        raise BadRequestError("Duplicate file_name found in attachments data")

    cfdi = company_session.query(CFDI).filter_by(UUID=cfdi_uuid).one_or_none()
    if not cfdi:
        raise NotFoundError(f"CFDI with UUID {cfdi_uuid} does not exist")

    # Si ya hay al menos un attachment con el mismo file_name para el cfdi_uuid, se lanza error
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
            s3_key=get_s3_key(company_identifier, cfdi_uuid, att_data.file_name),
        )
        attachments.append(attachment)
    company_session.add_all(attachments)
    company_session.refresh(cfdi)
    return {
        attachment.file_name: get_upload_s3_url_from_attachment(attachment)
        for attachment in attachments
    }


def _get_download_urls(
    company_session: Session,
    cfdi_uuid: str,
) -> dict[str, HttpUrl]:
    cfdi = company_session.query(CFDI).filter_by(UUID=cfdi_uuid).one_or_none()
    if not cfdi:
        raise NotFoundError(f"CFDI with UUID {cfdi_uuid} does not exist")

    return {
        attachment.file_name: get_download_s3_url_from_attachment(attachment)
        for attachment in cfdi.attachments
    }


def get_s3_key(company_identifier: str, cfdi_uuid: str, file_name: str) -> str:
    return f"{company_identifier}/{cfdi_uuid}/{file_name}"


def get_upload_s3_url_from_attachment(attachment: Attachment) -> HttpUrl:
    s3_upload_url: HttpUrl = s3_client().generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": envars.S3_FILESATTACH,
            "Key": attachment.s3_key,
        },
        ExpiresIn=int(UPLOAD_URL_EXPIRATION.total_seconds()),
    )  # pyright: ignore[reportAssignmentType]
    return s3_upload_url


def get_download_s3_url_from_attachment(attachment: Attachment) -> HttpUrl:
    s3_download_url: HttpUrl = s3_client().generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": envars.S3_FILESATTACH,
            "Key": attachment.s3_key,
        },
        ExpiresIn=int(DOWNLOAD_URL_EXPIRATION.total_seconds()),
    )  # pyright: ignore[reportAssignmentType]
    return s3_download_url
