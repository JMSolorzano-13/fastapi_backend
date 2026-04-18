import uuid
from collections.abc import Callable

import pytest
from chalice import NotFoundError
from pydantic import HttpUrl
from sqlalchemy.orm import Session

from chalicelib.blueprints.attachment import (
    CreateRequest,
    _create_many,
    _delete_attachment,
    _get_download_urls,
)
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User


def test_delete(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments: dict[str, HttpUrl],
):
    to_delete = attachments.keys().__iter__().__next__()
    delete_response = _delete_attachment(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
        file_name=to_delete,
        user=user,
    )
    assert delete_response == {
        "message": f"Attachment {to_delete} deleted successfully from CFDI {cfdi.UUID}"
    }

    url_downloads_after_delete = _get_download_urls(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
    )
    assert len(url_downloads_after_delete) == len(attachments) - 1
    assert to_delete not in url_downloads_after_delete

    deleted_db = (
        company_session.query(Attachment)
        .filter_by(cfdi_uuid=cfdi.UUID, state=Attachment.StateEnum.DELETED)
        .all()
    )
    assert len(deleted_db) == 1
    assert deleted_db[0].file_name == to_delete
    assert deleted_db[0].deleter_identifier == user.identifier
    assert deleted_db[0].deleted_at is not None


def test_raise_if_attachment_does_not_exist(
    company_session: Session,
    user: User,
    cfdi: CFDI,
):
    with pytest.raises(
        NotFoundError,
        match=f"Attachment with file_name .* does not exist for CFDI {cfdi.UUID}",
    ):
        _delete_attachment(
            company_session=company_session,
            cfdi_uuid=cfdi.UUID,
            file_name="non_existent_file.pdf",
            user=user,
        )


def test_raise_if_uuid_does_not_exist(
    company_session: Session,
    user: User,
):
    with pytest.raises(
        NotFoundError,
        match="CFDI with UUID .* does not exist",
    ):
        _delete_attachment(
            company_session=company_session,
            cfdi_uuid=str(uuid.uuid4()),
            file_name="non_existent_file.pdf",
            user=user,
        )


def test_delete_already_deleted(
    company_session: Session, user: User, cfdi: CFDI, attachments: dict[str, HttpUrl]
):
    to_delete = attachments.keys().__iter__().__next__()
    # First deletion
    _delete_attachment(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
        file_name=to_delete,
        user=user,
    )

    # Second deletion attempt
    with pytest.raises(
        NotFoundError,
        match=f"Attachment with file_name {to_delete} does not exist for CFDI {cfdi.UUID}",
    ):
        _delete_attachment(
            company_session=company_session,
            cfdi_uuid=cfdi.UUID,
            file_name=to_delete,
            user=user,
        )


def test_deleted_not_counted_in_size(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments: dict[str, HttpUrl],
):
    """Archivos eliminados no cuentan para límite de tamaño."""
    to_delete = attachments.keys().__iter__().__next__()
    prev_size = cfdi.attachments_size
    # Delete one attachment
    _delete_attachment(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
        file_name=to_delete,
        user=user,
    )

    # Get total size of remaining attachments
    new_size = cfdi.attachments_size

    # Ensure total size is less than MAX_ATTACHMENT_SIZE
    assert new_size < prev_size


def test_deleted_allow_duplicate_filename(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    attachments: dict[str, HttpUrl],
    attachments_data_factory: Callable[[int], CreateRequest],
):
    """Archivos eliminados permiten reusar nombre."""
    to_delete = attachments.keys().__iter__().__next__()
    # Delete one attachment
    _delete_attachment(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
        file_name=to_delete,
        user=user,
    )

    # Re-create attachment with same file_name
    new_attachment_data = attachments_data_factory(1)
    new_attachment_data.items[0].file_name = to_delete  # Reuse deleted file
    urls = _create_many(
        company_identifier="test_company",
        company_session=company_session,
        user=user,
        cfdi_uuid=cfdi.UUID,
        attachments_data=new_attachment_data,
    )

    assert to_delete in urls
