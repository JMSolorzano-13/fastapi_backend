import uuid
from collections.abc import Callable

import pytest
from chalice import BadRequestError, NotFoundError
from pydantic import HttpUrl
from sqlalchemy.orm import Session

from chalicelib.blueprints.attachment import (
    MAX_ATTACHMENT_SIZE,
    CreateRequest,
    _create_many,
)
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User


def test_create(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    TO_CREATE = 2
    attachments_data = attachments_data_factory(TO_CREATE)

    attachments_urls = _create_many(
        company_identifier=company.identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=cfdi.UUID,
        attachments_data=attachments_data,
    )

    assert len(attachments_urls) == TO_CREATE

    attachments_db = company_session.query(Attachment).filter_by(cfdi_uuid=cfdi.UUID).all()
    assert len(attachments_db) == TO_CREATE

    assert attachments_db[0].file_name in attachments_urls
    assert attachments_db[0].creator_identifier == user.identifier
    assert attachments_db[1].created_at is not None

    assert all(str(s3_url).startswith("https://") for s3_url in attachments_urls.values())
    assert set(attachments_urls.keys()) == {
        attachments_data_item.file_name for attachments_data_item in attachments_data.items
    }


def test_raise_if_uuid_does_not_exists(
    company_session: Session,
    user: User,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    attachments_data = attachments_data_factory(1)
    with pytest.raises(NotFoundError, match="CFDI with UUID .* does not exist"):
        _create_many(
            company_identifier=company.identifier,
            company_session=company_session,
            user=user,
            cfdi_uuid=str(uuid.uuid4()),
            attachments_data=attachments_data,
        )


def test_raise_if_file_duplicated(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    attachments_data = attachments_data_factory(1)

    attachments_data.items.append(attachments_data.items[0].model_copy())  # Duplicate file_name
    with pytest.raises(BadRequestError, match="Duplicate file_name found in attachments data"):
        _create_many(
            company_identifier=company.identifier,
            company_session=company_session,
            user=user,
            cfdi_uuid=cfdi.UUID,
            attachments_data=attachments_data,
        )


def test_raise_if_already_exists(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
    attachments: dict[str, HttpUrl],
):
    attachments_data = attachments_data_factory(1)
    attachments_data.items[0].file_name = list(attachments.keys())[0]  # Existing file_name

    with pytest.raises(
        BadRequestError, match="Attachments with file_name\\(s\\) .* already exist for CFDI .*"
    ):
        _create_many(
            company_identifier=company.identifier,
            company_session=company_session,
            user=user,
            cfdi_uuid=cfdi.UUID,
            attachments_data=attachments_data,
        )


@pytest.mark.parametrize(
    "sizes",
    [
        [MAX_ATTACHMENT_SIZE + 1],
        [MAX_ATTACHMENT_SIZE // 2, MAX_ATTACHMENT_SIZE // 2 + 1],
    ],
)
def test_raise_if_exceeds_size_limit_same_request(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
    sizes: list[int],
):
    attachments_data = attachments_data_factory(len(sizes))
    for i, size in enumerate(sizes):
        attachments_data.items[i].size = size

    # Un solo elemento que excede el limite
    with pytest.raises(
        BadRequestError, match="Total attachments size .* exceeds the maximum allowed limit .*"
    ):
        _create_many(
            company_identifier=company.identifier,
            company_session=company_session,
            user=user,
            cfdi_uuid=cfdi.UUID,
            attachments_data=attachments_data,
        )


def test_raise_if_exceeds_size_limit_existing_attachment(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
    attachments: dict[str, HttpUrl],
):
    new_attachment_data = attachments_data_factory(1)
    new_attachment_data.items[0].size = MAX_ATTACHMENT_SIZE - 1

    with pytest.raises(
        BadRequestError, match="Total attachments size .* exceeds the maximum allowed limit .*"
    ):
        _create_many(
            company_identifier=company.identifier,
            company_session=company_session,
            user=user,
            cfdi_uuid=cfdi.UUID,
            attachments_data=new_attachment_data,
        )
