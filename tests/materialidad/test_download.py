import uuid
from collections.abc import Callable

import pytest
from chalice import NotFoundError
from pydantic import HttpUrl
from sqlalchemy.orm import Session

from chalicelib.blueprints.attachment import (
    CreateRequest,
    _create_many,
    _get_download_urls,
)
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User


def test_download(
    company_session: Session,
    cfdi: CFDI,
    attachments: dict[str, HttpUrl],
):
    attachment_urls = _get_download_urls(
        company_session=company_session,
        cfdi_uuid=cfdi.UUID,
    )

    assert len(attachment_urls) == len(attachments)
    assert all(str(s3_url).startswith("https://") for s3_url in attachment_urls.values())
    assert set(attachment_urls.keys()) == {
        attachment_file_name for attachment_file_name in attachments
    }


def test_raise_if_not_uuid_exists(company_session: Session):
    with pytest.raises(NotFoundError, match="CFDI with UUID .* does not exist"):
        _get_download_urls(
            company_session=company_session,
            cfdi_uuid=str(uuid.uuid4()),
        )


# Tests de aislamiento y tracking
def test_multiple_cfdis_isolated(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    """Attachments de diferentes CFDIs están aislados."""
    cfdi2 = CFDI.demo()
    company_session.add(cfdi2)
    company_session.flush()

    attachment_cfdi_1 = _create_many(
        company_identifier=company.identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=cfdi.UUID,
        attachments_data=attachments_data_factory(1),
    )
    attachment_cfdi_2 = _create_many(
        company_identifier=company.identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=cfdi2.UUID,
        attachments_data=attachments_data_factory(1),
    )

    assert attachment_cfdi_1 != attachment_cfdi_2
    assert company_session.query(Attachment).filter_by(cfdi_uuid=cfdi.UUID).count() == 1
    assert company_session.query(Attachment).filter_by(cfdi_uuid=cfdi2.UUID).count() == 1
