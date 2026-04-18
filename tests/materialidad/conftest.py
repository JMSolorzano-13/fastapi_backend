import random
import uuid
from collections.abc import Callable

import boto3
import pytest
from moto import mock_aws
from pydantic import HttpUrl
from sqlalchemy.orm import Session

from chalicelib.blueprints.attachment import (
    MAX_ATTACHMENT_SIZE,
    CreateRequest,
    CreateRequestAttachment,
    _create_many,
    _get_download_urls,
)
from chalicelib.new.config.infra import envars
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User


@pytest.fixture
def attachments_data_factory() -> Callable[[int], CreateRequest]:
    def factory(
        count: int,
    ) -> CreateRequest:
        return CreateRequest(
            items=[
                CreateRequestAttachment(
                    file_name=f"{uuid.uuid4()}.pdf",
                    size=random.randint(1, MAX_ATTACHMENT_SIZE // 3),
                    content_hash=str(uuid.uuid4()),
                )
                for _ in range(count)
            ]
        )

    return factory


@pytest.fixture
def attachments(
    company_session: Session,
    user: User,
    cfdi: CFDI,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
) -> dict[str, HttpUrl]:
    TO_CREATE = 3
    attachments_data = attachments_data_factory(TO_CREATE)
    _create_many(
        company_identifier=company.identifier,
        company_session=company_session,
        user=user,
        cfdi_uuid=cfdi.UUID,
        attachments_data=attachments_data,
    )
    company_session.commit()
    url_downloads = _get_download_urls(company_session=company_session, cfdi_uuid=cfdi.UUID)
    return url_downloads


@pytest.fixture(autouse=True)
def s3_filesattach_fixture():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=envars.S3_FILESATTACH)
        yield client
