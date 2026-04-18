from sqlalchemy.orm import Session

from chalicelib.schema.models.tenant.attachment import Attachment
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User


def test_model_definition(company_session: Session, user: User):
    cfdi = CFDI.demo()
    attachment = Attachment(
        cfdi_uuid=cfdi.UUID,
        file_name="test.pdf",
        s3_key="s3://bucket/test.pdf",
        size=1234,
        content_hash="abc123",
        creator_identifier=user.identifier,
    )
    company_session.add(cfdi)
    company_session.add(attachment)
    company_session.flush()
