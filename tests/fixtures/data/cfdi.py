import pytest
from sqlalchemy.orm import Session

from chalicelib.schema.models.tenant.cfdi import CFDI


@pytest.fixture
def cfdi(company_session: Session) -> CFDI:
    _cfdi = CFDI.demo()
    company_session.add(_cfdi)
    company_session.commit()
    return _cfdi
