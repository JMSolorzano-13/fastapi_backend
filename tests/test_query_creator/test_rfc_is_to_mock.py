from sqlalchemy.orm import Session

from chalicelib.new.query.domain.query_creator import is_cid_to_mock
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.user import User


def test_not_rfc_is_to_mock(session: Session, company: Company, user: User):
    assert not is_cid_to_mock(company.identifier, session)


def test_rfc_is_to_mock(session: Session, company: Company, user: User):
    company.rfc = "PGD1009214W0"
    user.email = "a@mozmail.com"
    session.add_all([company, user])
    session.flush()
    assert is_cid_to_mock(company.identifier, session)
