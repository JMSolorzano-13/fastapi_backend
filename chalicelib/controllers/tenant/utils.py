from sqlalchemy.orm import Session

from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.company import Company


def company_from_identifier(
    company_identifier: Identifier,
    session: Session,
) -> Company:
    company = session.query(Company).filter(Company.identifier == company_identifier).one_or_none()
    if not company:
        raise ValueError(f"Company with identifier {company_identifier} not found")
    return company


def tenant_url_from_identifier(
    company_identifier: Identifier,
    session: Session,
) -> str:
    """Build SQLAlchemy tenant URL ``postgresql://.../dbname.<schema>`` from persisted company row.

    The ``.<schema>`` suffix is parsed by ``get_tenant_url_and_schema`` (rsplit on last dot).
    """
    company = company_from_identifier(company_identifier, session)
    return company.tenant_db_url_with_schema
