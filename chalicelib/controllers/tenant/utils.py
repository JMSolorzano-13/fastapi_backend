from pydantic import PostgresDsn
from sqlalchemy.orm import Session

from chalicelib.new.config.infra import envars
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
    # TODO restructuracion
    return str(
        PostgresDsn.build(
            scheme="postgresql",
            username=envars.DB_USER,
            password=envars.DB_PASSWORD,
            host=envars.DB_HOST,
            port=int(envars.DB_PORT) or 5432,
            path=f"{envars.DB_NAME}.{company_identifier}",
        )
    )
    company = company_from_identifier(company_identifier, session)
    return company.tenant_db_url_with_schema
