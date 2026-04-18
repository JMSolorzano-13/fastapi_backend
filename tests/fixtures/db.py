from collections.abc import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from chalicelib.schema import connection_uri
from chalicelib.schema.models.company import Company


@pytest.fixture(scope="session")
def commit_session(request) -> bool:
    return request.config.getoption("--commit")


@pytest.fixture(scope="session")
def engine() -> Engine:
    _engine = create_engine(
        connection_uri,
        poolclass=NullPool,
    )
    yield _engine
    _engine.dispose()


@pytest.fixture()
def session(
    engine: Engine,
    commit_session: bool,
) -> Generator[Session, None, None]:
    connection = engine.connect()
    transaction = connection.begin()
    db = Session(bind=connection)
    yield db

    if commit_session:
        db.commit()
        db.close()
        transaction.commit()
    else:
        db.close()
        transaction.rollback()


@pytest.fixture(scope="function")
def company_session_other(company_other: Company, commit_session: bool):
    engine = create_engine(
        company_other.tenant_db_url,
        poolclass=NullPool,
    ).execution_options(schema_translate_map={"per_tenant": company_other.tenant_db_schema})
    connection = engine.connect()
    session = Session(bind=connection)

    yield session

    if commit_session:
        session.commit()
    else:
        session.rollback()


@pytest.fixture(scope="function")
def company_session(company: Company, commit_session: bool):
    engine = create_engine(
        company.tenant_db_url,
        poolclass=NullPool,
    )
    conn = engine.connect().execution_options(
        schema_translate_map={"per_tenant": company.tenant_db_schema}
    )

    session = Session(bind=conn)
    yield session

    if commit_session:
        session.commit()
    else:
        session.rollback()
    session.close()
    session.bind.close()
