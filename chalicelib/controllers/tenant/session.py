import functools
import inspect
from collections.abc import Generator
from contextlib import contextmanager
from datetime import timedelta
from enum import Enum
from logging import DEBUG, ERROR

from psycopg2 import errors
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm import Session

from chalicelib.controllers.tenant.utils import tenant_url_from_identifier
from chalicelib.logger import EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message.sqs_company import SQSCompany
from chalicelib.schema import common_engine_connection_args

engine_by_tenant_db_url: dict[str, Engine] = {}


@contextmanager
def new_company_session_from_company_identifier(
    company_identifier: Identifier,
    session: Session,
    comment="",
    read_only: bool = True,
    statement_timeout: timedelta | None = None,
) -> Generator[Session, None, None]:
    """
    Create a new company session based on the company identifier.
    This is a context manager that yields a session for the specified company.
    """
    tenant_url = tenant_url_from_identifier(company_identifier, session)
    if not comment:
        comment = "unknown_py"
        for level in inspect.stack():
            if "chalicelib" in level.filename:
                comment = f"{level.filename}::{level.function}"
                break

    with new_company_session(
        tenant_url, comment=comment, read_only=read_only, statement_timeout=statement_timeout
    ) as company_session:
        yield company_session


@contextmanager
def new_company_session(
    tenant_url: str,
    comment: str,
    read_only: bool = True,
    statement_timeout: timedelta | None = None,
):
    if not isinstance(comment, str):
        if isinstance(comment, Enum):
            comment = comment.value
        else:
            log(
                Modules.DB,
                ERROR,
                "INVALID_COMMENT",
                {
                    "comment": comment,
                },
            )
            comment = ""

    def is_endpoint(comment: str) -> bool:
        return comment.startswith("end_")

    if statement_timeout is None:
        if is_endpoint(comment):
            statement_timeout = envars.sql.STATEMENT_TIMEOUT_ENDPOINT
        else:
            statement_timeout = envars.sql.STATEMENT_TIMEOUT

    return _new_company_session(tenant_url, comment, read_only, statement_timeout)


def _new_company_session(
    tenant_url: str,
    comment: str,
    read_only: bool,
    statement_timeout: timedelta,
):
    log(
        Modules.DB,
        DEBUG,
        "NEW_TENANT_SESSION",
        {
            "comment": comment,
            "read_only": read_only,
            "statement_timeout": statement_timeout.total_seconds(),
            "tenant_url": tenant_url,
        },
    )
    company_session = get_tenant_session(tenant_url, read_only)

    company_session.execute(text(f"SET application_name = '{comment}'"))

    company_session.execute(
        f"SET SESSION statement_timeout = {statement_timeout.total_seconds() * 1000}"
    )

    try:
        yield company_session
        if not read_only and not envars.LOCAL_INFRA:
            company_session.commit()
    except OperationalError as e:
        if isinstance(e.orig, errors.QueryCanceled):
            log(
                Modules.DB,
                EXCEPTION,
                "STATEMENT_TIMEOUT",
                {
                    "comment": comment,
                    "exception": e,
                },
            )
            company_session.rollback()
            raise
    except DatabaseError as e:
        log(
            Modules.DB,
            EXCEPTION,
            "DATABASE_ERROR",
            {
                "comment": comment,
                "exception": e,
            },
        )
        company_session.rollback()
        raise
    finally:
        company_session.close()
        company_session.bind.close()


def get_tenant_url_and_schema(
    tenant_url_with_schema: str,
) -> tuple[str, str]:
    tenant_url, schema_name = tenant_url_with_schema.rsplit(".", 1)
    return tenant_url, schema_name


def translate_tenant_url_to_readonly_if_needed(
    tenant_url: str,
    read_only: bool,
) -> str:
    """Translate the tenant URL to a read-only URL if needed.

    Solo funciona si el tenant_url tiene el formato <cluster>.<type>.<region>.rds.amazonaws.com
    """
    if not read_only:
        return tenant_url
    if ".rds.amazonaws.com" not in tenant_url:
        return tenant_url
    parts = tenant_url.split(".")
    cluster_type = parts[1]
    ro_prefix = "cluster-ro-"
    if not cluster_type.startswith(ro_prefix):
        no_ro_prefix = "cluster-"
        parts[1] = parts[1].replace(no_ro_prefix, ro_prefix, 1)
    return ".".join(parts)


def get_tenant_engine(
    tenant_db_url: str,
) -> Engine:
    if tenant_db_url not in engine_by_tenant_db_url:
        engine_by_tenant_db_url[tenant_db_url] = create_engine(
            tenant_db_url,
            **common_engine_connection_args,
        )
    return engine_by_tenant_db_url[tenant_db_url]


def get_tenant_session(
    tenant_url_with_schema: str,
    read_only: bool,
) -> Session:
    tenant_db_url, schema_name = get_tenant_url_and_schema(tenant_url_with_schema)

    tenant_db_url = translate_tenant_url_to_readonly_if_needed(tenant_db_url, read_only)

    tenant_engine = get_tenant_engine(tenant_db_url)
    conn = tenant_engine.connect().execution_options(
        schema_translate_map={"per_tenant": schema_name}
    )

    session = Session(bind=conn)

    return session


def with_company_session_from_message_reuse_connection(session, read_only: bool = True):
    def get_company_identifier_from_message(message: SQSCompany) -> str:
        return message.company_identifier

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract message from function parameters
            message = kwargs.get("message")
            if not message:
                raise ValueError(
                    "Message parameter is required for with_company_session_from_message decorator"
                )

            # Extract company_identifier from message
            company_identifier = get_company_identifier_from_message(message)
            if not company_identifier:
                raise ValueError("Message must have company_identifier attribute")

            # Get tenant URL and create company session
            tenant_url = tenant_url_from_identifier(company_identifier, session)
            comment = func.__name__

            company_session = get_tenant_session(tenant_url, read_only)
            company_session.execute(text(f"SET application_name = '{comment}'"))
            res = func(*args, company_session=company_session, **kwargs)
            if not read_only:
                company_session.commit()
            company_session.close()
            company_session.bind.close()
            return res

        return wrapper

    return decorator
