import functools
from contextlib import contextmanager
from datetime import timedelta
from enum import Enum

from sqlalchemy import text
from sqlalchemy.exc import DatabaseError, OperationalError

from chalicelib.logger import ERROR, EXCEPTION, INFO, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.schema import get_session_maker


def with_session(read_only: bool = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            comment = func.__name__
            with new_session(comment=comment, read_only=read_only) as session:
                return func(*args, session=session, **kwargs)

        return wrapper

    return decorator


@contextmanager
def new_session(
    comment: str,
    read_only: bool = False,
    statement_timeout: timedelta = None,
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

    yield from _new_session(comment, read_only, statement_timeout)


def _new_session(comment: str, read_only: bool, statement_timeout: timedelta):
    info = {"comment": comment}
    log(
        Modules.DB,
        INFO,
        "NEW_SESSION",
        {
            "comment": comment,
            "read_only": read_only,
            "statement_timeout": statement_timeout.total_seconds(),
        },
    )
    session = get_session_maker(read_only)(info=info)
    session.execute(text(f"SET application_name = '{comment}'"))
    session.execute(f"SET SESSION statement_timeout = {statement_timeout.total_seconds() * 1000}")
    try:
        yield session
        if not read_only:
            session.commit()
    except OperationalError as e:
        from psycopg2 import errors

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
            session.rollback()
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
        session.rollback()
        raise
    finally:
        session.close()
