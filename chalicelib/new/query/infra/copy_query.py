from io import BytesIO
from typing import IO, TextIO

from sqlalchemy.orm import Query, Session

from chalicelib.new.query.infra.cursor_utils import cursor_with_schema


def copy_query(
    session: Session,
    query: str | Query,
    file: TextIO | BytesIO | IO[bytes],
    headers: bool = True,
) -> None:
    """Copy the result of a query to a file.

    The query must be a SELECT statement.
    The file must be open in writable and in binary mode.
    After the function is called, the file pointer will be at the beginning of the file.
    Does not close the file.
    """
    if isinstance(query, Query):
        query = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
    query = query.replace("per_tenant.", "")
    copy_query = f"""
        COPY ({query}) TO STDOUT
        WITH (
            FORMAT CSV,
            HEADER {"TRUE" if headers else "FALSE"}
        )
    """

    cursor = cursor_with_schema(session)
    cursor.copy_expert(copy_query, file)
    file.flush()  # Ensure the data is written to the file
    file.seek(0)  # Move the file pointer to the beginning of the file
