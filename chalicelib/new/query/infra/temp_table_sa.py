import csv
import uuid
from collections.abc import Callable, Iterable
from tempfile import NamedTemporaryFile
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.sql import ClauseElement

from chalicelib.new.config.infra import envars
from chalicelib.new.query.infra.cursor_utils import cursor_with_schema

TableName = str
FieldType = tuple[str, str]
Record = Any | dict[str, Any]
Transformations = dict[str, Callable[[Any, Any], Any]]


def temp_table_as(
    session: Session,
    name: TableName,
    as_query: str,
    randomize: bool = True,
) -> TableName:
    """Create a temporary table in the database as the result of a query.

    The table will be dropped on session close.

    Returns the name of the table.
    """
    if randomize:
        name = f"{name}_r_{uuid.uuid4().hex}"
    cursor = cursor_with_schema(session)
    cursor.execute(f'CREATE TEMPORARY TABLE "{name}" AS {as_query}')
    return name


def temp_table(
    session: Session,
    name: TableName,
    *,
    parent_table: TableName | None = None,
    field_types: Iterable[FieldType] | None = None,
    fields: Iterable[str] | None = None,
    records: Iterable[Record] | None = None,
    randomize: bool = True,
    transformations: Transformations | None = None,
) -> TableName:
    """Create a temporary table in the database.

    The table will be dropped on session close.

    If `parent_table` is specified, the table will be created as a copy of the parent table.
    Otherwise, `field_types` must be specified.

    If `records` is specified, the table will be populated with the records.
    Otherwise, the table will be empty.


    If `transformations` is specified, it must be a dictionary mapping field names to
    functions that transform the values of the field. The function will receive the record and the value

    Returns the name of the table.
    """  # noqa E501

    if parent_table:
        if not fields:
            raise ValueError("Must specify `fields` when using `parent_table`")
    elif not field_types:
        raise ValueError("Must specify `field_types` when not using `parent_table`")
    if fields and field_types:
        raise ValueError("Cannot specify both `fields` and `field_types`")

    if randomize:
        name = f"{name}_r_{uuid.uuid4().hex}"

    cursor = cursor_with_schema(session)

    if parent_table:
        cursor.execute(
            f'CREATE TEMPORARY TABLE "{name}" (LIKE "{parent_table}" INCLUDING DEFAULTS)'
            " ON COMMIT DROP"
        )
    else:
        fields_header = ",".join(f'"{field}" {type_}' for field, type_ in field_types)
        cursor.execute(f'CREATE TEMPORARY TABLE "{name}" ({fields_header}) ON COMMIT DROP')

    if not records:
        return name
    fields = fields or [field for field, _ in field_types]
    fields_names = ",".join(f'"{field}"' for field in fields)

    with NamedTemporaryFile("w", suffix=".csv", encoding="UTF-8") as temp_file:
        records_to_csv(records, fields, temp_file, transformations=transformations)
        with open(temp_file.name, encoding="UTF-8") as csv_file:
            cursor.copy_expert(
                f'COPY "{name}"({fields_names}) FROM STDIN WITH (FORMAT CSV)',
                csv_file,
            )
    return name


def default_cfdi_transformations(retriever):
    def is_too_big(xml_content):
        if not xml_content:
            return False
        return len(xml_content) > envars.MAX_FILE_SIZE_KB * 1024

    return {
        "xml_content": lambda record, value: (
            "" if is_too_big(retriever(record, "xml_content")) else value
        ),
        "is_too_big": lambda record, value: is_too_big(retriever(record, "xml_content")),
        "from_xml": lambda record, value: (
            False if is_too_big(retriever(record, "xml_content")) else value
        ),
    }


def records_to_csv(
    records: Iterable[Record],  # At least one record is required
    fields: Iterable[str],
    csv_file: Any,
    transformations: Transformations = None,
    with_header: bool = False,
) -> None:
    # TODO unify with `metadata_to_csv`
    is_dict = None

    def retriever(record, field):
        nonlocal is_dict
        if is_dict is None:
            is_dict = isinstance(record, dict)
        return record[field] if is_dict else getattr(record, field)

    if transformations is None:
        transformations = default_cfdi_transformations(retriever)
    transformations = transformations or {}

    writer = csv.writer(csv_file)

    if with_header:
        writer.writerow(fields)

    if not records:
        csv_file.flush()  # To ensure the file is written to disk
        return

    if transformations:
        rows = (
            (
                transformations.get(field, lambda _, x: x)(record, retriever(record, field))
                for field in fields
            )
            for record in records
        )
    else:
        rows = ((retriever(record, field) for field in fields) for record in records)

    writer.writerows(rows)

    csv_file.flush()  # To ensure the file is written to disk


def update_multiple(
    *,
    dest_table: str,
    key: str | tuple[str, ...],
    field_types: list[FieldType],
    records: Iterable[Record],
    session: Session,
    fields_to_update: list[str] | None = None,
    fields_to_update_same_table: dict[str, str] | None = None,
    fields_to_update_hardcoded: dict[str, Any] | None = None,
    where_clause: str | None = None,
    schema_name: str = "",
):
    """Update multiple records in a table.

    Create a temporary table with the records and update the destination table with a join.
    """
    fields_to_update = fields_to_update or []
    fields_to_update_same_table = fields_to_update_same_table or {}
    fields_to_update_hardcoded = fields_to_update_hardcoded or {}

    if not isinstance(where_clause, str) and where_clause is not None:
        where_clause = where_clause.compile(compile_kwargs={"literal_binds": True})

    temp_table_name = temp_table(
        session=session,
        name="upd",
        field_types=field_types,
        records=records,
        transformations=False,
    )

    set_clause_tmp = ", ".join(
        f'"{field}" = "{temp_table_name}"."{field}"' for field in fields_to_update
    )
    set_clause_same_table = ", ".join(
        f'"{field}" = "{source_field}"'
        for field, source_field in fields_to_update_same_table.items()
    )
    set_clause_hardcoded = ", ".join(
        f'"{field}" = {value}' for field, value in fields_to_update_hardcoded.items()
    )
    set_clause = ", ".join(
        filter(None, [set_clause_tmp, set_clause_same_table, set_clause_hardcoded])
    )

    if isinstance(key, str):
        key = (key,)
    where_tmp = " AND ".join(
        f'"{dest_table}"."{field}" = "{temp_table_name}"."{field}"' for field in key
    )
    where_clause = f"({where_tmp}) AND ({where_clause})" if where_clause else where_tmp

    query = f"""
        UPDATE "{dest_table}"
        SET {set_clause}
        FROM "{temp_table_name}"
        WHERE {where_clause}
        """
    if schema_name:
        if '"' not in schema_name:
            schema_name = f'"{schema_name}"'
        query = query.replace("per_tenant", schema_name)
    session.execute(query)


def select_multiple(
    *,
    source_table: str,
    key: str | tuple[str, str] | list[tuple[str, str]],
    field_types: list[FieldType],
    records: list[Record],
    session: Session,
    columns_to_select: list[str],
    where_clause: Any = None,
    schema_name: str = "",
) -> set[Any]:
    """
    Realiza un SELECT múltiple usando JOIN con tabla temporal

    Args:
        source_table: Nombre de la tabla principal
        key: Campo(s) para el JOIN
        field_types: Tipos de campos para la tabla temporal
        records: Registros para la tabla temporal
        columns_to_select: Columnas a seleccionar
        where_clause: Condición WHERE como string o expresión SQLAlchemy
    """
    if not records:
        return set()

    temp_table_name = temp_table(
        session=session, name="sel_temp", field_types=field_types, records=records, randomize=True
    )

    if isinstance(key, str):
        keys = [(key, key)]
    elif isinstance(key, tuple):
        keys = [key]
    else:
        keys = key

    join_conditions = " AND ".join(
        f'"{source_table}"."{src}" = "{temp_table_name}"."{temp}"' for src, temp in keys
    )

    where_str = ""
    if where_clause is not None:
        if isinstance(where_clause, ClauseElement):
            where_clause = where_clause.self_group()

        if hasattr(where_clause, "compile"):
            compiled = where_clause.compile(
                compile_kwargs={"literal_binds": True, "render_postcompile": True}
            )
            where_str = f"AND {str(compiled)}"
        else:
            where_str = f"AND {where_clause}"

    columns = ", ".join(f'"{source_table}"."{col}"' for col in (columns_to_select or [keys[0][0]]))

    query = f"""
        SELECT {columns}
        FROM "{source_table}"
        JOIN "{temp_table_name}"
        ON {join_conditions}
        {where_str}
    """
    if schema_name:
        if '"' not in schema_name:
            schema_name = f'"{schema_name}"'
        query = query.replace("per_tenant", schema_name)

    result = session.execute(query).fetchall()
    return {row[0] for row in result} if columns_to_select else set(result)
