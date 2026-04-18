from sqlalchemy.orm import Session


def cursor_with_schema(session: Session):
    """Get a cursor with the schema translate map applied."""
    execution_options = session.connection().get_execution_options()
    schema_translate_map = execution_options.get("schema_translate_map", {})
    cursor = session.connection().connection.cursor()
    if schema_translate_map:
        cursor.execute(
            f"SET search_path TO {', '.join(f'"{v}"' for v in schema_translate_map.values())}"
        )
    return cursor
