"""
Shared utilities for Alembic migrations.
"""

import uuid
from functools import lru_cache

from alembic import context


# @lru_cache
def resolve_schema(schema_token: str) -> str:
    """
    Resolve schema token to actual schema using schema_translate_map.

    This function is used in migrations to dynamically resolve the schema
    based on the current tenant context. It reads from the schema_translate_map
    configured in the Alembic context.

    Args:
        schema_token: The placeholder schema name (e.g., "per_tenant")

    Returns:
        The actual schema name for the current tenant (e.g., UUID)

    Example:
        >>> schema = resolve_schema("per_tenant")
        >>> op.create_table("my_table", ..., schema=schema)
    """
    current_map = context.get_context().opts.get("schema_translate_map", {})
    return current_map.get(schema_token, schema_token)


@lru_cache
def schema_to_uuid(schema: str) -> str:
    try:
        return str(uuid.UUID(schema))
    except ValueError:
        return str(uuid.UUID("00000000-0000-0000-0000-000000000000"))
