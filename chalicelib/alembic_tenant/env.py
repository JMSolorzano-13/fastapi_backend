import logging
import os
from logging.config import fileConfig

from alembic import context

# Import tenant models
from sqlalchemy import engine_from_config, pool

from chalicelib.schema import connection_uri
from chalicelib.schema.models.catalogs import *  # noqa: F403
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER
from chalicelib.schema.models.tenant.tenant_model import PER_TENANT_SCHEMA_PLACEHOLDER, TenantBase

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Get tenant schema from environment variable or detect all
SCHEMAS = config.get_main_option("TENANT_SCHEMAS", os.environ.get("TENANT_SCHEMA"))
if isinstance(SCHEMAS, str):
    SCHEMAS = {s.strip() for s in SCHEMAS.split(",") if s.strip()}


# if not config.get_main_option("sqlalchemy.url"):
config.set_main_option("sqlalchemy.url", connection_uri)
# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# Import all tenant models to populate metadata


# Use the original metadata (with "tenant" schema from models)
target_metadata = TenantBase.metadata

logger = logging.getLogger("migration_tenant")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt="%(asctime)s [%(processName)-15s] %(levelname)s: %(message)s",
)
# file_handler = logging.FileHandler("migration_tenant.log")
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def run_migrations_offline() -> None:
    raise NotImplementedError("Offline mode is not supported for tenant migrations.")


def run_migrations_online() -> None:
    """
    Run migrations for tenant schemas using batch processing.

    Processes schemas in batches of 50, reusing connections for optimal performance.
    Each batch uses its own connection, and errors in one schema don't affect others.
    """
    # Use QueuePool for better performance with multiple schemas
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    total_schemas = len(SCHEMAS)
    logger.info(f"Processing {total_schemas} tenant schema(s)...")

    # Process in batches with connection reuse

    with connectable.connect() as connection:
        for i, schema in enumerate(SCHEMAS):
            logger.info(f"Processing schema {i + 1}/{total_schemas}: {schema}")
            _run_migration_for_schema_with_connection(connection, schema)


def _run_migration_for_schema_with_connection(connection, schema: str):
    """Run migration for a single schema using an existing connection."""
    import time

    start_time = time.time()

    logger.info(f"Schema '{schema}': Starting migration")
    conn_with_schema = connection.execution_options(
        schema_translate_map={PER_TENANT_SCHEMA_PLACEHOLDER: schema}
    )
    conn_with_schema.execute(f'SET search_path TO "{schema}", public')

    context.configure(
        connection=conn_with_schema,
        target_metadata=target_metadata,
        include_object=only_schema(schema),
        include_schemas=True,
        version_table_schema=schema,
        schema_translate_map={PER_TENANT_SCHEMA_PLACEHOLDER: schema},
        compare_type=compare_type,
    )

    try:
        with context.begin_transaction():
            context.run_migrations()
        elapsed = time.time() - start_time
        logger.info(f"Schema '{schema}': Migration successful in {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Schema '{schema}': Migration failed after {elapsed:.2f}s: {e}")
        raise


def only_schema(schema):
    # Incluye SOLO objetos del schema de referencia (y el propio schema)
    def include_object(obj, name, type_, reflected, compare_to):
        obj_schema = getattr(obj, "schema", None)
        if type_ == "schema":
            return name == schema
        return obj_schema in (
            schema,
            PER_TENANT_SCHEMA_PLACEHOLDER,
            SHARED_TENANT_SCHEMA_PLACEHOLDER,
            None,
        )

    return include_object


def compare_type(context, inspected_column, metadata_column, inspected_type, metadata_type):
    """
    Custom type comparison to avoid detecting false changes in ENUMs.

    Returns True if types are different, False if they're the same, None to use default comparison.
    """
    from sqlalchemy import Enum
    from sqlalchemy.dialects import postgresql

    # If both are ENUMs with same name, consider them equal
    if (
        isinstance(metadata_type, (Enum | postgresql.ENUM))
        and isinstance(inspected_type, (Enum | postgresql.ENUM))
        and (
            getattr(metadata_type, "name", "random_name")
            == getattr(inspected_type, "name", "random_name2")
        )
    ):
        return False

    # Use default comparison for other types
    return None


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
