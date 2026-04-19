import os
import sys

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from chalicelib.schema.models.company import Company

_FA_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_TENANT_ALEMBIC_INI = os.path.join(_FA_ROOT, "chalicelib", "alembic_tenant.ini")


def create_tenant_database_and_schema(company: Company):
    """Create tenant database and apply migrations if it doesn't exist.

    Args:
        company: Company instance with tenant database configuration

    Raises:
        ValueError: If company doesn't have required tenant DB configuration
        RuntimeError: If database creation or migrations fail
    """
    # Get tenant database URL
    tenant_db_url_with_schema = company.tenant_db_url_with_schema
    tenant_db_url = company.tenant_db_url
    if not tenant_db_url_with_schema:
        raise ValueError("Company must have tenant database configuration set")

    # Parse connection details
    schema_name = company.tenant_db_schema
    # Try to connect to the tenant database
    try:
        engine = create_engine(tenant_db_url)
        with engine.connect():  # noqa: SIM117
            # Database exists and is accessible
            # Know if schema exists
            with engine.begin() as conn:
                result = conn.execute(
                    text(
                        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"  # noqa: E501
                    ),
                    {"schema": schema_name},
                )
                if not result.fetchone():
                    # Schema doesn't exist, create it
                    conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    except OperationalError as e:
        # Database doesn't exist, create it
        if "database does not exist" in str(e):
            admin_url = tenant_db_url.replace(f"/{company.tenant_db_name}", "/postgres")
            _create_schema(admin_url, schema_name)
        else:
            raise

    # Apply alembic migrations
    _apply_tenant_migrations(schema_name)


def _create_schema(admin_url: str, schema_name: str):
    """Create the tenant database using admin connection."""
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    with admin_engine.connect() as conn:
        # Create database with autocommit isolation level
        conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))


def _apply_tenant_migrations(schema_name: str):
    """Apply alembic tenant migrations to the database."""
    if _FA_ROOT not in sys.path:
        sys.path.insert(0, _FA_ROOT)
    if not os.path.isfile(_TENANT_ALEMBIC_INI):
        raise FileNotFoundError(f"Alembic tenant config not found: {_TENANT_ALEMBIC_INI}")
    alembic_cfg = Config(_TENANT_ALEMBIC_INI)
    alembic_cfg.set_main_option("TENANT_SCHEMAS", schema_name)
    command.upgrade(alembic_cfg, "head")
