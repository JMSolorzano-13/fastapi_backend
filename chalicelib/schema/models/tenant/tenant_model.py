from datetime import datetime

from sqlalchemy import Column, DateTime, Table
from sqlalchemy.orm import declarative_base

from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.new.shared.infra.primitives import IdentifierORM

TenantBase = declarative_base()

PER_TENANT_SCHEMA_PLACEHOLDER = "per_tenant"


class TenantBaseModel(TenantBase):
    __abstract__ = True
    __table__: Table
    __table_args__ = {"schema": PER_TENANT_SCHEMA_PLACEHOLDER}  # <- placeholder


TenantBase.metadata.naming_convention = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "%(table_name)s_pkey",
}


class TenantCreatedUpdatedModel(TenantBaseModel):
    __abstract__ = True
    __table__: Table

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )
    updated_at = Column(
        DateTime,
        onupdate=datetime.utcnow,
    )


class TenantIdentifiedModel(TenantCreatedUpdatedModel):
    __abstract__ = True
    __table__: Table

    identifier = Column(
        IdentifierORM(),
        primary_key=True,
        default=identifier_default_factory,
    )
