from datetime import date, datetime
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.schema import Table

from chalicelib.new.shared.domain.primitives import identifier_default_factory
from chalicelib.new.shared.infra.primitives import IdentifierORM

from .. import meta

Base: Any = declarative_base(metadata=meta)
SHARED_TENANT_SCHEMA_PLACEHOLDER = "public"


class BasicModel(Base):
    __abstract__ = True
    __table__: Table

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
    )
    updated_at = Column(
        DateTime,
        onupdate=datetime.utcnow,
    )

    def __iter__(self):
        """Hace que dict(obj) funcione directamente"""
        for c in inspect(self).mapper.column_attrs:
            yield c.key, getattr(self, c.key)


class IdentifiedModel(BasicModel):
    __abstract__ = True
    __table__: Table

    identifier = Column(
        IdentifierORM(),
        primary_key=True,
        default=identifier_default_factory,
    )


class Model(BasicModel):
    """Base model for all the models to be persisted in the database"""

    __abstract__ = True
    __table__: Table

    id = Column(  # TODO remove
        Integer,
        primary_key=True,
    )
    identifier = Column(
        IdentifierORM(),
        index=True,
        unique=True,
        default=identifier_default_factory,
    )


class CodeName(Base):
    """Base model for all the models to be peristed in the database
    using Code and Name as all fields"""

    __abstract__ = True
    __table__: Table
    __table_args__ = {"schema": SHARED_TENANT_SCHEMA_PLACEHOLDER}

    id = Column(
        Integer,
        primary_key=True,
    )
    identifier = Column(
        IdentifierORM(),
        index=True,
    )
    code = Column(
        String,
        index=True,
        nullable=False,
        unique=True,
    )
    name = Column(
        String,
        index=True,
        nullable=False,
    )


class ColumnBetween(Column):
    inherit_cache = True

    def between(self, start, end, symmetric=False):
        if isinstance(end, date):
            # Consider the end of the day
            end = datetime.combine(end, datetime.max.time())
        return super().between(start, end, symmetric=symmetric)
