from datetime import datetime, timedelta

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func

from chalicelib.schema.models.user import User

from .model import Model


class Workspace(Model):
    __tablename__ = "workspace"

    name = Column(  # TODO remove this column
        String,
        index=True,
        nullable=True,
    )
    owner_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="RESTRICT"),
        index=True,
        nullable=True,
    )
    license: dict = Column(
        MutableDict.as_mutable(JSONB),
        nullable=False,
        default=dict,
    )
    valid_until = Column(
        DateTime,
        index=True,
    )
    odoo_id = Column(
        Integer,
        index=True,
    )
    stripe_status = Column(
        String,
        index=True,
    )
    pasto_worker_id = Column(
        String,
        index=True,
        unique=True,
    )
    pasto_license_key = Column(
        String,
        index=True,
        unique=True,
    )
    pasto_installed = Column(
        Boolean,
    )
    pasto_worker_token = Column(
        String,
    )
    add_permission = Column(
        Boolean,
        index=True,
        default=False,
    )

    owner = relationship(
        User,
        backref=backref("workspace", uselist=False),
    )

    @hybrid_property
    def is_active(self) -> bool:
        """
        Check if the workspace is active based on the valid_until date.
        """
        return self.valid_until > (datetime.utcnow() - timedelta(days=1))

    @is_active.expression
    def is_active(cls):
        """
        SQL expression to check if the workspace is active.
        """
        return cls.valid_until > (func.now() - timedelta(days=1))
