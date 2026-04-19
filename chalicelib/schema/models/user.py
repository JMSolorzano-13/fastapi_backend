from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.inspection import inspect

from .model import Model


class User(Model):
    __tablename__ = "user"

    name = Column(
        String,
        index=True,
    )
    email = Column(
        String,
        nullable=False,
        index=True,
    )
    cognito_sub = Column(
        String,
        index=True,
        unique=True,
    )
    # Bcrypt hash for AUTH_BACKEND=local_jwt (matches public.user.password_hash in control DDL)
    password_hash = Column(
        String,
        nullable=True,
    )
    invited_by_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="SET NULL"),
        index=True,
    )
    source_name = Column(
        String,
        index=True,
    )
    phone = Column(
        String,
    )
    odoo_identifier = Column(
        Integer,
    )
    stripe_identifier = Column(
        String,
    )
    stripe_subscription_identifier = Column(
        String,
    )

    def __iter__(self):
        """Omit ``password_hash`` from ``dict(user)`` / API serialization."""
        for c in inspect(self).mapper.column_attrs:
            if c.key == "password_hash":
                continue
            yield c.key, getattr(self, c.key)
