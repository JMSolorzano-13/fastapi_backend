from sqlalchemy import Column, ForeignKey, Integer, String

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
