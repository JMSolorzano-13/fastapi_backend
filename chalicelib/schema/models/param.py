from sqlalchemy import Column, String

from .model import Model


class Param(Model):
    __tablename__ = "param"

    name = Column(
        String,
        index=True,
        nullable=False,
    )
    value = Column(
        String,
        index=True,
    )
