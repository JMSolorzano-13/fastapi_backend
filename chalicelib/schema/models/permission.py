import enum

from sqlalchemy import Column, Enum, ForeignKey, Integer
from sqlalchemy.orm import relationship

from chalicelib.modules import NameEnum

from .model import Model


class Permission(Model):
    __tablename__ = "permission"

    user_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    company_id = Column(
        Integer,
        ForeignKey("company.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    class RoleEnum(NameEnum):
        OPERATOR = enum.auto()
        PAYROLL = enum.auto()

    role = Column(
        Enum(RoleEnum, name="enum_permission_role"),
        index=True,
        nullable=False,
    )

    user = relationship(
        "User",
        backref="permissions",
    )
    company = relationship(
        "Company",
        backref="permissions",
    )
