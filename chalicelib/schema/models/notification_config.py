import enum

from sqlalchemy import Column, Enum, ForeignKey, Integer
from sqlalchemy.orm import relationship

from chalicelib.new.shared.infra.primitives import IdentifierORM

from .model import Model


class NotificationConfig(Model):
    __tablename__ = "notification_config"

    user_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    workspace_id = Column(
        Integer,
        index=True,
        nullable=False,
    )
    workspace_identifier = Column(
        IdentifierORM(),
        ForeignKey("workspace.identifier", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    class NotificationTypeEnum(enum.Enum):
        ERROR = enum.auto()
        EFOS = enum.auto()
        CANCELED = enum.auto()

    notification_type = Column(
        Enum(NotificationTypeEnum, name="notificationtypeenum"),
        index=True,
        nullable=False,
    )

    user = relationship(
        "User",
        backref="notification_configs",
    )
    workspace = relationship(
        "Workspace",
        backref="notification_configs",
    )
