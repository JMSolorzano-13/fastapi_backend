import enum

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from chalicelib.modules import NameEnum
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel
from chalicelib.schema.models.user import User


class Attachment(TenantIdentifiedModel):
    __tablename__ = "attachment"

    cfdi_uuid = Column(
        IdentifierORM(),
        # TODO agregar FK cuando los CFDIs solo tengan el UUID como PK,
        # la migración NO tiene el constraint
        ForeignKey("per_tenant.cfdi.UUID", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    creator_identifier = Column(
        IdentifierORM(),
        # TODO por ahora no podemos usar una FK a user, la migración NO tiene el constraint
        ForeignKey(User.identifier, ondelete="CASCADE"),
        nullable=False,
    )
    deleter_identifier = Column(
        IdentifierORM(),
        # TODO por ahora no podemos usar una FK a user, la migración NO tiene el constraint
        ForeignKey(User.identifier, ondelete="CASCADE"),
    )
    deleted_at = Column(
        DateTime,
    )

    size = Column(  # Received from request
        Integer,
        nullable=False,
    )
    file_name = Column(  # Received from request
        String,
        nullable=False,
        index=True,
    )
    content_hash = Column(  # Received from request
        String,
        nullable=False,
    )
    s3_key = Column(  # Computed
        String,
        nullable=False,
    )

    class StateEnum(NameEnum):
        PENDING = enum.auto()
        CONFIRMED = enum.auto()
        DELETED = enum.auto()

    state = Column(
        Enum(StateEnum, name="attachment_state"),
        nullable=False,
        index=True,
        default=StateEnum.PENDING,
    )

    cfdi = relationship(
        "CFDI",
        backref="attachments",
        primaryjoin=(
            "and_(CFDI.UUID == foreign(Attachment.cfdi_uuid), Attachment.state != 'DELETED')"
        ),
    )
    creator = relationship(
        User,
        foreign_keys=[creator_identifier],
    )
    deleter = relationship(
        User,
        foreign_keys=[deleter_identifier],
    )


Index(
    "idx_attachment_cfdi_filename_unique",
    Attachment.cfdi_uuid,
    Attachment.file_name,
    unique=True,
    postgresql_where=(Attachment.state != Attachment.StateEnum.DELETED),
)
