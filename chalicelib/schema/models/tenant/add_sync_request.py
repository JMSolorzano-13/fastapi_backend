import enum

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    func,
)

from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class ADDSyncRequest(TenantIdentifiedModel):
    __tablename__ = "add_sync_request"

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    start = Column(
        Date,
        nullable=False,
    )
    end = Column(
        Date,
        nullable=False,
    )
    xmls_to_send = Column(
        Integer,
        nullable=False,
        default=0,
    )
    xmls_to_send_pending = Column(
        Integer,
        nullable=False,
        default=0,
        # TODO index
    )
    xmls_to_send_total = Column(
        Float,
        nullable=False,
        default=0,
    )
    cfdis_to_cancel = Column(
        Integer,
        nullable=False,
        default=0,
    )
    cfdis_to_cancel_pending = Column(
        Integer,
        nullable=False,
        default=0,
        # TODO index
    )
    cfdis_to_cancel_total = Column(
        Float,
        nullable=False,
        default=0,
    )
    pasto_sent_identifier = Column(
        IdentifierORM(),
    )
    pasto_cancel_identifier = Column(
        IdentifierORM(),
    )

    class StateEnum(enum.Enum):
        DRAFT = enum.auto()
        SENT = enum.auto()
        ERROR = enum.auto()

    state = Column(
        Enum(StateEnum, schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        nullable=False,
        default=StateEnum.DRAFT,
    )

    manually_triggered = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
