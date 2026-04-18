from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, ForeignKey, Integer, String

from chalicelib.new.query.domain.enums import (
    DownloadType,
    QueryState,
    RequestType,
    SATDownloadTechnology,
)
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class SATQuery(TenantIdentifiedModel):
    __tablename__ = "sat_query"

    name = Column(
        String,
        index=True,
        nullable=False,
        default="Draft",
    )
    start = Column(
        DateTime,
        nullable=False,
        index=True,
    )
    end = Column(
        DateTime,
        nullable=False,
        index=True,
    )
    download_type = Column(
        Enum(DownloadType, schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        nullable=False,
    )
    request_type = Column(
        Enum(RequestType, schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        nullable=False,
    )
    packages = Column(
        JSON,
    )
    cfdis_qty = Column(
        Integer,
    )
    state = Column(
        Enum(QueryState, schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        nullable=False,
    )
    sent_date = Column(
        DateTime,
        index=True,
    )
    is_manual = Column(
        Boolean,
        server_default="FALSE",
    )
    technology = Column(
        Enum(SATDownloadTechnology, schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        nullable=False,
        default=SATDownloadTechnology.WebService.value,
        server_default=SATDownloadTechnology.WebService.value,
    )
    origin_identifier = Column(
        IdentifierORM(),
        ForeignKey("per_tenant.sat_query.identifier", ondelete="CASCADE"),
    )
