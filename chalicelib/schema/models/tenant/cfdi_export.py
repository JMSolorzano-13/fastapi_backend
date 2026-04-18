import enum

from sqlalchemy import Boolean, Column, DateTime, Enum, String

from chalicelib.new.cfdi_processor.domain.enums.cfdi_export_state import CfdiExportState
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER
from chalicelib.schema.models.tenant.tenant_model import TenantIdentifiedModel


class CfdiExport(TenantIdentifiedModel):
    __tablename__ = "cfdi_export"

    url = Column(
        String,
        index=True,
    )
    state = Column(
        Enum(CfdiExportState, name="cfdiexportstate", schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
    )
    expiration_date = Column(
        DateTime,
        index=True,
    )
    start = Column(
        String,
        index=True,
    )
    end = Column(
        String,
        index=True,
    )
    cfdi_type = Column(
        String,
        index=True,
    )
    download_type = Column(
        String,
        index=True,
    )
    format = Column(
        String,
        index=True,
    )
    external_request = Column(
        Boolean,
        default=False,
    )

    class ExportDataType(enum.Enum):
        CFDI = enum.auto()
        IVA = enum.auto()
        ISR = enum.auto()

    export_data_type = Column(
        Enum(ExportDataType, name="exportdatatype", schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        index=True,
        server_default=ExportDataType.CFDI.name,
    )
    displayed_name = Column(
        String,
        index=True,
        nullable=False,
        server_default="",
    )
    file_name = Column(
        String,
        index=True,
        nullable=False,
        server_default="",
    )
    domain = Column(
        String,
    )
