from dataclasses import dataclass
from datetime import datetime

from chalicelib.new.cfdi_processor.domain.enums.cfdi_export_state import CfdiExportState
from chalicelib.new.shared.domain.aggregation_root import AggregationRoot
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport


@dataclass
class Export(AggregationRoot):
    format: str
    start: datetime | None = None
    end: datetime | None = None
    cfdi_type: str | None = None
    download_type: str | None = None
    external_request: bool = False
    state: CfdiExportState = CfdiExportState.SENT
    export_data_type: CfdiExport.ExportDataType = CfdiExport.ExportDataType.CFDI
    url: str | None = None
    expiration_date: datetime | None = None
    domain: str = ""
    displayed_name: str = ""
    file_name: str = ""
