from chalicelib.new.shared.domain.event.event import CompanyEvent
from chalicelib.new.shared.domain.primitives import Identifier


class QueryReadyToDownloadEvent(CompanyEvent):
    query_identifier: Identifier
    sat_uuid: Identifier
    package_ids: list[str]
    cfdi_qty: int
