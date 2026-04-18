from dataclasses import dataclass

from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.ws_sat.domain.enums.verify import (
    VerifyQueryStatus,
    VerifyQueryStatusCode,
    VerifyStatusCode,
)


@dataclass
class QueryVerifyResponse:
    sat_uuid: Identifier
    status: VerifyStatusCode
    query_status: VerifyQueryStatus
    message: str
    status_code: VerifyQueryStatusCode
    cfdi_qty: int = 0
    package_ids: tuple[str, ...] = None
