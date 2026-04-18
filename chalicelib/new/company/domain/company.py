from dataclasses import dataclass
from datetime import datetime

from chalicelib.new.config.infra import envars
from chalicelib.new.shared.domain.aggregation_root import AggregationRoot
from chalicelib.new.shared.domain.primitives import Identifier


@dataclass
class Company(AggregationRoot):
    id: int
    rfc: str
    name: str
    workspace_id: int
    workspace_identifier: Identifier
    active: bool = True
    have_certificates: bool = False
    exceed_metadata_limit: bool = False
    permission_to_sync: bool = False
    last_notification: datetime = (None,)
    emails_to_send_efos: str = None
    emails_to_send_errors: str = None
    emails_to_send_canceled: str = None
    pasto_company_identifier: Identifier = None

    def is_especial(self) -> bool:
        return self.rfc in envars.SPECIAL_RFCS
