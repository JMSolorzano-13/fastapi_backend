from dataclasses import dataclass
from datetime import datetime

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot


@dataclass
class Workspace(AggregationRoot):
    valid_until: datetime = None
    pasto_worker_id: str = None
    pasto_license_key: str = None
    pasto_installed: bool = False
    pasto_worker_token: str = None
