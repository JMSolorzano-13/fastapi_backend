from dataclasses import dataclass

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot
from chalicelib.new.shared.domain.primitives import Identifier


@dataclass
class Package(AggregationRoot):
    sat_uuid: Identifier
    zip_content: bytes
