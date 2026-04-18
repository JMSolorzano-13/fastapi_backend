from dataclasses import dataclass
from typing import Any

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot


@dataclass
class Product(AggregationRoot):
    stripe_identifier: str
    characteristics: dict[str, Any]
    price: int
    stripe_price_identifier: str
    stripe_name: str

    def __post_init__(self):
        self.identifier = self.stripe_identifier
