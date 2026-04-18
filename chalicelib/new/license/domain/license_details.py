from collections.abc import Iterable
from dataclasses import dataclass, field

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot

from .product_line import ProductLine


@dataclass
class LicenseDetails(AggregationRoot):
    max_emails_enroll: int
    max_companies: int
    product_lines: Iterable[ProductLine] = field(default_factory=list)

    @classmethod
    def from_product_lines(cls, product_lines: Iterable[ProductLine]) -> "LicenseDetails":
        max_emails_enroll = (
            "unlimited"
            if any(
                l.product.characteristics.get("max_emails_enroll", 0) == "unlimited"
                for l in product_lines  # noqa E501
            )
            else sum(l.product.characteristics.get("max_emails_enroll", 0) for l in product_lines)  # noqa E501
        )
        max_companies = (
            "unlimited"
            if any(
                l.product.characteristics.get("max_companies", 0) == "unlimited"
                for l in product_lines  # noqa E501
            )
            else sum(int(l.product.characteristics.get("max_companies", 0)) for l in product_lines)  # noqa E501
        )
        return LicenseDetails(
            max_emails_enroll=max_emails_enroll,
            max_companies=max_companies,
            product_lines=product_lines,
        )
