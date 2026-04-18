from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from chalicelib.new.product.domain import Product


@dataclass
class ProductLine:
    product: Product
    quantity: int

    @classmethod
    def from_dict(
        cls, product_repo, product_lines: Iterable[dict[str, Any]]
    ) -> tuple["ProductLine"]:
        res = []
        for product_line in product_lines:
            product = product_repo.get_by_identifier(product_line["identifier"])
            res.append(ProductLine(product=product, quantity=product_line["quantity"]))
        return tuple(res)
