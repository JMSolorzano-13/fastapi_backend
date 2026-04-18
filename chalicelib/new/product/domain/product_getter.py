from dataclasses import dataclass

from .product import Product
from .product_repository import ProductRepository


@dataclass
class ProductGetter:
    product_repo: ProductRepository

    def get_all(self) -> list[Product]:
        return self.product_repo.get_all()
