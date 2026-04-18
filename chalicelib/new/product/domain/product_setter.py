from dataclasses import dataclass

from chalicelib.new.product.domain import Product, ProductRepository


@dataclass
class ProductSetter:
    product_repo: ProductRepository

    def set_product(self, product: Product) -> None:
        self.product_repo.save(product)
