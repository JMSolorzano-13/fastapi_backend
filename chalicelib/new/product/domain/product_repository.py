from typing import Protocol

from chalicelib.new.product.domain import Product


class ProductRepository(Protocol):
    def save(self, package: Product): ...

    def get_all(self) -> list[Product]: ...

    def get_by_identifiers(self, identifiers: list[str]) -> list[Product]: ...
