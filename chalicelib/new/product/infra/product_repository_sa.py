from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.logger import log_in
from chalicelib.new.product.domain import Product
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.schema.models import Product as ProductORM


@dataclass
class ProductRepositorySA(SQLAlchemyRepo):
    session: Session
    _model: Product = Product
    _model_orm: ProductORM = ProductORM

    def _search_by_identifier(self, identifier: str) -> Product | None:
        return (
            self.session.query(self._model_orm)
            .filter(self._model_orm.stripe_identifier == identifier)
            .first()
        )

    def _model_from_orm(self, record_orm: ProductORM) -> Product:
        return Product(
            stripe_identifier=record_orm.stripe_identifier,
            characteristics=record_orm.characteristics,
            price=record_orm.price,
            stripe_price_identifier=record_orm.stripe_price_identifier,
            stripe_name=record_orm.stripe_name,
        )

    def _update_orm(self, record_orm: ProductORM, model: Product) -> None:
        record_orm.stripe_identifier = model.stripe_identifier
        record_orm.characteristics = model.characteristics
        record_orm.price = model.price
        record_orm.stripe_price_identifier = model.stripe_price_identifier
        record_orm.stripe_name = model.stripe_name

    def _create_record_orm(self, model: Product) -> None:
        record_orm = self._model_orm(
            stripe_identifier=model.stripe_identifier,
            characteristics=model.characteristics,
            price=model.price,
            stripe_price_identifier=model.stripe_price_identifier,
            stripe_name=model.stripe_name,
        )
        self.session.add(record_orm)

    def get_all(self) -> list[Product]:
        return [
            self._model_from_orm(record_orm)
            for record_orm in self.session.query(self._model_orm).order_by(self._model_orm.price)
        ]

    def get_by_identifiers(self, identifiers: list[str]) -> list[Product]:
        log_in(identifiers)
        return [
            self._model_from_orm(record_orm)
            for record_orm in self.session.query(self._model_orm).filter(
                self._model_orm.stripe_identifier.in_(identifiers)
            )
        ]
