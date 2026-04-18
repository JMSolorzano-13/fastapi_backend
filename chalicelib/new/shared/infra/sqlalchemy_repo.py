from abc import ABC
from typing import TYPE_CHECKING, Any

from sqlalchemy import update

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot
from chalicelib.new.shared.domain.exceptions.not_found_exception import NotFoundException
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import Model as ModelORM

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class SQLAlchemyRepo(ABC):  # noqa E501
    def __init__(self):
        self.session: Session
        self._model: AggregationRoot
        self._model_orm: ModelORM

    def save(self, model: AggregationRoot, auto_commit: bool = True) -> None:
        if record_orm := self._search_by_identifier(model.identifier):
            self._update_orm(record_orm=record_orm, model=model)
        else:
            self._create_record_orm(model)
        if auto_commit:
            self.session.commit()

    def create(self, model: AggregationRoot, auto_commit: bool = True) -> None:
        self._create_record_orm(model)
        if auto_commit:
            self.session.commit()

    def update(
        self,
        model: AggregationRoot,
        values: dict[str, Any] = None,
        auto_commit: bool = True,
    ) -> None:
        if not values:
            values = model.to_orm_dict()
            values.pop("identifier")
        self._update_orm_directly(model.identifier, values)
        for attr_name, value in values.items():
            setattr(model, attr_name, value)
        if auto_commit:
            self.session.commit()

    def _update_orm_directly(self, identifier: Identifier, values: dict[str, Any]) -> None:
        stmt = (
            update(self._model_orm).where(self._model_orm.identifier == identifier).values(**values)
        )
        # TODO update only the values that changed
        self.session.execute(stmt)

    def _search_by_identifier(self, identifier: Identifier) -> ModelORM | None:
        return (
            self.session.query(self._model_orm)
            .filter(self._model_orm.identifier == identifier)
            .first()
        )

    def get_by_identifier(self, identifier: Identifier) -> Any:
        if record_orm := self._search_by_identifier(identifier):
            return self._model_from_orm(record_orm)
        raise NotFoundException(identifier)

    def _model_from_orm(self, record_orm: ModelORM) -> AggregationRoot:
        raise NotImplementedError

    def _update_orm(self, record_orm: ModelORM, model: AggregationRoot) -> None:
        raise NotImplementedError

    def _create_record_orm(self, model: AggregationRoot) -> None:
        raise NotImplementedError
