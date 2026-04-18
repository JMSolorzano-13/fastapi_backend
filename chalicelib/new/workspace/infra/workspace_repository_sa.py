from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo
from chalicelib.new.workspace.domain.workspace import Workspace
from chalicelib.schema.models import Workspace as WorkspaceORM


@dataclass
class WorkspaceRepositorySA(SQLAlchemyRepo):
    session: Session
    _model = Workspace
    _model_orm = WorkspaceORM

    def save(self, model: Workspace, auto_commit: bool = True):
        if record_orm := self._search_by_identifier(model.identifier):
            self._update_orm(record_orm=record_orm, model=model)
        else:
            self._create_record_orm(model)

    def _create_record_orm(self, model: _model) -> None:
        record_orm = WorkspaceORM(
            identifier=model.identifier,
            valid_until=model.valid_until,
            pasto_worker_id=model.pasto_worker_id,
            pasto_license_key=model.pasto_license_key,
            pasto_installed=model.pasto_installed,
            pasto_worker_token=model.pasto_worker_token,
        )
        self.session.add(record_orm)

    def _model_from_orm(self, record_orm: _model_orm) -> _model:
        return Workspace(
            valid_until=record_orm.valid_until,
            pasto_worker_id=record_orm.pasto_worker_id,
            pasto_license_key=record_orm.pasto_license_key,
            pasto_installed=record_orm.pasto_installed,
            pasto_worker_token=record_orm.pasto_worker_token,
        ).set_identifier(record_orm.identifier)

    def _update_orm(self, record_orm: _model_orm, model: _model) -> None:
        record_orm.pasto_worker_id = model.pasto_worker_id
        record_orm.pasto_license_key = model.pasto_license_key
        record_orm.pasto_installed = model.pasto_installed
        record_orm.pasto_worker_token = model.pasto_worker_token
