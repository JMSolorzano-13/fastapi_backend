from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.new.cfdi.domain import CFDI
from chalicelib.new.shared.infra.sqlalchemy_repo import SQLAlchemyRepo


@dataclass
class CFDIRepository(SQLAlchemyRepo):
    session: Session
    _model = CFDI
    _model_orm = CFDIORM  # noqa E501
