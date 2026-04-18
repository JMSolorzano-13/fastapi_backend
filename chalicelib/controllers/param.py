from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.schema.models import Param


class ParamController(CommonController):
    model = Param

    @staticmethod
    def get_param(
        name: str,
        default="",
        *,
        session: Session,
    ) -> str:
        if param := session.query(Param.value).filter(Param.name == name).first():
            return param.value
        return default

    @staticmethod
    def set(name: str, value: str, *, session: Session):
        if param := session.query(Param).filter(Param.name == name).first():
            param.value = value
        else:
            param = Param(name=name, value=value)
            session.add(param)
