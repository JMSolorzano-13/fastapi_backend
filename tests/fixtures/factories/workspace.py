from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.pytest_plugin import register_fixture

from chalicelib.schema.models.workspace import Workspace


@register_fixture
class WorkspaceFactory(SQLAlchemyFactory[Workspace]):
    __random_seed__ = 1
    __allow_none_optionals__ = False
