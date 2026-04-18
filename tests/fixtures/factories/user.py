from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.pytest_plugin import register_fixture

from chalicelib.schema.models.user import User


@register_fixture
class UserFactory(SQLAlchemyFactory[User]):
    __random_seed__ = 1
    __allow_none_optionals__ = False
