from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.pytest_plugin import register_fixture

from chalicelib.schema.models.company import Company


@register_fixture
class CompanyFactory(SQLAlchemyFactory[Company]):
    __random_seed__ = 1
    __allow_none_optionals__ = False
