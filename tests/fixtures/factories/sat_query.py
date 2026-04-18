from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.pytest_plugin import register_fixture

from chalicelib.schema.models.tenant.sat_query import SATQuery


@register_fixture
class SATQueryFactory(SQLAlchemyFactory[SATQuery]):
    __random_seed__ = 1
    __allow_none_optionals__ = False
