from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.pytest_plugin import register_fixture

from chalicelib.schema.models.pasto_company import PastoCompany


@register_fixture
class PastoCompanyFactory(SQLAlchemyFactory[PastoCompany]):
    __random_seed__ = 1
    __allow_none_optionals__ = False
