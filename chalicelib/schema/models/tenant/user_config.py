from sqlalchemy import JSON, Column
from sqlalchemy.ext.mutable import MutableDict

from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.tenant.tenant_model import TenantCreatedUpdatedModel

DEFAULT_DATA = {
    "dashboardIds": ["totals", "linecharttotals", "iva_period_widget_data"],
    "validationIds": ["issuedcfdis", "receivedcfdis", "efos"],
    "pivotLayouts": {},
    "tableColumns": {},
    "IVAIds": ["iva-widget"],
}


class UserConfig(TenantCreatedUpdatedModel):
    __tablename__ = "user_config"

    user_identifier = Column(
        IdentifierORM(),
        # No se puede usar como foreign key porque la tabla `User` no está en los tenant
        # foreign_key="user.identifier",
        primary_key=True,
    )
    data = Column(
        # MutableDict Permite que los cambios en el contenido del JSON sean detectados por SQLAlchemy  # noqa: E501
        MutableDict.as_mutable(JSON),
        nullable=False,
        default=lambda: DEFAULT_DATA,
    )
