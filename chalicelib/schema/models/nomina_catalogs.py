from sqlalchemy import Column, String
from sqlalchemy.sql.schema import Table

from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER

from .model import Base


class Catalog(Base):
    __abstract__ = True
    __table__: Table
    __table_args__ = {"schema": SHARED_TENANT_SCHEMA_PLACEHOLDER}

    code = Column(
        String,
        nullable=False,
        index=True,
        primary_key=True,
    )
    name = Column(
        String,
        index=True,
    )


class CatTipoNomina(Catalog):
    __tablename__ = "cat_nom_tipo_nomina"


class CatTipoContrato(Catalog):
    __tablename__ = "cat_nom_tipo_contrato"


class CatTipoJornada(Catalog):
    __tablename__ = "cat_nom_tipo_jornada"


class CatTipoRegimen(Catalog):
    __tablename__ = "cat_nom_tipo_regimen"


class CatRiesgoPuesto(Catalog):
    __tablename__ = "cat_nom_riesgo_puesto"


class CatPeriodicidadPago(Catalog):
    __tablename__ = "cat_nom_periodicidad_pago"


class CatBanco(Catalog):
    __tablename__ = "cat_nom_banco"


class CatClaveEntFed(Catalog):
    __tablename__ = "cat_nom_clave_ent_fed"
