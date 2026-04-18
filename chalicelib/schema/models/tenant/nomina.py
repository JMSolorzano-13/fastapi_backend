import csv
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    cast,
)
from sqlalchemy.orm import column_property, relationship

from chalicelib.controllers.cfdi_utils.parsers import get_complementos
from chalicelib.new.shared.infra.primitives import IdentifierORM
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER
from chalicelib.schema.models.nomina_catalogs import (
    CatBanco,
    CatClaveEntFed,
    CatPeriodicidadPago,
    CatRiesgoPuesto,
    CatTipoContrato,
    CatTipoJornada,
    CatTipoNomina,
    CatTipoRegimen,
)
from chalicelib.schema.models.tenant.tenant_model import TenantBaseModel


def codes_from_csv(cat_name: str) -> tuple[str, ...]:
    with open(f"chalicelib/data/{cat_name}.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # Skip header
        return tuple(row[0] for row in reader)


class Nomina(TenantBaseModel):
    __tablename__ = "nomina"

    company_identifier = Column(
        IdentifierORM(),
        nullable=False,
        primary_key=True,
    )
    cfdi_uuid = Column(  # In case of multiple nomina per CFDI, we will use the first one
        IdentifierORM(),
        nullable=False,
        index=True,
        primary_key=True,
    )

    # Attributes
    Version = Column(
        Enum("1.1", "1.2", name="enum_nom_version", schema=SHARED_TENANT_SCHEMA_PLACEHOLDER),
        nullable=False,
        index=True,
    )
    TipoNomina = Column(
        Enum(
            *codes_from_csv("cat_nom_tipo_nomina"),
            name="enum_tipo_nomina",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        nullable=False,
        index=True,
    )
    FechaPago = Column(
        DateTime,
        nullable=False,
        index=True,
    )
    FechaInicialPago = Column(
        DateTime,
        nullable=False,
        index=True,
    )
    FechaFinalPago = Column(
        DateTime,
        nullable=False,
        index=True,
    )
    NumDiasPagados = Column(
        Numeric,
        nullable=False,
    )
    TotalPercepciones = Column(
        Numeric,
    )
    TotalDeducciones = Column(
        Numeric,
    )
    TotalOtrosPagos = Column(
        Numeric,
    )

    # Emisor
    EmisorRegistroPatronal = Column(
        String,
        index=True,
    )

    # Receptor
    ReceptorCurp = Column(
        String,
        nullable=False,
        index=True,
    )
    ReceptorNumSeguridadSocial = Column(
        String,
        index=True,
    )
    ReceptorFechaInicioRelLaboral = Column(
        DateTime,
    )
    ReceptorAntigüedad = Column(
        String,
    )
    ReceptorTipoContrato = Column(
        Enum(
            *codes_from_csv("cat_nom_tipo_contrato"),
            name="enum_tipo_contrato",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        nullable=False,
        index=True,
    )
    ReceptorSindicalizado = Column(
        Boolean,
    )
    ReceptorTipoJornada = Column(
        Enum(
            *codes_from_csv("cat_nom_tipo_jornada"),
            name="enum_tipo_jornada",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        index=True,
    )
    ReceptorTipoRegimen = Column(
        Enum(
            *codes_from_csv("cat_nom_tipo_regimen"),
            name="enum_tipo_regimen",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        nullable=False,
        index=True,
    )
    ReceptorNumEmpleado = Column(
        String,
        nullable=False,
        index=True,
    )
    ReceptorDepartamento = Column(
        String,
        index=True,
    )
    ReceptorPuesto = Column(
        String,
        index=True,
    )
    ReceptorRiesgoPuesto = Column(
        Enum(
            *codes_from_csv("cat_nom_riesgo_puesto"),
            name="enum_riesgo_puesto",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        index=True,
    )
    ReceptorPeriodicidadPago = Column(
        Enum(
            *codes_from_csv("cat_nom_periodicidad_pago"),
            name="enum_periodicidad_pago",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        nullable=False,
        index=True,
    )
    ReceptorBanco = Column(
        Enum(
            *codes_from_csv("cat_nom_banco"),
            name="enum_banco",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        index=True,
    )
    ReceptorCuentaBancaria = Column(
        String,
    )
    ReceptorSalarioBaseCotApor = Column(
        Numeric,
    )
    ReceptorSalarioDiarioIntegrado = Column(
        Numeric,
    )
    ReceptorClaveEntFed = Column(
        Enum(
            *codes_from_csv("cat_nom_clave_ent_fed"),
            name="enum_clave_ent_fed",
            schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
        ),
        nullable=False,
        index=True,
    )

    # Percepciones
    PercepcionesTotalSueldos = Column(
        Numeric,
    )
    PercepcionesTotalGravado = Column(
        Numeric,
    )
    PercepcionesTotalExento = Column(
        Numeric,
    )
    PercepcionesSeparacionIndemnizacion = Column(
        Numeric,
    )
    PercepcionesJubilacionPensionRetiro = Column(
        Numeric,
    )

    # Deducciones
    DeduccionesTotalOtrasDeducciones = Column(
        Numeric,
    )
    DeduccionesTotalImpuestosRetenidos = Column(
        Numeric,
    )

    # OtrosPagos
    SubsidioCausado = Column(  # SUM of all SubsidioCausado
        Numeric,
    )
    AjusteISRRetenido = Column(  # SUM of /OtroPago[@Importe] if @TipoOtroPago in (001, 004, 005)
        Numeric,
    )
    # Computed
    OtrasPercepciones = column_property(TotalPercepciones - PercepcionesTotalSueldos)
    NetoAPagar = column_property(TotalPercepciones + TotalOtrosPagos - TotalDeducciones)
    TotalPercepcionesYOtrosPagos = column_property(TotalPercepciones + TotalOtrosPagos)

    # Relationships
    c_TipoNomina = relationship(
        CatTipoNomina,
        foreign_keys=[TipoNomina],
        primaryjoin=cast(TipoNomina, Text) == CatTipoNomina.code,
    )
    c_ReceptorTipoContrato = relationship(
        CatTipoContrato,
        foreign_keys=[ReceptorTipoContrato],
        primaryjoin=cast(ReceptorTipoContrato, Text) == CatTipoContrato.code,
    )
    c_ReceptorTipoJornada = relationship(
        CatTipoJornada,
        foreign_keys=[ReceptorTipoJornada],
        primaryjoin=cast(ReceptorTipoJornada, Text) == CatTipoJornada.code,
    )
    c_ReceptorTipoRegimen = relationship(
        CatTipoRegimen,
        foreign_keys=[ReceptorTipoRegimen],
        primaryjoin=cast(ReceptorTipoRegimen, Text) == CatTipoRegimen.code,
    )
    c_ReceptorRiesgoPuesto = relationship(
        CatRiesgoPuesto,
        foreign_keys=[ReceptorRiesgoPuesto],
        primaryjoin=cast(ReceptorRiesgoPuesto, Text) == CatRiesgoPuesto.code,
    )
    c_ReceptorPeriodicidadPago = relationship(
        CatPeriodicidadPago,
        foreign_keys=[ReceptorPeriodicidadPago],
        primaryjoin=cast(ReceptorPeriodicidadPago, Text) == CatPeriodicidadPago.code,
    )
    c_ReceptorBanco = relationship(
        CatBanco,
        foreign_keys=[ReceptorBanco],
        primaryjoin=cast(ReceptorBanco, Text) == CatBanco.code,
    )
    c_ReceptorClaveEntFed = relationship(
        CatClaveEntFed,
        foreign_keys=[ReceptorClaveEntFed],
        primaryjoin=cast(ReceptorClaveEntFed, Text) == CatClaveEntFed.code,
    )

    UniqueConstraint(
        "cfdi_uuid",
    )

    cfdi = relationship(
        "CFDI",
        uselist=False,
        foreign_keys=[cfdi_uuid],
        primaryjoin="foreign(CFDI.UUID) == Nomina.cfdi_uuid",
    )

    def _parse_nomina_nodes(self, node: str) -> list[dict[str, Any]]:
        res = []
        for nomina in get_complementos(self.cfdi.xml_dict, "Nomina"):
            if node in nomina:
                subnodes = nomina[node]
                if isinstance(subnodes, dict):
                    subnodes = [subnodes]
                res.extend(subnodes)
        return res

    @property
    def Percepciones(self):
        return self._parse_nomina_nodes(
            "Percepciones",
        )

    @property
    def Deducciones(self):
        return self._parse_nomina_nodes(
            "Deducciones",
        )

    @property
    def OtrosPagos(self):
        return self._parse_nomina_nodes(
            "OtrosPagos",
        )
