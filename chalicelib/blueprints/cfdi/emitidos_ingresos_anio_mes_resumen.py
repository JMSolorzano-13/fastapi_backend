import uuid
from datetime import datetime
from typing import Annotated, Self

from dateutil.relativedelta import relativedelta
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    computed_field,
    model_validator,
    validate_call,
)
from sqlalchemy import case, extract, func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import coalesce

from chalicelib.blueprints import common
from chalicelib.schema.models.tenant import CFDI as CFDIORM

from .bp import bp


class IngresosNominales(BaseModel):
    vigentes: NonNegativeInt = 0
    cancelados: NonNegativeInt = 0
    subtotal_mxn: NonNegativeFloat = 0
    descuento_mxn: NonNegativeFloat = 0

    def __add__(self, other):
        return IngresosNominales(
            vigentes=self.vigentes + other.vigentes,
            cancelados=self.cancelados + other.cancelados,
            subtotal_mxn=self.subtotal_mxn + other.subtotal_mxn,
            descuento_mxn=self.descuento_mxn + other.descuento_mxn,
        )


class Resumen(BaseModel):  # TODO hacer genérico y mover a su propio archivo
    datos: dict[int, IngresosNominales]
    limit: int = Field(exclude=True, ge=0)

    @computed_field
    def total(self) -> IngresosNominales:
        return sum(self.datos.values(), IngresosNominales())

    @model_validator(mode="after")
    def complete_datos(self) -> Self:
        for i in range(1, self.limit + 1):
            if i not in self.datos:
                self.datos[i] = IngresosNominales()
        self.datos = dict(sorted(self.datos.items()))
        return self


@bp.route(
    "{cid}/emitidos/ingresos/{anio}/{mes}/resumen",
    cors=common.cors_config,
)
@validate_call(validate_return=True, config=ConfigDict(arbitrary_types_allowed=True))
def emitidos_ingresos_anio_mes_resumen(
    cid: uuid.UUID, anio: int, mes: int, company_session: Session
) -> Resumen:
    """Ingresos Nominales de una Compañía,
    desde 1ero de Enero del año seleccionado y hasta el mes indicado (incluyéndolo)
    obtiene los datos agrupados por mes y un total acumulado de los mismos
    """
    return _emitidos_ingresos_anio_mes_resumen(company_session=company_session, anio=anio, mes=mes)


Anio = Annotated[int, Field(ge=2000, le=2100)]
Mes = Annotated[int, Field(ge=1, le=12)]


def get_start_end_monthly(anio: Anio, mes: Mes):
    """
        Dado un año y mes, regresa 2 fechas, el inicio del año y el último día del mes
    ambos a las 00:00
    """
    return datetime(anio, 1, 1), datetime(anio, mes, 1) + relativedelta(months=1, days=-1)


def _emitidos_ingresos_anio_mes_resumen(
    *,
    company_session: Session,
    anio: Anio | None = None,
    mes: Mes | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> Resumen:
    if not ((anio is not None and mes is not None) or (start_date and end_date)):
        raise ValueError("Debe proporcionar (anio, mes) o (start_date, end_date)")

    if start_date and end_date:
        start = start_date
        end = end_date
        limit = (end.year - start.year) * 12 + end.month - start.month + 1
    else:
        start, end = get_start_end_monthly(anio, mes)
        limit = mes

    res = company_session.execute(
        select(
            [
                extract("month", CFDIORM.FechaFiltro).label("mes"),
                func.count(case([(CFDIORM.Estatus, 1)], else_=None)).label("vigentes"),
                func.count(case([(~CFDIORM.Estatus, 1)], else_=None)).label("cancelados"),
                func.sum(
                    case(
                        [(CFDIORM.Estatus, coalesce(CFDIORM.SubTotalMXN, 0))],
                    )
                ).label("subtotal_mxn"),
                func.sum(
                    case(
                        [(CFDIORM.Estatus, coalesce(CFDIORM.DescuentoMXN, 0))],
                    )
                ).label("descuento_mxn"),
            ]
        )
        .where(
            CFDIORM.is_issued,
            CFDIORM.TipoDeComprobante == "I",
            CFDIORM.FechaFiltro.between(start, end),
        )
        .group_by("mes")
    )
    return Resumen(
        datos={
            row.mes: IngresosNominales(
                vigentes=row.vigentes,
                cancelados=row.cancelados,
                subtotal_mxn=row.subtotal_mxn or 0,
                descuento_mxn=row.descuento_mxn or 0,
            )
            for row in res
        },
        limit=limit,
    )
