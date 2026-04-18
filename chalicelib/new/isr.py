from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import count as SQL_COUNT
from sqlalchemy.sql.functions import sum as SQL_SUM

from chalicelib.new.isr_deducciones import (
    calcular_deducciones_autorizadas_y_compras,
)
from chalicelib.new.iva import IVAGetter
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import CFDI as CFDIORM

ISRResult = dict[str, Any]
INCOMES_ISSUED = True
DEDUCTIONS_ISSUED = False

ISR_NAMES = {
    "all": "",
    "invoice_pue": "Facturas de contado",
    "payments": "CFDI de pagos",
    "moved": "Periodo ISR Reasignado",
    "excluded": "No considerados ISR",
}


@dataclass
class WindowDates:
    tra_start: date
    tra_end: date
    prev_start: date
    current_start: date


def end_month(original: date) -> date:
    new_date = original + timedelta(days=1)
    while new_date.month == original.month:
        new_date += timedelta(days=1)
    new_date -= timedelta(days=1)
    return new_date


@dataclass
class ISRGetter:
    session: Session

    def get_full_filter(self, period, yearly, isr, issued):
        if isr == "all":
            isr_filter = self.get_or_filters(period, yearly, issued)
        elif isr == "moved" or isr == "excluded":
            isr_filter = self.get_filters(period, yearly, issued, CFDIORM.FechaFiltro)[isr]
        else:
            isr_filter = self.get_filters(period, yearly, issued)[isr]
        return self.add_common_filter(isr_filter, issued, isr)

    def get_export_display_name(self, isr, issued):
        issued_names = {
            True: "Ingresos",
            False: "Deducciones",
        }
        name = issued_names[issued]
        name += f" - {ISR_NAMES[isr]}" if ISR_NAMES[isr] else ""
        return name

    @classmethod
    def basic_filter(cls, issued):
        return and_(
            CFDIORM.is_issued.is_(issued),
            CFDIORM.Estatus,
            CFDIORM.Version == "4.0",
        )

    def add_common_filter(self, filter, issued, isr="all"):
        use_excluded = CFDIORM.ExcludeFromISR if isr == "excluded" else ~CFDIORM.ExcludeFromISR
        common_filter = and_(
            self.basic_filter(issued),
            use_excluded,
        )

        return and_(filter, common_filter)

    @staticmethod
    def date_field():
        return CFDIORM.PaymentDate

    def get_filters(
        self,
        period: date,
        yearly: bool,
        is_issued: bool,
        date_field=None,
    ) -> dict[str, Iterable[Any]]:
        dates = IVAGetter.get_window_dates(period, yearly)
        if not date_field:
            date_field = CFDIORM.FechaFiltro
        period_window = date_field.between(dates.period_or_exercise_start, dates.period_end)

        is_pago = CFDIORM.TipoDeComprobante == "P"
        is_ingreso = and_(CFDIORM.TipoDeComprobante == "I", CFDIORM.MetodoPago == "PUE")

        res = {
            "invoice_pue": and_(
                is_ingreso,
                period_window,
            ),
            "payments": and_(
                is_pago,
                period_window,
            ),
        }

        res["excluded"] = and_(
            or_(
                *res.values(),
            ),
            CFDIORM.ExcludeFromISR,
        )
        res["moved"] = and_(
            or_(
                *res.values(),
            ),
            CFDIORM.is_moved,
        )
        return res

    def get_or_filters(
        self, period: date, yearly: bool, is_issued: bool, date_field=None
    ) -> Iterable[Any]:
        filters = self.get_filters(period, yearly, is_issued, date_field=date_field)
        return or_(*filters.values())

    def _get_isr(self, period: date, issued: bool, yearly=False) -> float:
        def _get_isr_component(
            total_fields: tuple[Any], filters, *, info_fields: tuple[Any] = None
        ):
            info_fields = info_fields or ()
            all_fields = total_fields + info_fields
            row = (
                self.session.query(
                    *(SQL_SUM(field).label(field.key) for field in all_fields),
                    SQL_COUNT().label("qty"),
                )
                .filter(*filters)
                .one()
            )
            res = {
                field.key: float(round((getattr(row, field.key) or 0), 2)) for field in all_fields
            }
            res["qty"] = row.qty or 0
            res["total"] = round(sum(float(res[field.key]) for field in total_fields), 2)
            return res

        def _get_excluded_qty(issued, period: date, yearly=False):
            filters = self.get_or_filters(period, yearly, issued, date_field=CFDIORM.FechaFiltro)
            filters = and_(filters, self.basic_filter(issued))

            query = self.session.query(func.count()).filter(
                *filters,
                CFDIORM.ExcludeFromISR,
            )
            return query.scalar()

        common_fields = (
            CFDIORM.BaseIVA16,
            CFDIORM.BaseIVA8,
            CFDIORM.BaseIVA0,
            CFDIORM.BaseIVAExento,
        )
        filters = self.get_filters(period, yearly, issued)
        for iva_type, iva_filter in filters.items():
            filters[iva_type] = self.add_common_filter(
                iva_filter,
                issued,
            )

        retenciones = (CFDIORM.RetencionesISRMXN,)

        components = {
            "invoice_pue": _get_isr_component(
                common_fields,
                filters=filters["invoice_pue"],
                info_fields=retenciones,
            ),
            "payments": _get_isr_component(
                common_fields,
                filters=filters["payments"],
                info_fields=retenciones,
            ),
        }

        def _get_totals(components):
            return {
                "total": sum(c["total"] for c in components.values()),
                "qty": sum(c["qty"] for c in components.values()),
            }

        totals = _get_totals(components)
        components.update(totals)
        components["excluded_qty"] = _get_excluded_qty(issued, period, yearly)
        return components

    def get_isr(self, period: date, company: Company) -> ISRResult:
        data = calcular_deducciones_autorizadas_y_compras(
            session=self.session, period=period, company=company
        )

        def get_time_window(window_start: date, yearly=False):
            incomes = self._get_isr(
                period=period,
                issued=INCOMES_ISSUED,
                yearly=yearly,
            )
            deductions = data
            return {
                "incomes": incomes,
                "deductions": deductions,
            }

        period_res = get_time_window(period)
        exercise_res = get_time_window(period, yearly=True)

        return {
            "period": period_res,
            "exercise": exercise_res,
        }
