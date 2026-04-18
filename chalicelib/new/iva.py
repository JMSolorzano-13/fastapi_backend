from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import count as SQL_COUNT
from sqlalchemy.sql.functions import sum as SQL_SUM

from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM

IVAResult = dict[str, Any]
CREDITABLE_ISSUED = False
TRANSFERRED_ISSUED = True

IVA_NAMES = {
    "all": "",
    "i_tra": "Facturas de contado",  # TODO i18n
    "p_tra": "Facturas de crédito",  # TODO i18n
    "totals": "Totales",
    "moved": "Periodo IVA Reasignado",
    "excluded": "No considerados IVA",
    "credit_notes": "Notas de crédito",
    "OpeConTer": "Operaciones con terceros",
}


def need_prev(issued: bool) -> bool:
    return issued == CREDITABLE_ISSUED


@dataclass
class WindowDates:
    period_or_exercise_start: date
    period_end: date
    prev_start: date
    prev_end: date
    period_start: date


def end_month(original: date) -> date:
    new_date = original + timedelta(days=1)
    while new_date.month == original.month:
        new_date += timedelta(days=1)
    new_date -= timedelta(days=1)
    return new_date


@dataclass
class IVAGetter:
    company_session: Session

    def get_full_filter(self, period, yearly, iva, issued):
        if iva == "all":
            iva_filter = self.get_or_filters(period, yearly, issued)
        elif iva == "moved" or iva == "excluded" or (iva == "moved" or iva == "excluded"):
            iva_filter = self.get_filters(period, yearly, issued, CFDIORM.FechaFiltro)[iva]
        else:
            iva_filter = self.get_filters(period, yearly, issued)[iva]
        return self.add_common_filter(iva_filter, issued, iva)

    def get_export_display_name(self, period, yearly, iva, issued):
        issued_names = {
            True: "Trasladado",
            False: "Acreditable",
        }
        cobro_pago = "Cobro " if issued else "Pago "
        cobro_pago = cobro_pago if "crédito" in IVA_NAMES[iva] else ""
        name = issued_names[issued]
        name += f" - {cobro_pago}{IVA_NAMES[iva]}" if IVA_NAMES[iva] else ""
        return name

    @classmethod
    def basic_filter(cls, issued):
        return and_(
            CFDIORM.is_issued.is_(issued),
            CFDIORM.Estatus,
            CFDIORM.Version == "4.0",
        )

    def add_common_filter(self, filter, issued, iva="all"):
        use_excluded = CFDIORM.ExcludeFromIVA if iva == "excluded" else ~CFDIORM.ExcludeFromIVA
        common_filter = and_(
            self.basic_filter(issued),
            use_excluded,
        )
        return and_(filter, common_filter)

    @classmethod
    def get_window_dates(cls, period: date, yearly: bool) -> WindowDates:
        period_or_exercise_start = period.replace(month=1, day=1) if yearly else period
        period_end = end_month(period)
        prev_start = (period_or_exercise_start - timedelta(days=1)).replace(day=1)
        prev_end = end_month(prev_start)
        period_start = period.replace(day=1)
        return WindowDates(
            period_or_exercise_start=period_or_exercise_start,
            period_end=period_end,
            prev_start=prev_start,
            prev_end=prev_end,
            period_start=period_start,
        )

    @staticmethod
    def iva_date_field():
        # CFDIORM.FechaFiltro
        return CFDIORM.PaymentDate

    def get_filters(
        self,
        period: date,
        yearly: bool,
        is_issued: bool,
        date_field=None,
    ) -> dict[str, Iterable[Any]]:
        dates = self.get_window_dates(period, yearly)
        if not date_field:
            date_field = self.iva_date_field()
        period_or_exercise_window = date_field.between(
            dates.period_or_exercise_start, dates.period_end
        )
        period_window = date_field.between(dates.period_start, dates.period_end)

        rel_docto_period_or_exercise_window = DoctoRelacionadoORM.FechaPago.between(
            dates.period_or_exercise_start, dates.period_end
        )
        rel_docto_window = DoctoRelacionadoORM.FechaPago.between(
            dates.period_start, dates.period_end
        )

        is_pago = CFDIORM.TipoDeComprobante == "P"
        is_ingreso = and_(
            CFDIORM.TipoDeComprobante == "I",
            CFDIORM.MetodoPago == "PUE",
        )

        is_egreso = CFDIORM.TipoDeComprobante == "E"

        res = {
            "i_tra": and_(
                is_ingreso,
                period_or_exercise_window,
            ),
            "p_tra": (
                and_(
                    is_pago,
                    period_or_exercise_window,
                )
                if is_issued
                else rel_docto_period_or_exercise_window
            ),
            "credit_notes": and_(
                is_egreso,
                period_or_exercise_window,
            ),
            "curr_i_ret": and_(
                is_ingreso,
                period_window,
            ),
            "curr_p_ret": (
                and_(
                    is_pago,
                    period_window,
                )
                if is_issued
                else rel_docto_window
            ),
        }

        res["excluded"] = and_(or_(res["i_tra"], res["credit_notes"], res["p_tra"]))

        res["moved"] = and_(
            or_(res["i_tra"], res["credit_notes"], res["p_tra"])
            if is_issued
            else or_(res["i_tra"], res["credit_notes"]),
        )
        return res

    def get_or_filters(
        self, period: date, yearly: bool, is_issued: bool, date_field=None
    ) -> Iterable[Any]:
        filters = self.get_filters(period, yearly, is_issued, date_field=date_field)
        return or_(*filters.values())

    def _get_iva(self, period: date, issued: bool, yearly=False) -> IVAResult:
        def _get_row_data(fields, *filters):
            row = (
                self.company_session.query(
                    *(SQL_SUM(field).label(field.key) for field in fields),
                    SQL_COUNT().label("qty"),
                )
                .filter(*filters)
                .one()
            )
            return row

        def _get_docto_data(fields, *filters):
            row = (
                self.company_session.query(
                    *(SQL_SUM(field).label(field.key) for field in fields),
                    SQL_COUNT(1).label("qty"),
                )
                .select_from(DoctoRelacionadoORM)
                .join(
                    CFDIORM,
                    DoctoRelacionadoORM.UUID_related == CFDIORM.UUID,
                )
                .filter(
                    ~DoctoRelacionadoORM.ExcludeFromIVA,
                    ~DoctoRelacionadoORM.is_issued,
                    DoctoRelacionadoORM.Estatus,
                    CFDIORM.TipoDeComprobante == "I",
                    or_(
                        CFDIORM.from_xml,
                        CFDIORM.is_too_big,
                    ),
                    ~CFDIORM.is_issued,
                    CFDIORM.Estatus,
                    *filters,
                )
                .one()
            )

            return row

        def _get_iva_component_credit_notes(fields, *filters):
            row = _get_row_data(fields, *filters)
            res = {field.key: float(round((getattr(row, field.key) or 0), 2)) for field in fields}
            res["qty"] = row.qty or 0
            new_total = float(
                round(
                    res["IVATrasladado16"] + res["IVATrasladado8"],
                    2,
                )
            )
            res["total"] = new_total
            return res

        def _get_iva_component_doctos(fields, *filters):
            row = _get_docto_data(fields, *filters)
            res = {field.key: float(round((getattr(row, field.key) or 0), 2)) for field in fields}
            res["qty"] = row.qty or 0
            res["total"] = res["IVATrasladado16"] + res["IVATrasladado8"]
            return res

        def _get_iva_component(fields, *filters):
            row = _get_row_data(fields, *filters)
            res = {field.key: float(round((getattr(row, field.key) or 0), 2)) for field in fields}
            res["qty"] = row.qty or 0
            first_key = fields[0].key
            res["total"] = res[first_key]
            return res

        def _get_excluded_qty(issued, period: date, yearly=False):
            if issued:
                all_filters = self.get_filters(period, yearly, issued, CFDIORM.FechaFiltro)
                filters = all_filters["excluded"]
                filters = and_(filters, self.basic_filter(issued))

                query = self.company_session.query(func.count()).filter(
                    *filters,
                    CFDIORM.ExcludeFromIVA,
                )
                return query.scalar()
            else:
                dates = self.get_window_dates(period, yearly)

                cfdis_excluded = (
                    self.company_session.query(func.count())
                    .filter(
                        self.basic_filter(issued),
                        CFDIORM.ExcludeFromIVA,
                        CFDIORM.TipoDeComprobante.in_(["I", "E"]),
                        CFDIORM.FechaFiltro.between(
                            dates.period_or_exercise_start, dates.period_end
                        ),
                    )
                    .scalar()
                )

                doctos_excluded = (
                    self.company_session.query(
                        func.count(),
                    )
                    .filter(
                        DoctoRelacionadoORM.is_issued.is_(issued),
                        DoctoRelacionadoORM.cfdi_related,
                        DoctoRelacionadoORM.ExcludeFromIVA,
                        DoctoRelacionadoORM.Estatus,
                        DoctoRelacionadoORM.FechaPago.between(
                            dates.period_or_exercise_start, dates.period_end
                        ),
                    )
                    .scalar()
                )
                return cfdis_excluded + doctos_excluded

        def _get_moved_qty(issued, period: date, yearly=False):
            all_filters = self.get_filters(period, yearly, issued, CFDIORM.FechaFiltro)
            filters = all_filters["moved"]
            filters = self.add_common_filter(filters, issued)
            query = self.company_session.query(func.count()).filter(
                *filters,
                CFDIORM.is_moved,
            )
            return query.scalar()

        common_fields = (
            CFDIORM.BaseIVA16,
            CFDIORM.BaseIVA8,
            CFDIORM.BaseIVA0,
            CFDIORM.BaseIVAExento,
            CFDIORM.IVATrasladado16,
            CFDIORM.IVATrasladado8,
            CFDIORM.pr_count.label("PaymentRelatedCount"),
        )

        docto_relacionado_fields = (
            DoctoRelacionadoORM.BaseIVA16,
            DoctoRelacionadoORM.BaseIVA8,
            DoctoRelacionadoORM.BaseIVA0,
            DoctoRelacionadoORM.BaseIVAExento,
            DoctoRelacionadoORM.IVATrasladado16,
            DoctoRelacionadoORM.IVATrasladado8,
            DoctoRelacionadoORM.RetencionesIVAMXN,
        )

        iva_filters = self.get_filters(period, yearly, issued)

        for iva_type, iva_filter in iva_filters.items():
            if not issued and iva_type in ("p_tra", "curr_p_ret"):
                pass
            else:
                iva_filters[iva_type] = self.add_common_filter(
                    iva_filter,
                    issued,
                )

        traslados = (CFDIORM.TrasladosIVAMXN,)
        retenciones = (CFDIORM.RetencionesIVAMXN,)

        components = {
            "i_tra": _get_iva_component(
                traslados + common_fields,
                iva_filters["i_tra"],
            ),
            "p_tra": _get_iva_component(
                traslados + common_fields,
                iva_filters["p_tra"],
            )
            if issued
            else _get_iva_component_doctos(
                docto_relacionado_fields,
                iva_filters["p_tra"],
            ),
            "credit_notes": _get_iva_component_credit_notes(
                retenciones + common_fields + traslados,
                iva_filters["credit_notes"],
            ),
            # TODO: check if need in future
            "curr_i_ret": _get_iva_component(
                retenciones,
                iva_filters["curr_i_ret"],
            ),
            "curr_p_ret": _get_iva_component(
                retenciones,
                iva_filters["curr_p_ret"],
            )
            if issued
            else _get_iva_component_doctos(
                docto_relacionado_fields,
                iva_filters["curr_p_ret"],
            ),
        }

        def _get_totals(components):
            ignore = {
                "credit_notes",
            }
            return {
                "total": sum(c["total"] for k, c in components.items() if k not in ignore),
                "qty": sum(c["qty"] for c in components.values()),
            }

        curr_i_ret = components.pop("curr_i_ret")
        components["i_tra"][CFDIORM.RetencionesIVAMXN.key] = curr_i_ret["total"]
        curr_p_ret = components.pop("curr_p_ret")
        components["p_tra"][DoctoRelacionadoORM.RetencionesIVAMXN.key] = curr_p_ret[
            "RetencionesIVAMXN"
        ]  # add is_issued_condition

        totals = _get_totals(components)
        components.update(totals)
        components["excluded_qty"] = _get_excluded_qty(issued, period, yearly)
        components["moved_qty"] = _get_moved_qty(issued, period, yearly)
        return components

    def get_time_window(self, period: date, yearly=False):
        creditable = self._get_iva(
            period=period,
            issued=CREDITABLE_ISSUED,
            yearly=yearly,
        )
        transferred = self._get_iva(
            period=period,
            issued=TRANSFERRED_ISSUED,
            yearly=yearly,
        )
        return {
            "creditable": creditable,
            "transferred": transferred,
            "diff": transferred["total"] - creditable["total"],
        }

    def get_iva(self, period: date) -> IVAResult:
        period_res = self.get_time_window(period)
        exercise_res = self.get_time_window(period, yearly=True)

        return {"period": period_res, "exercise": exercise_res}
