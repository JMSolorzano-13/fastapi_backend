"""CFDI routes — search, resume, IVA/ISR calculations, exports.

Ported from: backend/chalicelib/blueprints/cfdi/__init__.py
             backend/chalicelib/blueprints/cfdi/emitidos_ingresos_anio_mes_resumen.py
22 routes total (core business domain).
"""

import asyncio
import uuid
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Body, Depends
from pydantic import ConfigDict, validate_call
from sqlalchemy.orm import Session

from chalicelib.bus import get_global_bus
from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.cfdi_export import CfdiExportController
from chalicelib.controllers.enums import ResumeType
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import (
    CFDIExportRepositorySA,
)
from chalicelib.new.isr import ISRGetter
from chalicelib.new.isr_deducciones import (
    build_total_deducciones_cfdi_query,
    build_total_deducciones_pagos_query,
    calcular_totales_nomina_data,
)
from chalicelib.new.isr_exportaciones import (
    ISR_CFDI,
    _export_isr_generic,
    create_export_record,
    export_total_isr_page,
    save_export_to_s3,
)
from chalicelib.new.iva import IVAGetter
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport as ExportRequestORM
from chalicelib.schema.models.user import User
from dependencies import (
    common,
    get_company,
    get_company_identifier_rw,
    get_company_rw,
    get_company_session,
    get_company_session_rw,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
    get_json_body,
)
from exceptions import BadRequestError, ForbiddenError
from helpers.cfdi_resumen import (
    Resumen,
)
from helpers.cfdi_resumen import (
    emitidos_ingresos_anio_mes_resumen as _emitidos_ingresos_anio_mes_resumen,
)

router = APIRouter(tags=["CFDI"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, CFDIController, session=company_session)


@router.post("/export")
def export(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return common.export(json_body, CFDIController, company_session=company_session, user=user)


@router.post("/massive_export")
def massive_export(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session_rw),
    company_identifier: Identifier = Depends(get_company_identifier_rw),
):
    json_body = common.massive_export(json_body, CFDIController, session=company_session)
    bus = get_global_bus()
    export_class = CFDIExporter(
        company_session=company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
        bus=bus,
    )
    result = export_class.export_event(json_body=json_body, company_identifier=company_identifier)
    return result


@router.post("/export_iva")
def export_iva(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    period = datetime.fromisoformat(body["period"])
    yearly = body["yearly"]
    iva = body["iva"]
    issued = body["issued"]
    company_identifier = body["company_identifier"]
    export_data = body["export_data"]

    export_class = CFDIExporter(
        company_session=company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
        bus=get_global_bus(),
    )
    iva_getter = IVAGetter(company_session=company_session)
    if iva == "OpeConTer":
        export_filter = None
    else:
        export_filter = iva_getter.get_full_filter(period, yearly, iva, issued)
    displayed_name = iva_getter.get_export_display_name(period, yearly, iva, issued)
    export = export_class.publish_export(
        company_identifier=company_identifier,
        period=period,
        displayed_name=displayed_name,
        export_filter=export_filter,
        export_data_type=ExportRequestORM.ExportDataType.IVA,
        format="XLSX",
        is_issued=issued,
        is_yearly=yearly,
        export_data=export_data,
        json_body=body,
    )
    return {"export_identifier": export}


@router.post("/get_export_cfdi")
def get_export_cfdi(
    body: dict = Body(...),
    session: Session = Depends(get_db_session),
):
    export_identifier = body.get("cfdi_export_identifier", "")
    cfdi_export_repository = CfdiExportController.get_cfdi_export_repository(session=session)
    res = cfdi_export_repository.get_by_identifier(export_identifier)
    return {
        "identifier": res.identifier,
        "url": res.url,
        "expiration_date": str(res.expiration_date),
    }


@router.post("/get_exports")
def get_exports(
    body: dict = Body(...),
    session: Session = Depends(get_db_session),
):
    company_identifier = body.get("company_identifier", "")
    cfdi_export_repository = CfdiExportController.get_cfdi_export_repository(session=session)
    exports = cfdi_export_repository.get_records_by_company(company_identifier)
    exports_list = []
    for res in exports:
        state = str(res.state).replace("CfdiExportState.", "")
        exports_list.append(
            {
                "created_at": str(res.created_at),
                "identifier": str(res.identifier),
                "url": res.url,
                "expiration_date": str(res.expiration_date),
                "company_identifier": str(res.company_identifier),
                "start": str(res.start),
                "end": str(res.end),
                "cfdi_type": str(res.cfdi_type),
                "state": state,
                "format": res.format,
                "download_type": res.download_type,
            }
        )
    return exports_list


@router.post("/get_xml")
def get_xml():
    raise ForbiddenError("Not yet implemented")


@router.post("/get_by_period")
def get_by_period(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    domain = body.get("domain", [])
    return CFDIController.get_by_period(domain, session=company_session)


@router.post("/resume")
def resume(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    domain = body.get("domain", [])
    fuzzy_search = body.get("fuzzy_search", [])
    resume_type = body.get("TipoDeComprobante", ResumeType.BASIC.name)
    resume_type = ResumeType[resume_type]
    log(
        Modules.RESUME,
        DEBUG,
        "RESUME",
        {
            "endpoint": "CFDI/resume",
            "company_identifier": body["domain"][0][2],
            "body": body,
        },
    )
    return CFDIController.resume(
        domain,
        fuzzy_search,
        resume_type=resume_type,
        session=company_session,
    )


@router.post("/get_count_cfdis")
def get_count_cfdis(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    domain = body.get("domain", [])
    fuzzy_search = body.get("fuzzy_search", [])
    return CFDIController.count_cfdis_by_type(
        domain,
        fuzzy_search,
        session=company_session,
    )


@router.post("/get_iva")
def get_iva(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    period = body.get("period")
    if not period:
        raise BadRequestError("period is required")
    period = date.fromisoformat(period)

    log(
        Modules.IVA,
        DEBUG,
        "GET_IVA",
        {
            "endpoint": "CFDI/get_iva",
            "period": period,
            "body": body,
        },
    )
    getter = IVAGetter(company_session)
    return getter.get_iva(period)


# -- Async helpers for get_iva_all (ported from Chalice) --


def _get_monthly_period_data_with_session(getter, year, month, exercise):
    monthly_period = date(year, month, 1)
    period_data = getter.get_time_window(monthly_period, yearly=False)
    return f"{month:02}", {"period": period_data, "exercise": exercise}


async def _get_all_periods_concurrent(getter, year, end_month, exercise):
    tasks = []
    for month in range(1, end_month + 1):
        task = asyncio.create_task(
            asyncio.to_thread(_get_monthly_period_data_with_session, getter, year, month, exercise)
        )
        tasks.append(task)
    results_list = await asyncio.gather(*tasks)
    return {month_key: month_data for month_key, month_data in results_list}


@router.post("/get_iva_all")
async def get_iva_all(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    period_str = body["period"]
    period_date = date.fromisoformat(period_str)
    year = period_date.year
    end_month = period_date.month

    getter = IVAGetter(company_session=company_session)
    exercise = getter.get_time_window(period_date, yearly=True)
    results = await _get_all_periods_concurrent(getter, year, end_month, exercise)
    return results


@router.post("/get_isr")
def get_isr(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
    company: Company = Depends(get_company),
):
    period = body["period"]
    period = date.fromisoformat(period)
    getter = ISRGetter(company_session)
    return getter.get_isr(period, company)


@router.post("/search_iva")
def search_iva(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session),
):
    search_attrs = common.get_search_attrs(body)

    period = body["period"]
    period = date.fromisoformat(period)
    yearly = body.get("yearly", False)
    is_issued = body.get("is_issued", False)
    date_field_str = body.get("date_field")

    date_field = None
    if date_field_str:
        date_field = getattr(CFDIORM, date_field_str)

    getter = IVAGetter(company_session)
    internal_domain = getter.get_or_filters(period, yearly, is_issued, date_field)
    log(
        Modules.IVA,
        DEBUG,
        "SEARCH_IVA",
        {
            "endpoint": "CFDI/search_iva",
            "company_identifier": body["domain"][0][2],
            "body": body,
        },
    )
    pos, next_page, total_records = CFDIController.search(
        **search_attrs, internal_domain=internal_domain, session=company_session
    )
    dict_repr = CFDIController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }


@router.post("/update")
def update(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    company_identifier: Identifier = Depends(get_company_identifier_rw),
):
    cfdis: dict[Identifier, dict[str, Any]] = body["cfdis"]
    cfdi_controller = CFDIController()
    model_keys = {"company_identifier": company_identifier}
    cfdi_controller.update_multiple(records=cfdis, session=company_session, model_keys=model_keys)
    return {"message": "ok"}


@router.post("/export_isr")
def export_isr(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    company: Company = Depends(get_company_rw),
):
    period = datetime.fromisoformat(body["period"])
    yearly = body["yearly"]
    isr = body["isr"]
    issued = body["issued"]
    company_identifier = body["company_identifier"]
    export_data = body["export_data"]

    export_class = CFDIExporter(
        company_session=company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
        bus=get_global_bus(),
    )
    isr_getter = ISRGetter(session=company_session)
    export_filter = isr_getter.get_full_filter(period, yearly, isr, issued)
    display_name = isr_getter.get_export_display_name(isr, issued)
    export = export_class.publish_export(
        company_identifier=company_identifier,
        period=period,
        displayed_name=display_name,
        export_filter=export_filter,
        export_data_type=ExportRequestORM.ExportDataType.ISR,
        format="XLSX",
        is_issued=issued,
        is_yearly=yearly,
        export_data=export_data,
        json_body=body,
    )
    return {"export_identifier": export}


@router.post("/total_deducciones_cfdi")
def total_deducciones_cfdi(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    domain = body.get("domain", [])
    fields = body.get("fields", [])

    log(
        Modules.RESUME,
        DEBUG,
        "TOTAL_FACTURAS_CONTADO_ISR",
        {
            "endpoint": "CFDI/total_deducciones_cfdi",
            "company_identifier": domain[0][2] if domain else None,
            "body": body,
        },
    )

    query = build_total_deducciones_cfdi_query(company_session, domain, fields)
    result = query.first()

    if not result:
        return {field: 0 for field in ["ConteoCFDIs"] + fields}

    return {
        "ConteoCFDIs": result[0] or 0,
        **{field: result[i + 1] or 0 for i, field in enumerate(fields)},
    }


@router.post("/total_deducciones_pagos")
def total_deducciones_pagos(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    domain = body.get("domain", [])
    fields = body.get("fields", [])

    log(
        Modules.RESUME,
        DEBUG,
        "TOTAL_DEDUCCIONES_PAGOS_ISR",
        {
            "endpoint": "CFDI/total_deducciones_pagos",
            "company_identifier": domain[0][2] if domain else None,
            "body": body,
        },
    )

    query = build_total_deducciones_pagos_query(company_session, domain, fields)
    result = query.first()

    if not result:
        return {field: 0 for field in ["ConteoCFDIs"] + fields}

    return {
        "ConteoCFDIs": result[0] or 0,
        **{field: result[i + 1] or 0 for i, field in enumerate(fields)},
    }


@router.post("/totales")
def calcular_totales_nomina(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    session: Session = Depends(get_db_session_rw),
    company: Company = Depends(get_company_rw),
):
    period = body["period"]
    domain = date.fromisoformat(period)
    return calcular_totales_nomina_data(company_session, session, company, domain)


@router.post("/export_isr_totales")
def export_isr_totales(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    session: Session = Depends(get_db_session_rw),
    company: Company = Depends(get_company_rw),
):
    period_date = datetime.fromisoformat(body.get("period")).date()
    isr_data = calcular_totales_nomina_data(company_session, session, company, period_date)

    workbook_bytes = export_total_isr_page(isr_data)
    export_request = create_export_record(company_session, body)
    save_export_to_s3(company_session, workbook_bytes, export_request, body["export_data"])

    return {"export_identifier": export_request.identifier}


@router.post("/export_isr_cfdi")
def export_isr_cfdi(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    workbook_bytes = _export_isr_generic(
        company_session=company_session,
        export_payload=body,
        controller_class=CFDIController,
        column_mapping=ISR_CFDI,
        total_key="total_cfdi",
        build_total_query_func=build_total_deducciones_cfdi_query,
    )

    export_request = create_export_record(company_session, body)
    save_export_to_s3(company_session, workbook_bytes, export_request, body["export_data"])

    return {"export_identifier": export_request.identifier}


# ---------------------------------------------------------------------------
# Emitidos / Ingresos resume (from emitidos_ingresos_anio_mes_resumen.py)
# ---------------------------------------------------------------------------


@router.get("/{cid}/emitidos/ingresos/{anio}/{mes}/resumen")
@validate_call(validate_return=True, config=ConfigDict(arbitrary_types_allowed=True))
def emitidos_ingresos_anio_mes_resumen(
    cid: uuid.UUID,
    anio: int,
    mes: int,
    company_session: Session = Depends(get_company_session),
) -> Resumen:
    return _emitidos_ingresos_anio_mes_resumen(company_session=company_session, anio=anio, mes=mes)
