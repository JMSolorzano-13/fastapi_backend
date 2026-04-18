import asyncio
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from chalice import BadRequestError, ForbiddenError, Response
from pydantic import BaseModel, Field, computed_field, validate_call
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.bus import get_global_bus
from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.cfdi_export import CfdiExportController
from chalicelib.controllers.company import CompanyController
from chalicelib.controllers.enums import ResumeType
from chalicelib.controllers.tenant.utils import (
    company_from_identifier,
    tenant_url_from_identifier,
)
from chalicelib.controllers.user import UserController
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import (
    CFDIExportRepositorySA,
)
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
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
from chalicelib.new.utils.session import with_session
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport as ExportRequestORM
from chalicelib.schema.models.user import User

from . import emitidos_ingresos_anio_mes_resumen
from .bp import bp


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, CFDIController, session=company_session)


@bp.route("/export", methods=["POST"], cors=common.cors_config, read_only=False)
def export(company_session: Session, user: User):
    return common.export(bp, CFDIController, company_session=company_session, user=user)


@bp.route("/massive_export", methods=["POST"], cors=common.cors_config, read_only=False)
def massive_export(company_session: Session, company_identifier: Identifier):
    json_body = common.massive_export(bp, CFDIController, session=company_session)
    export_class = CFDIExporter(
        company_session=company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
        bus=get_global_bus(),
    )
    return export_class.export_event(json_body=json_body, company_identifier=company_identifier)


@bp.route("/export_iva", methods=["POST"], cors=common.cors_config, read_only=False)
def export_iva(company_session: Session):
    json_body = bp.current_request.json_body or {}
    period = datetime.fromisoformat(json_body["period"])
    yearly = json_body["yearly"]
    iva = json_body["iva"]
    issued = json_body["issued"]
    company_identifier = json_body["company_identifier"]
    export_data = json_body["export_data"]

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
        json_body=json_body,
    )
    return json.dumps({"export_identifier": export})


@bp.route("/get_export_cfdi", methods=["POST"], cors=common.cors_config)
def get_export_cfdi(session: Session):
    json_body = bp.current_request.json_body or {}
    export_identifier = json_body.get("cfdi_export_identifier", "")
    cfdi_export_repository = CfdiExportController.get_cfdi_export_repository(session=session)
    res = cfdi_export_repository.get_by_identifier(export_identifier)

    return json.dumps(
        {
            "identifier": res.identifier,
            "url": res.url,
            "expiration_date": str(res.expiration_date),
        }
    )


@bp.route("/get_exports", methods=["POST"], cors=common.cors_config)
def get_exports(session: Session):
    json_body = bp.current_request.json_body or {}
    company_identifier = json_body.get("company_identifier", "")
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
    return json.dumps(exports_list)


@bp.route("/get_xml", methods=["POST"], cors=common.cors_config)
def get_xml():
    # TODO implement
    raise ForbiddenError("Not yet implemented")


@bp.route("/get_by_period", methods=["POST"], cors=common.cors_config)
def get_by_period(company_session: Session):
    json_body = bp.current_request.json_body or {}

    domain = json_body.get("domain", [])
    return CFDIController.get_by_period(domain, session=company_session)


@bp.route("/resume", methods=["POST"], cors=common.cors_config)
def resume(company_session: Session):
    json_body = bp.current_request.json_body or {}

    domain = json_body.get("domain", [])
    fuzzy_search = json_body.get("fuzzy_search", [])
    resume_type = json_body.get("TipoDeComprobante", ResumeType.BASIC.name)
    resume_type = ResumeType[resume_type]
    log(
        Modules.RESUME,
        DEBUG,
        "RESUME",
        {
            "endpoint": "CFDI/resume",
            "company_identifier": json_body["domain"][0][2],
            "body": json_body,
        },
    )
    return CFDIController.resume(
        domain,
        fuzzy_search,
        resume_type=resume_type,
        session=company_session,
    )


@bp.route("/get_count_cfdis", methods=["POST"], cors=common.cors_config)
def get_count_cfdis(company_session: Session):
    json_body = bp.current_request.json_body or {}

    domain = json_body.get("domain", [])
    fuzzy_search = json_body.get("fuzzy_search", [])
    return CFDIController.count_cfdis_by_type(
        domain,
        fuzzy_search,
        session=company_session,
    )


@bp.route("/get_iva", methods=["POST"], cors=common.cors_config)
def get_iva(company_session: Session):
    json_body = bp.current_request.json_body or {}
    period = json_body.get("period")

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
            "body": json_body,
        },
    )
    getter = IVAGetter(company_session)
    return getter.get_iva(period)


def _get_monthly_period_data_with_session(getter, year, month, exercise):
    monthly_period = date(year, month, 1)
    period_data = getter.get_time_window(monthly_period, yearly=False)

    return f"{month:02}", {"period": period_data, "exercise": exercise}


async def _get_all_periods_concurrent(getter, year, end_month, exercise):
    # Create tasks for all months
    tasks = []
    for month in range(1, end_month + 1):
        task = asyncio.create_task(
            asyncio.to_thread(_get_monthly_period_data_with_session, getter, year, month, exercise)
        )
        tasks.append(task)

    # Execute all tasks concurrently
    results_list = await asyncio.gather(*tasks)

    # Convert list of tuples to dictionary
    results = {}
    for month_key, month_data in results_list:
        results[month_key] = month_data

    return results


@bp.route("/get_iva_all", methods=["POST"], cors=common.cors_config)
def get_iva_all(company_session: Session):
    json_body = bp.current_request.json_body or {}
    period_str = json_body["period"]

    period_date = date.fromisoformat(period_str)

    year = period_date.year
    end_month = period_date.month

    getter = IVAGetter(company_session=company_session)

    # Get exercise data once (yearly calculation)
    exercise = getter.get_time_window(period_date, yearly=True)

    # Get all monthly periods concurrently
    results = asyncio.run(_get_all_periods_concurrent(getter, year, end_month, exercise))

    return results


@bp.route("/get_isr", methods=["POST"], cors=common.cors_config)
def get_isr(company_session: Session, company: Company):
    json_body = bp.current_request.json_body or {}
    period = json_body["period"]
    period = date.fromisoformat(period)

    getter = ISRGetter(company_session)
    return getter.get_isr(period, company)


@bp.route("/search_iva", methods=["POST"], cors=common.cors_config)
def search_iva(company_session: Session):
    json_body = bp.current_request.json_body or {}

    search_attrs = common.get_search_attrs(json_body)

    # Custom
    period = json_body["period"]
    period = date.fromisoformat(period)
    yearly = json_body.get("yearly", False)
    is_issued = json_body.get("is_issued", False)
    date_field_str = json_body.get("date_field")

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
            "company_identifier": json_body["domain"][0][2],
            "body": json_body,
        },
    )
    pos, next_page, total_records = CFDIController.search(
        **search_attrs, internal_domain=internal_domain, session=company_session
    )
    # End custom
    dict_repr = CFDIController.to_nested_dict(pos)
    return {
        "data": dict_repr,
        "next_page": next_page,
        "total_records": total_records,
    }


@bp.route("/update", methods=["POST"], cors=common.cors_config, read_only=False)
def update(company_session: Session, company_identifier: Identifier):
    json_body = bp.current_request.json_body or {}
    cfdis: dict[Identifier, dict[str, Any]] = json_body["cfdis"]

    cfdi_controller = CFDIController()
    model_keys = {
        "company_identifier": company_identifier,
    }
    cfdi_controller.update_multiple(records=cfdis, session=company_session, model_keys=model_keys)
    return Response(body=json.dumps({"message": "ok"}), status_code=200)


@bp.route("/export_isr", methods=["POST"], cors=common.cors_config, read_only=False)
def export_isr(company_session: Session, company: Company):
    json_body = bp.current_request.json_body or {}

    period = datetime.fromisoformat(json_body["period"])
    yearly = json_body["yearly"]
    isr = json_body["isr"]
    issued = json_body["issued"]
    company_identifier = json_body["company_identifier"]
    export_data = json_body["export_data"]
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
        json_body=json_body,
    )
    return json.dumps({"export_identifier": export})


@bp.route("/total_deducciones_cfdi", methods=["POST"], cors=common.cors_config, read_only=False)
def total_deducciones_cfdi(company_session: Session):
    json_body = bp.current_request.json_body or {}

    domain = json_body.get("domain", [])
    fields = json_body.get("fields", [])

    log(
        Modules.RESUME,
        DEBUG,
        "TOTAL_FACTURAS_CONTADO_ISR",
        {
            "endpoint": "CFDI/total_deducciones_cfdi",
            "company_identifier": domain[0][2] if domain else None,
            "body": json_body,
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


@bp.route("/total_deducciones_pagos", methods=["POST"], cors=common.cors_config, read_only=False)
def total_deducciones_pagos(company_session: Session):
    json_body = bp.current_request.json_body or {}

    domain = json_body.get("domain", [])
    fields = json_body.get("fields", [])

    log(
        Modules.RESUME,
        DEBUG,
        "TOTAL_DEDUCCIONES_PAGOS_ISR",
        {
            "endpoint": "CFDI/total_deducciones_pagos",
            "company_identifier": domain[0][2] if domain else None,
            "body": json_body,
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


@bp.route("/totales", methods=["POST"], cors=common.cors_config, read_only=False)
def calcular_totales_nomina(company_session: Session, session: Session, company: Company):
    json_body = bp.current_request.json_body or {}
    period = json_body["period"]
    domain = date.fromisoformat(period)

    return calcular_totales_nomina_data(company_session, session, company, domain)


@bp.route("/export_isr_totales", methods=["POST"], cors=common.cors_config, read_only=False)
def export_isr_totales(company_session: Session, session: Session, company: Company):
    """Endpoint para exportación de totales de nómina ISR únicamente"""
    json_body = bp.current_request.json_body or {}

    period_date = datetime.fromisoformat(json_body.get("period")).date()
    isr_data = calcular_totales_nomina_data(company_session, session, company, period_date)

    # GENERAR WORKBOOK BYTES
    workbook_bytes = export_total_isr_page(isr_data)

    # GENERAR REGISTRO EN DB CfdiExport
    export_request = create_export_record(company_session, json_body)

    # GUARDAR EN S3
    save_export_to_s3(company_session, workbook_bytes, export_request, json_body["export_data"])

    # Return export identifier
    return {
        "export_identifier": export_request.identifier,
    }


@bp.route("/export_isr_cfdi", methods=["POST"], cors=common.cors_config, read_only=False)
def export_isr_cfdi(company_session: Session):
    """Endpoint para exportación de CFDIs ISR con totales obligatorios"""
    json_body = bp.current_request.json_body or {}

    # GENERAR WORKBOOK
    workbook_bytes = _export_isr_generic(
        company_session=company_session,
        export_payload=json_body,
        controller_class=CFDIController,
        column_mapping=ISR_CFDI,
        total_key="total_cfdi",
        build_total_query_func=build_total_deducciones_cfdi_query,
    )

    # GENERAR REGISTRO EN DB CfdiExport
    export_request = create_export_record(company_session, json_body)

    # GUARDAR EN S3
    save_export_to_s3(company_session, workbook_bytes, export_request, json_body["export_data"])

    # REGRESA ID DE LA DB
    return {
        "export_identifier": export_request.identifier,
    }
