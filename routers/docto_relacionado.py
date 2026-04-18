"""Documento Relacionado routes — search, update, ISR pagos export.

Ported from: backend/chalicelib/blueprints/docto_relacionado.py
3 routes total.
"""

from typing import Any

from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session

from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.new.isr_deducciones import build_total_deducciones_pagos_query
from chalicelib.new.isr_exportaciones import (
    ISR_DOCTORELACIONADO,
    _export_isr_generic,
    create_export_record,
    save_export_to_s3,
)
from chalicelib.new.shared.domain.primitives import Identifier
from dependencies import (
    common,
    get_company_identifier_rw,
    get_company_session,
    get_company_session_rw,
    get_json_body,
)

router = APIRouter(tags=["DoctoRelacionado"])


@router.post("/search")
def search(
    json_body: dict = Depends(get_json_body),
    company_session: Session = Depends(get_company_session),
):
    return common.search(json_body, DoctoRelacionadoController, session=company_session)


@router.post("/update")
def set_exclude(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
    company_identifier: Identifier = Depends(get_company_identifier_rw),
):
    doctos: dict[Identifier, dict[str, Any]] = body["cfdis"]
    docto_controller = DoctoRelacionadoController()
    model_keys = {"company_identifier": company_identifier}
    return docto_controller.update_multiple(
        records=doctos,
        session=company_session,
        model_keys=model_keys,
    )


@router.post("/export_isr_pagos")
def export_isr_pagos(
    body: dict = Body(...),
    company_session: Session = Depends(get_company_session_rw),
):
    workbook_bytes = _export_isr_generic(
        company_session=company_session,
        export_payload=body,
        controller_class=DoctoRelacionadoController,
        column_mapping=ISR_DOCTORELACIONADO,
        total_key="total_pagos",
        build_total_query_func=build_total_deducciones_pagos_query,
    )

    export_request = create_export_record(company_session, body)
    save_export_to_s3(company_session, workbook_bytes, export_request, body["export_data"])

    return {"export_identifier": export_request.identifier}
