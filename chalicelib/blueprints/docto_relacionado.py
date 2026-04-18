from typing import Any

from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.controllers.docto_relacionado import DoctoRelacionadoController
from chalicelib.new.isr_deducciones import build_total_deducciones_pagos_query
from chalicelib.new.isr_exportaciones import (
    ISR_DOCTORELACIONADO,
    _export_isr_generic,
    create_export_record,
    save_export_to_s3,
)
from chalicelib.new.shared.domain.primitives import Identifier

bp = SuperBlueprint(__name__)


@bp.route("/search", methods=["POST"], cors=common.cors_config)
def search(company_session: Session):
    return common.search(bp, DoctoRelacionadoController, session=company_session)


@bp.route("/update", methods=["POST"], cors=common.cors_config, read_only=False)
def set_exclude(company_session: Session, company_identifier: Identifier):
    json_body = bp.current_request.json_body or {}
    doctos: dict[Identifier, dict[str, Any]] = json_body["cfdis"]
    docto_controller = DoctoRelacionadoController()
    model_keys = {"company_identifier": company_identifier}
    return docto_controller.update_multiple(
        records=doctos,
        session=company_session,
        model_keys=model_keys,
    )


@bp.route("/export_isr_pagos", methods=["POST"], cors=common.cors_config, read_only=False)
def export_isr_pagos(company_session: Session):
    """Endpoint para exportación de documentos relacionados (pagos) con totales"""
    json_body = bp.current_request.json_body or {}

    # GENERAR WORKBOOK
    workbook_bytes = _export_isr_generic(
        company_session=company_session,
        export_payload=json_body,
        controller_class=DoctoRelacionadoController,
        column_mapping=ISR_DOCTORELACIONADO,
        total_key="total_pagos",
        build_total_query_func=build_total_deducciones_pagos_query,
    )

    # GENERAR REGISTRO EN DB CfdiExport
    export_request = create_export_record(company_session, json_body)

    # GUARDAR EN S3
    save_export_to_s3(company_session, workbook_bytes, export_request, json_body["export_data"])

    # REGRESA ID DE LA DB
    return {
        "export_identifier": export_request.identifier,
    }
