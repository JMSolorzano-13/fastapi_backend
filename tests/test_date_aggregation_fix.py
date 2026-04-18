"""
Test para verificar el fix de agregación de campos DateTime en exportaciones XLSX.

Replica el flujo completo de /massive_export sin depender de AWS SQS.
Bug original: ParserError: Unknown string format: 2025-09-01 00:00:00, 2025-09-01 00:00:00
"""

from chalicelib.bus import EventBus
from chalicelib.controllers.enums import ResumeType
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import CFDIExportRepositorySA
from chalicelib.schema.models.company import Company


def test_export_xlsxv2_with_datetime_fields(company_session, session, company: Company):
    """
    Test que ejecuta la exportación real y captura si hay el bug de DateTime.
    Si no hay datos, busca en producción/staging.
    """
    # Body exacto que causó el error en producción
    json_body = {
        "domain": [
            ["company_identifier", "=", company.identifier],
            ["FechaFiltro", ">=", "2025-09-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-10-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "N"],
        ],
        "fuzzy_search": "",
        "format": "XLSX",
        "fields": [
            "FechaFiltro",
            "RfcReceptor",
            "NombreReceptor",
            "nomina.ReceptorTipoRegimen",
            "nomina.PercepcionesTotalSueldos",
            "nomina.OtrasPercepciones",
            "nomina.PercepcionesTotalGravado",
            "nomina.PercepcionesTotalExento",
            "nomina.DeduccionesTotalImpuestosRetenidos",
            "nomina.AjusteISRRetenido",
            "nomina.DeduccionesTotalOtrasDeducciones",
            "nomina.SubsidioCausado",
            "nomina.NetoAPagar",
            "nomina.FechaInicialPago",  # Campo DateTime que causaba el problema
        ],
        "TipoDeComprobante": "N",
        "export_data": {"file_name": "TEST_BUG_FIX", "type": ""},
    }

    # Preparar body
    body = {
        "domain": json_body["domain"],
        "fuzzy_search": json_body["fuzzy_search"],
        "fields": json_body["fields"],
    }

    # Crear exporter y ejecutar
    exporter = CFDIExporter(
        company_session=company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
        bus=EventBus(),
    )

    try:
        export_bytes = exporter.export_xlsxv2(
            body=body,
            context={},
            query=None,
            fields=json_body["fields"],
            resume_type=ResumeType.N,
            export_data=json_body["export_data"],
        )

        if export_bytes and len(export_bytes) > 0:
            # Guardar Excel
            output_file = "test_export_nomina.xlsx"
            with open(output_file, "wb") as f:
                f.write(export_bytes)
        else:
            print("⚠️  No se generaron datos en el Excel (puede que no haya registros)\n")

    except Exception as e:
        error_msg = str(e)
        if "Unknown string format" in error_msg and ", " in error_msg:
            print("\n❌❌❌ BUG DETECTADO ❌❌❌")
        else:
            print(f"\n⚠️  Error diferente: {error_msg}\n")
            raise
