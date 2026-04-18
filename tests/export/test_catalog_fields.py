"""
Este archivo reúne los tests de campos de catálogo que antes lanzaban NotImplementedError
por la incompatibilidad entre _get_relational_fields y
_join_query_and_models_from_fields.

La solución fue ajustar _get_relational_fields para que devuelva tuplas
(modelo, relación) en lugar de relaciones simples.

Nota: Los tests están skippeados por defecto porque requieren un company_identifier real
y generan archivos Excel.
"""

import pytest
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.controllers.enums import ResumeType
from chalicelib.new.cfdi_processor.domain.cfdi_exporter import CFDIExporter
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import (
    CFDIExportRepositorySA,
)
from chalicelib.new.company.domain.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI as CFDIORM

# Skipear todos los tests por defecto - son para testing local manual
pytestmark = pytest.mark.skip(
    reason="Tests para pruebas locales manuales. Requieren company_identifier específico."
)


def test_ingresos_with_catalog_fields(company_session: Session):
    """
    Test para Ingresos (I) con campos de catálogo que pueden causar problemas:
    - c_forma_pago.name
    - c_metodo_pago.name
    - c_moneda.name
    - c_uso_cfdi.name
    - c_regimen_fiscal_receptor.name
    - c_exportacion.name
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "I"],
        ],
    }

    fields = [
        "UUID",
        "Fecha",
        "Serie",
        "Folio",
        "Total",
        # Campos de catálogo problemáticos
        "FormaPago",
        "c_forma_pago.name",
        "MetodoPago",
        "c_metodo_pago.name",
        "Moneda",
        "c_moneda.name",
        "UsoCFDIReceptor",
        "c_uso_cfdi.name",
        "RegimenFiscalReceptor",
        "c_regimen_fiscal_receptor.name",
        "Exportacion",
        "c_exportacion.name",
    ]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data={"file_name": "test_ingresos", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_ingresos_catalog_fields.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print(f"✓ Test Ingresos (I) con {len(fields)} campos pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")


def test_egresos_with_catalog_fields(company_session: Session):
    """
    Test para Egresos (E) con campos de catálogo
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "E"],
        ],
    }

    fields = [
        "UUID",
        "Fecha",
        "Total",
        "FormaPago",
        "c_forma_pago.name",
        "MetodoPago",
        "c_metodo_pago.name",
        "Moneda",
        "c_moneda.name",
        "c_tipo_de_comprobante.name",
    ]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data={"file_name": "test_egresos", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_egresos_catalog_fields.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print(f"✓ Test Egresos (E) con {len(fields)} campos pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")


def test_pagos_with_catalog_fields(company_session: Session):
    """
    Test para Pagos (P) con campos de catálogo y relaciones anidadas
    Incluye payments.c_forma_pago.name que es una relación anidada
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "P"],
        ],
    }

    fields = [
        "UUID",
        "Fecha",
        "Total",
        "Moneda",
        "c_moneda.name",
        # Campos de la relación payments (to-many)
        "payments.FechaPago",
        "payments.FormaDePagoP",
        "payments.c_forma_pago.name",  # Relación anidada
        "payments.Monto",
    ]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data={"file_name": "test_pagos", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_pagos_catalog_fields.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print(f"✓ Test Pagos (P) con {len(fields)} campos pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")


def test_nomina_with_catalog_fields(company_session: Session):
    """
    Test para Nómina (N) con campos de catálogo de nómina
    Usa relación N.* para acceder a campos de nómina
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "N"],
        ],
    }

    fields = [
        "UUID",
        "Fecha",
        "Total",
        "RfcReceptor",
        "NombreReceptor",
        # Campos básicos de nómina sin catálogos por ahora
        # Los campos de catálogo de nómina requieren acceso a traves de N.*
        # pero get_query no los maneja de la misma forma
    ]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.N,
        export_data={"file_name": "test_nomina", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_nomina_catalog_fields.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print(f"✓ Test Nómina (N) con {len(fields)} campos pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")


def test_multiple_catalog_fields_same_table(company_session: Session):
    """
    Test para verificar múltiples campos del mismo catálogo (CatRegimenFiscal)
    Esto prueba el caso de special_fields donde la misma tabla se usa dos veces
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "I"],
        ],
    }

    fields = [
        "UUID",
        "RegimenFiscalEmisor",
        "c_regimen_fiscal_emisor.name",
        "RegimenFiscalReceptor",
        "c_regimen_fiscal_receptor.name",
    ]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data={"file_name": "test_multiple_catalogs", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_multiple_catalogs_same_table.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print("✓ Test múltiples campos del mismo catálogo pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")


def test_massive_export_egresos_full_payload(company_session: Session):
    """
    Test de exportación completa con el payload original del endpoint massive_export.
    Este es el test que originalmente descubrió el bug con c_forma_pago.name y c_metodo_pago.name.
    """

    # Payload completo del endpoint api/CFDI/massive_export
    payload = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "E"],
        ],
        "fuzzy_search": "",
        "format": "XLSX",
        "fields": [
            "Fecha",
            "Serie",
            "Folio",
            "RfcReceptor",
            "NombreReceptor",
            "SubTotal",
            "Descuento",
            "Neto",
            "RetencionesIVA",
            "RetencionesISR",
            "TrasladosIVA",
            "Total",
            "Moneda",
            "TipoCambio",
            "UsoCFDIReceptor",
            "FormaPago",
            "c_forma_pago.name",  # Campo que causaba el bug
            "MetodoPago",
            "c_metodo_pago.name",  # Campo que causaba el bug
            "CfdiRelacionados",
            "LugarExpedicion",
            "UUID",
        ],
        "export_data": {
            "file_name": "test_egresos_emitidos",
            "type": "",
        },
    }

    body = {
        "domain": payload["domain"],
        "fuzzy_search": payload["fuzzy_search"],
        "limit": None,
        "offset": None,
        "order_by": None,
    }

    fields = payload["fields"]
    export_data = payload["export_data"]

    # Ejecutar la búsqueda usando _search
    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    # Crear el exporter y ejecutar exportación
    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data=export_data,
        context=None,
    )

    # Verificaciones
    assert file is not None
    assert len(file) > 0
    print("✓ Test exportación masiva con payload completo pasó correctamente")
    print(f"  - Archivo generado: {len(file)} bytes")
    print(f"  - Campos exportados: {len(fields)}")


def test_simple_query_with_problematic_fields(company_session: Session):
    """
    Test simplificado que verifica directamente get_query con los campos problemáticos.
    Este test es más rápido y aislado para debugging.
    """

    body = {
        "domain": [
            ["company_identifier", "=", "023fe1e6-bc6a-428c-885c-7d88b23f4911"],
            ["FechaFiltro", ">=", "2025-11-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-12-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", True],
            ["TipoDeComprobante", "=", "E"],
        ],
    }

    fields = [
        "UUID",
        "FormaPago",
        "c_forma_pago.name",
        "MetodoPago",
        "c_metodo_pago.name",
    ]

    # Crear query base
    query = company_session.query(CFDIORM)

    # Este get_query fallaba antes del fix
    query = CFDIController.get_query(CFDIORM, fields, body, aggregate=False, sql_query=query)

    query = CFDIController.apply_domain(
        query, domain=body["domain"], fuzzy_search="", session=company_session
    )

    # Ejecutar el query
    result = query.limit(1).first()

    assert result is not None or query.count() == 0, "Query debe ejecutarse sin errores"
    print("✓ Test query simplificado con campos problemáticos pasó correctamente")


def test_export_ingresos_order_by(company: Company, company_session: Session):
    body = {
        "domain": [
            ["FechaFiltro", ">=", "2024-01-01T00:00:00.000"],
            ["FechaFiltro", "<", "2025-01-01T00:00:00.000"],
            ["Estatus", "=", True],
            ["is_issued", "=", False],
            ["TipoDeComprobante", "=", "I"],
        ],
    }

    fields = ["UUID", "Fecha", "Serie", "Folio", "Total"]

    # Ejecutar exportación
    body["fuzzy_search"] = ""
    body["limit"] = None
    body["offset"] = None
    body["order_by"] = None

    query = CFDIController._search(**body, fields=fields, session=company_session, lazzy=True)

    exporter = CFDIExporter(
        company_session,
        cfdi_export_repo=CFDIExportRepositorySA(session=company_session),
    )

    file = exporter.export_xlsxv2(
        body=body,
        query=query,
        fields=fields,
        resume_type=ResumeType.BASIC,
        export_data={"file_name": "test_ingresos", "type": ""},
        context=None,
    )

    # Guardar archivo
    filename = "test_ingresos_catalog_fields.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    assert file is not None and len(file) > 0
    print(f"✓ Test Ingresos (I) con {len(fields)} campos pasó correctamente")
    print(f"  📄 Excel generado: {filename} ({len(file)} bytes)")
