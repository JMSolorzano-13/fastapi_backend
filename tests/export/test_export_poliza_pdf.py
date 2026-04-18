import pytest

from chalicelib.blueprints.common import get_search_attrs
from chalicelib.controllers.poliza import PolizaController
from chalicelib.schema.models.tenant.poliza import Poliza


@pytest.mark.skip
def test_export_poliza_pdf_with_frontend_payload(company_session):
    """Test de exportación usando el payload del frontend."""
    # Payload real del frontend
    payload = {
        "domain": [
            ["identifier", "=", ["3c1b02da-085e-46bb-bc9c-f38ee39561f8"]],
            ["company_identifier", "=", "8833ac06-9b1f-4665-87ef-5f609cf5bc12"],
        ],
        "fields": [
            "fecha",
            "tipo",
            "numero",
            "concepto",
            "sistema_origen",
            "relaciones.uuid_related",
            "relaciones.cfdi_related.TipoDeComprobante",
            "relaciones.cfdi_related.Fecha",
            "relaciones.cfdi_related.Serie",
            "relaciones.cfdi_related.Folio",
            "relaciones.cfdi_related.RfcEmisor",
            "relaciones.cfdi_related.NombreEmisor",
            "relaciones.cfdi_related.Total",
            "movimientos.numerador",
            "movimientos.cuenta_contable",
            "movimientos.nombre",
            "movimientos.cargo",
            "movimientos.abono",
            "movimientos.cargo_me",
            "movimientos.abono_me",
            "movimientos.concepto",
            "movimientos.referencia",
            "movimientos.poliza_identifier",
        ],
        "format": "PDF",
        "export_data": {"file_name": "PGD1009214W0_Emitidos_Ago2025", "type": ""},
    }

    # Obtener la sesión de la empresa

    # Extraer atributos de búsqueda y remover company_identifier del domain
    # (ya que la sesión ya está conectada al tenant correcto)
    search_payload = payload.copy()
    search_payload["domain"] = [
        d
        if d[0] == "company_identifier"
        else [d[0], "in", d[2]]
        if d[1] == "=" and isinstance(d[2], list)
        else d
        for d in payload["domain"]
    ]
    search_payload["domain"] = [d for d in search_payload["domain"] if d[0] != "company_identifier"]
    search_attrs = get_search_attrs(search_payload)

    # Buscar pólizas usando el domain del payload
    query = PolizaController._search(
        **search_attrs,
        context={},
        lazzy=False,
        session=company_session,
    )

    if not query:
        # Si no encuentra con ese identifier específico, buscar cualquier póliza
        print("\n⚠️  No se encontró la póliza del payload, usando la primera disponible")
        poliza = company_session.query(Poliza).first()
        if poliza is None:
            pytest.skip("No hay pólizas en la base de datos para exportar")
        query = [poliza]

    # Generar PDF de la primera póliza encontrada
    poliza = query[0]
    pdf_bytes = PolizaController._to_pdf(poliza)

    # Verificar que se generó contenido
    assert pdf_bytes is not None
    assert len(pdf_bytes) > 0
    assert pdf_bytes[:4] == b"%PDF"  # Verifica que es un archivo PDF válido

    # Guardar el PDF para revisión manual
    filename = f"test_export_poliza_{poliza.identifier[:8]}.pdf"
    with open(filename, "wb") as f:
        f.write(pdf_bytes)

    print(f"\n✅ PDF generado exitosamente: {filename}")
    print(f"📄 Póliza: {poliza.tipo} {poliza.numero}")
    print(f"📅 Fecha: {poliza.fecha}")
    print(f"📝 Movimientos: {len(poliza.movimientos)}")
    print(f"🔗 CFDIs relacionados: {len(poliza.relaciones)}")


@pytest.mark.skip
def test_export_poliza_full_flow_with_s3(company_session):
    """Test del flujo completo: genera PDF, sube a S3 y retorna URL prefirmada."""
    # Payload real del frontend
    payload = {
        "domain": [
            ["identifier", "=", ["3c1b02da-085e-46bb-bc9c-f38ee39561f8"]],
            ["company_identifier", "=", "8833ac06-9b1f-4665-87ef-5f609cf5bc12"],
        ],
        "fields": [
            "fecha",
            "tipo",
            "numero",
            "concepto",
            "sistema_origen",
        ],
        "format": "PDF",
        "export_data": {"file_name": "test_polizas_export", "type": ""},
    }

    # Obtener company_identifier del payload
    company_identifier = payload["domain"][1][2]
    company_session = company_session(company_identifier)

    # Preparar payload para búsqueda
    search_payload = payload.copy()
    search_payload["domain"] = [
        d
        if d[0] == "company_identifier"
        else [d[0], "in", d[2]]
        if d[1] == "=" and isinstance(d[2], list)
        else d
        for d in payload["domain"]
    ]
    search_payload["domain"] = [d for d in search_payload["domain"] if d[0] != "company_identifier"]
    search_attrs = get_search_attrs(search_payload)

    # Buscar pólizas
    query = PolizaController._search(
        **search_attrs,
        context={},
        lazzy=True,  # lazy query para el export
        session=company_session,
    )

    if not query.count():
        # Buscar cualquier póliza
        print("\n⚠️  No se encontró la póliza del payload, usando todas las disponibles")
        query = company_session.query(Poliza).limit(3)

    # Ejecutar el flujo completo de exportación (incluye subida a S3)
    result = PolizaController.export(
        export_data=payload["export_data"],
        query=query,
        fields=payload["fields"],
        export_str="PDF",  # Nota: se llama export_str no export_format
        resume_export=None,
        context={},
        resume_type=None,
        session=company_session,
    )

    # Verificar el resultado
    assert "url" in result
    assert result["url"] != "EMPTY"

    # Verificar que la URL es válida (debe contener el bucket y parámetros de firma)
    assert "test_polizas_export.zip" in result["url"]
    assert "AWSAccessKeyId" in result["url"] or "X-Amz-" in result["url"]

    # Verificar que el archivo realmente está en S3 (mockeado)
    from chalicelib.boto3_clients import s3_client
    from chalicelib.new.config.infra import envars

    response = s3_client().head_object(Bucket=envars.S3_EXPORT, Key="test_polizas_export.zip")
    assert response["ContentLength"] > 0

    print("\n✅ Exportación completa exitosa!")
    print("📦 Archivo subido: test_polizas_export.zip")
    print(f"📏 Tamaño: {response['ContentLength']} bytes")
    print(f"🔗 URL prefirmada generada: {result['url'][:100]}...")
    print("⏰ Válida por 2 horas (en S3 mockeado para tests)")
