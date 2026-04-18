import json
import uuid
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path

import pytest
from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.controllers.cfdi import CFDIController
from chalicelib.modules.export.pdf import get_cfdi_pdf
from chalicelib.new.config.infra.envars import envars
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI

TEST_OUTPUTS = Path("tests/outputs")
TEST_OUTPUTS.mkdir(exist_ok=True, parents=True)


@pytest.fixture(autouse=True)
def have_wkhtmltopdf(cfdi_e: CFDI):
    try:
        get_cfdi_pdf(cfdi_e)
    except OSError:
        pytest.skip("wkhtmltopdf is not installed, skipping PDF export tests.")


@pytest.fixture
def cfdi_e(company: Company, company_session: Session) -> CFDI:
    xml_path = Path("tests/data/cfdi_e.xml")
    cfdi_repo = CFDIRepositorySA(company_session)
    xml_content = [xml_path.read_text(encoding="utf-8")]
    processor = XMLProcessor(cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session)
    processor.process_xml_files(
        company_identifier=company.identifier, xmls_contents=xml_content, rfc=company.rfc
    )
    return company_session.query(CFDI).filter_by(TipoDeComprobante="E").first()


def test_export_pdf_egreso(cfdi_e: CFDI):
    with open(TEST_OUTPUTS / "test_export_pdf.pdf", "wb") as f:
        f.write(get_cfdi_pdf(cfdi_e))


def test_export_pdf_cuenta_predial_single():
    conceptos_data = {
        "Concepto": [
            {
                "@ClaveProdServ": "84111506",
                "@Cantidad": "1",
                "@ClaveUnidad": "ACT",
                "@Descripcion": "Producto de prueba",
                "@ValorUnitario": "100.00",
                "@Importe": "100.00",
                "CuentaPredial": {"@Numero": "123456789012345678900"},
            },
        ]
    }

    cfdi = CFDI.demo(
        UUID=str(uuid.uuid4()),
        Impuestos={},
        TipoDeComprobante="I",
        is_issued=False,
        FechaFiltro=datetime(2021, 2, 1),
        Fecha=datetime(2021, 2, 1),
        RfcEmisor="EMISOR010101000",
        RfcReceptor="RECEPTOR010101000",
        BaseIVA0=0,
        BaseIVA16=0,
        BaseIVA8=0,
        BaseIVAExento=0,
        IVATrasladado16=0,
        IVATrasladado8=0,
        Total=Decimal("200.00"),
        SubTotal=200,
        TipoCambio=0,
        Neto=0,
        TrasladosIVA=0,
        TrasladosIEPS=0,
        TrasladosISR=0,
        RetencionesIVA=0,
        RetencionesIEPS=0,
        RetencionesISR=0,
        TotalMXN=0,
        SubTotalMXN=0,
        NetoMXN=0,
        TrasladosIVAMXN=0,
        DescuentoMXN=0,
        TrasladosIEPSMXN=0,
        TrasladosISRMXN=0,
        RetencionesIVAMXN=0,
        RetencionesIEPSMXN=0,
        RetencionesISRMXN=0,
        NoCertificado="000000",
        PaymentDate=datetime(2025, 2, 1),
        Descuento=Decimal("0.00"),
        pr_count=Decimal("0"),
        Estatus=True,
        FechaCertificacionSat=datetime(2021, 2, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        Conceptos=json.dumps(conceptos_data),
    )

    with open(TEST_OUTPUTS / "test_export_pdf.pdf", "wb") as f:
        f.write(get_cfdi_pdf(cfdi))


def test_export_pdf_cuenta_predial_multiple():
    conceptos_data = {
        "Concepto": [
            {
                "@ClaveProdServ": "84111506",
                "@Cantidad": "1",
                "@ClaveUnidad": "ACT",
                "@Descripcion": "Producto de prueba",
                "@ValorUnitario": "100.00",
                "@Importe": "100.00",
                "CuentaPredial": [
                    {"@Numero": "123456789012345678900"},
                    {"@Numero": "123456789012345678900"},
                ],
            },
        ]
    }

    cfdi = CFDI.demo(
        UUID=str(uuid.uuid4()),
        Impuestos={},
        TipoDeComprobante="I",
        is_issued=False,
        FechaFiltro=datetime(2021, 2, 1),
        Fecha=datetime(2021, 2, 1),
        RfcEmisor="EMISOR010101000",
        RfcReceptor="RECEPTOR010101000",
        BaseIVA0=0,
        BaseIVA16=0,
        BaseIVA8=0,
        BaseIVAExento=0,
        IVATrasladado16=0,
        IVATrasladado8=0,
        Total=Decimal("200.00"),
        SubTotal=200,
        TipoCambio=0,
        Neto=0,
        TrasladosIVA=0,
        TrasladosIEPS=0,
        TrasladosISR=0,
        RetencionesIVA=0,
        RetencionesIEPS=0,
        RetencionesISR=0,
        TotalMXN=0,
        SubTotalMXN=0,
        NetoMXN=0,
        TrasladosIVAMXN=0,
        DescuentoMXN=0,
        TrasladosIEPSMXN=0,
        TrasladosISRMXN=0,
        RetencionesIVAMXN=0,
        RetencionesIEPSMXN=0,
        RetencionesISRMXN=0,
        NoCertificado="000000",
        PaymentDate=datetime(2025, 2, 1),
        Descuento=Decimal("0.00"),
        pr_count=Decimal("0"),
        Estatus=True,
        FechaCertificacionSat=datetime(2021, 2, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        Conceptos=json.dumps(conceptos_data),
    )

    with open(TEST_OUTPUTS / "test_export_pdf_multiple.pdf", "wb") as f:
        f.write(get_cfdi_pdf(cfdi))


def test_export_egresos_pdf_integration(company_session: Session, company):
    """Integration test for PDF export of egreso CFDI with related ingreso CFDIs."""
    from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado

    # Create an ingreso CFDI
    cfdi_ingreso = CFDI.demo(
        TipoDeComprobante="I",
        Estatus=True,
        company_identifier=company.identifier,
        Conceptos=json.dumps({"Concepto": [{"@Descripcion": "Test", "@Importe": "100"}]}),
        Impuestos=json.dumps({}),
    )
    company_session.add(cfdi_ingreso)

    # Create an egreso CFDI
    cfdi_egreso = CFDI.demo(
        TipoDeComprobante="E",
        Estatus=True,
        company_identifier=company.identifier,
        Conceptos=json.dumps({"Concepto": [{"@Descripcion": "Test Egreso", "@Importe": "100"}]}),
        Impuestos=json.dumps({}),
    )
    company_session.add(cfdi_egreso)
    company_session.flush()

    # Create relationship between egreso and ingreso
    cfdi_relacionado = CfdiRelacionado(
        company_identifier=company.identifier,
        uuid_origin=cfdi_egreso.UUID,
        uuid_related=cfdi_ingreso.UUID,
        TipoRelacion="01",
        TipoDeComprobante="I",
        is_issued=True,
        Estatus=True,
    )
    company_session.add(cfdi_relacionado)
    company_session.commit()

    # Prepare the export body
    body = {
        "domain": [
            ["UUID", "in", [cfdi_egreso.UUID]],
            ["company_identifier", "=", company.identifier],
        ],
        "fields": [
            "UUID",
            "TipoDeComprobante",
            "Estatus",
            "Conceptos",
            "Impuestos",
            "RfcEmisor",
            "NombreEmisor",
            "RfcReceptor",
            "NombreReceptor",
            "Total",
            "Sello",
            "cfdi_related.uuid_origin",
            "cfdi_related.Estatus",
            "cfdi_related.TipoRelacion",
            "cfdi_related.TipoDeComprobante",
            "cfdi_origin.uuid_related",
            "cfdi_origin.cfdi_related.UUID",
            "cfdi_origin.cfdi_related.TipoDeComprobante",
            "cfdi_origin.cfdi_related.Estatus",
            "cfdi_origin.TipoRelacion",
        ],
        "format": "PDF",
        "export_data": {"file_name": "test_egresos_pdf", "type": ""},
    }

    # Execute export - call to_pdf directly to avoid S3 upload
    cfdi_controller = CFDIController()

    query = company_session.query(CFDI)
    query = CFDIController.apply_domain(query, body["domain"], session=company_session)

    # Call to_pdf directly to get the PDF bytes
    pdf_bytes = cfdi_controller.to_pdf(query, None, company_session, context=None)

    # Verify PDF was generated
    assert pdf_bytes is not None
    assert len(pdf_bytes) > 0

    # Save PDF to file for inspection
    output_path = TEST_OUTPUTS / "test_integration_egreso_with_related_ingreso.zip"
    with open(output_path, "wb") as f:
        f.write(pdf_bytes)


def test_export_egreso_pdf_with_related_ingreso():
    """Unit test: verify PDF template renders correctly for egreso with related ingreso."""
    from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado

    # Create an ingreso CFDI (mock) - adding @ClaveUnidad and @Unidad to avoid the line 119 error
    cfdi_ingreso = CFDI.demo(
        TipoDeComprobante="I",
        Estatus=True,
        Conceptos=json.dumps(
            {
                "Concepto": [
                    {
                        "@Descripcion": "Test",
                        "@Importe": "100",
                        "@ClaveUnidad": "E48",
                        "@Unidad": "Servicio",
                    }
                ]
            }
        ),
        Impuestos=json.dumps({}),
    )

    # Create an egreso CFDI (mock)
    cfdi_egreso = CFDI.demo(
        TipoDeComprobante="E",
        Estatus=True,
        Conceptos=json.dumps(
            {
                "Concepto": [
                    {
                        "@Descripcion": "Test Egreso",
                        "@Importe": "100",
                        "@ClaveUnidad": "E48",
                        "@Unidad": "Servicio",
                    }
                ]
            }
        ),
        Impuestos=json.dumps({}),
    )

    # Create relationship (mock)
    cfdi_relacionado = CfdiRelacionado(
        company_identifier=cfdi_egreso.company_identifier,
        uuid_origin=cfdi_egreso.UUID,
        uuid_related=cfdi_ingreso.UUID,
        TipoRelacion="01",
        TipoDeComprobante="I",
        is_issued=True,
        Estatus=True,
    )

    # Manually add relationship to egreso (simulating ORM load)
    cfdi_egreso.cfdi_origin = [cfdi_relacionado]

    # Test that PDF generation works without errors
    pdf_bytes = get_cfdi_pdf(cfdi_egreso)

    # Verify PDF was generated
    assert pdf_bytes is not None
    assert len(pdf_bytes) > 0
    assert pdf_bytes[:4] == b"%PDF"  # PDF file signature

    # Save PDF to file for inspection
    with open(TEST_OUTPUTS / "test_egreso_with_related_ingreso.pdf", "wb") as f:
        f.write(pdf_bytes)


def test_export_via_endpoint(
    client_authenticated: Client,
    company_session: Session,
    company: Company,
    cfdi_e: CFDI,
):
    # Prepare the export request body
    company_session.add(cfdi_e)
    company_session.commit()
    body = {
        "domain": [
            ["UUID", "in", [cfdi_e.UUID]],
            ["company_identifier", "=", company.identifier],
        ],
        "fields": [
            "UUID",
            "TipoDeComprobante",
            "Estatus",
            "Conceptos",
            "Impuestos",
            "RfcEmisor",
            "NombreEmisor",
            "RfcReceptor",
            "NombreReceptor",
            "Total",
            "Sello",
        ],
        "format": "PDF",
        "export_data": {"file_name": "test_export_endpoint_pdf", "type": ""},
    }

    # Call the export endpoint
    response = client_authenticated.http.post("/CFDI/export", body=body)

    # Verify response
    assert response.status_code == HTTPStatus.OK, response.json_body
    assert response.json_body["url"]

    # Save response content to file for inspection
    s3_client().download_file(
        Bucket=envars.S3_EXPORT,
        Key=body["export_data"]["file_name"] + ".zip",
        Filename=str(TEST_OUTPUTS / "test_export_endpoint_pdf.zip"),
    )
