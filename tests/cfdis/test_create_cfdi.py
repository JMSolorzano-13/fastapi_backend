# import json
import json
import uuid
from datetime import datetime
from decimal import Decimal

from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_create_cfdi_company(
    client_authenticated: Client,
    company_session: Session,
    company: Company,
) -> None:
    cfdi = CFDI.demo(
        FechaFiltro=datetime.fromisoformat("2025-01-01T00:00:00.000"),
        Estatus=True,
        is_issued=True,
        TipoDeComprobante="I",
    )

    cfdi2 = CFDI.demo(
        FechaFiltro=datetime.fromisoformat("2025-01-01T00:00:00.000"),
        Estatus=True,
        is_issued=True,
        TipoDeComprobante="E",  # No pasa el filtro
    )
    company_session.add_all([cfdi, cfdi2])
    company_session.commit()

    result = client_authenticated.http.post(
        "/CFDI/search",
        body=json.dumps(
            {
                "domain": [
                    ["company_identifier", "=", company.identifier],
                    ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
                    ["FechaFiltro", "<", "2026-01-01T00:00:00.000"],
                    ["Estatus", "=", True],
                    ["is_issued", "=", True],
                    ["TipoDeComprobante", "=", "I"],
                ],
                "fields": ["UUID", "Fecha", "TipoDeComprobante"],
                "fuzzy_search": "",
                "limit": 30,
                "offset": 0,
            }
        ),
    )
    # Agregar los asserts necesarios que veas convenientes
    assert result.status_code == 200, f"Status: {result.status_code}, Body: {result.json_body}"
    assert len(result.json_body["data"]) == 1
    assert result.json_body["data"][0]["UUID"] == cfdi.UUID


def test_create_cfdi_company_other(
    client: Client,
    company_session_other: Session,
    company_other: Company,
    user_other_token: str,
) -> None:
    cfdi = CFDI.demo(
        is_issued=True,
        FechaFiltro=datetime(2025, 2, 1),
        Fecha=datetime(2025, 2, 1),
        UUID=str(uuid.uuid4()),
        RfcEmisor="EMISOR010101000",
        RfcReceptor="RECEPTOR010101000",
        BaseIVA0=0,
        BaseIVA16=0,
        BaseIVA8=0,
        BaseIVAExento=0,
        IVATrasladado16=0,
        IVATrasladado8=0,
        Total=Decimal("0.00"),
        SubTotal=0,
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
        FechaCertificacionSat=datetime(2025, 2, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        Descuento=Decimal("0.00"),
        pr_count=Decimal("0"),
        Estatus=True,
        TipoDeComprobante="I",
    )

    cfdi2 = CFDI.demo(
        is_issued=True,
        FechaFiltro=datetime(2025, 3, 1),
        Fecha=datetime(2025, 3, 1),
        UUID=str(uuid.uuid4()),
        RfcEmisor="EMISOR010101000",
        RfcReceptor="RECEPTOR010101000",
        BaseIVA0=0,
        BaseIVA16=0,
        BaseIVA8=0,
        BaseIVAExento=0,
        IVATrasladado16=0,
        IVATrasladado8=0,
        Total=Decimal("0.00"),
        SubTotal=0,
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
        PaymentDate=datetime(2025, 3, 1),
        Descuento=Decimal("0.00"),
        pr_count=Decimal("0"),
        TipoDeComprobante="I",
        Estatus=True,
        FechaCertificacionSat=datetime(2025, 3, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    company_session_other.add_all([cfdi, cfdi2])
    company_session_other.commit()

    result = client.http.post(
        "/CFDI/search",
        body=json.dumps(
            {
                "domain": [
                    ["company_identifier", "=", company_other.identifier],
                    ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
                    ["FechaFiltro", "<", "2026-01-01T00:00:00.000"],
                    ["Estatus", "=", True],
                    ["is_issued", "=", True],
                    ["TipoDeComprobante", "=", "I"],
                ],
                "fields": ["UUID", "Fecha", "TipoDeComprobante"],
                "fuzzy_search": "",
                "limit": 30,
                "offset": 0,
            }
        ),
        headers={
            "Content-Type": "application/json",
            "access_token": user_other_token,
        },
    )
    # Agregar los asserts necesarios que veas convenientes
    assert result.status_code == 200, f"Status: {result.status_code}, Body: {result.json_body}"
    assert len(result.json_body["data"]) == 2
    assert result.json_body["data"][0]["UUID"] == cfdi.UUID
    assert result.json_body["data"][1]["UUID"] == cfdi2.UUID
