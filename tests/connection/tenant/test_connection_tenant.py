# import json
import json
import uuid
from datetime import datetime
from decimal import Decimal

from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_connection_tenant(
    client_authenticated: Client,
    company_session: Session,
    company: Company,
    company_session_other: Session,
    company_other: Company,
    user_other_token: str,
) -> None:
    cfdi_tenant1 = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 2, 1),
        Fecha=datetime(2025, 2, 1),
        Total=Decimal("100.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101AAA",
        RfcReceptor="RECEPTOR010101AAA",
        PaymentDate=datetime(2025, 2, 1),
        FechaCertificacionSat=datetime(2025, 2, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    cfdi_tenant2 = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 3, 1),
        Fecha=datetime(2025, 3, 1),
        Total=Decimal("200.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101BBB",
        RfcReceptor="RECEPTOR010101BBB",
        PaymentDate=datetime(2025, 3, 1),
        FechaCertificacionSat=datetime(2025, 3, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    cfdi_tenant_other1 = CFDI(
        company_identifier=company_other.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 4, 1),
        Fecha=datetime(2025, 4, 1),
        Total=Decimal("300.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101CCC",
        RfcReceptor="RECEPTOR010101CCC",
        PaymentDate=datetime(2025, 4, 1),
        FechaCertificacionSat=datetime(2025, 4, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    cfdi_tenant_other2 = CFDI(
        company_identifier=company_other.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 5, 1),
        Fecha=datetime(2025, 5, 1),
        Total=Decimal("400.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101DDD",
        RfcReceptor="RECEPTOR010101DDD",
        PaymentDate=datetime(2025, 5, 1),
        FechaCertificacionSat=datetime(2025, 5, 1),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    company_session.add_all([cfdi_tenant1, cfdi_tenant2])
    company_session.commit()

    company_session_other.add_all([cfdi_tenant_other1, cfdi_tenant_other2])
    company_session_other.commit()

    result_tenant = client_authenticated.http.post(
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

    result_tenant_other = client_authenticated.http.post(
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
    # Agregar los asserts necesarios que veas convenientes para empresa 1
    assert result_tenant.status_code == 200, result_tenant.json_body
    assert len(result_tenant.json_body["data"]) == 2
    result_uuids = [item["UUID"] for item in result_tenant.json_body["data"]]
    assert cfdi_tenant1.UUID in result_uuids
    assert cfdi_tenant2.UUID in result_uuids

    # Agregar los asserts necesarios que veas convenientes para empresa 2
    assert result_tenant_other.status_code == 200, result_tenant_other.json_body
    assert len(result_tenant_other.json_body["data"]) == 2
    result_other_uuids = [item["UUID"] for item in result_tenant_other.json_body["data"]]
    assert cfdi_tenant_other1.UUID in result_other_uuids
    assert cfdi_tenant_other2.UUID in result_other_uuids
