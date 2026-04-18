import enum
import random
import uuid
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import freezegun
import httpx
import pytest
from pydantic import EmailStr
from sqlalchemy.orm import Session

from chalicelib.new.license.infra.siigo_marketing import (
    BASE_PRODUCT_ENABLE,
    MARKETING_EMAILS_KEY,
    MailType,
    Status,
    get_data_ppd,
    send_marketing_emails,
)
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado
from chalicelib.schema.models.tenant.payment import Payment
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace

FREEZED_TIME = datetime.fromisoformat("2024-06-15")


class TIPO_COMPROBANTE(enum.StrEnum):
    INGRESO = "I"
    EGRESO = "E"
    TRASLADO = "T"
    NOMINA = "N"
    PAGO = "P"


def _create_siigo_response(
    email: str,
    status: int,
    days_ago: int,
    free_trial_days: str = "15",
    inactive_days: str = "0",
) -> dict:
    """Helper para crear respuestas mock de Siigo."""
    start_date = datetime.now() - timedelta(days=days_ago)
    return {
        "id": str(uuid.uuid4()),
        "rfc": "XAXX010101000",
        "name": "Test",
        "lastName": "User",
        "status": status,
        "portalUserName": email,
        "freeTrialDays": free_trial_days,
        "inactiveDays": inactive_days,
        "initialDiscountPercentage": "20",
        "finalDiscountPercentage": "10",
        "freeTrialStartDate": start_date.isoformat() + "Z",
        "freeTrialActivationDate": start_date.isoformat() + "Z",
    }


@pytest.mark.third_party
@freezegun.freeze_time(FREEZED_TIME)
@pytest.mark.parametrize(
    "has_base_product, has_data, days_passed, already_sent_email, email",
    [
        # 1. Sin producto base
        (False, False, 0, False, "buddy-citrus-scoop@duck.com"),
        # 2. Con producto base
        #    2.1 Sin datos
        (True, False, 0, False, "deed-july-splendor@duck.com"),
        #    2.2 Con datos
        #        2.2.1 0 días
        (True, True, 0, False, "yin-trowel-dismiss@duck.com"),
        #        2.2.2 1 día
        #
        (True, True, 1, False, "kilt-onscreen-each@duck.com"),
        (True, True, 1, True, "causal-kiln-screen@duck.com"),
        #        2.2.3 4 días
        (True, True, 4, False, "nacho-replica-tusk@duck.com"),
        #        2.2.4 5 días
        #
        (True, True, 5, False, "say-dry-diabetic@duck.com"),
        (True, True, 5, True, "strive-rack-plural@duck.com"),
        #        2.2.5 15 días
        (True, True, 15, False, "pesky-lark-smartly@duck.com"),
        #        2.2.6 29 días
        (True, True, 29, False, "eject-grout-brunt@duck.com"),
    ],
)
def test_full_trip(
    session: Session,
    company_session: Session,
    workspace_with_base_product: Workspace,
    company: dict,
    user: dict,
    has_base_product: bool,
    has_data: bool,
    days_passed: int,
    already_sent_email: bool,
    email: EmailStr,
):
    user: User = user["user"]
    workspace = workspace_with_base_product
    company: Company = company["company"]

    status = Status.ACTIVATED if has_base_product else Status.EXPIRED

    random.seed(42)
    cfdis = [
        CFDI.demo(
            is_issued=False,
            MetodoPago="PPD",
            Estatus=random.choices([True, False], weights=[90, 10])[0],
            TipoDeComprobante="I",
            from_xml=True,
            TotalMXN=random.uniform(100, 1000),
            Total=random.uniform(100, 1000),
            SubTotalMXN=random.uniform(80, 900),
            DescuentoMXN=random.uniform(0, 50),
            FechaFiltro=FREEZED_TIME - timedelta(days=random.randint(0, 60)),
            BaseIVA16=random.uniform(0, 1000),
            BaseIVA8=random.uniform(0, 1000),
            BaseIVA0=random.uniform(0, 1000),
            BaseIVAExento=random.uniform(0, 1000),
            IVATrasladado16=random.uniform(0, 1000),
            IVATrasladado8=random.uniform(0, 1000),
        )
        for _ in range(1000)
    ]
    doctos = [
        DoctoRelacionado.demo(
            company_identifier=random.choice(cfdis).company_identifier,
            FechaPago=FREEZED_TIME - timedelta(days=random.randint(0, 60)),
            active=True,
            cfdi_origin=random.choice(cfdis),
            cfdi_related=random.choice(cfdis),
            ExcludeFromIVA=False,
            is_issued=False,
            Estatus=True,
            payment_related=Payment(
                company_identifier=random.choice(cfdis).company_identifier,
                FormaDePagoP=random.choice(["02", "03", "04", "05", "06", "28", "29"]),
                uuid_origin=random.choice(cfdis).UUID,
                index=0,
                FechaPago=FREEZED_TIME - timedelta(days=random.randint(0, 60)),
                MonedaP="MXN",
                Monto=random.uniform(500, 2000),
            ),
            BaseIVA16=random.uniform(0, 1000),
            BaseIVA8=random.uniform(0, 1000),
            BaseIVA0=random.uniform(0, 1000),
            BaseIVAExento=random.uniform(0, 1000),
            IVATrasladado16=random.uniform(0, 1000),
            IVATrasladado8=random.uniform(0, 1000),
            RetencionesIVAMXN=random.uniform(0, 1000),
        )
        for _ in range(100)
    ]

    user.email = email
    workspace.license[BASE_PRODUCT_ENABLE] = has_base_product
    workspace.license[MARKETING_EMAILS_KEY][MailType.ONBOARDING] = already_sent_email
    workspace.license[MARKETING_EMAILS_KEY][MailType.PPD] = already_sent_email

    async def mock_get(*_args, **kwargs):
        params = kwargs.get("params", {})
        email = params.get("portalUserName", "")

        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = _create_siigo_response(
            email, status=status, days_ago=days_passed
        )
        return mock_resp

    session.add_all([workspace, company, user])
    if has_data:
        company_session.add_all(cfdis)
        company_session.add_all(doctos)
        company_session.commit()
        # Test data valid, not related to test logic
        data = get_data_ppd(
            company_session=company_session, start=FREEZED_TIME, end=FREEZED_TIME, user_name="X"
        )
        assert data["data_cfdi"]["vigentes qty"] > 0
    session.flush()

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        send_marketing_emails(session=session, limit=10, offset=0)
