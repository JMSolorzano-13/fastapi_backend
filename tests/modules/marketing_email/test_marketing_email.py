import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest
from sqlalchemy.orm import Session

from chalicelib.new.license.infra.siigo_marketing import (
    BASE_PRODUCT_ENABLE,
    EmailData,
    MailType,
    Status,
    _fetch_trial_info,
    emails_to_notify,
    get_company_identifier_from_workspaces,
    get_data_onboarding,
    get_data_ppd,
    get_days_from_start_trial,
    get_email_content_by_type,
    get_email_data,
    get_email_data_by_type,
    mark_workspaces_as_notified,
    mark_workspaces_base_product_status,
    send_marketing_emails,
    send_marketing_emails_by_type,
)
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace


def day_to_email(days: int) -> str:
    """Helper para crear emails de prueba según días."""
    return f"user{days}@test.com"


def email_to_day(email: str) -> int:
    """Helper para extraer días desde email de prueba."""
    return int(email.split("@")[0].replace("user", ""))


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


@pytest.mark.parametrize(
    "status,status_name",
    [
        (2, "paid"),  # Status pagado
        (3, "expired"),  # Status expirado
    ],
)
def test_get_days_from_start_trial(status: int, status_name: str):
    """Test con status pagado (2) y expirado (3)."""
    email = f"{status_name}@test.com"

    # Mock de respuesta con status diferente a trial activo
    mock_response_data = _create_siigo_response(
        email,
        status=status,
        days_ago=10,  # 10 días desde que inició
    )

    async def mock_get(*_args, **_kwargs):
        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = mock_response_data
        return mock_resp

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        result = get_days_from_start_trial({email})

        assert email in result
        assert result[email]["days"] is None


def test_get_days_from_start_trial_multiple_emails_parallel():
    """Test de procesamiento paralelo con múltiples emails."""
    days_to_test = {
        0,
        1,
        4,
        15,
        20,
    }

    emails = {day_to_email(d) for d in days_to_test}

    # Mock que responde diferente por email
    async def mock_get(*_args, **kwargs):
        params = kwargs.get("params", {})
        email = params.get("portalUserName", "")
        days_ago = email_to_day(email)

        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = _create_siigo_response(email, status=1, days_ago=days_ago)
        return mock_resp

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        result = get_days_from_start_trial(emails)

        # Verificar que todos los emails fueron procesados
        assert len(result) == len(emails)
        for email in emails:
            assert email in result
            assert result[email]["days"] == email_to_day(email)


def test_get_mails_grouped():
    """Test de procesamiento paralelo con múltiples emails."""
    days_to_test = {
        -1: MailType.TOO_EARLY,  # Caso borde: días negativos
        0: MailType.TOO_EARLY,
        1: MailType.ONBOARDING,
        4: MailType.ONBOARDING,
        5: MailType.PPD,
        15: MailType.PPD,
        20: MailType.TOO_LATE,
    }

    emails = {day_to_email(d) for d in days_to_test}

    emails_data = {
        email: EmailData(name="Test", wid=str(uuid.uuid4()), marketing_emails={})
        for email in emails
    }

    # Mock que responde diferente por email
    async def mock_get(*_args, **kwargs):
        params = kwargs.get("params", {})
        email = params.get("portalUserName", "")
        days_ago = email_to_day(email)

        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = _create_siigo_response(email, status=1, days_ago=days_ago)
        return mock_resp

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        result = emails_to_notify(emails_data)

        for days, mail_type in days_to_test.items():
            email = day_to_email(days)
            assert email in result[mail_type]


def test_get_days_from_start_trial_api_error():
    """Test cuando la API de Siigo retorna error."""
    email = "error@test.com"

    # Mock de error 404
    async def mock_get(*_args, **_kwargs):
        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 404
        return mock_resp

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        result = get_days_from_start_trial({email})

        assert email in result
        assert result[email]["days"] is None


def test_get_days_from_start_trial_http_exception():
    """Test cuando ocurre una excepción HTTP."""
    email = "exception@test.com"

    # Mock que lanza una excepción
    async def mock_get(*_args, **_kwargs):
        raise httpx.HTTPError("Connection error")

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        result = get_days_from_start_trial({email})

        assert email in result
        assert result[email]["days"] is None


def test_get_data_prev_and_actual_structure(company_session: Session, company: Company):
    """Test de estructura de get_data_prev_and_actual con datos reales."""

    # Crear CFDIs de prueba para Enero 2025
    cfdi_enero_1 = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 1, 15),
        Fecha=datetime(2025, 1, 15),
        Total=Decimal("1160.00"),
        SubTotalMXN=Decimal("1000.00"),
        DescuentoMXN=Decimal("100.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101AAA",
        RfcReceptor="RECEPTOR010101AAA",
        PaymentDate=datetime(2025, 1, 15),
        FechaCertificacionSat=datetime(2025, 1, 15),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    cfdi_enero_2 = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 1, 20),
        Fecha=datetime(2025, 1, 20),
        Total=Decimal("2320.00"),
        SubTotalMXN=Decimal("2000.00"),
        DescuentoMXN=Decimal("200.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101BBB",
        RfcReceptor="RECEPTOR010101BBB",
        PaymentDate=datetime(2025, 1, 20),
        FechaCertificacionSat=datetime(2025, 1, 20),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    # Crear un CFDI cancelado para Enero
    cfdi_enero_cancelado = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 1, 25),
        Fecha=datetime(2025, 1, 25),
        Total=Decimal("580.00"),
        SubTotalMXN=Decimal("500.00"),
        DescuentoMXN=Decimal("50.00"),
        TipoDeComprobante="I",
        Estatus=False,  # Cancelado
        RfcEmisor="EMISOR010101CCC",
        RfcReceptor="RECEPTOR010101CCC",
        PaymentDate=datetime(2025, 1, 25),
        FechaCertificacionSat=datetime(2025, 1, 25),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    # Crear CFDIs de prueba para Febrero 2025
    cfdi_febrero_1 = CFDI(
        company_identifier=company.identifier,
        is_issued=True,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 2, 10),
        Fecha=datetime(2025, 2, 10),
        Total=Decimal("3480.00"),
        SubTotalMXN=Decimal("3000.00"),
        DescuentoMXN=Decimal("300.00"),
        TipoDeComprobante="I",
        Estatus=True,
        RfcEmisor="EMISOR010101DDD",
        RfcReceptor="RECEPTOR010101DDD",
        PaymentDate=datetime(2025, 2, 10),
        FechaCertificacionSat=datetime(2025, 2, 10),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    company_session.add_all([cfdi_enero_1, cfdi_enero_2, cfdi_enero_cancelado, cfdi_febrero_1])
    company_session.commit()

    # Ejecutar función con el company_session real del fixture
    # end_date debe incluir todo febrero, no solo el día 1
    result = get_data_onboarding(
        start=datetime(2025, 1, 1),
        end=datetime(2025, 2, 28),  # Último día de Febrero
        company_session=company_session,
        company_identifier=company.identifier,
        user_name="Test User",
    )

    # Validar estructura básica
    assert result["user"] == "Test User"
    assert "data_cfdi" in result
    assert "data_iva" in result

    # Validar data_cfdi con valores específicos de Enero 2025
    prev_month = result["data_cfdi"]["prev_month"]
    assert prev_month["vigentes"] == 2
    assert prev_month["cancelados"] == 1
    assert prev_month["Ingresos nominales"] == 3000.0
    assert prev_month["Descuentos"] == 300.0
    assert prev_month["Ingresos netos"] == 2700.0

    # Validar data_cfdi con valores específicos de Febrero 2025
    current_month = result["data_cfdi"]["current_month"]
    assert current_month["vigentes"] == 1
    assert current_month["cancelados"] == 0
    assert current_month["Ingresos nominales"] == 3000.0
    assert current_month["Ingresos netos"] == 2700.0

    # Validar que data_iva tiene la estructura correcta
    assert "prev_month" in result["data_iva"]
    assert "current_month" in result["data_iva"]


def test_get_data_day_five(company_session: Session, company: Company):
    """Test de get_data_day_five con datos reales."""

    # Crear CFDIs PPD para Noviembre 2025
    cfdi_ppd_1 = CFDI(
        company_identifier=company.identifier,
        is_issued=False,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 11, 10),
        Fecha=datetime(2025, 11, 10),
        Total=Decimal("1160.00"),
        SubTotalMXN=Decimal("1000.00"),
        TipoDeComprobante="I",
        MetodoPago="PPD",
        Estatus=True,
        RfcEmisor="EMISOR010101AAA",
        RfcReceptor="RECEPTOR010101AAA",
        PaymentDate=datetime(2025, 11, 10),
        FechaCertificacionSat=datetime(2025, 11, 10),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    cfdi_ppd_2 = CFDI(
        company_identifier=company.identifier,
        is_issued=False,
        UUID=str(uuid.uuid4()),
        FechaFiltro=datetime(2025, 11, 15),
        Fecha=datetime(2025, 11, 15),
        Total=Decimal("2320.00"),
        SubTotalMXN=Decimal("2000.00"),
        TipoDeComprobante="I",
        MetodoPago="PPD",
        Estatus=True,
        RfcEmisor="EMISOR010101BBB",
        RfcReceptor="RECEPTOR010101BBB",
        PaymentDate=datetime(2025, 11, 15),
        FechaCertificacionSat=datetime(2025, 11, 15),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    company_session.add_all([cfdi_ppd_1, cfdi_ppd_2])
    company_session.commit()

    # Ejecutar función
    result = get_data_ppd(
        company_session=company_session,
        company_identifier=company.identifier,
        start=datetime(2025, 11, 1),
        end=datetime(2025, 11, 30),
        user_name="Test User",
    )

    # Validar propiedades esenciales
    assert result["user"] == "Test User"
    assert "Noviembre 2025" in result["data_cfdi"]["date"]

    # Validar conteos (qty)
    assert result["data_cfdi"]["vigentes qty"] == 2

    # Validar montos - total de vigentes debe ser 3000.00 (1000 + 2000)
    assert result["data_cfdi"]["vigentes"] == 3000.0

    # Validar que existen los campos de totalmente pagadas y pendiente de pago
    assert "Totalmente pagadas" in result["data_cfdi"]
    assert "Totalmente pagadas qty" in result["data_cfdi"]

    assert "Pendiente de pago" in result["data_cfdi"]
    assert "Pendiente de pago qty" in result["data_cfdi"]

    # Validar que los montos son números válidos (>= 0)
    assert result["data_cfdi"]["Totalmente pagadas"] >= 0
    assert result["data_cfdi"]["Pendiente de pago"] >= 0


def test_get_email_data(
    session: Session,
    workspace_with_base_product: Workspace,
    user: User,
    #   company: Company
):
    session.add(workspace_with_base_product)
    data = get_email_data(session, limit=10, offset=0)
    assert len(data) == 1
    d0 = data[user.email]
    assert d0.wid == workspace_with_base_product.identifier
    assert d0.name == user.name
    assert d0.marketing_emails == {}
    # assert d0.cid == company["company"].identifier


def test_mark_as_notified(session: Session, workspace_with_base_product: Workspace, user: User):
    session.add(workspace_with_base_product)
    mark_workspaces_as_notified(
        session, MailType.ONBOARDING, [workspace_with_base_product.identifier]
    )
    data = get_email_data(session, limit=10, offset=0)
    assert len(data) == 1
    d0 = data[user.email]
    assert d0.wid == workspace_with_base_product.identifier
    assert d0.name == user.name
    assert d0.marketing_emails == {MailType.ONBOARDING: True}


# Realmente consulta el API de Siigo, tanto para el status como envío de emails
@pytest.mark.third_party
def test_send_marketing_emails(
    session: Session,
    workspace_with_base_product: Workspace,
    user: User,
    company: Company,
):
    session.add_all([workspace_with_base_product, company["company"]])
    send_marketing_emails(session=session, limit=10, offset=0)

    data = get_email_data(session, limit=10, offset=0)
    assert len(data) == 1
    d0 = data[user.email]
    assert d0.wid == workspace_with_base_product.identifier
    assert d0.name == user.name
    assert d0.marketing_emails == {MailType.ONBOARDING: True}


@pytest.mark.asyncio
async def test_fetch_trial_info_expired_status():
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.status = Status.EXPIRED

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.async_get_siigo_free_trial",
        return_value=mock_response,
    ):
        email, days, _ = await _fetch_trial_info(mock_client, "test@example.com")
        assert email == "test@example.com"
        assert days is None


@pytest.mark.asyncio
async def test_fetch_trial_info_calculation():
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.status = Status.ACTIVATED
    now_utc = datetime.now(UTC)
    activation_date = now_utc - timedelta(days=5)
    mock_response.freeTrialActivationDate = activation_date

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.async_get_siigo_free_trial",
        return_value=mock_response,
    ):
        email, days, _ = await _fetch_trial_info(mock_client, "calc@example.com")
        assert email == "calc@example.com"
        assert days == 5


@pytest.mark.asyncio
async def test_fetch_trial_info_invalid_statuses():
    mock_client = AsyncMock()

    # CASE 1: CREATED
    resp1 = MagicMock()
    resp1.status = Status.CREATED
    resp1.freeTrialActivationDate = None

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.async_get_siigo_free_trial",
        return_value=resp1,
    ):
        email, days, _ = await _fetch_trial_info(mock_client, "created@example.com")
        assert days is None

    # CASE 2: ACTIVATED but None date
    resp2 = MagicMock()
    resp2.status = Status.ACTIVATED
    resp2.freeTrialActivationDate = None

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.async_get_siigo_free_trial",
        return_value=resp2,
    ):
        email, days, _ = await _fetch_trial_info(mock_client, "nodate@example.com")
        assert days is None


@pytest.mark.asyncio
async def test_fetch_trial_info_api_error():
    mock_client = AsyncMock()

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.async_get_siigo_free_trial",
        side_effect=Exception("API Error"),
    ):
        email, days, _ = await _fetch_trial_info(mock_client, "error@example.com")
        assert days is None


def test_emails_to_notify_segmentation():
    email_data = {
        "early@test.com": EmailData(
            name="Early User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
        "onboard@test.com": EmailData(
            name="Onboard User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
        "ppd@test.com": EmailData(
            name="PPD User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
        "late@test.com": EmailData(
            name="Late User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
        "very_late@test.com": EmailData(
            name="Very Late User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
        "ignored@test.com": EmailData(
            name="Ignored User",
            wid=str(uuid.uuid4()),
            marketing_emails={},
        ),
    }

    mock_days_data = {
        "early@test.com": {"days": 0, "name": "Early User"},
        "onboard@test.com": {"days": 3, "name": "Onboard User"},
        "ppd@test.com": {"days": 7, "name": "PPD User"},
        "late@test.com": {"days": 16, "name": "Late User"},
        "very_late@test.com": {"days": 100, "name": "Very Late User"},
        "ignored@test.com": {"days": None, "name": "Ignored User"},
    }

    with patch(
        "chalicelib.new.license.infra.siigo_marketing.get_days_from_start_trial",
        return_value=mock_days_data,
    ):
        grouped = emails_to_notify(email_data)

        assert "early@test.com" in grouped[MailType.TOO_EARLY]
        assert "onboard@test.com" in grouped[MailType.ONBOARDING]
        assert "ppd@test.com" in grouped[MailType.PPD]
        assert "late@test.com" in grouped[MailType.TOO_LATE]
        assert "very_late@test.com" in grouped[MailType.TOO_LATE]

        all_emails = [e for sublist in grouped.values() for e in sublist]
        assert "ignored@test.com" not in all_emails


def test_disable_expired_workspaces(session: Session):
    ws1 = Workspace()
    ws1.license = {"some": "data"}
    ws2 = Workspace()
    workspaces = [ws1, ws2]
    session.add_all(workspaces)
    session.flush()

    mark_workspaces_base_product_status(
        session=session, workspace_identifiers=[ws.identifier for ws in workspaces], status=False
    )

    for ws in workspaces:
        session.refresh(ws)

    assert ws1.license[BASE_PRODUCT_ENABLE] is False
    assert ws2.license[BASE_PRODUCT_ENABLE] is False


def test_rendered_email_content(session: Session, company_session: Session):
    cfdi_1 = CFDI.demo(
        Total=Decimal("1160.542"),
        SubTotalMXN=Decimal("1000.456"),
        DescuentoMXN=Decimal(
            "100.435",
        ),
    )
    company_session.add(cfdi_1)
    company_session.commit()

    email_data = get_email_data(session, limit=10, offset=0)
    company_identifier_by_wid = get_company_identifier_from_workspaces(
        session=session,
        workspace_identifiers=[data.wid for data in email_data.values()],
    )

    # Mock que responde diferente por email
    async def mock_get(*_args, **kwargs):
        params = kwargs.get("params", {})
        email = params.get("portalUserName", "")
        days_ago = 1

        mock_resp = Mock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = _create_siigo_response(email, status=1, days_ago=days_ago)
        return mock_resp

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get)):
        emails_by_notification = emails_to_notify(email_data)

        email_data_by_type = get_email_data_by_type(
            emails_by_notification=emails_by_notification,
            email_data=email_data,
            company_identifier_by_wid=company_identifier_by_wid,
            session=session,
        )
        email_content_by_type = get_email_content_by_type(email_data_by_type)

        send_marketing_emails_by_type(email_content_by_type)

        html_content = email_content_by_type[MailType.ONBOARDING][0][1]

        with open("test_email_content.html", "w", encoding="utf-8") as f:
            f.write(html_content)

        assert html_content is not None
