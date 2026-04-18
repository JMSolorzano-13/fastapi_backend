from datetime import datetime, timedelta

import freezegun
from sqlalchemy.orm import Session

from chalicelib.controllers.notification import (
    NotificationController,
    NotificationSectionType,
    update_last_notification,
)
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI as CFDIORM
from chalicelib.schema.models.user import User as UserORM


def _seed_cfdis_for_notifications(company_identifier: str, company_session: Session):
    now = datetime.utcnow()
    # CFDI que debe aparecer en "con errores (emitidos)"
    cfdi_error = CFDIORM(
        company_identifier=company_identifier,
        is_issued=True,
        UUID="11111111-1111-1111-1111-111111111111",
        Fecha=now,
        FechaFiltro=now,
        FechaCertificacionSat=now,
        PaymentDate=now,
        Total=0,
        TipoDeComprobante="I",
        Estatus=True,
        created_at=now,
        MetodoPago="PUE",
        TipoDeComprobante_I_MetodoPago_PUE=True,
        NombreReceptor="Cliente Demo",
        RfcReceptor="XAXX010101000",
        NombreEmisor="Emisor Demo",
        RfcEmisor="XAXX010101000",
    )
    # CFDI que debe aparecer en "cancelado en otro mes (emitidos)"
    last_month = now - timedelta(days=31)
    cfdi_cancelled = CFDIORM(
        company_identifier=company_identifier,
        is_issued=True,
        UUID="22222222-2222-2222-2222-222222222222",
        Fecha=last_month,
        FechaFiltro=last_month,
        FechaCertificacionSat=last_month,
        PaymentDate=last_month,
        Total=0,
        TipoDeComprobante="I",
        Estatus=True,
        created_at=last_month,
        FechaCancelacion=now,
        NombreReceptor="Cliente Cancelado",
        RfcReceptor="XAXX010101000",
        NombreEmisor="Emisor Demo",
        RfcEmisor="XAXX010101000",
    )

    company_session.add_all([cfdi_error, cfdi_cancelled])
    company_session.commit()


def test_notification_body_renders_without_query_len_error(
    session: Session, company: Company, company_session: Session
):
    # Preparación: objetos centrales
    company_obj = company

    # Tomar cualquier usuario existente de la BD central (creado por fixtures)
    user_obj: UserORM = session.query(UserORM).first()
    assert user_obj is not None
    email = user_obj.email

    # Asegurar que el correo esté incluido en el conjunto de notificaciones de errores
    company_obj.emails_to_send_errors = [email]
    session.commit()

    # Sembrar CFDIs mínimos en la BD tenant para cubrir ambas secciones corregidas
    _seed_cfdis_for_notifications(company_obj.identifier, company_session)

    controller = NotificationController(session=session)

    # Construir secciones con el controlador para ejercitar los fixes de .all()
    filter_date = datetime.utcnow() - timedelta(days=2)
    sections = controller._get_notification_sections(filter_date, company_session)

    # Mantener solo las secciones relevantes al correo configurado (errores)
    email_sets = controller.get_emails_sets(company_obj)
    assert email in email_sets[NotificationSectionType.ERRORS]

    # Acción: renderizar el cuerpo de la notificación para este correo
    body = controller.get_notification_body(
        user=user_obj,
        email=email,
        company=company_obj,
        company_emails_sets=email_sets,
        notification_sections=sections,
    )

    # Verificación: el cuerpo es string y contiene contenido esperado
    assert isinstance(body, str)
    assert "CFDIs" in body  # fragmentos de encabezados del template
    assert "11111111-1111-1111-1111-111111111111" in body  # UUID del CFDI sembrado


@freezegun.freeze_time()
def test_send_pending_by_company(company: Company, session: Session, company_session: Session):
    company_ids = set()

    NotificationController(session=session).send_pending_by_company(
        company, company_session, company_ids
    )

    update_last_notification(
        session=session,
        company_ids_to_update=company_ids,
    )
    session.refresh(company)
    assert company.last_notification == utc_now()
