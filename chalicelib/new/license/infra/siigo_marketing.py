import asyncio
import enum
import json
from collections import defaultdict
from datetime import date, datetime
from logging import CRITICAL, INFO, WARNING
from typing import Any

import httpx
import jinja2
from chalice import BadRequestError, ChaliceViewError, NotFoundError
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, EmailStr
from sqlalchemy import extract, func, text, update
from sqlalchemy.orm import Session

from chalicelib.controllers.notification import send_email_siigo
from chalicelib.controllers.tenant.session import new_company_session_from_company_identifier
from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.iva import IVAGetter
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import get_start_date_relativedelta, mx_now
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace

BASE_PRODUCT_ENABLE = "base_product_enable"
MARKETING_EMAILS_KEY = "marketing_emails"
MARKETING_TEMPLATES_PATH = "chalicelib/data/"
ONBOARDING_TEMPLATE = "notifications_templates/download_info_trial_notification.html.jinja"
ONBOARDING_IMAGE_URL = "https://solucioncp-sharefiles-sgdev-uv10.s3.us-east-1.amazonaws.com/img/correo_primera_descarga.png"
PPD_TEMPLATE = "notifications_templates/ppd_notification_template.html.jinja"
PPD_IMAGE_URL = (
    "https://solucioncp-sharefiles-sgdev-uv10.s3.us-east-1.amazonaws.com/img/correo_quinto_dia.png"
)

VITE_APP_LOGO_URL = "https://solucioncp-sharefiles-sgdev-uv10.s3.us-east-1.amazonaws.com/logos/logo_siigo_blanco_small.png"

SPANISH_MONTHS = [
    "",  # Placeholder para índice 0
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]


class MailType(enum.IntEnum):
    # Días desde el inicio del trial para cada tipo de correo
    TOO_EARLY = 0
    ONBOARDING = 1
    PPD = 5
    TOO_LATE = 16


WORKSPACES_IN_FREE_TRIAL_FILTER = (
    # Workspace.license.op("->>")(BASE_PRODUCT_ENABLE).cast(Boolean) == True
    True
)


def get_total_workspaces_in_free_trial(session: Session) -> int:
    """Obtiene el total de workspaces que están en free trial."""
    return (
        session.query(func.count())
        .select_from(Workspace)
        .filter(WORKSPACES_IN_FREE_TRIAL_FILTER)
        .scalar()
    )


def set_license_key(
    session: Session, workspace_identifiers: list[Identifier], key: str, value: Any
) -> None:
    """Desactiva los workspaces que han expirado su free trial."""
    stmt = (
        update(Workspace)
        .values(
            license=func.jsonb_set(
                Workspace.license,
                key,
                func.to_jsonb(value),
                True,  # create_missing
            )
        )
        .where(Workspace.identifier.in_(workspace_identifiers))
    )
    session.execute(stmt)


def mark_workspaces_base_product_status(
    session: Session, workspace_identifiers: list[Identifier], status: bool
) -> None:
    """Marca el estado del producto base en los workspaces indicados."""
    key = f"{{{BASE_PRODUCT_ENABLE}}}"
    set_license_key(session, workspace_identifiers, key, status)


def mark_workspaces_as_notified(
    session: Session, mail_type: MailType, workspace_identifiers: list[Identifier]
) -> None:
    key = f"{{{MARKETING_EMAILS_KEY}}}"
    value = text(f"'{json.dumps({str(mail_type.value): True})}'::jsonb")
    set_license_key(session, workspace_identifiers, key, value)


def get_workspaces_in_free_trial(session: Session) -> list[tuple[Identifier, EmailStr]]:
    """Obtiene los workspaces que están en free trial."""
    return (
        session.query(
            Workspace.identifier,
            User.email,
        )
        .join(Workspace.owner)
        .filter(WORKSPACES_IN_FREE_TRIAL_FILTER)
    )


class Status(enum.IntEnum):
    UNCONFIRMED = -1
    CREATED = 0
    ACTIVATED = 1
    PURCHASED = 2
    EXPIRED = 3


class SiigoFreeTrialResponse(BaseModel):
    """Schema de validación para la respuesta del endpoint de Siigo FreeTrial."""

    id: str
    rfc: str
    name: str
    lastName: str
    status: Status
    portalUserName: str
    freeTrialDays: str
    inactiveDays: str
    initialDiscountPercentage: str  # Contains a str like "50%"
    finalDiscountPercentage: str  # Contains a str like "50%"
    freeTrialStartDate: datetime
    freeTrialActivationDate: datetime | None = None


class EmailData(BaseModel):
    name: str | None = None
    wid: Identifier
    marketing_emails: dict[MailType, bool]


class TrialInfo(BaseModel):
    days: int | None
    name: str


async def async_get_siigo_free_trial(
    client: httpx.AsyncClient,
    portal_user_name: str,
    base_url: str = envars.SIIGO_FREETRIAL_BASE_URL,
    timeout: int = envars.SIIGO_FREETRIAL_TIMEOUT,
) -> SiigoFreeTrialResponse:
    url = f"{base_url}/get"
    params = {"portalUserName": portal_user_name}

    try:
        response = await client.get(url, params=params, timeout=timeout)
    except httpx.TimeoutException as e:
        raise ChaliceViewError("Error al consultar información de prueba gratuita de Siigo") from e
    except httpx.RequestError as e:
        raise ChaliceViewError("Error al consultar información de prueba gratuita de Siigo") from e

    match response.status_code:
        case 200:
            data = response.json()
            validated = SiigoFreeTrialResponse.model_validate(data)
            return validated
        case 404:
            raise NotFoundError(
                "No se encontró registro de prueba gratis para el email proporcionado"
            )
        case 400:
            raise BadRequestError(f"Error de validación de Siigo: {response.text}")
        case code if code >= 500:
            raise ChaliceViewError("Error al consultar información de prueba gratuita de Siigo")
        case _:
            raise BadRequestError(
                f"Error al consultar FreeTrial de Siigo. Status: {response.status_code}, "
                f"Response: {response.text}"
            )


def get_siigo_free_trial(
    portal_user_name: str,
) -> SiigoFreeTrialResponse:
    async def _fetch() -> SiigoFreeTrialResponse:
        async with httpx.AsyncClient() as client:
            return await async_get_siigo_free_trial(
                client,
                portal_user_name,
            )

    return asyncio.run(_fetch())


def emails_to_notify(emails_data: dict[EmailStr, EmailData]) -> dict[MailType, list[EmailStr]]:
    """Agrupa las empresas que deben recibir correos de marketing según el tipo de correo.

    Args:
        emails: Conjunto de correos electrónicos de las empresas a evaluar

    Returns:
        Diccionario con el tipo de correo como clave y la lista de correos electrónicos como valor
    """
    grouped_emails: dict[MailType, list[EmailStr]] = defaultdict(list)

    try:
        days_from_trial = get_days_from_start_trial(emails=set(emails_data.keys()))

        mail_types_sorted_desc = sorted(MailType, key=lambda mt: -mt.value)

        def _find_mail_type(days: int) -> MailType:
            for mail_type in mail_types_sorted_desc:
                if days >= mail_type:
                    return mail_type
            return MailType.TOO_EARLY

        for email, info in days_from_trial.items():
            if info["days"] is None:
                continue
            emails_data[email].name = info["name"]
            stage = _find_mail_type(info["days"])
            grouped_emails[stage].append(email)
    except Exception as e:
        log(
            Modules.MARKETING_EMAIL,
            CRITICAL,
            "ERROR grouping emails",
            {"error": str(e), "emails": list(emails_data.keys())},
        )
    return grouped_emails


async def _fetch_trial_info(
    client: httpx.AsyncClient, email: EmailStr
) -> tuple[EmailStr, None | int, str]:
    """Obtiene los días desde el inicio del trial para una empresa."""

    try:
        parsed_response = await async_get_siigo_free_trial(client, email)
    except Exception:
        log(
            Modules.MARKETING_EMAIL,
            CRITICAL,
            "ERROR fetching trial info",
            {"email": email},
        )
        return (email, None, "")

    name = parsed_response.name

    if (
        parsed_response.status != Status.ACTIVATED
        or parsed_response.freeTrialActivationDate is None
    ):
        return (email, None, name)

    # Parse ISO date format
    days_elapsed = (
        datetime.now(parsed_response.freeTrialActivationDate.tzinfo)
        - parsed_response.freeTrialActivationDate
    ).days

    return (email, days_elapsed, name)


def get_days_from_start_trial(emails: set[EmailStr]) -> dict[EmailStr, TrialInfo]:
    """Obtiene los días desde el inicio del trial para múltiples empresas en paralelo.

    Args:
        companies: Lista de empresas para consultar

    Returns:
        Diccionario con la empresa como clave y los días desde el inicio como valor,
        o None si no se pudo obtener
    """

    async def _fetch_all() -> dict[EmailStr, None | int]:
        async with httpx.AsyncClient() as client:
            tasks = [_fetch_trial_info(client, email) for email in emails]
            results = await asyncio.gather(*tasks)
            return {email: {"days": days, "name": name} for email, days, name in results}

    return asyncio.run(_fetch_all())


def get_data_common(
    user_name: str,
    company_identifier: Identifier,
    mail_type: MailType,
    date: datetime | None = None,
) -> dict:
    now = mx_now()
    send_date = f"{now.day} de {SPANISH_MONTHS[now.month].lower()} del {now.year}"

    if mail_type == MailType.ONBOARDING:
        main_image_url = ONBOARDING_IMAGE_URL
        main_button_url = f"{envars.FRONTEND_BASE_URL}/?cid={company_identifier}"
    else:
        main_image_url = PPD_IMAGE_URL
        main_button_url = f"{envars.FRONTEND_BASE_URL}/cfdi-received/?cid={company_identifier}&type=ingress&period={date.year}-{SPANISH_MONTHS[date.month].lower()}&ppd=PPD&ppd_type=bothinvoice"  # noqa: E501

    return {
        "user": user_name,
        "cid": company_identifier,
        "VITE_APP_LOGO_URL": VITE_APP_LOGO_URL,
        "main_image_url": main_image_url,
        "main_button_url": main_button_url,
        "send_date": send_date,
    }


def get_data_onboarding(
    company_session: Session,
    company_identifier: Identifier,
    start: datetime,
    end: datetime,
    user_name: str,
):
    """Genera datos de CFDI e IVA para dos meses consecutivos.

    Args:
        start_date: Fecha del mes anterior (ej: 2025-01-01)
        end_date: Fecha del mes actual (ej: 2025-02-01)
        company_session: Sesión de la compañía
        user_name: Nombre del usuario

    Returns:
        Diccionario con datos formateados para email template
    """
    from chalicelib.blueprints.cfdi.emitidos_ingresos_anio_mes_resumen import (
        IngresosNominales,
        _emitidos_ingresos_anio_mes_resumen,
    )

    # Obtener datos de CFDI para ambos meses
    data_cfdi = _emitidos_ingresos_anio_mes_resumen(
        company_session=company_session, start_date=start, end_date=end
    )

    # Obtener datos de IVA para ambos meses
    getter = IVAGetter(company_session)
    data_iva_prev = getter.get_iva(date(start.year, start.month, 1))
    data_iva_actual = getter.get_iva(date(end.year, end.month, 1))

    # Obtener datos de cada mes
    mes_anterior = start.month
    mes_actual = end.month

    cfdi_prev = data_cfdi.datos.get(mes_anterior, IngresosNominales())
    cfdi_actual = data_cfdi.datos.get(mes_actual, IngresosNominales())

    # Formatear fecha de envío (día actual)

    # Construir estructura de datos
    def data_cfdi_month(cfdi_data, month, year):
        return {
            "date": f"{SPANISH_MONTHS[month]} {year}",
            "cancelados": cfdi_data.cancelados,
            "vigentes": cfdi_data.vigentes,
            "Ingresos nominales": cfdi_data.subtotal_mxn,
            "Descuentos": cfdi_data.descuento_mxn,
            "Ingresos netos": cfdi_data.subtotal_mxn - cfdi_data.descuento_mxn,
        }

    def data_iva_month(data_iva, month, year):
        return {
            "date": f"{SPANISH_MONTHS[month]} {year}",
            "IVA trasladado": data_iva["period"]["transferred"]["total"],
            "IVA acreditable": data_iva["period"]["creditable"]["total"],
            "IVA a cargo": data_iva["period"]["diff"],
            "Retenciones IVA": data_iva["period"]["transferred"]["i_tra"]["RetencionesIVAMXN"],
        }

    return get_data_common(
        user_name=user_name, company_identifier=company_identifier, mail_type=MailType.ONBOARDING
    ) | {
        "data_cfdi": {
            "prev_month": data_cfdi_month(cfdi_prev, mes_anterior, start.year),
            "current_month": data_cfdi_month(cfdi_actual, mes_actual, end.year),
        },
        "data_iva": {
            "prev_month": data_iva_month(data_iva_prev, mes_anterior, start.year),
            "current_month": data_iva_month(data_iva_actual, mes_actual, end.year),
        },
    }


def get_data_ppd(
    company_session: Session,
    company_identifier: Identifier,
    start: datetime,
    end: datetime,
    user_name: str,
) -> dict:
    """Genera datos de CFDIs de ingreso recibidos PPD vigentes para un mes específico.

    Filtra por:
    - Ingreso (TipoDeComprobante == "I")
    - Recibidos (is_issued == False)
    - PPD - Pago en Parcialidades o Diferido (MetodoPago == "PPD")
    - Vigentes (Estatus == True)
    - Todo en MXN

    Args:
        company_session: Sesión de la compañía
        period: Fecha del mes a consultar (se usa mes y año)
        user_name: Nombre del usuario

    Returns:
        Diccionario con datos de CFDIs vigentes, totalmente pagadas y pendientes de pago
    """

    # Nombres de meses en español

    # Consultar CFDIs: Ingreso, recibidos, PPD, vigentes, del mes especificado
    cfdis_filter = (
        ~CFDI.is_issued,
        CFDI.TipoDeComprobante == "I",
        CFDI.MetodoPago == "PPD",
        CFDI.Estatus,
        extract("year", CFDI.FechaFiltro) == start.year,
        extract("month", CFDI.FechaFiltro) == start.month,
    )

    (
        vigentes_total,
        vigentes_total_amount,
        totalmente_pagadas,
        totalmente_pagadas_amount,
        pendiente_pago,
        pendiente_pago_amount,
    ) = (
        company_session.query(
            # Todos
            func.count(),
            func.coalesce(func.sum(CFDI.SubTotalMXN), 0),
            # Totalmente pagadas
            func.count().filter(CFDI.balance <= 0),
            func.coalesce(func.sum(CFDI.SubTotalMXN).filter(CFDI.balance <= 0), 0),
            # Pendiente de pago
            func.count().filter(CFDI.balance > 0),
            func.coalesce(func.sum(CFDI.SubTotalMXN).filter(CFDI.balance > 0), 0),
        )
        .filter(*cfdis_filter)
        .one()
    )

    # Construir estructura de datos
    return get_data_common(
        user_name=user_name,
        company_identifier=company_identifier,
        mail_type=MailType.PPD,
        date=start,
    ) | {
        "data_cfdi": {
            "date": f"{SPANISH_MONTHS[start.month]} {start.year}",
            "vigentes": vigentes_total_amount,
            "vigentes qty": vigentes_total,
            "Totalmente pagadas": totalmente_pagadas_amount,
            "Totalmente pagadas qty": totalmente_pagadas,
            "Pendiente de pago": pendiente_pago_amount,
            "Pendiente de pago qty": pendiente_pago,
        },
    }


def get_company_identifier_from_workspaces(
    session: Session, workspace_identifiers: list[Identifier]
) -> dict[Identifier, Identifier]:
    """Obtiene los identificadores de compañía para los workspaces indicados."""
    results = (
        session.query(
            Company.workspace_identifier,
            Company.identifier,
        )
        .join(Company.workspace)
        .filter(Company.workspace_identifier.in_(workspace_identifiers))
    )

    return {r.workspace_identifier: r.identifier for r in results}


def get_email_data(session: Session, limit: int, offset: int) -> dict[EmailStr, EmailData]:
    """Obtiene datos de email para workspaces en free trial."""
    results = (
        session.query(
            User.email,
            User.name,
            Workspace.identifier.label("wid"),
            Workspace.license.op("->")(MARKETING_EMAILS_KEY).label("marketing_emails"),
        )
        .join(Workspace.owner)
        .filter(WORKSPACES_IN_FREE_TRIAL_FILTER)
        .offset(offset)
        .limit(limit)
    )

    res: dict[EmailStr, EmailData] = {}
    for r in results:
        try:
            r_dict = r._asdict()
            if r_dict["marketing_emails"] is None:
                r_dict["marketing_emails"] = {}
            email = r_dict.pop("email")
            res[email] = EmailData.model_validate(r_dict)
        except Exception as e:
            log(
                Modules.MARKETING_EMAIL,
                CRITICAL,
                "ERROR_get_email_data_row",
                {
                    "error": str(e),
                    "email": r_dict.get("email", None),
                    "row": str(r),
                },
            )
    return res


def get_email_data_by_type(
    emails_by_notification: dict[MailType, list[EmailStr]],
    email_data: dict[EmailStr, EmailData],
    company_identifier_by_wid: dict[Identifier, Identifier],
    session: Session,
) -> dict[MailType, list[tuple[str, dict]]]:
    data_by_type: dict[MailType, list[tuple[str, dict]]] = {
        MailType.ONBOARDING: [],
        MailType.PPD: [],
    }
    mail_data_functions = {
        MailType.ONBOARDING: get_data_onboarding,
        MailType.PPD: get_data_ppd,
    }

    end_date = mx_now()
    prev_month_start = get_start_date_relativedelta(end_date, relativedelta(months=-1))
    for mail_type, email_list in emails_by_notification.items():
        for email in email_list:
            try:
                email_data_entry = email_data[email]
                if email_data_entry.marketing_emails.get(mail_type):
                    continue  # Ya se le envió este correo
                cid = company_identifier_by_wid[email_data_entry.wid]

                with new_company_session_from_company_identifier(
                    company_identifier=cid, session=session
                ) as company_session:
                    if not company_session.query(func.count()).select_from(CFDI).scalar():
                        data_by_type[mail_type].append((email, {}))
                        continue  # No hay datos
                    data: dict = mail_data_functions[mail_type](
                        company_session=company_session,
                        company_identifier=cid,
                        start=prev_month_start,
                        end=end_date,
                        user_name=email_data_entry.name,
                    )
                    data_by_type[mail_type].append((email, data))
            except Exception as e:
                log(
                    Modules.MARKETING_EMAIL,
                    CRITICAL,
                    "ERROR_get_email_data_by_type",
                    {
                        "error": str(e),
                        "email": email,
                        "mail_type": mail_type.name,
                    },
                )
    return data_by_type


def get_email_content_by_type(
    email_data_by_type: dict[MailType, list[tuple[str, dict]]],
) -> dict[MailType, list[tuple[str, str]]]:
    env = Environment(
        loader=FileSystemLoader(MARKETING_TEMPLATES_PATH),
        autoescape=True,  # si es HTML/XML
        cache_size=50,  # cache de plantillas compiladas
    )
    templates_by_mail_type: dict[MailType, jinja2.Template] = {
        MailType.ONBOARDING: env.get_template(ONBOARDING_TEMPLATE),
        MailType.PPD: env.get_template(PPD_TEMPLATE),
    }
    email_content_by_type = {
        MailType.ONBOARDING: [],
        MailType.PPD: [],
    }
    for mail_type, email_data_list in email_data_by_type.items():
        for email, data in email_data_list:
            try:
                rendered = templates_by_mail_type[mail_type].render(**data) if data else ""
                email_content_by_type[mail_type].append((email, rendered))
            except Exception as e:
                log(
                    Modules.MARKETING_EMAIL,
                    CRITICAL,
                    "ERROR_rendering_email",
                    {
                        "error": str(e),
                        "email": email,
                        "mail_type": mail_type.name,
                    },
                )

    return email_content_by_type


def send_marketing_emails(session: Session, offset: int, limit: int) -> None:
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "START_MARKETING",
        {
            "offset": offset,
            "limit": limit,
        },
    )
    email_data = get_email_data(session=session, offset=offset, limit=limit)

    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_data",
        {
            "len": len(email_data),
        },
    )
    company_identifier_by_wid = get_company_identifier_from_workspaces(
        session=session,
        workspace_identifiers=[data.wid for data in email_data.values()],
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "company_identifier_by_wid",
        {
            "len": len(company_identifier_by_wid),
        },
    )

    log(
        Modules.MARKETING_EMAIL,
        INFO,
        "_marketing_email_cron",
        {
            "companies": len(email_data),
        },
    )

    emails_by_notification = emails_to_notify(emails_data=email_data)
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "emails_by_notification",
        {
            "len": len(emails_by_notification),
        },
    )

    # Calculamos las fechas que necesitamos (mes actual y mes anterior)

    emails_by_notification.pop(MailType.TOO_EARLY, [])  # No hacemos nada con estos
    to_mark_as_no_longer_to_notify = emails_by_notification.pop(MailType.TOO_LATE, [])

    email_data_by_type = get_email_data_by_type(
        emails_by_notification=emails_by_notification,
        email_data=email_data,
        company_identifier_by_wid=company_identifier_by_wid,
        session=session,
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_data_by_type",
        {
            "len": len(email_data_by_type),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_data_by_type['ONBOARDING']",
        {
            "len": len(email_data_by_type.get(MailType.ONBOARDING, [])),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_data_by_type['PPD']",
        {
            "len": len(email_data_by_type.get(MailType.PPD, [])),
        },
    )
    email_content_by_type = get_email_content_by_type(email_data_by_type)

    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_content_by_type",
        {
            "len": len(email_content_by_type),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_content_by_type['ONBOARDING']",
        {
            "len": len(email_content_by_type.get(MailType.ONBOARDING, [])),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "email_content_by_type['PPD']",
        {
            "len": len(email_content_by_type.get(MailType.PPD, [])),
        },
    )

    sent_emails_by_type = send_marketing_emails_by_type(email_content_by_type)

    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "sent_emails_by_type",
        {
            "len": len(sent_emails_by_type),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "sent_emails_by_type['ONBOARDING']",
        {
            "len": len(sent_emails_by_type.get(MailType.ONBOARDING, [])),
        },
    )
    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "sent_emails_by_type['PPD']",
        {
            "len": len(sent_emails_by_type.get(MailType.PPD, [])),
        },
    )

    for mail_type, sent_list in sent_emails_by_type.items():
        workspaces = [email_data[email].wid for email in sent_list]
        mark_workspaces_as_notified(session, mail_type, workspaces)

    log(
        Modules.MARKETING_EMAIL,
        WARNING,
        "FINISHED",
    )

    expired_workspaces = [email_data[email].wid for email in to_mark_as_no_longer_to_notify]
    # Marcamos los workspaces expirados
    mark_workspaces_base_product_status(session, expired_workspaces, status=False)


def send_marketing_emails_by_type(
    email_content_by_type: dict[MailType, list[tuple[str, str]]],
) -> dict[MailType, list[str]]:
    sent_emails_by_type: dict[MailType, list[str]] = {
        MailType.ONBOARDING: [],
        MailType.PPD: [],
    }

    email_subjects = {
        MailType.ONBOARDING: "¡Tu primera descarga en Siigo Fiscal está lista!",
        MailType.PPD: "Consulta el resumen de tus movimientos en Siigo Fiscal",
    }

    for mail_type, email_content_list in email_content_by_type.items():
        for email, content in email_content_list:
            if not content:
                continue  # No hay contenido para enviar, aún no hay CFDIs
            try:
                send_email_siigo(
                    email=email,
                    email_id=f"marketing-{mail_type}-{email}-{mx_now()}",
                    html_part=content,
                    subject=email_subjects[mail_type],
                    text_part="",
                )
            except Exception:
                log(
                    Modules.MARKETING_EMAIL,
                    CRITICAL,
                    "ERROR_sending_email",
                    {
                        "email": email,
                        "mail_type": mail_type.name,
                    },
                )
            sent_emails_by_type[mail_type].append(email)

    return sent_emails_by_type
