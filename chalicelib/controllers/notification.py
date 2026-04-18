import enum
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

import jinja2
import requests
from botocore.exceptions import ParamValidationError
from html2text import html2text
from pydantic import BaseModel
from sqlalchemy import extract, func, or_
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import secretsmanager_get, ses_client
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.user import UserController
from chalicelib.logger import DEBUG, EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.utils.datetime import mx_now, utc_now
from chalicelib.schema.models import Company
from chalicelib.schema.models import User as UserORM
from chalicelib.schema.models.efos import EFOS, EFOS_DATE_FORMAT_PSQL
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM

NOTIFICATION_JINJA_TEMPLATE_PATH = "cfdi-mail-notifications.html.jinja"

NAME_ISSUED = {
    True: "issued",
    False: "received",
}
EMAIL_PROVIDER_SECRET = "siigo/mailprovider/token"

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader("chalicelib/data/email"),
)


class NotificationSectionType(enum.Enum):
    ERRORS = "errors"
    CANCELED = "canceled"
    EFOS = "efos"
    PUE_WITH_PAYMENTS = "pue_with_payments"


class EmailProviderType(BaseModel):
    endpoint: str
    token: str
    email: str
    name: str


@dataclass
class NotificationSection:
    name: str
    type: NotificationSectionType
    cfdis: list[CFDIORM]


@dataclass
class NotificationController(CommonController):
    session: Session

    def _get_notification_sections(
        self, filter_date: datetime, company_session: Session
    ) -> list[NotificationSection]:
        notification_section_generators: tuple[
            Callable[[Session, datetime], NotificationSection], ...
        ] = (
            partial(self.new_cfdis_with_errors, is_issued=True),
            partial(self.new_cfdis_with_errors, is_issued=False),
            partial(self.new_cfdis_cancelled_other_month, is_issued=True),
            partial(self.new_cfdis_cancelled_other_month, is_issued=False),
            self.new_cfdis_with_efos,
            partial(self.new_cfdis_pue_with_payments, is_issued=True),
            partial(self.new_cfdis_pue_with_payments, is_issued=False),
        )
        return [
            generator(company_session, filter_date) for generator in notification_section_generators
        ]

    def send_pending_by_company(
        self,
        company: Company,
        company_session: Session,
        company_ids_to_update: set[int],  # TODO usar identifier cuando sea su PK
    ):
        last_notification_date = company.last_notification or envars.DEFAULT_LAST_NOTIFICATION_DATE
        filter_date = last_notification_date - envars.TIMEDELTA_ISSUE_WINDOW
        sections = self._get_notification_sections(
            filter_date=filter_date, company_session=company_session
        )
        self.send_notification(
            company,
            sections,
        )
        company_ids_to_update.add(company.id)

    def new_cfdis_with_errors(
        self, company_session: Session, date: datetime, is_issued: bool
    ) -> NotificationSection:
        cfdis: list[CFDIORM] = (
            company_session.query(CFDIORM)
            .filter(
                CFDIORM.is_issued.is_(is_issued),
                CFDIORM.created_at >= date,
                or_(
                    CFDIORM.TipoDeComprobante_I_MetodoPago_PUE.is_(True),
                    CFDIORM.TipoDeComprobante_E_CfdiRelacionados_None.is_(True),
                ),
            )
            .order_by(
                CFDIORM.FechaFiltro.desc(),
            )
            .limit(envars.MAX_RESULTS_PER_SECTION)
        ).all()
        return NotificationSection(
            name=f"cfdis_with_errors_{NAME_ISSUED[is_issued]}",
            type=NotificationSectionType.ERRORS,
            cfdis=cfdis,
        )

    def new_cfdis_cancelled_other_month(
        self, company_session: Session, date: datetime, is_issued: bool
    ):
        cfdis: list[CFDIORM] = (
            company_session.query(CFDIORM)
            .filter(
                CFDIORM.is_issued.is_(is_issued),
                CFDIORM.FechaCancelacion >= date,
                or_(
                    extract("year", CFDIORM.FechaCancelacion) > extract("year", CFDIORM.Fecha),
                    extract("month", CFDIORM.FechaCancelacion) > extract("month", CFDIORM.Fecha),
                ),
            )
            .order_by(
                CFDIORM.FechaFiltro.desc(),
            )
            .limit(envars.MAX_RESULTS_PER_SECTION)
        ).all()
        return NotificationSection(
            name=f"cfdis_cancelled_{NAME_ISSUED[is_issued]}",
            type=NotificationSectionType.CANCELED,
            cfdis=cfdis,
        )

    def new_cfdis_with_efos(self, company_session: Session, date: datetime) -> NotificationSection:
        new_cfdis_with_efos: list[CFDIORM] = (
            company_session.query(CFDIORM)
            .join(EFOS, EFOS.rfc == CFDIORM.RfcEmisor)
            .filter(
                ~CFDIORM.is_issued,
                CFDIORM.Estatus,
                EFOS.state == EFOS.StateEnum.ALLEGED,
                or_(
                    CFDIORM.created_at >= date,
                    func.to_date(EFOS.sat_publish_alleged_date, EFOS_DATE_FORMAT_PSQL) >= date,
                ),
            )
            .order_by(
                CFDIORM.FechaFiltro.desc(),
            )
            .limit(envars.MAX_RESULTS_PER_SECTION)
        ).all()
        return NotificationSection(
            name="cfdis_with_efos",
            type=NotificationSectionType.EFOS,
            cfdis=new_cfdis_with_efos,
        )

    def new_cfdis_pue_with_payments(
        self, company_session: Session, date: datetime, is_issued: bool
    ):
        new_cfdis_with_payments = (
            company_session.query(CFDIORM)
            .join(
                DoctoRelacionadoORM,
                DoctoRelacionadoORM.UUID_related == CFDIORM.UUID,
            )
            .filter(
                CFDIORM.is_issued.is_(is_issued),
                CFDIORM.TipoDeComprobante == "I",
                CFDIORM.MetodoPago == "PUE",
                or_(
                    CFDIORM.created_at >= date,
                    DoctoRelacionadoORM.created_at >= date,
                ),
            )
            .order_by(
                CFDIORM.FechaFiltro.desc(),
            )
            .limit(envars.MAX_RESULTS_PER_SECTION)
        ).all()

        return NotificationSection(
            name=f"cfdis_pue_with_payments_{NAME_ISSUED[is_issued]}",
            type=NotificationSectionType.PUE_WITH_PAYMENTS,
            cfdis=new_cfdis_with_payments,
        )

    def populate_data_to_notify(
        self,
        data: dict[str, Any],
        section: NotificationSection,
        email: str,
        company_emails_sets: dict[NotificationSectionType, set[str]],
    ) -> bool:
        if section.cfdis and email in company_emails_sets[section.type]:
            data[section.name] = section.cfdis
            return True
        return False

    def _get_links(self, company: Company, date: datetime) -> dict[str, str]:
        year = date.year
        month = date.month
        period = f"{year}-{month}"
        app_url = envars.FRONTEND_BASE_URL

        return {
            "errors_issued_link": f"{app_url}/validations?cid={company.id}&period={period}",  # noqa: E501
            "errors_received_link": f"{app_url}/validations?cid={company.id}&period={period}",  # noqa: E501
            "cancelled_issued_link": f"{app_url}/cfdi-issued?cid={company.id}&type=ingress&period={period}&state=inactive&cancelled=cmea",  # noqa: E501
            "cancelled_received_link": f"{app_url}/cfdi-received?cid={company.id}&type=ingress&period={period}&state=inactive&cancelled=cmea",  # noqa: E501
            "efos_link": f"{app_url}/efos?cid={company.id}&type=ingress&period={period}&state=active",  # noqa: E501
            "errors_pue_issued_link": f"{app_url}/validations?cid={company.id}&period={period}",  # noqa: E501
            "errors_pue_received_link": f"{app_url}/validations?cid={company.id}&period={period}",  # noqa: E501
        }

    def get_notification_body(
        self,
        user: UserORM,
        email: str,
        company: Company,
        company_emails_sets: dict[NotificationSectionType, set[str]],
        notification_sections: Iterable[NotificationSection],
    ) -> str:
        now = utc_now()
        envars_dict = {
            "IS_SIIGO": envars.IS_SIIGO,
            "FRONTEND_BASE_URL": envars.FRONTEND_BASE_URL,
            "VITE_APP_LOGO_URL": envars.VITE_APP_LOGO_URL,
        }

        data_dict: dict[str, Any] = {
            "user": user,
            "company": company,
            "envars": envars_dict,
            **self._get_links(company, now),
        }

        sections = [
            self.populate_data_to_notify(
                data_dict,
                section,
                email,
                company_emails_sets,
            )
            for section in notification_sections
        ]

        return self._render_notification_body(data_dict) if any(sections) else ""

    def _render_notification_body(self, data_dict: dict[str, Any]) -> str:
        template = env.get_template(NOTIFICATION_JINJA_TEMPLATE_PATH)
        return template.render(**data_dict)

    def send_notification(
        self,
        company: Company,
        notification_sections: Iterable[NotificationSection],
    ):
        company_emails_sets = self.get_emails_sets(company)
        all_emails = set().union(*company_emails_sets.values())  # type: ignore
        for email in all_emails:
            user = UserController.get_user_by_email(email, session=self.session)
            if notifications_body := self.get_notification_body(
                user,
                email,
                company,
                company_emails_sets,
                notification_sections,
            ):
                self.send_email(user and user.email or email, company, notifications_body)

    def get_emails_sets(self, company: Company) -> dict[NotificationSectionType, set[str]]:
        return {
            NotificationSectionType.EFOS: set(company.emails_to_send_efos or []),
            NotificationSectionType.ERRORS: set(company.emails_to_send_errors or []),
            NotificationSectionType.PUE_WITH_PAYMENTS: set(
                company.emails_to_send_errors or []  # TODO change to new field
            ),
            NotificationSectionType.CANCELED: set(company.emails_to_send_canceled or []),
        }

    def get_subject(self, company) -> str:
        PRODUCT_NAME = "Siigo Fiscal" if envars.IS_SIIGO else "ezaudita"
        return f"{PRODUCT_NAME} - Validaciones CFDI para {company.rfc}"  # TODO

    def send_email(self, email: str, company: Company, notifications_body: str):
        subject = self.get_subject(company)
        body_text = html2text(notifications_body)

        try:
            if envars.IS_SIIGO:
                email_id = f"{company.identifier}-{email}-{mx_now()}"
                send_email_siigo(email, email_id, notifications_body, subject, body_text)
            else:
                ses_client().send_email(
                    Source=envars.SES_MAIL,
                    Destination={"ToAddresses": [email]},
                    Message={
                        "Subject": {"Data": subject},
                        "Body": {
                            "Text": {"Data": body_text},
                            "Html": {"Data": notifications_body},
                        },
                    },
                )
            log(
                Modules.NOTIFICATION,
                DEBUG,
                "SENT_EMAIL",
                {
                    "company_identifier": company.identifier,
                    "email": email,
                },
            )
        except ParamValidationError as e:
            log(
                Modules.NOTIFICATION,
                EXCEPTION,
                "FAILED_SENDING_EMAIL",
                {
                    "company_identifier": company.identifier,
                    "email": email,
                    "exception": e,
                },
            )
        except Exception as e:
            log(
                Modules.NOTIFICATION,
                EXCEPTION,
                "FAILED_UNKNOWN",
                {
                    "company_identifier": company.identifier,
                    "email": email,
                    "exception": e,
                },
            )


def send_email_siigo(email: str, email_id: str, html_part: str, subject: str, text_part: str):
    secrets_json = secretsmanager_get(EMAIL_PROVIDER_SECRET)
    email_secrets = EmailProviderType.model_validate(secrets_json)
    request = requests.post(
        url=email_secrets.endpoint,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {email_secrets.token}",
        },
        json={
            "ExternalSendID": email_id,
            "From": {
                "Email": email_secrets.email,
                "Name": email_secrets.name,
            },
            "To": [
                {"Email": email},
            ],
            "Cc": [],
            "Bcc": [],
            "TextPart": text_part,
            "HTMLPart": html_part,
            "Subject": subject,
            "Attachments": [],
            "GroupId": "Ezaudita",  # TODO
            "Source": "Ezaudita",  # TODO
            "Country": "MX",
        },
    )
    if not request.ok:
        raise Exception(f"Failed to send email: {request.text}")


def update_last_notification(
    session: Session,
    company_ids_to_update: set[int],  # TODO usar identifier cuando sea su PK
):
    now = utc_now()
    session.bulk_update_mappings(
        Company,
        [{"id": company_id, "last_notification": now} for company_id in company_ids_to_update],
    )
