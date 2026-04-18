from sqlalchemy.orm import Session

from chalicelib.controllers.notification import (
    NotificationController,
    NotificationSection,
    NotificationSectionType,
)
from chalicelib.schema.models.tenant.cfdi import CFDI
from tests.fixtures.factories.company import CompanyFactory
from tests.fixtures.factories.user import UserFactory


def test_send_email(session: Session):
    notification_controller = NotificationController(session=session)
    company = CompanyFactory.build(
        identifier="test-company-12345",
        name="Test Company",
        rfc="TODASMIAS12345",
        workspace_identifier="132435-56789-90123-45678",
        workspace_id=1,
    )

    user = UserFactory.build(email="test_notif@gmail.com")

    cfdi = CFDI.demo()

    notification_body = notification_controller.get_notification_body(
        user=user,
        company=company,
        email="test_notif@gmail.com",
        company_emails_sets={NotificationSectionType.ERRORS: {"test_notif@gmail.com"}},
        notification_sections=[
            NotificationSection(
                name="cfdis_with_errors_issued",
                type=NotificationSectionType.ERRORS,
                cfdis=[cfdi],
            )
        ],
    )

    # TODO: Check how to test the send_email function if it's possible
    # sm = boto3.client("secretsmanager", region_name="us-east-1")
    # sm.create_secret(
    #     Name="siigo/mailprovider/token",
    #     SecretString=json.dumps(
    #         {
    #             "endpoint": "{{url}}",
    #             "token": "{{token}}",
    #             "email": "{{email}}",
    #             "name": "Ezaudita Mailer",
    #         }
    #     ),
    # )

    # notification_controller.send_email(
    #     email="samuel.garcia@ezaudita.com",
    #     company=company,
    #     notifications_body=notification_body,
    # )

    assert "CFDIs Emitidos con error" in notification_body
