import random
import uuid
from unittest.mock import Mock, patch

from sqlalchemy.orm import Session

from chalicelib.controllers.company import CompanyController
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace


def read_files(path):
    with open(path, "rb") as f:
        return f.read()


cer_path = "tests/load_data/companies/company1/certificado.cer"
key_path = "tests/load_data/companies/company1/llave.key"


def test_send_scrap_on_company_create(session: Session):
    cer = read_files(cer_path)
    key = read_files(key_path)
    password = "12345678a"

    user = User(
        identifier=str(uuid.uuid4()),
        id=random.randint(2000, 10000),
        invited_by_id=None,
        email="",
    )
    session.add(user)
    session.flush()
    workspace = Workspace(
        identifier=str(uuid.uuid4()),
        owner_id=user.id,
        license={
            "date_start": "2025-06-24T19:10:19.886491",
            "date_end": "2025-07-04T19:10:19.886491",
            "details": {"max_companies": 1, "max_emails_enroll": 1},
        },
    )
    session.add(workspace)

    context = {"user": user}

    published_events = []

    def mock_publish(event_type, event_data):
        published_events.append((event_type, event_data))

    with patch("chalicelib.controllers.pdf_scraper.get_global_bus") as mock_bus_factory:
        mock_bus = Mock()
        mock_bus.publish = mock_publish
        mock_bus_factory.return_value = mock_bus

        company = CompanyController.create_from_certs(
            workspace_identifier=workspace.identifier,
            workspace_id=workspace.id,
            cer=cer,
            key=key,
            password=password,
            session=session,
            context=context,
        )

        # La compañía debería tener el current estatus pending para ambos documentos
        assert company.data["scrap_status_constancy"]["current_status"] == "pending"
        assert company.data["scrap_status_opinion"]["current_status"] == "pending"

        # Verificamos que se hayan publicado dos eventos y que hayan sido del tipo SAT_SCRAP_PDF
        assert len(published_events) == 2
        assert all(event_type == EventType.SAT_SCRAP_PDF for event_type, _ in published_events)
