import json
import uuid

from chalice import BadRequestError
from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.controllers.company import get_company_isr_percentage
from chalicelib.new.config.infra.envars.control import ISR_DEFAULT_PERCENTAGE
from tests.fixtures.factories.company import CompanyFactory
from tests.fixtures.factories.user import UserFactory
from tests.fixtures.factories.workspace import WorkspaceFactory


def test_isr_get_percentage(session: Session):
    user = UserFactory.build(identifier=str(uuid.uuid4()), invited_by_id=None)
    workspace = WorkspaceFactory.build(identifier=str(uuid.uuid4()), owner_id=user.id)
    user.workspace = workspace
    session.add(user)

    company = CompanyFactory.build(
        identifier="00000000-0000-0000-0000-000000000004",
        workspace_identifier=workspace.identifier,
        workspace_id=workspace.id,
        pasto_company_identifier=None,
        data={
            "scrap_status_constancy": {"current_status": "", "updated_at": ""},
            "scrap_status_opinion": {"current_status": "", "updated_at": ""},
        },
    )

    # We validate default retunr value is the ISR_DEFAULT_PERCENTAGE value when there's no percentage
    assert get_company_isr_percentage(company) == ISR_DEFAULT_PERCENTAGE

    session.add(company)

    percentage = 0.53

    # This is part of the set logic
    company.data["isr_percentage"] = percentage

    # We validate value is correct
    assert company.data.get("isr_percentage") == 0.53

    # We validate other data info wasn't changed
    assert company.data.get("scrap_status_constancy") == {"current_status": "", "updated_at": ""}
    assert company.data.get("scrap_status_opinion") == {"current_status": "", "updated_at": ""}


def test_isr_set_valid_percentage_endpoint(client_authenticated: Client, company):
    # Happy path scenario
    response = client_authenticated.http.put(
        "/Company/set_isr_percentage",
        body=json.dumps(
            {
                "company_identifier": company.identifier,
                "percentage": ISR_DEFAULT_PERCENTAGE,
            }
        ),
    )

    assert response.status_code == 200


def test_isr_set_wrong_percentage_endpoint(client_authenticated: Client, company):
    # Invalid percentage scenario
    response = client_authenticated.http.put(
        "/Company/set_isr_percentage",
        body=json.dumps({"company_identifier": company.identifier, "percentage": 100}),
    )

    assert response.status_code == BadRequestError.STATUS_CODE
    assert "ISR percentage must be one of" in response.json_body["Message"]
