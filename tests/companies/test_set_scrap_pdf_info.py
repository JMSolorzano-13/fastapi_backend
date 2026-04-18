import uuid

import pytest
from sqlalchemy.orm import Session

from chalicelib.controllers.pdf_scraper import ScraperController
from tests.fixtures.factories.company import CompanyFactory
from tests.fixtures.factories.user import UserFactory
from tests.fixtures.factories.workspace import WorkspaceFactory


@pytest.mark.skip
def test_set_scrap_pdf_info(session: Session):
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

    company2 = CompanyFactory.build(
        identifier="00000000-0000-0000-0000-000000000005",
        workspace_identifier=workspace.identifier,
        workspace_id=workspace.id,
        pasto_company_identifier=None,
        data={
            "scrap_status_constancy": {"current_status": "", "updated_at": ""},
            "scrap_status_opinion": {"current_status": "", "updated_at": ""},
        },
    )

    session.add(workspace)
    session.add(company)

    payload = {
        "company_identifier": company.identifier,
        "document_type": "constancy",
    }
    ScraperController.set_scraper_status(
        "pending",
        payload.get("document_type"),
        payload.get("company_identifier"),
        session=session,
    )

    # We validate company requested has changed his status
    assert (
        company.data["scrap_status_constancy"]["current_status"] == "pending"
        and company.data["scrap_status_opinion"]["current_status"] == ""
    )

    # We validate not requested company remain with empty status
    assert company2.data["scrap_status_constancy"]["current_status"] == ""
