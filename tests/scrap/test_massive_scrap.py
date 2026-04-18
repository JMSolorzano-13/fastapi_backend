import json

from chalice.test import Client
from sqlalchemy import delete
from sqlalchemy.orm import Session

from chalicelib.controllers.tenant.session import new_company_session_from_company_identifier
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant import SATQuery


def delete_first_scrap_autosync(companies: list[Company], session: Session):
    for company in companies:
        with new_company_session_from_company_identifier(
            company_identifier=company.identifier,
            session=session,
            read_only=False,
        ) as company_session:
            stmt = delete(SATQuery)
            company_session.execute(stmt)
            company_session.commit()
            pass


def test_massive_scrap(client_authenticated: Client, session: Session, company: Company):
    companies = [company]

    delete_first_scrap_autosync(companies, session)

    response = client_authenticated.http.post(
        "/SATQuery/massive_scrap",
        body=json.dumps(
            {
                "start": "2025-01-01",
                "end": "2025-08-01",
            }
        ),
        headers={
            "Content-Type": "application/json",
        },
    )
    # VALIDA QUE TODAS LAS EMPRESAS SE HAYAN LANZADO SCRAP
    assert response.status_code == 200

    skipped = set(json.loads(response.body.decode("utf-8"))["companies"]["skipped"])
    published = set(json.loads(response.body.decode("utf-8"))["companies"]["published"])
    assert not skipped.intersection(published)
    assert published == {c.identifier for c in companies}


def test_massive_scrap_not_published(client_authenticated: Client, company: Company):
    companies = [company]
    response = client_authenticated.http.post(
        "/SATQuery/massive_scrap",
        body=json.dumps(
            {
                "start": "2025-01-01",
                "end": "2025-08-01",
            }
        ),
    )
    # VALIDA QUE TODAS LAS EMPRESAS NO HAYAN LANZADO SCRAP
    assert response.status_code == 200

    skipped = set(json.loads(response.body.decode("utf-8"))["companies"]["skipped"])
    published = set(json.loads(response.body.decode("utf-8"))["companies"]["published"])
    assert not skipped.intersection(published)
    assert skipped == {c.identifier for c in companies}


def test_massive_scrap_user_without_permission(
    client_authenticated: Client,
    session: Session,
    company_other: Company,
):
    companies = [company_other]
    delete_first_scrap_autosync(companies, session)

    response = client_authenticated.http.post(
        "/SATQuery/massive_scrap",
        body=json.dumps(
            {
                "start": "2025-01-01",
                "end": "2025-08-01",
            }
        ),
    )
    # VALIDA QUE AL user_other NO TENGA PERMISO Y DEVUELVE VACIO TANTO PUBLISHED COMO SKIPPED
    assert response.status_code == 200, response.json_body

    skipped = json.loads(response.body.decode("utf-8"))["companies"]["skipped"]
    published = json.loads(response.body.decode("utf-8"))["companies"]["published"]
    for c in companies:
        assert c.identifier not in skipped
        assert c.identifier not in published
