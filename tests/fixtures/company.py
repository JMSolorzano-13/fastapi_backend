import base64
import json

import pytest
from chalice.test import Client
from sqlalchemy.orm import Session

from chalicelib.schema.models.company import Company
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace


@pytest.fixture
def company_identifier_to_use(request) -> str:
    return request.config.getoption("--cid")


@pytest.fixture
def company(
    client_authenticated: Client,
    session: Session,
    workspace: Workspace,
    company_identifier_to_use: str,
) -> Company:
    """Fixture de Company que ya viene enlazada al user y su workspace

    Creada con el endpoint real de /Company para asegurar que todo el flujo funciona bien.
    """
    if company_identifier_to_use and (
        _company := session.query(Company)
        .filter(Company.identifier == company_identifier_to_use)
        .first()
    ):
        return _company

    cer_encoded = read_and_encode("tests/load_data/companies/company1/certificado.cer")
    key_encoded = read_and_encode("tests/load_data/companies/company1/llave.key")

    response = client_authenticated.http.post(
        "/Company",
        body=json.dumps(
            {
                "cer": cer_encoded,
                "key": key_encoded,
                "pas": "12345678a",
                "workspace_id": workspace.id,
                "workspace_identifier": workspace.identifier,
            }
        ),
    )

    assert response.status_code == 200, response.json_body

    company = (
        session.query(Company)
        .filter(Company.workspace_identifier == workspace.identifier)
        .order_by(Company.created_at.desc())
        .first()
    )

    return company


@pytest.fixture
def workspace_other(user_other: User) -> Workspace:
    return user_other.workspace


@pytest.fixture
def company_other(
    workspace_other: Workspace,
    client: Client,
    session,
    user_other_token: str,
) -> Company:
    cer_encoded = read_and_encode("tests/load_data/companies/company2/certificado.cer")
    key_encoded = read_and_encode("tests/load_data/companies/company2/llave.key")

    response = client.http.post(
        "/Company",
        body=json.dumps(
            {
                "cer": cer_encoded,
                "key": key_encoded,
                "pas": "12345678a",
                "workspace_id": workspace_other.id,
                "workspace_identifier": workspace_other.identifier,
            }
        ),
        headers={
            "Content-Type": "application/json",
            "access_token": user_other_token,
        },
    )

    assert response.status_code == 200, response.json_body

    return (
        session.query(Company)
        .filter(
            Company.workspace_id == workspace_other.id,
            Company.workspace_identifier == workspace_other.identifier,
        )
        .one()
    )


# AUX METHODS
def read_and_encode(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")
