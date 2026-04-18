"""
Tests para CommonController.record_to_dict con manejo de listas JSONB
"""

import uuid

from sqlalchemy.orm import Session

from chalicelib.controllers.company import CompanyController
from chalicelib.schema.models import Company, User, Workspace


def test_record_to_dict_with_empty_jsonb_lists(session: Session):
    """
    Prueba que record_to_dict maneja correctamente listas JSONB vacías.

    Esto verifica el fix para columnas JSONB que pueden contener listas vacías,
    las cuales no deberían causar crash al serializar a dict.
    """
    workspace = Workspace(
        name="Test Workspace",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()

    user = User(
        identifier=str(uuid.uuid4()),
        name="Test User",
        email="test@example.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1234567890",
    )
    session.add(user)
    session.flush()

    workspace.owner_id = user.id
    session.flush()

    company = Company(
        identifier=str(uuid.uuid4()),
        name="Test Company",
        rfc="TEST123456XXX",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        emails_to_send_efos=[],
        emails_to_send_errors=[],
        emails_to_send_canceled=[],
    )
    session.add(company)
    session.flush()

    result = CompanyController.record_to_dict(company)

    assert result["emails_to_send_efos"] == []
    assert result["emails_to_send_errors"] == []
    assert result["emails_to_send_canceled"] == []


def test_record_to_dict_with_populated_jsonb_lists(session: Session):
    """
    Prueba que record_to_dict maneja correctamente listas JSONB pobladas.

    Esto verifica que listas JSONB con valores primitivos (strings)
    se serializan correctamente sin intentar convertirlos a dicts.
    """
    workspace = Workspace(
        name="Test Workspace",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()

    user = User(
        identifier=str(uuid.uuid4()),
        name="Test User",
        email="test@example.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1234567890",
    )
    session.add(user)
    session.flush()

    workspace.owner_id = user.id
    session.flush()

    test_emails = ["email1@test.com", "email2@test.com"]
    company = Company(
        identifier=str(uuid.uuid4()),
        name="Test Company",
        rfc="TEST123456XXX",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        emails_to_send_efos=test_emails,
        emails_to_send_errors=test_emails,
        emails_to_send_canceled=test_emails,
    )
    session.add(company)
    session.flush()

    result = CompanyController.record_to_dict(company)

    assert result["emails_to_send_efos"] == test_emails
    assert result["emails_to_send_errors"] == test_emails
    assert result["emails_to_send_canceled"] == test_emails

    assert isinstance(result["emails_to_send_efos"], list)
    assert all(isinstance(email, str) for email in result["emails_to_send_efos"])


def test_to_nested_dict_with_jsonb_lists(session: Session):
    """
    Prueba que to_nested_dict (que usa record_to_dict) funciona con listas JSONB.

    Este es el flujo real usado por la API al retornar datos de empresa.
    """
    workspace = Workspace(
        name="Test Workspace",
        license={"details": {"products": [], "max_companies": 10}, "stripe_status": "active"},
    )
    session.add(workspace)
    session.flush()

    user = User(
        identifier=str(uuid.uuid4()),
        name="Test User",
        email="test@example.com",
        cognito_sub=uuid.uuid4().hex,
        phone="1234567890",
    )
    session.add(user)
    session.flush()

    workspace.owner_id = user.id
    session.flush()

    company = Company(
        identifier=str(uuid.uuid4()),
        name="Test Company",
        rfc="TEST123456XXX",
        workspace_id=workspace.id,
        workspace_identifier=workspace.identifier,
        emails_to_send_efos=["test@example.com"],
        emails_to_send_errors=[],
        emails_to_send_canceled=["admin@example.com", "support@example.com"],
    )
    session.add(company)
    session.flush()

    result = CompanyController.to_nested_dict(company)

    # Verificar que retorna una lista con un dict
    assert isinstance(result, list)
    assert len(result) == 1

    company_dict = result[0]

    assert company_dict["emails_to_send_efos"] == ["test@example.com"]
    assert company_dict["emails_to_send_errors"] == []
    assert company_dict["emails_to_send_canceled"] == ["admin@example.com", "support@example.com"]
