"""FIS-611: Tests para endpoint GET /License/{workspace_identifier}."""

from unittest.mock import Mock, patch

from chalice.test import Client
from sqlalchemy.orm import Session

from app import app
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace


def test_fis611_endpoint_success_end_to_end(session: Session, user: User, workspace: Workspace):
    """Test end-to-end: workspace → email → Siigo → respuesta."""
    session.add(user)
    session.flush()

    session.add(workspace)
    session.commit()

    mock_siigo_response = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "rfc": "XAXX010101000",
        "name": "Juan",
        "lastName": "Pérez García",
        "status": 1,
        "portalUserName": "juan.perez@siigo.com",
        "freeTrialDays": "15",
        "inactiveDays": "0",
        "initialDiscountPercentage": "20%",
        "finalDiscountPercentage": "10%",
        "freeTrialStartDate": "2024-01-01T00:00:00Z",
        "freeTrialActivationDate": "2024-01-08T10:30:00Z",
    }

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_siigo_response
        mock_get.return_value = mock_response

        with Client(app) as client:
            response = client.http.get(f"/License/{workspace.identifier}")

            assert response.status_code == 200
            body = response.json_body

            assert body["id"] == "123e4567-e89b-12d3-a456-426614174000"
            assert body["rfc"] == "XAXX010101000"
            assert body["status"] == 1
            assert body["portalUserName"] == "juan.perez@siigo.com"
            assert body["freeTrialDays"] == "15"
            assert body["initialDiscountPercentage"] == "20%"
            assert body["finalDiscountPercentage"] == "10%"
            assert "freeTrialStartDate" in body
            assert "freeTrialActivationDate" in body


def test_fis611_endpoint_error_not_found(session: Session, workspace: Workspace):
    """Test error 404 cuando Siigo no encuentra prueba gratis."""
    session.add(workspace)
    session.commit()

    with patch("httpx.AsyncClient.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "No se encontró registro de prueba gratis"
        mock_get.return_value = mock_response

        with Client(app) as client:
            response = client.http.get(f"/License/{workspace.identifier}")
            assert response.status_code == 404
