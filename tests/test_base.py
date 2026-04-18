from chalice.test import Client

from app import app


def test_health():
    with Client(app) as client:
        result = client.http.get("/status/health/api")
        assert result.status_code == 200


def test_health_db():
    with Client(app) as client:
        result = client.http.get("/status/health/db")
        assert result.status_code == 200


def test_version():
    with Client(app) as client:
        result = client.http.get("/status/version")
        assert result.status_code == 200
