from chalice.test import Client

from app import app


def test_health_central():
    with Client(app) as client:
        result = client.http.get("/status/health/api")
        assert result.status_code == 200
