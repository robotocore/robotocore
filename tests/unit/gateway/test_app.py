"""Tests for the gateway ASGI application."""


def test_health_endpoint(client):
    response = client.get("/_robotocore/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "running"
