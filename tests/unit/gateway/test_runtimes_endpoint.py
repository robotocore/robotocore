"""Unit tests for GET /_robotocore/runtimes."""

from unittest.mock import patch

import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

import robotocore.gateway.app as _app


@pytest.fixture(scope="module")
def client():
    app = Starlette(
        routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
    )
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture(autouse=True)
def _reset_java_cache():
    """Reset the Java functional probe cache between tests so mocks take effect."""
    _app._java_functional = None
    yield
    _app._java_functional = None


class TestRuntimesEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/_robotocore/runtimes")
        assert resp.status_code == 200

    def test_response_has_available_and_all(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert "available" in data
        assert "all" in data

    def test_all_contains_expected_families(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert set(data["all"]) == {"python", "nodejs", "ruby", "java", "dotnet", "custom"}

    def test_all_matches_canonical_constant(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert set(data["all"]) == set(_app._ALL_RUNTIME_FAMILIES)

    def test_python_always_available(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert "python" in data["available"]

    def test_custom_always_available(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert "custom" in data["available"]

    def test_available_is_subset_of_all(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert set(data["available"]).issubset(set(data["all"]))

    def test_nodejs_present_when_node_on_path(self, client):
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            resp = client.get("/_robotocore/runtimes")
        assert "nodejs" in resp.json()["available"]

    def test_nodejs_absent_when_node_missing(self, client):
        def _which(name):
            return None if name == "node" else f"/usr/bin/{name}"

        with patch("robotocore.gateway.app.shutil.which", side_effect=_which):
            resp = client.get("/_robotocore/runtimes")
        assert "nodejs" not in resp.json()["available"]

    def test_ruby_present_when_ruby_on_path(self, client):
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            resp = client.get("/_robotocore/runtimes")
        assert "ruby" in resp.json()["available"]

    def test_ruby_absent_when_ruby_missing(self, client):
        def _which(name):
            return None if name == "ruby" else f"/usr/bin/{name}"

        with patch("robotocore.gateway.app.shutil.which", side_effect=_which):
            resp = client.get("/_robotocore/runtimes")
        assert "ruby" not in resp.json()["available"]

    def test_java_absent_when_binaries_missing(self, client):
        with patch("robotocore.gateway.app._is_java_functional", return_value=False):
            resp = client.get("/_robotocore/runtimes")
        assert "java" not in resp.json()["available"]

    def test_java_present_when_functional(self, client):
        with patch("robotocore.gateway.app._is_java_functional", return_value=True):
            resp = client.get("/_robotocore/runtimes")
        assert "java" in resp.json()["available"]

    def test_dotnet_present_when_dotnet_on_path(self, client):
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            resp = client.get("/_robotocore/runtimes")
        assert "dotnet" in resp.json()["available"]

    def test_dotnet_absent_when_dotnet_missing(self, client):
        def _which(name):
            return None if name == "dotnet" else f"/usr/bin/{name}"

        with patch("robotocore.gateway.app.shutil.which", side_effect=_which):
            resp = client.get("/_robotocore/runtimes")
        assert "dotnet" not in resp.json()["available"]

    def test_available_is_sorted(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert data["available"] == sorted(data["available"])
