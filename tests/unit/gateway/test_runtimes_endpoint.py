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


class TestRuntimesEndpointVersions:
    """Version-specific availability under the new ``versions`` field."""

    @pytest.fixture
    def client(self):
        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        return TestClient(app, raise_server_exceptions=True)

    def test_versions_key_present_with_all_families(self, client):
        data = client.get("/_robotocore/runtimes").json()
        assert "versions" in data
        assert set(data["versions"].keys()) == set(_app._ALL_RUNTIME_FAMILIES)

    def test_nodejs_versions_reflect_which(self, client):
        # All node binaries present
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            data = client.get("/_robotocore/runtimes").json()
        assert set(data["versions"]["nodejs"]) >= {"nodejs18.x", "nodejs20.x", "nodejs22.x"}

    def test_ruby_versions_reflect_which(self, client):
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            data = client.get("/_robotocore/runtimes").json()
        assert set(data["versions"]["ruby"]) >= {"ruby3.2", "ruby3.3", "ruby3.4"}

    def test_no_versions_when_no_binaries(self, client):
        with patch("robotocore.gateway.app.shutil.which", return_value=None):
            with patch("robotocore.gateway.app._is_java_functional", return_value=False):
                data = client.get("/_robotocore/runtimes").json()
        assert data["versions"]["nodejs"] == []
        assert data["versions"]["ruby"] == []
        assert data["versions"]["java"] == []
        assert data["versions"]["dotnet"] == []

    def test_custom_versions_empty(self, client):
        # provided.* runtimes have no version dispatch
        data = client.get("/_robotocore/runtimes").json()
        assert data["versions"]["custom"] == []

    def test_python_versions_reports_host_match(self, client):
        # The endpoint should report whichever pythonX.Y matches the host.
        import sys

        host = (sys.version_info.major, sys.version_info.minor)
        data = client.get("/_robotocore/runtimes").json()
        # The host version may or may not be in our map (e.g. python3.12);
        # whatever's reported must match that map.
        from robotocore.services.lambda_.runtimes.python import _RUNTIME_BINARY as PY_MAP

        expected = [rt for rt, ver in PY_MAP.items() if ver == host]
        assert data["versions"]["python"] == sorted(expected)

    def test_dotnet_versions_only_reports_host_max(self):
        # On a {6, 8, 9} host, we can only faithfully execute dotnet9 (our
        # _detect_tfm() always builds at host max). The endpoint must report
        # only dotnet9 — reporting dotnet6/dotnet8 too would advertise
        # version fidelity we don't deliver.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
                data = TestClient(app).get("/_robotocore/runtimes").json()
        assert data["versions"]["dotnet"] == ["dotnet9"]

    def test_dotnet_versions_when_only_one_major_installed(self):
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.object(dotnet_mod, "_installed_majors", {8}):
                data = TestClient(app).get("/_robotocore/runtimes").json()
        assert data["versions"]["dotnet"] == ["dotnet8"]
