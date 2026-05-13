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

    def test_python_versions_reports_host_match_when_no_versioned_binaries(self):
        # No versioned python binaries on PATH → only the host's version
        # appears under versions["python"].
        import sys

        from robotocore.services.lambda_.runtimes.python import _RUNTIME_BINARY as PY_MAP

        host = (sys.version_info.major, sys.version_info.minor)

        def _which(name):
            # Pretend "dotnet" / "node" / "ruby" / "java" / "javac" are unavailable
            # so other branches don't add noise; also no python3.X versioned binaries.
            return None

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=_which):
            data = TestClient(app).get("/_robotocore/runtimes").json()
        expected = [rt for rt, ver in PY_MAP.items() if ver == host]
        assert data["versions"]["python"] == sorted(expected)

    def test_python_versions_includes_versioned_binaries_when_present(self):
        # When python3.10 / python3.11 / python3.13 are on PATH, the endpoint
        # advertises them too — PythonExecutor will subprocess-dispatch.
        import sys

        from robotocore.services.lambda_.runtimes.python import _RUNTIME_BINARY as PY_MAP

        host = (sys.version_info.major, sys.version_info.minor)
        installed = {"python3.10", "python3.11", "python3.13"}

        def _which(name):
            if name in installed:
                return f"/usr/local/bin/{name}"
            return None

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=_which):
            data = TestClient(app).get("/_robotocore/runtimes").json()
        # All four (host's version via in-process + three versioned binaries)
        # should appear, deduped and sorted.
        host_rt = next((rt for rt, ver in PY_MAP.items() if ver == host), None)
        expected = installed | ({host_rt} if host_rt else set())
        assert set(data["versions"]["python"]) == expected

    def test_dotnet_versions_reports_each_installed_sdk(self):
        # With SDKs for net6/net8/net9 all installed, every Lambda dotnet
        # runtime is faithfully executable (per-TFM dispatch).
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
                data = TestClient(app).get("/_robotocore/runtimes").json()
        assert set(data["versions"]["dotnet"]) == {"dotnet6", "dotnet8", "dotnet9"}

    def test_status_field_marks_installed_vs_available(self):
        # When no binaries on PATH but the plan is registered, the status
        # field should mark the runtime as "available_to_install" — telling
        # the user that POST /runtimes/install can pre-warm it.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", return_value=None):
            with patch("robotocore.gateway.app._is_java_functional", return_value=False):
                with patch.object(dotnet_mod, "_installed_majors", set()):
                    data = TestClient(app).get("/_robotocore/runtimes").json()
        # Plans exist for ruby3.3, java17, dotnet8, etc. — none installed.
        assert data["status"].get("java17") == "available_to_install"
        assert data["status"].get("ruby3.3") == "available_to_install"

    def test_status_field_marks_installed_when_binary_present(self):
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        # node20 is on PATH → versions["nodejs"] includes nodejs20.x → status=installed.
        with patch(
            "robotocore.gateway.app.shutil.which",
            side_effect=lambda x: f"/usr/bin/{x}" if x == "node20" or x == "node" else None,
        ):
            with patch("robotocore.gateway.app._is_java_functional", return_value=False):
                with patch.object(dotnet_mod, "_installed_majors", set()):
                    data = TestClient(app).get("/_robotocore/runtimes").json()
        assert data["status"]["nodejs20.x"] == "installed"

    def test_dotnet_versions_only_advertises_installed_sdks(self):
        # Only SDK 8 installed → only dotnet8 is faithfully executable.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        app = Starlette(
            routes=[Route("/_robotocore/runtimes", _app.runtimes_endpoint, methods=["GET"])]
        )
        with patch("robotocore.gateway.app.shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.object(dotnet_mod, "_installed_majors", {8}):
                data = TestClient(app).get("/_robotocore/runtimes").json()
        assert data["versions"]["dotnet"] == ["dotnet8"]


class TestRuntimesInstallEndpoint:
    """POST /_robotocore/runtimes/install."""

    @pytest.fixture
    def client(self):
        app = Starlette(
            routes=[
                Route(
                    "/_robotocore/runtimes/install",
                    _app.runtimes_install_endpoint,
                    methods=["POST"],
                )
            ]
        )
        return TestClient(app)

    def test_missing_body_returns_400(self, client):
        resp = client.post("/_robotocore/runtimes/install")
        assert resp.status_code == 400

    def test_unknown_runtime_returns_no_installer(self, client):
        resp = client.post("/_robotocore/runtimes/install", json={"runtimes": ["cobol42"]})
        assert resp.status_code == 200
        assert resp.json()["results"]["cobol42"] == "no_installer"

    def test_already_installed_short_circuits(self, client, monkeypatch):
        from robotocore.services.lambda_.runtimes import install as install_mod

        class _AlwaysInstalledPlan:
            runtime = "test_already"

            def is_installed(self):
                return True

        monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", False)
        monkeypatch.setitem(install_mod._PLANS, "test_already", _AlwaysInstalledPlan())
        resp = client.post("/_robotocore/runtimes/install", json={"runtimes": ["test_already"]})
        assert resp.json()["results"]["test_already"] == "already_installed"

    def test_disabled_returns_400(self, client, monkeypatch):
        from robotocore.services.lambda_.runtimes import install as install_mod

        monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", True)
        resp = client.post("/_robotocore/runtimes/install", json={"runtimes": ["java17"]})
        assert resp.status_code == 400
        assert "disabled" in resp.json()["error"]

    def test_install_invoked_for_uninstalled_runtime(self, client, monkeypatch, tmp_path):
        # Wire a fake plan whose .install() just writes the sentinel.
        from robotocore.services.lambda_.runtimes import install as install_mod
        from robotocore.services.lambda_.runtimes.install import InstallPlan

        cache = tmp_path / "cache"
        wrappers = tmp_path / "bin"
        cache.mkdir()
        wrappers.mkdir()
        monkeypatch.setattr(install_mod, "CACHE_DIR", cache)
        monkeypatch.setattr(install_mod, "WRAPPER_BIN_DIR", wrappers)
        monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", False)

        class _StubPlan(InstallPlan):
            def install(self):
                self._write_wrapper("#!/bin/sh\necho stub\n")
                self._mark_installed()

        plan = _StubPlan(
            runtime="test_install",
            family="test",
            prefix=cache / "test_install",
            binary_name="test_install",
        )
        monkeypatch.setitem(install_mod._PLANS, "test_install", plan)
        resp = client.post("/_robotocore/runtimes/install", json={"runtimes": ["test_install"]})
        assert resp.json()["results"]["test_install"] == "installed"
        assert (cache / "test_install" / ".installed").exists()
        assert (wrappers / "test_install").exists()
