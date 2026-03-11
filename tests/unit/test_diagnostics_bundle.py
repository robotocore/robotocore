"""Unit tests for the diagnostic bundle endpoint."""

import os
import sys
from unittest.mock import patch

from robotocore.diagnostics_bundle import collect_diagnostics


class TestSystemSection:
    def test_includes_python_version(self):
        result = collect_diagnostics(sections=["system"])
        assert "system" in result
        assert result["system"]["python_version"] == sys.version

    def test_includes_platform(self):
        result = collect_diagnostics(sections=["system"])
        assert "platform" in result["system"]
        assert isinstance(result["system"]["platform"], str)

    def test_includes_architecture(self):
        result = collect_diagnostics(sections=["system"])
        assert "architecture" in result["system"]

    def test_includes_pid(self):
        result = collect_diagnostics(sections=["system"])
        assert result["system"]["pid"] == os.getpid()

    def test_includes_cwd(self):
        result = collect_diagnostics(sections=["system"])
        assert result["system"]["working_directory"] == os.getcwd()


class TestServerSection:
    def test_includes_uptime(self):
        result = collect_diagnostics(sections=["server"])
        assert "server" in result
        assert "uptime_seconds" in result["server"]
        assert isinstance(result["server"]["uptime_seconds"], (int, float))

    def test_includes_version(self):
        result = collect_diagnostics(sections=["server"])
        assert "version" in result["server"]

    def test_includes_port(self):
        result = collect_diagnostics(sections=["server"])
        assert "port" in result["server"]

    def test_includes_start_time(self):
        result = collect_diagnostics(sections=["server"])
        assert "start_time" in result["server"]


class TestConfigSection:
    def test_includes_relevant_env_vars(self):
        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": "/tmp/test", "DEBUG": "1"}):
            result = collect_diagnostics(sections=["config"])
        assert "config" in result
        assert result["config"]["ROBOTOCORE_STATE_DIR"] == "/tmp/test"
        assert result["config"]["DEBUG"] == "1"

    def test_masks_sensitive_values(self):
        with patch.dict(
            os.environ,
            {
                "ROBOTOCORE_SECRET_KEY": "super-secret-123",
                "ROBOTOCORE_API_TOKEN": "tok-abc",
                "ROBOTOCORE_PASSWORD": "hunter2",
            },
        ):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["ROBOTOCORE_SECRET_KEY"] == "***MASKED***"
        assert result["config"]["ROBOTOCORE_API_TOKEN"] == "***MASKED***"
        assert result["config"]["ROBOTOCORE_PASSWORD"] == "***MASKED***"

    def test_includes_lambda_env(self):
        with patch.dict(os.environ, {"LAMBDA_EXECUTOR": "local"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["LAMBDA_EXECUTOR"] == "local"

    def test_includes_sqs_env(self):
        with patch.dict(os.environ, {"SQS_ENDPOINT_STRATEGY": "domain"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["SQS_ENDPOINT_STRATEGY"] == "domain"

    def test_includes_standard_config_keys(self):
        with patch.dict(
            os.environ,
            {
                "ENFORCE_IAM": "1",
                "PERSISTENCE": "1",
                "LOG_LEVEL": "DEBUG",
                "LOG_FORMAT": "json",
                "SERVICES": "s3,sqs",
            },
        ):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["ENFORCE_IAM"] == "1"
        assert result["config"]["PERSISTENCE"] == "1"
        assert result["config"]["LOG_LEVEL"] == "DEBUG"
        assert result["config"]["LOG_FORMAT"] == "json"
        assert result["config"]["SERVICES"] == "s3,sqs"


class TestServicesSection:
    def test_counts_native_vs_moto(self):
        result = collect_diagnostics(sections=["services"])
        assert "services" in result
        assert "native_count" in result["services"]
        assert "moto_count" in result["services"]
        assert result["services"]["native_count"] > 0
        assert result["services"]["moto_count"] > 0

    def test_lists_native_providers(self):
        result = collect_diagnostics(sections=["services"])
        assert "native_providers" in result["services"]
        assert isinstance(result["services"]["native_providers"], list)
        assert "s3" in result["services"]["native_providers"]

    def test_includes_total_count(self):
        result = collect_diagnostics(sections=["services"])
        total = result["services"]["native_count"] + result["services"]["moto_count"]
        assert result["services"]["total_count"] == total


class TestStateSection:
    def test_reports_snapshot_info(self):
        result = collect_diagnostics(sections=["state"])
        assert "state" in result
        assert "persistence_enabled" in result["state"]
        assert "state_directory" in result["state"]

    def test_reports_snapshot_count(self):
        result = collect_diagnostics(sections=["state"])
        assert "snapshot_count" in result["state"]
        assert isinstance(result["state"]["snapshot_count"], int)


class TestBackgroundEnginesSection:
    def test_lists_threads(self):
        result = collect_diagnostics(sections=["background_engines"])
        assert "background_engines" in result
        engines = result["background_engines"]
        assert isinstance(engines, list)
        # Should list at least some known engine names
        names = [e["name"] for e in engines]
        # There should be at least some background threads found
        assert isinstance(names, list)

    def test_thread_entries_have_alive_status(self):
        result = collect_diagnostics(sections=["background_engines"])
        for engine in result["background_engines"]:
            assert "name" in engine
            assert "alive" in engine
            assert isinstance(engine["alive"], bool)


class TestMemorySection:
    def test_returns_rss(self):
        result = collect_diagnostics(sections=["memory"])
        assert "memory" in result
        assert "rss_bytes" in result["memory"]
        assert result["memory"]["rss_bytes"] > 0

    def test_returns_vms(self):
        result = collect_diagnostics(sections=["memory"])
        assert "vms_bytes" in result["memory"]


class TestAuditSection:
    def test_includes_request_counts(self):
        result = collect_diagnostics(sections=["audit"])
        assert "audit" in result
        assert "total_requests" in result["audit"]
        assert isinstance(result["audit"]["total_requests"], int)

    def test_includes_error_count(self):
        result = collect_diagnostics(sections=["audit"])
        assert "error_count" in result["audit"]

    def test_includes_last_errors(self):
        result = collect_diagnostics(sections=["audit"])
        assert "last_errors" in result["audit"]
        assert isinstance(result["audit"]["last_errors"], list)


class TestSectionFiltering:
    def test_single_section(self):
        result = collect_diagnostics(sections=["config"])
        assert "config" in result
        assert "system" not in result
        assert "server" not in result

    def test_multiple_sections(self):
        result = collect_diagnostics(sections=["config", "services"])
        assert "config" in result
        assert "services" in result
        assert "system" not in result
        assert "memory" not in result

    def test_full_bundle_includes_all_sections(self):
        result = collect_diagnostics()
        expected_sections = [
            "system",
            "server",
            "config",
            "services",
            "state",
            "background_engines",
            "memory",
            "audit",
            "extensions",
        ]
        for section in expected_sections:
            assert section in result, f"Missing section: {section}"


class TestExtensionsSection:
    def test_returns_plugin_list(self):
        result = collect_diagnostics(sections=["extensions"])
        assert "extensions" in result
        assert "plugins" in result["extensions"]
        assert isinstance(result["extensions"]["plugins"], list)


class TestEndpointAccess:
    """Test that the endpoint respects DEBUG/DIAG env vars."""

    def test_returns_403_when_not_enabled(self):
        # Build a minimal Starlette app with just this endpoint
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from robotocore.diagnostics_bundle import diagnose_endpoint

        test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
        client = TestClient(test_app, raise_server_exceptions=False)

        with patch.dict(os.environ, {}, clear=False):
            # Ensure DEBUG and ROBOTOCORE_DIAG are not set
            os.environ.pop("DEBUG", None)
            os.environ.pop("ROBOTOCORE_DIAG", None)
            resp = client.get("/diagnose")
        assert resp.status_code == 403

    def test_returns_200_when_debug_enabled(self):
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from robotocore.diagnostics_bundle import diagnose_endpoint

        test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
        client = TestClient(test_app, raise_server_exceptions=False)

        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200
        data = resp.json()
        assert "system" in data

    def test_returns_200_when_diag_enabled(self):
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from robotocore.diagnostics_bundle import diagnose_endpoint

        test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
        client = TestClient(test_app, raise_server_exceptions=False)

        with patch.dict(os.environ, {"ROBOTOCORE_DIAG": "1"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200

    def test_section_query_param(self):
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from robotocore.diagnostics_bundle import diagnose_endpoint

        test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
        client = TestClient(test_app, raise_server_exceptions=False)

        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=config,services")
        assert resp.status_code == 200
        data = resp.json()
        assert "config" in data
        assert "services" in data
        assert "system" not in data
