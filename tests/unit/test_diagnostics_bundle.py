"""Unit tests for the diagnostic bundle endpoint."""

import os
import sys
import threading
from unittest.mock import MagicMock, patch

from robotocore.diagnostics_bundle import (
    _COLLECTORS,
    ALL_SECTIONS,
    _is_sensitive,
    collect_diagnostics,
)

# ---------------------------------------------------------------------------
# _is_sensitive helper
# ---------------------------------------------------------------------------


class TestIsSensitive:
    def test_secret_key_is_sensitive(self):
        assert _is_sensitive("ROBOTOCORE_SECRET_KEY") is True

    def test_token_is_sensitive(self):
        assert _is_sensitive("ROBOTOCORE_API_TOKEN") is True

    def test_password_is_sensitive(self):
        assert _is_sensitive("DB_PASSWORD") is True

    def test_key_substring_is_sensitive(self):
        assert _is_sensitive("AWS_ACCESS_KEY_ID") is True

    def test_normal_var_is_not_sensitive(self):
        assert _is_sensitive("ROBOTOCORE_STATE_DIR") is False

    def test_debug_is_not_sensitive(self):
        assert _is_sensitive("DEBUG") is False

    def test_case_insensitive_detection(self):
        assert _is_sensitive("my_secret_var") is True
        assert _is_sensitive("My_Token") is True

    def test_empty_string(self):
        assert _is_sensitive("") is False


# ---------------------------------------------------------------------------
# System section
# ---------------------------------------------------------------------------


class TestSystemSection:
    def test_includes_python_version(self):
        result = collect_diagnostics(sections=["system"])
        assert "system" in result
        assert result["system"]["python_version"] == sys.version

    def test_includes_platform(self):
        result = collect_diagnostics(sections=["system"])
        assert "platform" in result["system"]
        assert isinstance(result["system"]["platform"], str)
        assert len(result["system"]["platform"]) > 0

    def test_includes_architecture(self):
        result = collect_diagnostics(sections=["system"])
        assert "architecture" in result["system"]
        assert isinstance(result["system"]["architecture"], str)

    def test_includes_pid(self):
        result = collect_diagnostics(sections=["system"])
        assert result["system"]["pid"] == os.getpid()

    def test_includes_cwd(self):
        result = collect_diagnostics(sections=["system"])
        assert result["system"]["working_directory"] == os.getcwd()

    def test_system_has_exactly_expected_keys(self):
        result = collect_diagnostics(sections=["system"])
        expected = {"python_version", "platform", "architecture", "pid", "working_directory"}
        assert set(result["system"].keys()) == expected


# ---------------------------------------------------------------------------
# Server section
# ---------------------------------------------------------------------------


class TestServerSection:
    def test_includes_uptime(self):
        result = collect_diagnostics(sections=["server"])
        assert "server" in result
        assert "uptime_seconds" in result["server"]
        assert isinstance(result["server"]["uptime_seconds"], (int, float))

    def test_uptime_is_non_negative(self):
        result = collect_diagnostics(sections=["server"])
        assert result["server"]["uptime_seconds"] >= 0

    def test_includes_version(self):
        from robotocore import __version__

        result = collect_diagnostics(sections=["server"])
        assert result["server"]["version"] == __version__

    def test_default_port_is_4566(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GATEWAY_LISTEN", None)
            os.environ.pop("PORT", None)
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["port"] == 4566

    def test_port_from_gateway_listen_env(self):
        with patch.dict(os.environ, {"GATEWAY_LISTEN": "5555"}):
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["port"] == 5555

    def test_port_from_port_env(self):
        with patch.dict(os.environ, {"PORT": "7777"}, clear=False):
            os.environ.pop("GATEWAY_LISTEN", None)
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["port"] == 7777

    def test_gateway_listen_takes_precedence_over_port(self):
        with patch.dict(os.environ, {"GATEWAY_LISTEN": "8888", "PORT": "9999"}):
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["port"] == 8888

    def test_host_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOTOCORE_HOST", None)
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["host"] == "0.0.0.0"

    def test_host_from_env(self):
        with patch.dict(os.environ, {"ROBOTOCORE_HOST": "127.0.0.1"}):
            result = collect_diagnostics(sections=["server"])
        assert result["server"]["host"] == "127.0.0.1"

    def test_includes_start_time(self):
        result = collect_diagnostics(sections=["server"])
        assert "start_time" in result["server"]

    def test_uptime_zero_when_no_start_time(self):
        with patch("robotocore.diagnostics_bundle._collect_server") as mock_server:
            # Simulate _server_start_time being None
            mock_server.return_value = {"uptime_seconds": 0, "version": "test", "port": 4566}
            # Actually test the real function by patching the import
        with patch("robotocore.gateway.app._server_start_time", None):
            result = collect_diagnostics(sections=["server"])
            assert result["server"]["uptime_seconds"] == 0


# ---------------------------------------------------------------------------
# Config section
# ---------------------------------------------------------------------------


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

    def test_excludes_unrelated_env_vars(self):
        with patch.dict(os.environ, {"HOME": "/home/user", "RANDOM_VAR": "xyz"}):
            result = collect_diagnostics(sections=["config"])
        assert "HOME" not in result["config"]
        assert "RANDOM_VAR" not in result["config"]

    def test_includes_dynamodb_prefix(self):
        with patch.dict(os.environ, {"DYNAMODB_SHARE_DB": "1"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["DYNAMODB_SHARE_DB"] == "1"

    def test_includes_dns_prefix(self):
        with patch.dict(os.environ, {"DNS_RESOLVE_DOMAINS": "example.com"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["DNS_RESOLVE_DOMAINS"] == "example.com"

    def test_includes_smtp_prefix(self):
        with patch.dict(os.environ, {"SMTP_HOST": "mail.example.com"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["SMTP_HOST"] == "mail.example.com"

    def test_includes_snapshot_prefix(self):
        with patch.dict(os.environ, {"SNAPSHOT_DIR": "/tmp/snaps"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["SNAPSHOT_DIR"] == "/tmp/snaps"

    def test_config_values_sorted_by_key(self):
        with patch.dict(
            os.environ,
            {"ROBOTOCORE_ZZZ": "z", "ROBOTOCORE_AAA": "a"},
        ):
            result = collect_diagnostics(sections=["config"])
        keys = list(result["config"].keys())
        assert keys == sorted(keys)

    def test_masks_key_with_key_substring(self):
        """An env var with KEY in the name should be masked even if not SECRET."""
        with patch.dict(os.environ, {"ROBOTOCORE_ENCRYPTION_KEY": "mykey123"}):
            result = collect_diagnostics(sections=["config"])
        assert result["config"]["ROBOTOCORE_ENCRYPTION_KEY"] == "***MASKED***"


# ---------------------------------------------------------------------------
# Services section
# ---------------------------------------------------------------------------


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
        svc = result["services"]
        total = svc["native_count"] + svc["moto_count"] + svc["disabled_count"]
        assert svc["total_count"] == total

    def test_native_providers_sorted(self):
        result = collect_diagnostics(sections=["services"])
        providers = result["services"]["native_providers"]
        assert providers == sorted(providers)

    def test_disabled_services_is_list(self):
        result = collect_diagnostics(sections=["services"])
        assert isinstance(result["services"]["disabled_services"], list)

    def test_disabled_count_matches_list(self):
        result = collect_diagnostics(sections=["services"])
        assert result["services"]["disabled_count"] == len(result["services"]["disabled_services"])

    def test_native_count_matches_list(self):
        result = collect_diagnostics(sections=["services"])
        assert result["services"]["native_count"] == len(result["services"]["native_providers"])


# ---------------------------------------------------------------------------
# State section
# ---------------------------------------------------------------------------


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

    def test_persistence_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PERSISTENCE", None)
            result = collect_diagnostics(sections=["state"])
        assert result["state"]["persistence_enabled"] is False

    def test_persistence_enabled_when_set(self):
        with patch.dict(os.environ, {"PERSISTENCE": "1"}):
            result = collect_diagnostics(sections=["state"])
        assert result["state"]["persistence_enabled"] is True

    def test_state_file_sizes_is_dict(self):
        result = collect_diagnostics(sections=["state"])
        assert isinstance(result["state"]["state_file_sizes"], dict)


# ---------------------------------------------------------------------------
# Background engines section
# ---------------------------------------------------------------------------


class TestBackgroundEnginesSection:
    def test_lists_threads(self):
        result = collect_diagnostics(sections=["background_engines"])
        assert "background_engines" in result
        engines = result["background_engines"]
        assert isinstance(engines, list)

    def test_thread_entries_have_alive_status(self):
        result = collect_diagnostics(sections=["background_engines"])
        for engine in result["background_engines"]:
            assert "name" in engine
            assert "alive" in engine
            assert isinstance(engine["alive"], bool)

    def test_thread_entries_have_daemon_field(self):
        result = collect_diagnostics(sections=["background_engines"])
        for engine in result["background_engines"]:
            assert "daemon" in engine
            assert isinstance(engine["daemon"], bool)

    def test_detects_known_engine_thread(self):
        """A thread with a known engine name pattern should appear in results."""
        stop_event = threading.Event()

        def target():
            stop_event.wait()

        t = threading.Thread(target=target, name="lambda-worker-test", daemon=True)
        t.start()
        try:
            result = collect_diagnostics(sections=["background_engines"])
            names = [e["name"] for e in result["background_engines"]]
            assert "lambda-worker-test" in names
        finally:
            stop_event.set()
            t.join(timeout=1)

    def test_ignores_main_thread(self):
        """The MainThread should not appear (it's not daemon and doesn't match patterns)."""
        result = collect_diagnostics(sections=["background_engines"])
        names = [e["name"] for e in result["background_engines"]]
        assert "MainThread" not in names

    def test_includes_daemon_threads_without_known_name(self):
        """Daemon threads should be included even without a known pattern name."""
        stop_event = threading.Event()

        def target():
            stop_event.wait()

        t = threading.Thread(target=target, name="custom-daemon-xyz", daemon=True)
        t.start()
        try:
            result = collect_diagnostics(sections=["background_engines"])
            names = [e["name"] for e in result["background_engines"]]
            assert "custom-daemon-xyz" in names
        finally:
            stop_event.set()
            t.join(timeout=1)


# ---------------------------------------------------------------------------
# Memory section
# ---------------------------------------------------------------------------


class TestMemorySection:
    def test_returns_rss(self):
        result = collect_diagnostics(sections=["memory"])
        assert "memory" in result
        assert "rss_bytes" in result["memory"]
        assert result["memory"]["rss_bytes"] > 0

    def test_returns_vms(self):
        result = collect_diagnostics(sections=["memory"])
        assert "vms_bytes" in result["memory"]
        assert isinstance(result["memory"]["vms_bytes"], int)

    def test_returns_max_rss(self):
        result = collect_diagnostics(sections=["memory"])
        assert "max_rss_bytes" in result["memory"]
        assert result["memory"]["max_rss_bytes"] > 0

    def test_all_memory_values_are_integers(self):
        result = collect_diagnostics(sections=["memory"])
        mem = result["memory"]
        assert isinstance(mem["rss_bytes"], int)
        assert isinstance(mem["vms_bytes"], int)
        assert isinstance(mem["max_rss_bytes"], int)

    def test_memory_has_exactly_expected_keys(self):
        result = collect_diagnostics(sections=["memory"])
        expected = {"rss_bytes", "vms_bytes", "max_rss_bytes"}
        assert set(result["memory"].keys()) == expected


# ---------------------------------------------------------------------------
# Audit section
# ---------------------------------------------------------------------------


class TestAuditSection:
    def test_includes_request_counts(self):
        result = collect_diagnostics(sections=["audit"])
        assert "audit" in result
        assert "total_requests" in result["audit"]
        assert isinstance(result["audit"]["total_requests"], int)

    def test_includes_error_count(self):
        result = collect_diagnostics(sections=["audit"])
        assert "error_count" in result["audit"]
        assert isinstance(result["audit"]["error_count"], int)

    def test_includes_last_errors(self):
        result = collect_diagnostics(sections=["audit"])
        assert "last_errors" in result["audit"]
        assert isinstance(result["audit"]["last_errors"], list)

    def test_error_count_lte_total_requests(self):
        result = collect_diagnostics(sections=["audit"])
        assert result["audit"]["error_count"] <= result["audit"]["total_requests"]

    def test_last_errors_max_five(self):
        result = collect_diagnostics(sections=["audit"])
        assert len(result["audit"]["last_errors"]) <= 5

    def test_audit_with_mocked_errors(self):
        """Verify error filtering works with injected audit entries."""
        fake_entries = [
            {"service": "s3", "operation": "GetObject", "status_code": 200},
            {"service": "sqs", "operation": "SendMessage", "status_code": 500},
            {"service": "s3", "operation": "PutObject", "status_code": 403},
            {"service": "iam", "operation": "GetUser", "status_code": 200},
        ]
        mock_log = MagicMock()
        mock_log.recent.return_value = fake_entries
        with patch("robotocore.audit.log.get_audit_log", return_value=mock_log):
            from robotocore.diagnostics_bundle import _collect_audit

            result = _collect_audit()
        assert result["total_requests"] == 4
        assert result["error_count"] == 2
        assert len(result["last_errors"]) == 2
        assert result["last_errors"][0]["status_code"] == 500


# ---------------------------------------------------------------------------
# Extensions section
# ---------------------------------------------------------------------------


class TestExtensionsSection:
    def test_returns_plugin_list(self):
        result = collect_diagnostics(sections=["extensions"])
        assert "extensions" in result
        assert "plugins" in result["extensions"]
        assert isinstance(result["extensions"]["plugins"], list)


# ---------------------------------------------------------------------------
# Section filtering / collect_diagnostics
# ---------------------------------------------------------------------------


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
        for section in ALL_SECTIONS:
            assert section in result, f"Missing section: {section}"

    def test_all_sections_constant_matches_collectors(self):
        """ALL_SECTIONS must be in sync with _COLLECTORS keys."""
        assert set(ALL_SECTIONS) == set(_COLLECTORS.keys())

    def test_unknown_section_is_silently_ignored(self):
        result = collect_diagnostics(sections=["nonexistent_section"])
        assert result == {}

    def test_mix_of_known_and_unknown_sections(self):
        result = collect_diagnostics(sections=["system", "bogus_section"])
        assert "system" in result
        assert "bogus_section" not in result

    def test_empty_sections_list_returns_empty(self):
        result = collect_diagnostics(sections=[])
        assert result == {}

    def test_duplicate_sections_work(self):
        result = collect_diagnostics(sections=["system", "system"])
        # Should still have system, the second call just overwrites the first
        assert "system" in result

    def test_none_sections_returns_all(self):
        result = collect_diagnostics(sections=None)
        assert len(result) == len(ALL_SECTIONS)


# ---------------------------------------------------------------------------
# Endpoint HTTP behavior
# ---------------------------------------------------------------------------


class TestEndpointAccess:
    """Test that the endpoint respects DEBUG/DIAG env vars."""

    def _make_client(self):
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from robotocore.diagnostics_bundle import diagnose_endpoint

        test_app = Starlette(routes=[Route("/diagnose", diagnose_endpoint, methods=["GET"])])
        return TestClient(test_app, raise_server_exceptions=False)

    def test_returns_403_when_not_enabled(self):
        client = self._make_client()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DEBUG", None)
            os.environ.pop("ROBOTOCORE_DIAG", None)
            resp = client.get("/diagnose")
        assert resp.status_code == 403

    def test_403_body_contains_error_message(self):
        client = self._make_client()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DEBUG", None)
            os.environ.pop("ROBOTOCORE_DIAG", None)
            resp = client.get("/diagnose")
        data = resp.json()
        assert "error" in data
        assert "DEBUG=1" in data["error"]
        assert "ROBOTOCORE_DIAG=1" in data["error"]

    def test_returns_200_when_debug_enabled(self):
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200
        data = resp.json()
        assert "system" in data

    def test_returns_200_when_diag_enabled(self):
        client = self._make_client()
        with patch.dict(os.environ, {"ROBOTOCORE_DIAG": "1"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200

    def test_diag_any_truthy_value_enables(self):
        """ROBOTOCORE_DIAG with any non-empty value should enable the endpoint."""
        client = self._make_client()
        with patch.dict(os.environ, {"ROBOTOCORE_DIAG": "yes"}):
            resp = client.get("/diagnose")
        assert resp.status_code == 200

    def test_debug_zero_does_not_enable(self):
        """DEBUG=0 should not enable the endpoint."""
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "0"}, clear=False):
            os.environ.pop("ROBOTOCORE_DIAG", None)
            resp = client.get("/diagnose")
        assert resp.status_code == 403

    def test_section_query_param(self):
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=config,services")
        assert resp.status_code == 200
        data = resp.json()
        assert "config" in data
        assert "services" in data
        assert "system" not in data

    def test_single_section_query_param(self):
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=memory")
        assert resp.status_code == 200
        data = resp.json()
        assert "memory" in data
        assert len(data) == 1

    def test_empty_section_query_returns_all(self):
        """No section param means all sections."""
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose")
        data = resp.json()
        assert len(data) == len(ALL_SECTIONS)

    def test_response_is_json_content_type(self):
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose")
        assert "application/json" in resp.headers["content-type"]

    def test_section_with_whitespace_trimmed(self):
        """Sections like ' config , services ' should be trimmed."""
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section= config , services ")
        data = resp.json()
        assert "config" in data
        assert "services" in data

    def test_invalid_section_returns_empty_bundle(self):
        client = self._make_client()
        with patch.dict(os.environ, {"DEBUG": "1"}):
            resp = client.get("/diagnose?section=totally_fake")
        assert resp.status_code == 200
        data = resp.json()
        assert data == {}
