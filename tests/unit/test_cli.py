"""Unit tests for robotocore CLI — argument parsing, URL construction, command dispatch."""

from __future__ import annotations

import subprocess
from unittest import mock

from robotocore.cli import (
    DEFAULT_CONTAINER_NAME,
    DEFAULT_IMAGE,
    DEFAULT_PORT,
    DEFAULT_WAIT_TIMEOUT,
    _get_base_url,
    _get_container_name,
    _get_image,
    _get_port,
    _print_table,
    _run,
    build_parser,
    build_url,
)

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_start_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["start"])
        assert args.command == "start"
        assert args.image is None
        assert args.env is None

    def test_start_with_options(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--name",
                "custom",
                "--port",
                "5555",
                "start",
                "--image",
                "my/image:v1",
                "-e",
                "FOO=bar",
                "-e",
                "BAZ=qux",
            ]
        )
        assert args.name == "custom"
        assert args.port == 5555
        assert args.command == "start"
        assert args.image == "my/image:v1"
        assert args.env == ["FOO=bar", "BAZ=qux"]

    def test_start_wait_flag(self):
        parser = build_parser()
        args = parser.parse_args(["start", "--wait"])
        assert args.wait is True

    def test_start_wait_with_timeout(self):
        parser = build_parser()
        args = parser.parse_args(["start", "--wait", "--timeout", "60"])
        assert args.wait is True
        assert args.timeout == 60

    def test_stop(self):
        parser = build_parser()
        args = parser.parse_args(["stop"])
        assert args.command == "stop"

    def test_status(self):
        parser = build_parser()
        args = parser.parse_args(["status"])
        assert args.command == "status"

    def test_logs(self):
        parser = build_parser()
        args = parser.parse_args(["logs"])
        assert args.command == "logs"

    def test_logs_tail(self):
        parser = build_parser()
        args = parser.parse_args(["logs", "--tail", "50"])
        assert args.tail == 50

    def test_logs_no_follow(self):
        parser = build_parser()
        args = parser.parse_args(["logs", "--no-follow"])
        assert args.follow is False

    def test_restart(self):
        parser = build_parser()
        args = parser.parse_args(["restart"])
        assert args.command == "restart"

    def test_health(self):
        parser = build_parser()
        args = parser.parse_args(["health"])
        assert args.command == "health"

    def test_wait_default_timeout(self):
        parser = build_parser()
        args = parser.parse_args(["wait"])
        assert args.command == "wait"
        assert args.timeout == DEFAULT_WAIT_TIMEOUT

    def test_wait_custom_timeout(self):
        parser = build_parser()
        args = parser.parse_args(["wait", "--timeout", "60"])
        assert args.timeout == 60

    def test_version(self):
        parser = build_parser()
        args = parser.parse_args(["version"])
        assert args.command == "version"

    def test_state_save(self):
        parser = build_parser()
        args = parser.parse_args(["state", "save", "my-snap"])
        assert args.command == "state"
        assert args.state_command == "save"
        assert args.snapshot_name == "my-snap"

    def test_state_load(self):
        parser = build_parser()
        args = parser.parse_args(["state", "load", "my-snap"])
        assert args.command == "state"
        assert args.state_command == "load"
        assert args.snapshot_name == "my-snap"

    def test_state_list(self):
        parser = build_parser()
        args = parser.parse_args(["state", "list"])
        assert args.command == "state"
        assert args.state_command == "list"

    def test_state_reset(self):
        parser = build_parser()
        args = parser.parse_args(["state", "reset"])
        assert args.command == "state"
        assert args.state_command == "reset"

    def test_global_name_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--name", "my-container", "stop"])
        assert args.name == "my-container"
        assert args.command == "stop"

    def test_global_port_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--port", "9999", "health"])
        assert args.port == 9999

    # --- New command parsing ---

    def test_services(self):
        parser = build_parser()
        args = parser.parse_args(["services"])
        assert args.command == "services"
        assert args.format == "table"
        assert args.status == "all"

    def test_services_json_format(self):
        parser = build_parser()
        args = parser.parse_args(["services", "--format", "json"])
        assert args.format == "json"

    def test_services_native_filter(self):
        parser = build_parser()
        args = parser.parse_args(["services", "--status", "native"])
        assert args.status == "native"

    def test_config_get(self):
        parser = build_parser()
        args = parser.parse_args(["config", "get"])
        assert args.command == "config"
        assert args.config_command == "get"

    def test_config_set(self):
        parser = build_parser()
        args = parser.parse_args(["config", "set", "LOG_LEVEL", "DEBUG"])
        assert args.config_command == "set"
        assert args.key == "LOG_LEVEL"
        assert args.value == "DEBUG"

    def test_config_reset(self):
        parser = build_parser()
        args = parser.parse_args(["config", "reset", "LOG_LEVEL"])
        assert args.config_command == "reset"
        assert args.key == "LOG_LEVEL"

    def test_chaos_list(self):
        parser = build_parser()
        args = parser.parse_args(["chaos", "list"])
        assert args.command == "chaos"
        assert args.chaos_command == "list"

    def test_chaos_add(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "chaos",
                "add",
                "--service",
                "s3",
                "--error",
                "ThrottlingException",
                "--status-code",
                "429",
                "--operation",
                "PutObject",
                "--rate",
                "0.5",
                "--latency",
                "100",
            ]
        )
        assert args.chaos_command == "add"
        assert args.service == "s3"
        assert args.error == "ThrottlingException"
        assert args.status_code == 429
        assert args.operation == "PutObject"
        assert args.rate == 0.5
        assert args.latency == 100

    def test_chaos_remove(self):
        parser = build_parser()
        args = parser.parse_args(["chaos", "remove", "rule-123"])
        assert args.chaos_command == "remove"
        assert args.rule_id == "rule-123"

    def test_chaos_clear(self):
        parser = build_parser()
        args = parser.parse_args(["chaos", "clear"])
        assert args.chaos_command == "clear"

    def test_resources_no_service(self):
        parser = build_parser()
        args = parser.parse_args(["resources"])
        assert args.command == "resources"
        assert args.service is None

    def test_resources_with_service(self):
        parser = build_parser()
        args = parser.parse_args(["resources", "s3"])
        assert args.service == "s3"

    def test_audit(self):
        parser = build_parser()
        args = parser.parse_args(["audit"])
        assert args.command == "audit"
        assert args.limit == 20

    def test_audit_limit(self):
        parser = build_parser()
        args = parser.parse_args(["audit", "--limit", "50"])
        assert args.limit == 50

    def test_usage(self):
        parser = build_parser()
        args = parser.parse_args(["usage"])
        assert args.command == "usage"
        assert args.usage_command is None

    def test_usage_services(self):
        parser = build_parser()
        args = parser.parse_args(["usage", "services"])
        assert args.usage_command == "services"

    def test_usage_errors(self):
        parser = build_parser()
        args = parser.parse_args(["usage", "errors"])
        assert args.usage_command == "errors"

    def test_pods_list(self):
        parser = build_parser()
        args = parser.parse_args(["pods", "list"])
        assert args.command == "pods"
        assert args.pods_command == "list"

    def test_pods_save(self):
        parser = build_parser()
        args = parser.parse_args(["pods", "save", "my-pod"])
        assert args.pods_command == "save"
        assert args.pod_name == "my-pod"

    def test_pods_load(self):
        parser = build_parser()
        args = parser.parse_args(["pods", "load", "my-pod"])
        assert args.pods_command == "load"
        assert args.pod_name == "my-pod"

    def test_pods_info(self):
        parser = build_parser()
        args = parser.parse_args(["pods", "info", "my-pod"])
        assert args.pods_command == "info"
        assert args.pod_name == "my-pod"

    def test_pods_delete(self):
        parser = build_parser()
        args = parser.parse_args(["pods", "delete", "my-pod"])
        assert args.pods_command == "delete"
        assert args.pod_name == "my-pod"

    def test_ses_messages(self):
        parser = build_parser()
        args = parser.parse_args(["ses", "messages"])
        assert args.command == "ses"
        assert args.ses_command == "messages"

    def test_ses_messages_limit(self):
        parser = build_parser()
        args = parser.parse_args(["ses", "messages", "--limit", "10"])
        assert args.limit == 10

    def test_ses_clear(self):
        parser = build_parser()
        args = parser.parse_args(["ses", "clear"])
        assert args.ses_command == "clear"

    def test_iam_stream(self):
        parser = build_parser()
        args = parser.parse_args(["iam", "stream"])
        assert args.command == "iam"
        assert args.iam_command == "stream"

    def test_iam_stream_filters(self):
        parser = build_parser()
        args = parser.parse_args(["iam", "stream", "--limit", "50", "--decision", "DENY"])
        assert args.limit == 50
        assert args.decision == "DENY"

    def test_iam_suggest(self):
        parser = build_parser()
        args = parser.parse_args(["iam", "suggest", "arn:aws:iam::123:user/bob"])
        assert args.iam_command == "suggest"
        assert args.principal == "arn:aws:iam::123:user/bob"

    def test_diagnose(self):
        parser = build_parser()
        args = parser.parse_args(["diagnose"])
        assert args.command == "diagnose"


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


class TestConfigHelpers:
    def test_get_container_name_default(self):
        parser = build_parser()
        args = parser.parse_args(["stop"])
        assert _get_container_name(args) == DEFAULT_CONTAINER_NAME

    def test_get_container_name_custom(self):
        parser = build_parser()
        args = parser.parse_args(["--name", "custom", "stop"])
        assert _get_container_name(args) == "custom"

    def test_get_image_default(self):
        parser = build_parser()
        args = parser.parse_args(["start"])
        with mock.patch.dict("os.environ", {}, clear=True):
            assert _get_image(args) == DEFAULT_IMAGE

    def test_get_image_from_flag(self):
        parser = build_parser()
        args = parser.parse_args(["start", "--image", "custom:v2"])
        assert _get_image(args) == "custom:v2"

    def test_get_image_from_env(self):
        parser = build_parser()
        args = parser.parse_args(["start"])
        with mock.patch.dict("os.environ", {"ROBOTOCORE_IMAGE": "env-image:v3"}):
            assert _get_image(args) == "env-image:v3"

    def test_get_image_flag_overrides_env(self):
        parser = build_parser()
        args = parser.parse_args(["start", "--image", "flag:v1"])
        with mock.patch.dict("os.environ", {"ROBOTOCORE_IMAGE": "env:v2"}):
            assert _get_image(args) == "flag:v1"

    def test_get_port_default(self):
        parser = build_parser()
        args = parser.parse_args(["health"])
        with mock.patch.dict("os.environ", {}, clear=True):
            assert _get_port(args) == DEFAULT_PORT

    def test_get_port_from_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--port", "7777", "health"])
        assert _get_port(args) == 7777

    def test_get_port_from_env(self):
        parser = build_parser()
        args = parser.parse_args(["health"])
        with mock.patch.dict("os.environ", {"ROBOTOCORE_PORT": "8888"}):
            assert _get_port(args) == 8888

    def test_get_port_flag_overrides_env(self):
        parser = build_parser()
        args = parser.parse_args(["--port", "7777", "health"])
        with mock.patch.dict("os.environ", {"ROBOTOCORE_PORT": "8888"}):
            assert _get_port(args) == 7777

    def test_get_base_url(self):
        parser = build_parser()
        args = parser.parse_args(["--port", "5000", "health"])
        assert _get_base_url(args) == "http://localhost:5000"

    def test_get_base_url_default(self):
        parser = build_parser()
        args = parser.parse_args(["health"])
        with mock.patch.dict("os.environ", {}, clear=True):
            assert _get_base_url(args) == f"http://localhost:{DEFAULT_PORT}"


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


class TestBuildUrl:
    def test_health_url(self):
        assert build_url("http://localhost:4566", "/_robotocore/health") == (
            "http://localhost:4566/_robotocore/health"
        )

    def test_state_save_url(self):
        assert build_url("http://localhost:9999", "/_robotocore/state/save") == (
            "http://localhost:9999/_robotocore/state/save"
        )

    def test_state_snapshots_url(self):
        assert build_url("http://localhost:4566", "/_robotocore/state/snapshots") == (
            "http://localhost:4566/_robotocore/state/snapshots"
        )


# ---------------------------------------------------------------------------
# Table formatter
# ---------------------------------------------------------------------------


class TestPrintTable:
    def test_basic_table(self, capsys):
        _print_table(["NAME", "VALUE"], [["foo", "bar"], ["baz", "qux"]])
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) == 4  # header + separator + 2 rows
        assert "NAME" in lines[0]
        assert "VALUE" in lines[0]
        assert "----" in lines[1]
        assert "foo" in lines[2]
        assert "baz" in lines[3]

    def test_empty_rows(self, capsys):
        _print_table(["A", "B"], [])
        out = capsys.readouterr().out
        assert "(no data)" in out

    def test_column_alignment(self, capsys):
        _print_table(["X", "LONG_HEADER"], [["a", "b"], ["cc", "dd"]])
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        # All lines should be the same length (padded)
        widths = [len(line.rstrip()) for line in lines]
        # Header and separator should match
        assert widths[0] == widths[1]


# ---------------------------------------------------------------------------
# Command dispatch (mock docker/urllib)
# ---------------------------------------------------------------------------


class TestCmdStart:
    @mock.patch("robotocore.cli._run_docker")
    def test_start_runs_docker(self, mock_docker):
        # First call: ps check (no existing container)
        # Second call: rm (cleanup)
        # Third call: docker run
        mock_docker.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="abc123def456\n", stderr=""),
        ]
        rc = _run(["start"])
        assert rc == 0
        # Verify docker run was called with correct args
        run_call = mock_docker.call_args_list[2]
        docker_args = run_call[0][0]
        assert "run" in docker_args
        assert "-d" in docker_args
        assert "--name" in docker_args
        assert DEFAULT_CONTAINER_NAME in docker_args
        assert f"{DEFAULT_PORT}:4566" in docker_args

    @mock.patch("robotocore.cli._run_docker")
    def test_start_already_running(self, mock_docker):
        mock_docker.return_value = subprocess.CompletedProcess([], 0, stdout="abc123\n", stderr="")
        rc = _run(["start"])
        assert rc == 1

    @mock.patch("robotocore.cli._run_docker")
    def test_start_passes_env_vars(self, mock_docker):
        mock_docker.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="abc123\n", stderr=""),
        ]
        with mock.patch.dict("os.environ", {"ENFORCE_IAM": "1"}):
            rc = _run(["start", "-e", "MY_VAR=hello"])
        assert rc == 0
        run_call = mock_docker.call_args_list[2]
        docker_args = run_call[0][0]
        assert "ENFORCE_IAM=1" in docker_args
        assert "MY_VAR=hello" in docker_args

    @mock.patch("robotocore.cli._run_docker")
    def test_start_custom_image_and_port(self, mock_docker):
        mock_docker.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="abc123\n", stderr=""),
        ]
        rc = _run(["--port", "5555", "start", "--image", "myimage:v1"])
        assert rc == 0
        run_call = mock_docker.call_args_list[2]
        docker_args = run_call[0][0]
        assert "5555:4566" in docker_args
        assert "myimage:v1" in docker_args

    @mock.patch("robotocore.cli.cmd_wait")
    @mock.patch("robotocore.cli._run_docker")
    def test_start_with_wait(self, mock_docker, mock_wait):
        mock_docker.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="abc123\n", stderr=""),
        ]
        mock_wait.return_value = 0
        rc = _run(["start", "--wait"])
        assert rc == 0
        mock_wait.assert_called_once()


class TestCmdStop:
    @mock.patch("robotocore.cli._run_docker")
    def test_stop_success(self, mock_docker):
        mock_docker.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        rc = _run(["stop"])
        assert rc == 0
        assert mock_docker.call_args_list[0][0][0] == ["stop", DEFAULT_CONTAINER_NAME]

    @mock.patch("robotocore.cli._run_docker")
    def test_stop_failure(self, mock_docker):
        mock_docker.return_value = subprocess.CompletedProcess(
            [], 1, stdout="", stderr="No such container"
        )
        rc = _run(["stop"])
        assert rc == 1


class TestCmdStatus:
    @mock.patch("robotocore.cli._run_docker")
    def test_status_running(self, mock_docker):
        mock_docker.return_value = subprocess.CompletedProcess(
            [], 0, stdout="abc123\tUp 5 minutes\t0.0.0.0:4566->4566/tcp", stderr=""
        )
        rc = _run(["status"])
        assert rc == 0

    @mock.patch("robotocore.cli._run_docker")
    def test_status_not_running(self, mock_docker):
        mock_docker.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        rc = _run(["status"])
        assert rc == 1


class TestCmdHealth:
    @mock.patch("robotocore.cli._api_request")
    def test_health_success(self, mock_api):
        mock_api.return_value = {"status": "ok", "services": 147}
        rc = _run(["health"])
        assert rc == 0
        mock_api.assert_called_once_with(f"http://localhost:{DEFAULT_PORT}/_robotocore/health")

    @mock.patch("robotocore.cli._api_request")
    def test_health_custom_port(self, mock_api):
        mock_api.return_value = {"status": "ok"}
        rc = _run(["--port", "9999", "health"])
        assert rc == 0
        mock_api.assert_called_once_with("http://localhost:9999/_robotocore/health")

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_health_failure(self, mock_api):
        rc = _run(["health"])
        assert rc == 1


class TestCmdWait:
    @mock.patch("robotocore.cli.time")
    @mock.patch("robotocore.cli._api_request")
    def test_wait_immediate_success(self, mock_api, mock_time):
        mock_api.return_value = {"status": "ok"}
        mock_time.monotonic.side_effect = [0, 0.1]
        mock_time.sleep = mock.MagicMock()
        rc = _run(["wait", "--timeout", "5"])
        assert rc == 0

    @mock.patch("robotocore.cli.time")
    @mock.patch("robotocore.cli._api_request")
    def test_wait_timeout(self, mock_api, mock_time):
        mock_api.side_effect = ConnectionRefusedError("refused")
        # monotonic: start, check (past deadline)
        mock_time.monotonic.side_effect = [0, 100]
        mock_time.sleep = mock.MagicMock()
        rc = _run(["wait", "--timeout", "5"])
        assert rc == 1


class TestCmdVersion:
    @mock.patch("robotocore.cli._api_request")
    def test_version_success(self, mock_api, capsys):
        mock_api.return_value = {"status": "ok", "version": "2026.3.12"}
        rc = _run(["version"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "2026.3.12" in out

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_version_failure(self, mock_api):
        rc = _run(["version"])
        assert rc == 1


class TestCmdStateSave:
    @mock.patch("robotocore.cli._api_request")
    def test_save_success(self, mock_api):
        mock_api.return_value = {"status": "saved", "name": "my-snap"}
        rc = _run(["state", "save", "my-snap"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/state/save",
            method="POST",
            data={"name": "my-snap"},
        )

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_save_failure(self, mock_api):
        rc = _run(["state", "save", "my-snap"])
        assert rc == 1


class TestCmdStateLoad:
    @mock.patch("robotocore.cli._api_request")
    def test_load_success(self, mock_api):
        mock_api.return_value = {"status": "loaded", "name": "my-snap"}
        rc = _run(["state", "load", "my-snap"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/state/load",
            method="POST",
            data={"name": "my-snap"},
        )


class TestCmdStateList:
    @mock.patch("robotocore.cli._api_request")
    def test_list_success(self, mock_api):
        mock_api.return_value = {"snapshots": ["snap1", "snap2"]}
        rc = _run(["state", "list"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/state/snapshots",
            method="GET",
            data=None,
        )


class TestCmdStateReset:
    @mock.patch("robotocore.cli._api_request")
    def test_reset_success(self, mock_api):
        mock_api.return_value = {"status": "reset"}
        rc = _run(["state", "reset"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/state/reset",
            method="POST",
            data=None,
        )


# ---------------------------------------------------------------------------
# New command dispatch tests
# ---------------------------------------------------------------------------


class TestCmdServices:
    @mock.patch("robotocore.cli._api_request")
    def test_services_json(self, mock_api, capsys):
        mock_api.return_value = {
            "services": [
                {"name": "s3", "status": "NATIVE", "protocol": "rest-xml", "enabled": True},
                {"name": "sqs", "status": "NATIVE", "protocol": "query", "enabled": True},
            ]
        }
        rc = _run(["services", "--format", "json"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request")
    def test_services_table(self, mock_api, capsys):
        mock_api.return_value = {
            "services": [
                {"name": "s3", "status": "NATIVE", "protocol": "rest-xml", "enabled": True},
            ]
        }
        rc = _run(["services"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "SERVICE" in out
        assert "s3" in out

    @mock.patch("robotocore.cli._api_request")
    def test_services_native_filter(self, mock_api, capsys):
        mock_api.return_value = {
            "services": [
                {"name": "s3", "status": "NATIVE", "protocol": "rest-xml", "enabled": True},
                {"name": "acm", "status": "MOTO_BACKED", "protocol": "json", "enabled": True},
            ]
        }
        rc = _run(["services", "--status", "native"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "s3" in out
        assert "acm" not in out

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_services_connection_error(self, mock_api):
        rc = _run(["services"])
        assert rc == 1


class TestCmdConfig:
    @mock.patch("robotocore.cli._api_request")
    def test_config_get(self, mock_api):
        mock_api.return_value = {"LOG_LEVEL": "INFO", "ENFORCE_IAM": False}
        rc = _run(["config", "get"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request")
    def test_config_set(self, mock_api):
        mock_api.return_value = {"status": "updated"}
        rc = _run(["config", "set", "LOG_LEVEL", "DEBUG"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/config",
            method="POST",
            data={"LOG_LEVEL": "DEBUG"},
        )

    @mock.patch("robotocore.cli._api_request")
    def test_config_set_json_value(self, mock_api):
        mock_api.return_value = {"status": "updated"}
        rc = _run(["config", "set", "ENFORCE_IAM", "true"])
        assert rc == 0
        # "true" should be parsed as boolean
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/config",
            method="POST",
            data={"ENFORCE_IAM": True},
        )

    @mock.patch("robotocore.cli._api_request")
    def test_config_reset(self, mock_api):
        mock_api.return_value = {"status": "reset"}
        rc = _run(["config", "reset", "LOG_LEVEL"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/config/LOG_LEVEL",
            method="DELETE",
            data=None,
        )

    def test_config_no_subcommand(self):
        rc = _run(["config"])
        assert rc == 1


class TestCmdChaos:
    @mock.patch("robotocore.cli._api_request")
    def test_chaos_list_table(self, mock_api, capsys):
        mock_api.return_value = {
            "rules": [
                {
                    "rule_id": "r1",
                    "service": "s3",
                    "error_code": "ThrottlingException",
                    "status_code": 429,
                    "operation": "*",
                    "probability": 1.0,
                    "latency_ms": 0,
                }
            ]
        }
        rc = _run(["chaos", "list"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "r1" in out
        assert "ThrottlingException" in out

    @mock.patch("robotocore.cli._api_request")
    def test_chaos_list_empty(self, mock_api, capsys):
        mock_api.return_value = {"rules": []}
        rc = _run(["chaos", "list"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "No chaos rules" in out

    @mock.patch("robotocore.cli._api_request")
    def test_chaos_add(self, mock_api):
        mock_api.return_value = {"rule_id": "r1"}
        rc = _run(
            [
                "chaos",
                "add",
                "--service",
                "s3",
                "--error",
                "ThrottlingException",
                "--status-code",
                "429",
            ]
        )
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/chaos/rules",
            method="POST",
            data={"service": "s3", "error_code": "ThrottlingException", "status_code": 429},
        )

    @mock.patch("robotocore.cli._api_request")
    def test_chaos_add_with_options(self, mock_api):
        mock_api.return_value = {"rule_id": "r2"}
        rc = _run(
            [
                "chaos",
                "add",
                "--service",
                "sqs",
                "--error",
                "InternalError",
                "--status-code",
                "500",
                "--operation",
                "SendMessage",
                "--rate",
                "0.3",
                "--latency",
                "200",
            ]
        )
        assert rc == 0
        call_data = mock_api.call_args[1]["data"]
        assert call_data["operation"] == "SendMessage"
        assert call_data["probability"] == 0.3
        assert call_data["latency_ms"] == 200

    @mock.patch("robotocore.cli._api_request")
    def test_chaos_remove(self, mock_api):
        mock_api.return_value = {"status": "deleted", "rule_id": "r1"}
        rc = _run(["chaos", "remove", "r1"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/chaos/rules/r1",
            method="DELETE",
            data=None,
        )

    @mock.patch("robotocore.cli._api_request")
    def test_chaos_clear(self, mock_api):
        mock_api.return_value = {"status": "cleared"}
        rc = _run(["chaos", "clear"])
        assert rc == 0

    def test_chaos_no_subcommand(self):
        rc = _run(["chaos"])
        assert rc == 1


class TestCmdResources:
    @mock.patch("robotocore.cli._api_request")
    def test_resources_overview_table(self, mock_api, capsys):
        mock_api.return_value = {"s3": 5, "sqs": 3}
        rc = _run(["resources"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "s3" in out
        assert "5" in out

    @mock.patch("robotocore.cli._api_request")
    def test_resources_service_detail(self, mock_api):
        mock_api.return_value = {"buckets": ["b1", "b2"]}
        rc = _run(["resources", "s3"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_resources_failure(self, mock_api):
        rc = _run(["resources"])
        assert rc == 1


class TestCmdAudit:
    @mock.patch("robotocore.cli._api_request")
    def test_audit_table(self, mock_api, capsys):
        mock_api.return_value = {
            "entries": [
                {
                    "timestamp": "12:00",
                    "service": "s3",
                    "operation": "ListBuckets",
                    "status_code": 200,
                    "duration_ms": 5,
                }
            ]
        }
        rc = _run(["audit"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "ListBuckets" in out
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/audit?limit=20",
            method="GET",
            data=None,
        )

    @mock.patch("robotocore.cli._api_request")
    def test_audit_custom_limit(self, mock_api):
        mock_api.return_value = {"entries": []}
        rc = _run(["audit", "--limit", "50"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/audit?limit=50",
            method="GET",
            data=None,
        )


class TestCmdUsage:
    @mock.patch("robotocore.cli._api_request")
    def test_usage_summary(self, mock_api):
        mock_api.return_value = {"total_calls": 100}
        rc = _run(["usage"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request")
    def test_usage_services(self, mock_api, capsys):
        mock_api.return_value = {
            "services": [{"service": "s3", "request_count": 50, "error_count": 2}]
        }
        rc = _run(["usage", "services"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "s3" in out

    @mock.patch("robotocore.cli._api_request")
    def test_usage_errors(self, mock_api, capsys):
        mock_api.return_value = {
            "errors": [{"service": "s3", "operation": "PutObject", "error": "500", "count": 3}]
        }
        rc = _run(["usage", "errors"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "PutObject" in out


class TestCmdPods:
    @mock.patch("robotocore.cli._api_request")
    def test_pods_list(self, mock_api, capsys):
        mock_api.return_value = {
            "pods": [
                {
                    "name": "my-pod",
                    "created_at": "2026-03-12",
                    "size_bytes": 1024,
                    "version_count": 5,
                }
            ]
        }
        rc = _run(["pods", "list"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "my-pod" in out

    @mock.patch("robotocore.cli._api_request")
    def test_pods_list_empty(self, mock_api, capsys):
        mock_api.return_value = {"pods": []}
        rc = _run(["pods", "list"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "No pods" in out

    @mock.patch("robotocore.cli._api_request")
    def test_pods_save(self, mock_api):
        mock_api.return_value = {"status": "saved"}
        rc = _run(["pods", "save", "test-pod"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/pods/save",
            method="POST",
            data={"name": "test-pod"},
        )

    @mock.patch("robotocore.cli._api_request")
    def test_pods_load(self, mock_api):
        mock_api.return_value = {"status": "loaded"}
        rc = _run(["pods", "load", "test-pod"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request")
    def test_pods_info(self, mock_api):
        mock_api.return_value = {"name": "test-pod", "services": ["s3", "sqs"]}
        rc = _run(["pods", "info", "test-pod"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request")
    def test_pods_delete(self, mock_api):
        mock_api.return_value = {"status": "deleted"}
        rc = _run(["pods", "delete", "test-pod"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/pods/test-pod",
            method="DELETE",
            data=None,
        )

    def test_pods_no_subcommand(self):
        rc = _run(["pods"])
        assert rc == 1


class TestCmdSes:
    @mock.patch("robotocore.cli._api_request")
    def test_ses_messages_table(self, mock_api, capsys):
        mock_api.return_value = {
            "messages": [
                {
                    "timestamp": "12:00",
                    "from": "a@b.com",
                    "to": ["c@d.com"],
                    "subject": "Hello",
                }
            ]
        }
        rc = _run(["ses", "messages"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "Hello" in out

    @mock.patch("robotocore.cli._api_request")
    def test_ses_messages_with_limit(self, mock_api):
        mock_api.return_value = {"messages": []}
        rc = _run(["ses", "messages", "--limit", "5"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/ses/messages?limit=5",
            method="GET",
            data=None,
        )

    @mock.patch("robotocore.cli._api_request")
    def test_ses_clear(self, mock_api):
        mock_api.return_value = {"status": "cleared"}
        rc = _run(["ses", "clear"])
        assert rc == 0

    def test_ses_no_subcommand(self):
        rc = _run(["ses"])
        assert rc == 1


class TestCmdIam:
    @mock.patch("robotocore.cli._api_request")
    def test_iam_stream_table(self, mock_api, capsys):
        mock_api.return_value = {
            "entries": [
                {
                    "timestamp": "12:00",
                    "principal": "arn:aws:iam::123:user/bob",
                    "action": "s3:PutObject",
                    "resource": "arn:aws:s3:::bucket",
                    "decision": "ALLOW",
                }
            ]
        }
        rc = _run(["iam", "stream"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "ALLOW" in out

    @mock.patch("robotocore.cli._api_request")
    def test_iam_stream_with_filters(self, mock_api):
        mock_api.return_value = {"entries": []}
        rc = _run(["iam", "stream", "--limit", "10", "--decision", "DENY"])
        assert rc == 0
        mock_api.assert_called_once_with(
            f"http://localhost:{DEFAULT_PORT}/_robotocore/iam/policy-stream?limit=10&decision=DENY",
            method="GET",
            data=None,
        )

    @mock.patch("robotocore.cli._api_request")
    def test_iam_suggest(self, mock_api):
        mock_api.return_value = {"policy": {"Version": "2012-10-17", "Statement": []}}
        rc = _run(["iam", "suggest", "arn:aws:iam::123:user/bob"])
        assert rc == 0
        assert "principal=arn:aws:iam::123:user/bob" in mock_api.call_args[0][0]

    def test_iam_no_subcommand(self):
        rc = _run(["iam"])
        assert rc == 1


class TestCmdDiagnose:
    @mock.patch("robotocore.cli._api_request")
    def test_diagnose_success(self, mock_api):
        mock_api.return_value = {"status": "ok", "checks": []}
        rc = _run(["diagnose"])
        assert rc == 0

    @mock.patch("robotocore.cli._api_request", side_effect=ConnectionRefusedError("refused"))
    def test_diagnose_failure(self, mock_api):
        rc = _run(["diagnose"])
        assert rc == 1


class TestNoCommand:
    def test_no_command_returns_1(self):
        rc = _run([])
        assert rc == 1

    def test_state_no_subcommand_returns_1(self):
        # argparse will show help; state_command will be None
        rc = _run(["state"])
        assert rc == 1
