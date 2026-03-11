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
            f"http://localhost:{DEFAULT_PORT}/_robotocore/state/snapshots"
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
        )


class TestNoCommand:
    def test_no_command_returns_1(self):
        rc = _run([])
        assert rc == 1

    def test_state_no_subcommand_returns_1(self):
        # argparse will show help; state_command will be None
        rc = _run(["state"])
        assert rc == 1
