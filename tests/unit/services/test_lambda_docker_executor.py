"""Unit tests for the Docker-based Lambda executor.

Tests use mocked subprocess calls -- no real Docker required.
"""

import subprocess
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.docker_executor import (
    RUNTIME_IMAGES,
    DockerLambdaExecutor,
    _build_docker_run_cmd,
    get_executor_mode,
    get_image_for_runtime,
    is_docker_available,
)

# Path to patch get_code_cache where it's imported from
_CODE_CACHE_PATCH = "robotocore.services.lambda_.executor.get_code_cache"


# ---------------------------------------------------------------------------
# Runtime mapping tests
# ---------------------------------------------------------------------------


class TestRuntimeMapping:
    """Tests for get_image_for_runtime and RUNTIME_IMAGES."""

    def test_python_runtimes(self):
        assert get_image_for_runtime("python3.12") == "public.ecr.aws/lambda/python:3.12"
        assert get_image_for_runtime("python3.11") == "public.ecr.aws/lambda/python:3.11"
        assert get_image_for_runtime("python3.9") == "public.ecr.aws/lambda/python:3.9"

    def test_nodejs_runtimes(self):
        assert get_image_for_runtime("nodejs20.x") == "public.ecr.aws/lambda/nodejs:20"
        assert get_image_for_runtime("nodejs18.x") == "public.ecr.aws/lambda/nodejs:18"

    def test_java_runtimes(self):
        assert get_image_for_runtime("java21") == "public.ecr.aws/lambda/java:21"
        assert get_image_for_runtime("java17") == "public.ecr.aws/lambda/java:17"
        assert get_image_for_runtime("java11") == "public.ecr.aws/lambda/java:11"

    def test_dotnet_runtimes(self):
        assert get_image_for_runtime("dotnet8") == "public.ecr.aws/lambda/dotnet:8"
        assert get_image_for_runtime("dotnet6") == "public.ecr.aws/lambda/dotnet:6"

    def test_go_runtime(self):
        assert get_image_for_runtime("go1.x") == "public.ecr.aws/lambda/go:1"

    def test_ruby_runtimes(self):
        assert get_image_for_runtime("ruby3.3") == "public.ecr.aws/lambda/ruby:3.3"
        assert get_image_for_runtime("ruby3.2") == "public.ecr.aws/lambda/ruby:3.2"

    def test_provided_runtimes(self):
        assert get_image_for_runtime("provided.al2") == "public.ecr.aws/lambda/provided:al2"
        assert get_image_for_runtime("provided.al2023") == "public.ecr.aws/lambda/provided:al2023"

    def test_unknown_runtime_returns_none(self):
        assert get_image_for_runtime("unknown_runtime") is None
        assert get_image_for_runtime("") is None

    def test_all_runtimes_have_ecr_prefix(self):
        for runtime, image in RUNTIME_IMAGES.items():
            assert image.startswith("public.ecr.aws/lambda/"), (
                f"Runtime {runtime} image {image} missing ECR prefix"
            )


# ---------------------------------------------------------------------------
# Executor mode tests
# ---------------------------------------------------------------------------


class TestExecutorMode:
    """Tests for get_executor_mode."""

    def test_default_is_local(self):
        with patch.dict("os.environ", {}, clear=True):
            assert get_executor_mode() == "local"

    def test_lambda_executor_env(self):
        with patch.dict("os.environ", {"LAMBDA_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_lambda_executor_case_insensitive(self):
        with patch.dict("os.environ", {"LAMBDA_EXECUTOR": "Docker"}):
            assert get_executor_mode() == "docker"

    def test_backward_compat_env(self):
        with patch.dict("os.environ", {"LAMBDA_RUNTIME_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_lambda_executor_takes_precedence(self):
        with patch.dict(
            "os.environ",
            {"LAMBDA_EXECUTOR": "docker", "LAMBDA_RUNTIME_EXECUTOR": "local"},
        ):
            assert get_executor_mode() == "docker"


# ---------------------------------------------------------------------------
# Docker availability tests
# ---------------------------------------------------------------------------


class TestDockerAvailability:
    """Tests for is_docker_available."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_docker_available(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        assert is_docker_available() is True
        mock_run.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_docker_not_available(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        assert is_docker_available() is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_docker_not_installed(self, mock_run):
        mock_run.side_effect = FileNotFoundError("docker not found")
        assert is_docker_available() is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_docker_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=5)
        assert is_docker_available() is False


# ---------------------------------------------------------------------------
# Docker run command construction tests
# ---------------------------------------------------------------------------


class TestBuildDockerRunCmd:
    """Tests for _build_docker_run_cmd."""

    def test_basic_command(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test-container",
            code_dir="/tmp/code",
            handler="index.handler",
            function_name="my-func",
            timeout=30,
            memory_size=256,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=4566,
        )
        assert cmd[0] == "docker"
        assert cmd[1] == "run"
        assert "--rm" in cmd
        assert "--name" in cmd
        idx = cmd.index("--name")
        assert cmd[idx + 1] == "test-container"

    def test_code_mount(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test",
            code_dir="/tmp/lambda-code",
            handler="handler.main",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=4566,
        )
        assert "-v" in cmd
        idx = cmd.index("-v")
        assert cmd[idx + 1] == "/tmp/lambda-code:/var/task:ro"

    def test_env_vars_passed(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/nodejs:20",
            container_name="test",
            code_dir="/tmp/code",
            handler="index.handler",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars={"MY_VAR": "hello", "DB_HOST": "localhost"},
            region="us-west-2",
            account_id="111222333444",
            gateway_port=4566,
        )
        # Find all -e flags
        env_pairs = []
        for i, arg in enumerate(cmd):
            if arg == "-e" and i + 1 < len(cmd):
                env_pairs.append(cmd[i + 1])

        assert "MY_VAR=hello" in env_pairs
        assert "DB_HOST=localhost" in env_pairs
        assert "AWS_REGION=us-west-2" in env_pairs
        assert "AWS_ACCOUNT_ID=111222333444" in env_pairs

    def test_docker_network(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test",
            code_dir="/tmp/code",
            handler="handler.main",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=4566,
            docker_network="my-network",
        )
        assert "--network" in cmd
        idx = cmd.index("--network")
        assert cmd[idx + 1] == "my-network"

    def test_docker_dns(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test",
            code_dir="/tmp/code",
            handler="handler.main",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=4566,
            docker_dns="8.8.8.8",
        )
        assert "--dns" in cmd
        idx = cmd.index("--dns")
        assert cmd[idx + 1] == "8.8.8.8"

    def test_handler_is_last_arg(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test",
            code_dir="/tmp/code",
            handler="my_module.my_handler",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=4566,
        )
        assert cmd[-1] == "my_module.my_handler"
        assert cmd[-2] == "public.ecr.aws/lambda/python:3.12"

    def test_gateway_endpoint_url(self):
        cmd = _build_docker_run_cmd(
            image="public.ecr.aws/lambda/python:3.12",
            container_name="test",
            code_dir="/tmp/code",
            handler="handler.main",
            function_name="func",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            gateway_port=9999,
        )
        env_pairs = []
        for i, arg in enumerate(cmd):
            if arg == "-e" and i + 1 < len(cmd):
                env_pairs.append(cmd[i + 1])
        assert "AWS_ENDPOINT_URL=http://host.docker.internal:9999" in env_pairs


# ---------------------------------------------------------------------------
# DockerLambdaExecutor tests
# ---------------------------------------------------------------------------


class TestDockerLambdaExecutor:
    """Tests for the DockerLambdaExecutor class."""

    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_fallback_when_docker_unavailable(self, mock_avail):
        mock_avail.return_value = False
        executor = DockerLambdaExecutor()
        assert executor._docker_available is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_successful_execution(self, mock_avail, mock_run):
        mock_avail.return_value = True
        # Mock the docker run subprocess
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"statusCode": 200, "body": "hello"}',
            stderr="START RequestId: abc\n",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={"key": "value"},
                function_name="test-func",
                runtime="nodejs20.x",
            )

        assert result == {"statusCode": 200, "body": "hello"}
        assert error_type is None
        assert "START" in logs

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_timeout_enforcement(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=13)

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            # Also mock the cleanup call
            with patch.object(executor, "_force_remove_container") as mock_cleanup:
                result, error_type, logs = executor.execute(
                    code_zip=b"fake-zip",
                    handler="index.handler",
                    event={},
                    function_name="test-func",
                    runtime="nodejs20.x",
                    timeout=3,
                )

                # Container should be force-removed on timeout
                mock_cleanup.assert_called_once()

        assert error_type == "Task.TimedOut"
        assert "timed out" in logs

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_container_error(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout='{"errorMessage": "something broke", "errorType": "Error"}',
            stderr="error logs here",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={},
                function_name="test-func",
                runtime="python3.12",
            )

        assert error_type == "Handled"
        assert result["errorMessage"] == "something broke"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_unhandled_error(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="segfault",
            stderr="",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={},
                function_name="test-func",
                runtime="java21",
            )

        assert error_type == "Unhandled"
        assert result["errorType"] == "Runtime.ExitError"

    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_unknown_runtime_falls_back_to_local(self, mock_avail):
        mock_avail.return_value = True
        executor = DockerLambdaExecutor()

        with patch.object(executor, "_execute_local_fallback") as mock_fallback:
            mock_fallback.return_value = ({"ok": True}, None, "")
            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="handler.main",
                event={},
                function_name="test-func",
                runtime="unknown_runtime_v99",
            )

        mock_fallback.assert_called_once()
        assert result == {"ok": True}

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_empty_response(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="",
            stderr="",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={},
                function_name="test-func",
                runtime="python3.12",
            )

        assert result is None
        assert error_type is None

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_string_response(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="not valid json here",
            stderr="",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            result, error_type, logs = executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={},
                function_name="test-func",
                runtime="python3.12",
            )

        # Non-JSON output returned as string
        assert result == "not valid json here"
        assert error_type is None

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_docker_cli_not_found_falls_back(self, mock_avail, mock_run):
        """If docker CLI disappears between availability check and run."""
        mock_avail.return_value = True
        mock_run.side_effect = FileNotFoundError("docker not found")

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            with patch.object(executor, "_execute_local_fallback") as mock_fallback:
                mock_fallback.return_value = ({"fallback": True}, None, "")
                executor.execute(
                    code_zip=b"fake-zip",
                    handler="handler.main",
                    event={},
                    function_name="test-func",
                    runtime="python3.12",
                )

        mock_fallback.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_env_vars_forwarded(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='"ok"',
            stderr="",
        )

        executor = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mock_cache:
            mock_cache.return_value.get_or_extract.return_value = "/tmp/test-code"

            executor.execute(
                code_zip=b"fake-zip",
                handler="index.handler",
                event={},
                function_name="test-func",
                runtime="python3.12",
                env_vars={"MY_SECRET": "abc123"},
            )

        # Check the docker run command included the env var
        call_args = mock_run.call_args
        cmd = call_args[0][0] if call_args[0] else call_args[1].get("cmd", [])
        # Find -e MY_SECRET=abc123
        env_pairs = []
        for i, arg in enumerate(cmd):
            if arg == "-e" and i + 1 < len(cmd):
                env_pairs.append(cmd[i + 1])
        assert "MY_SECRET=abc123" in env_pairs

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_uses_provided_code_dir(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='"ok"',
            stderr="",
        )

        executor = DockerLambdaExecutor()

        executor.execute(
            code_zip=b"fake-zip",
            handler="index.handler",
            event={},
            function_name="test-func",
            runtime="python3.12",
            code_dir="/my/mounted/code",
        )

        call_args = mock_run.call_args
        cmd = call_args[0][0] if call_args[0] else call_args[1].get("cmd", [])
        # The volume mount should use the provided code_dir
        idx = cmd.index("-v")
        assert "/my/mounted/code:/var/task:ro" == cmd[idx + 1]


# ---------------------------------------------------------------------------
# Force remove container tests
# ---------------------------------------------------------------------------


class TestForceRemoveContainer:
    """Tests for container cleanup."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_force_remove(self, mock_avail, mock_run):
        mock_avail.return_value = True
        executor = DockerLambdaExecutor()

        # Reset mock after constructor's is_docker_available call
        mock_run.reset_mock()
        mock_run.return_value = MagicMock(returncode=0)

        executor._force_remove_container("my-container")
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ["docker", "rm", "-f", "my-container"]

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_force_remove_ignores_errors(self, mock_avail, mock_run):
        mock_avail.return_value = True
        executor = DockerLambdaExecutor()

        mock_run.side_effect = Exception("docker daemon gone")
        # Should not raise
        executor._force_remove_container("my-container")


# ---------------------------------------------------------------------------
# Docker network / DNS config tests
# ---------------------------------------------------------------------------


class TestDockerConfig:
    """Tests for Docker network and DNS configuration."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_docker_network_from_env(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout='"ok"', stderr="")

        with patch.dict("os.environ", {"LAMBDA_DOCKER_NETWORK": "my-net"}, clear=False):
            executor = DockerLambdaExecutor()
            assert executor._docker_network == "my-net"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_docker_dns_from_env(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout='"ok"', stderr="")

        with patch.dict("os.environ", {"LAMBDA_DOCKER_DNS": "1.2.3.4"}, clear=False):
            executor = DockerLambdaExecutor()
            assert executor._docker_dns == "1.2.3.4"
