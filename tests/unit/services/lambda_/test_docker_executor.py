"""Tests for the Docker-based Lambda executor (subprocess/CLI based)."""

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

# Patch target for get_code_cache (imported locally inside execute())
_CODE_CACHE_PATCH = "robotocore.services.lambda_.executor.get_code_cache"

# ---------------------------------------------------------------------------
# Runtime image mapping
# ---------------------------------------------------------------------------


class TestRuntimeImageMapping:
    """Test runtime-to-Docker-image mapping for all supported runtimes."""

    def test_python312(self):
        assert get_image_for_runtime("python3.12") == "public.ecr.aws/lambda/python:3.12"

    def test_python311(self):
        assert get_image_for_runtime("python3.11") == "public.ecr.aws/lambda/python:3.11"

    def test_python313(self):
        assert get_image_for_runtime("python3.13") == "public.ecr.aws/lambda/python:3.13"

    def test_python310(self):
        assert get_image_for_runtime("python3.10") == "public.ecr.aws/lambda/python:3.10"

    def test_python39(self):
        assert get_image_for_runtime("python3.9") == "public.ecr.aws/lambda/python:3.9"

    def test_python38(self):
        assert get_image_for_runtime("python3.8") == "public.ecr.aws/lambda/python:3.8"

    def test_nodejs20(self):
        assert get_image_for_runtime("nodejs20.x") == "public.ecr.aws/lambda/nodejs:20"

    def test_nodejs18(self):
        assert get_image_for_runtime("nodejs18.x") == "public.ecr.aws/lambda/nodejs:18"

    def test_nodejs16(self):
        assert get_image_for_runtime("nodejs16.x") == "public.ecr.aws/lambda/nodejs:16"

    def test_java21(self):
        assert get_image_for_runtime("java21") == "public.ecr.aws/lambda/java:21"

    def test_java17(self):
        assert get_image_for_runtime("java17") == "public.ecr.aws/lambda/java:17"

    def test_java11(self):
        assert get_image_for_runtime("java11") == "public.ecr.aws/lambda/java:11"

    def test_dotnet8(self):
        assert get_image_for_runtime("dotnet8") == "public.ecr.aws/lambda/dotnet:8"

    def test_dotnet6(self):
        assert get_image_for_runtime("dotnet6") == "public.ecr.aws/lambda/dotnet:6"

    def test_ruby33(self):
        assert get_image_for_runtime("ruby3.3") == "public.ecr.aws/lambda/ruby:3.3"

    def test_ruby32(self):
        assert get_image_for_runtime("ruby3.2") == "public.ecr.aws/lambda/ruby:3.2"

    def test_go1x(self):
        assert get_image_for_runtime("go1.x") == "public.ecr.aws/lambda/go:1"

    def test_provided_al2(self):
        assert get_image_for_runtime("provided.al2") == "public.ecr.aws/lambda/provided:al2"

    def test_provided_al2023(self):
        assert get_image_for_runtime("provided.al2023") == "public.ecr.aws/lambda/provided:al2023"

    def test_unknown_returns_none(self):
        assert get_image_for_runtime("cobol99") is None

    def test_empty_string_returns_none(self):
        assert get_image_for_runtime("") is None

    def test_all_images_have_ecr_prefix(self):
        for runtime, image in RUNTIME_IMAGES.items():
            assert image.startswith("public.ecr.aws/lambda/"), (
                f"Runtime {runtime} -> {image} missing ECR prefix"
            )


# ---------------------------------------------------------------------------
# Executor mode
# ---------------------------------------------------------------------------


class TestGetExecutorMode:
    """Test executor mode detection from environment variables."""

    def test_default_is_local(self):
        with patch.dict("os.environ", {}, clear=True):
            assert get_executor_mode() == "local"

    def test_lambda_executor_env(self):
        with patch.dict("os.environ", {"LAMBDA_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_case_insensitive(self):
        with patch.dict("os.environ", {"LAMBDA_EXECUTOR": "Docker"}):
            assert get_executor_mode() == "docker"

    def test_backward_compat_runtime_executor(self):
        with patch.dict("os.environ", {"LAMBDA_RUNTIME_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_lambda_executor_takes_precedence(self):
        with patch.dict(
            "os.environ",
            {"LAMBDA_EXECUTOR": "docker", "LAMBDA_RUNTIME_EXECUTOR": "local"},
        ):
            assert get_executor_mode() == "docker"


# ---------------------------------------------------------------------------
# Docker availability
# ---------------------------------------------------------------------------


class TestDockerAvailability:
    """Test Docker availability detection."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_available(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        assert is_docker_available() is True

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_not_available(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        assert is_docker_available() is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_not_installed(self, mock_run):
        mock_run.side_effect = FileNotFoundError()
        assert is_docker_available() is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    def test_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=5)
        assert is_docker_available() is False


# ---------------------------------------------------------------------------
# Docker run command construction
# ---------------------------------------------------------------------------


class TestBuildDockerRunCmd:
    """Test docker run command construction."""

    def _basic_cmd(self, **overrides):
        defaults = {
            "image": "public.ecr.aws/lambda/python:3.12",
            "container_name": "test-ctr",
            "code_dir": "/tmp/code",
            "handler": "handler.main",
            "function_name": "my-func",
            "timeout": 3,
            "memory_size": 128,
            "env_vars": None,
            "region": "us-east-1",
            "account_id": "123456789012",
            "gateway_port": 4566,
        }
        defaults.update(overrides)
        return _build_docker_run_cmd(**defaults)

    def test_starts_with_docker_run(self):
        cmd = self._basic_cmd()
        assert cmd[0] == "docker"
        assert cmd[1] == "run"
        assert "--rm" in cmd

    def test_container_name(self):
        cmd = self._basic_cmd(container_name="my-container")
        idx = cmd.index("--name")
        assert cmd[idx + 1] == "my-container"

    def test_volume_mount(self):
        cmd = self._basic_cmd(code_dir="/my/code")
        idx = cmd.index("-v")
        assert cmd[idx + 1] == "/my/code:/var/task:ro"

    def test_handler_is_last(self):
        cmd = self._basic_cmd(handler="index.handler")
        assert cmd[-1] == "index.handler"

    def test_image_is_second_to_last(self):
        cmd = self._basic_cmd(image="public.ecr.aws/lambda/nodejs:20")
        assert cmd[-2] == "public.ecr.aws/lambda/nodejs:20"

    def test_env_vars(self):
        cmd = self._basic_cmd(env_vars={"FOO": "bar", "BAZ": "qux"})
        pairs = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
        assert "FOO=bar" in pairs
        assert "BAZ=qux" in pairs

    def test_core_env_vars(self):
        cmd = self._basic_cmd(region="eu-west-1", account_id="999888777666", gateway_port=9999)
        pairs = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
        assert "AWS_REGION=eu-west-1" in pairs
        assert "AWS_ACCOUNT_ID=999888777666" in pairs
        assert "AWS_ENDPOINT_URL=http://host.docker.internal:9999" in pairs

    def test_docker_network(self):
        cmd = self._basic_cmd(docker_network="my-net")
        idx = cmd.index("--network")
        assert cmd[idx + 1] == "my-net"

    def test_no_network_by_default(self):
        cmd = self._basic_cmd()
        assert "--network" not in cmd

    def test_docker_dns(self):
        cmd = self._basic_cmd(docker_dns="8.8.8.8")
        idx = cmd.index("--dns")
        assert cmd[idx + 1] == "8.8.8.8"


# ---------------------------------------------------------------------------
# DockerLambdaExecutor
# ---------------------------------------------------------------------------


class TestDockerLambdaExecutor:
    """Test the executor class."""

    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_marks_unavailable(self, mock_avail):
        mock_avail.return_value = False
        ex = DockerLambdaExecutor()
        assert ex._docker_available is False

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_success(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"statusCode": 200}',
            stderr="logs\n",
        )
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            result, err, logs = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={"k": "v"},
                function_name="f",
                runtime="nodejs20.x",
            )

        assert result == {"statusCode": 200}
        assert err is None
        assert "logs" in logs

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_timeout(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=13)
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            with patch.object(ex, "_force_remove_container"):
                result, err, logs = ex.execute(
                    code_zip=b"z",
                    handler="h.h",
                    event={},
                    function_name="f",
                    runtime="python3.12",
                    timeout=3,
                )

        assert err == "Task.TimedOut"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_handled_error(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout='{"errorMessage": "bad", "errorType": "ValueError"}',
            stderr="err\n",
        )
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            result, err, logs = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={},
                function_name="f",
                runtime="java21",
            )

        assert err == "Handled"
        assert result["errorMessage"] == "bad"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_unhandled_error(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=1, stdout="crash", stderr="")
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            result, err, _ = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={},
                function_name="f",
                runtime="dotnet8",
            )

        assert err == "Unhandled"
        assert result["errorType"] == "Runtime.ExitError"

    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_unknown_runtime_falls_back(self, mock_avail):
        mock_avail.return_value = True
        ex = DockerLambdaExecutor()

        with patch.object(ex, "_execute_local_fallback") as fb:
            fb.return_value = ({"ok": True}, None, "")
            result, err, _ = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={},
                function_name="f",
                runtime="cobol99",
            )

        fb.assert_called_once()
        assert result == {"ok": True}

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_empty_response(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            result, err, _ = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={},
                function_name="f",
                runtime="python3.12",
            )

        assert result is None
        assert err is None

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_non_json_response(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout="plain text", stderr="")
        ex = DockerLambdaExecutor()

        with patch(_CODE_CACHE_PATCH) as mc:
            mc.return_value.get_or_extract.return_value = "/tmp/code"
            result, err, _ = ex.execute(
                code_zip=b"z",
                handler="h.h",
                event={},
                function_name="f",
                runtime="python3.12",
            )

        assert result == "plain text"
        assert err is None

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_uses_provided_code_dir(self, mock_avail, mock_run):
        mock_avail.return_value = True
        mock_run.return_value = MagicMock(returncode=0, stdout='"ok"', stderr="")
        ex = DockerLambdaExecutor()

        ex.execute(
            code_zip=b"z",
            handler="h.h",
            event={},
            function_name="f",
            runtime="python3.12",
            code_dir="/mounted/code",
        )

        cmd = mock_run.call_args[0][0]
        idx = cmd.index("-v")
        assert cmd[idx + 1] == "/mounted/code:/var/task:ro"


# ---------------------------------------------------------------------------
# Container cleanup
# ---------------------------------------------------------------------------


class TestForceRemoveContainer:
    """Test container force-removal."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_calls_docker_rm(self, mock_avail, mock_run):
        mock_avail.return_value = True
        ex = DockerLambdaExecutor()
        mock_run.reset_mock()
        mock_run.return_value = MagicMock(returncode=0)

        ex._force_remove_container("ctr-123")
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == ["docker", "rm", "-f", "ctr-123"]

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_ignores_errors(self, mock_avail, mock_run):
        mock_avail.return_value = True
        ex = DockerLambdaExecutor()
        mock_run.side_effect = Exception("boom")
        ex._force_remove_container("ctr-123")  # should not raise
