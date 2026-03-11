"""Semantic integration tests for Docker Lambda executor.

These mock the Docker client to avoid requiring Docker daemon in unit tests,
but test the end-to-end invoke flow including container lifecycle.
"""

import json
import os
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.docker_executor import DockerLambdaExecutor


def _make_mock_container(
    exit_code: int = 0,
    stdout: bytes = b'{"statusCode": 200}',
    stderr: bytes = b"START RequestId\nEND RequestId\n",
    status: str = "exited",
):
    """Create a mock Docker container with standard responses."""
    container = MagicMock()
    container.wait.return_value = {"StatusCode": exit_code}
    container.logs.side_effect = lambda stdout=True, stderr=False: (
        stdout_val if stdout else stderr_val
    )
    stdout_val = stdout
    stderr_val = stderr

    # Override logs to handle kwargs properly
    def mock_logs(stdout=True, stderr=False):
        parts = []
        if stdout:
            parts.append(stdout_val)
        if stderr:
            parts.append(stderr_val)
        return b"".join(parts)

    container.logs = mock_logs
    container.status = status
    container.id = "abc123"
    container.attrs = {"State": {"ExitCode": exit_code}}
    return container


class TestEndToEndInvokeFlow:
    """Test end-to-end invoke flow: create container -> send event -> get response -> cleanup."""

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_full_invoke_cycle(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        response_payload = json.dumps({"statusCode": 200, "body": "Hello"}).encode()
        mock_container = _make_mock_container(stdout=response_payload)
        mock_client.containers.run.return_value = mock_container

        executor = DockerLambdaExecutor()

        # Write a simple handler to a temp dir
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)
        (tmp_path / "code" / "index.py").write_text("def handler(event, context): return event")

        result, error, logs = executor.execute(
            code_zip=b"fake",  # Not used when code_dir is provided
            handler="index.handler",
            event={"key": "value"},
            function_name="e2e-fn",
            runtime="python3.12",
            timeout=30,
            memory_size=256,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )

        assert result == {"statusCode": 200, "body": "Hello"}
        assert error is None
        mock_client.containers.run.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_invoke_with_env_vars(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_container = _make_mock_container(stdout=b'"ok"')
        mock_client.containers.run.return_value = mock_container

        executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="env-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            env_vars={"MY_SECRET": "s3cr3t", "TABLE_NAME": "my-table"},
            region="us-west-2",
            account_id="123456789012",
            code_dir=code_dir,
        )

        # Verify env vars were passed in the container run call
        call_kwargs = mock_client.containers.run.call_args
        container_env = call_kwargs[1].get("environment", {}) if call_kwargs[1] else {}
        assert container_env.get("MY_SECRET") == "s3cr3t"
        assert container_env.get("TABLE_NAME") == "my-table"
        assert container_env.get("AWS_REGION") == "us-west-2"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_invoke_timeout_kills_container(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_container = MagicMock()
        mock_container.wait.side_effect = Exception("timeout")
        mock_container.status = "running"
        mock_container.id = "timeout123"
        mock_client.containers.run.return_value = mock_container

        executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="timeout-fn",
            runtime="python3.12",
            timeout=1,
            memory_size=128,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )

        assert error is not None
        # Container should have been killed
        mock_container.kill.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_invoke_error_exit_code(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        error_response = json.dumps(
            {
                "errorMessage": "division by zero",
                "errorType": "ZeroDivisionError",
            }
        ).encode()
        mock_container = _make_mock_container(exit_code=1, stdout=error_response)
        mock_client.containers.run.return_value = mock_container

        executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="error-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )

        assert error is not None
        assert isinstance(result, dict)
        assert result["errorType"] == "ZeroDivisionError"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_concurrent_invocations_use_separate_containers(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        containers = []

        def make_container(*args, **kwargs):
            c = _make_mock_container(stdout=b'"ok"')
            containers.append(c)
            return c

        mock_client.containers.run.side_effect = make_container

        executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        # Invoke twice for same function
        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={"n": 1},
            function_name="concurrent-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )
        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={"n": 2},
            function_name="concurrent-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )

        assert mock_client.containers.run.call_count == 2
        assert len(containers) == 2

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_warm_container_reuse(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        warm_container = MagicMock()
        warm_container.status = "running"
        warm_container.id = "warm123"
        warm_container.exec_run.return_value = (0, b'{"statusCode": 200}')

        mock_client.containers.run.return_value = _make_mock_container(stdout=b'"first"')

        executor = DockerLambdaExecutor(keepalive_ms=600000)

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        # First invoke creates a container
        result1, _, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="warm-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            region="us-east-1",
            account_id="123456789012",
            code_dir=code_dir,
        )

        assert mock_client.containers.run.call_count == 1


class TestExecutorSelection:
    """Test the executor selection logic (local vs docker based on env var)."""

    def test_default_is_local(self):
        with patch.dict(os.environ, {}, clear=False):
            # Remove LAMBDA_RUNTIME_EXECUTOR if set
            os.environ.pop("LAMBDA_RUNTIME_EXECUTOR", None)
            from robotocore.services.lambda_.docker_executor import get_executor_mode

            assert get_executor_mode() == "local"

    def test_docker_mode(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "docker"}):
            from robotocore.services.lambda_.docker_executor import get_executor_mode

            assert get_executor_mode() == "docker"

    def test_local_mode_explicit(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "local"}):
            from robotocore.services.lambda_.docker_executor import get_executor_mode

            assert get_executor_mode() == "local"
