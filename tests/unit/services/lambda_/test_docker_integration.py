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
):
    """Create a mock Docker container with standard responses."""
    container = MagicMock()
    container.wait.return_value = {"StatusCode": exit_code}
    container.status = "exited"
    container.id = "abc123"

    _stdout = stdout
    _stderr = stderr

    def mock_logs(stdout=True, stderr=False):
        if stdout and not stderr:
            return _stdout
        if stderr and not stdout:
            return _stderr
        parts = []
        if stdout:
            parts.append(_stdout)
        if stderr:
            parts.append(_stderr)
        return b"".join(parts)

    container.logs = mock_logs
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

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)
        (tmp_path / "code" / "index.py").write_text("def handler(event, context): return event")

        result, error, logs = executor.execute(
            code_zip=b"fake",
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

        assert result == "ok"
        assert error is None

        call_kwargs = mock_client.containers.run.call_args
        container_env = call_kwargs.kwargs.get("environment", {})
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

        assert error == "Task.TimedOut"
        assert result is None
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

        assert error == "Handled"
        assert isinstance(result, dict)
        assert result["errorType"] == "ZeroDivisionError"
        assert result["errorMessage"] == "division by zero"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_concurrent_invocations_create_separate_containers(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        containers = []

        def make_container(**kwargs):
            c = _make_mock_container(stdout=b'"ok"')
            containers.append(c)
            return c

        mock_client.containers.run.side_effect = make_container

        executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        for i in range(3):
            result, error, _ = executor.execute(
                code_zip=b"fake",
                handler="index.handler",
                event={"n": i},
                function_name="concurrent-fn",
                runtime="python3.12",
                timeout=3,
                memory_size=128,
                region="us-east-1",
                account_id="123456789012",
                code_dir=code_dir,
            )
            assert result == "ok"
            assert error is None

        assert mock_client.containers.run.call_count == 3
        assert len(containers) == 3

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_code_dir_mounted_as_var_task(self, mock_docker_mod, tmp_path):
        """Verify code_dir is mounted into /var/task in the container."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(stdout=b'"ok"')

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="mount-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=code_dir,
        )

        call_kwargs = mock_client.containers.run.call_args.kwargs
        volumes = call_kwargs["volumes"]
        assert code_dir in volumes
        assert volumes[code_dir]["bind"] == "/var/task"
        assert volumes[code_dir]["mode"] == "ro"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_invoke_with_docker_network(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client
        mock_client.containers.run.return_value = _make_mock_container(stdout=b'"ok"')

        with patch.dict(os.environ, {"LAMBDA_DOCKER_NETWORK": "my-net"}):
            executor = DockerLambdaExecutor()

        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="net-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=code_dir,
        )

        call_kwargs = mock_client.containers.run.call_args.kwargs
        assert call_kwargs["network"] == "my-net"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_invoke_list_response(self, mock_docker_mod, tmp_path):
        """Lambda can return a JSON list."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(stdout=b"[1, 2, 3]")

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="list-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=code_dir,
        )

        assert result == [1, 2, 3]
        assert error is None


class TestExecutorSelection:
    """Test the executor selection logic (local vs docker based on env var)."""

    def test_default_is_local(self):
        env = os.environ.copy()
        env.pop("LAMBDA_RUNTIME_EXECUTOR", None)
        with patch.dict(os.environ, env, clear=True):
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
