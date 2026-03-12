"""Semantic integration tests for Docker Lambda executor.

These mock subprocess.run to avoid requiring Docker daemon in unit tests,
but test the end-to-end invoke flow including container lifecycle.
"""

import json
import os
import subprocess
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.docker_executor import DockerLambdaExecutor, get_executor_mode


def _mock_docker_run(returncode=0, stdout="", stderr=""):
    """Create a mock subprocess.run result for docker invocations."""
    return MagicMock(returncode=returncode, stdout=stdout, stderr=stderr)


class TestEndToEndInvokeFlow:
    """Test end-to-end invoke flow with subprocess mocks."""

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_full_invoke_cycle(self, mock_avail, mock_run, tmp_path):
        mock_avail.return_value = True
        response = json.dumps({"statusCode": 200, "body": "Hello"})
        mock_run.return_value = _mock_docker_run(stdout=response, stderr="START\nEND\n")

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={"key": "value"},
            function_name="e2e-fn",
            runtime="python3.12",
            timeout=30,
            memory_size=256,
            code_dir=code_dir,
        )

        assert result == {"statusCode": 200, "body": "Hello"}
        assert error is None
        mock_run.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_invoke_with_env_vars(self, mock_avail, mock_run, tmp_path):
        mock_avail.return_value = True
        mock_run.return_value = _mock_docker_run(stdout='"ok"')

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, _ = executor.execute(
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

        # Check env vars in the docker command
        cmd = mock_run.call_args[0][0]
        pairs = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
        assert "MY_SECRET=s3cr3t" in pairs
        assert "TABLE_NAME=my-table" in pairs
        assert "AWS_REGION=us-west-2" in pairs

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_invoke_timeout_kills_container(self, mock_avail, mock_run, tmp_path):
        mock_avail.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=11)

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        with patch.object(executor, "_force_remove_container") as mock_cleanup:
            result, error, logs = executor.execute(
                code_zip=b"fake",
                handler="index.handler",
                event={},
                function_name="timeout-fn",
                runtime="python3.12",
                timeout=1,
                memory_size=128,
                code_dir=code_dir,
            )

        assert error == "Task.TimedOut"
        mock_cleanup.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_invoke_error_exit_code(self, mock_avail, mock_run, tmp_path):
        mock_avail.return_value = True
        error_response = json.dumps(
            {"errorMessage": "division by zero", "errorType": "ZeroDivisionError"}
        )
        mock_run.return_value = _mock_docker_run(returncode=1, stdout=error_response)

        executor = DockerLambdaExecutor()
        code_dir = str(tmp_path / "code")
        os.makedirs(code_dir, exist_ok=True)

        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="error-fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=code_dir,
        )

        assert error == "Handled"
        assert result["errorType"] == "ZeroDivisionError"
        assert result["errorMessage"] == "division by zero"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_concurrent_invocations_create_separate_containers(
        self, mock_avail, mock_run, tmp_path
    ):
        mock_avail.return_value = True
        mock_run.return_value = _mock_docker_run(stdout='"ok"')

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
                code_dir=code_dir,
            )
            assert result == "ok"
            assert error is None

        assert mock_run.call_count == 3

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_code_dir_mounted_as_var_task(self, mock_avail, mock_run, tmp_path):
        """Verify code_dir is mounted into /var/task in the container."""
        mock_avail.return_value = True
        mock_run.return_value = _mock_docker_run(stdout='"ok"')

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

        cmd = mock_run.call_args[0][0]
        idx = cmd.index("-v")
        assert cmd[idx + 1] == f"{code_dir}:/var/task:ro"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_invoke_with_docker_network(self, mock_avail, mock_run, tmp_path):
        mock_avail.return_value = True
        mock_run.return_value = _mock_docker_run(stdout='"ok"')

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

        cmd = mock_run.call_args[0][0]
        idx = cmd.index("--network")
        assert cmd[idx + 1] == "my-net"

    @patch("robotocore.services.lambda_.docker_executor.subprocess.run")
    @patch("robotocore.services.lambda_.docker_executor.is_docker_available")
    def test_invoke_list_response(self, mock_avail, mock_run, tmp_path):
        """Lambda can return a JSON list."""
        mock_avail.return_value = True
        mock_run.return_value = _mock_docker_run(stdout="[1, 2, 3]")

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
        with patch.dict(os.environ, {}, clear=True):
            assert get_executor_mode() == "local"

    def test_docker_mode(self):
        with patch.dict(os.environ, {"LAMBDA_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_local_mode_explicit(self):
        with patch.dict(os.environ, {"LAMBDA_EXECUTOR": "local"}):
            assert get_executor_mode() == "local"

    def test_backward_compat(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"
