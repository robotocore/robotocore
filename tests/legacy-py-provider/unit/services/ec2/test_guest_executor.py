"""Unit tests for EC2 Guest Executor.

These tests verify the core guest executor logic without requiring Docker.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

from robotocore.services.ec2.guest.executor import (
    ExecutionRecord,
    GuestExecutionResult,
    GuestExecutor,
)


class TestGuestExecutorParsing:
    """Tests for user-data parsing."""

    def test_parse_plain_shell_script(self):
        """Plain shell scripts should be parsed correctly."""
        executor = GuestExecutor()
        user_data = "#!/bin/bash\necho hello"

        parts = executor._parse_user_data(user_data)

        assert len(parts) == 1
        assert parts[0]["type"] == "shell"
        assert parts[0]["content"] == user_data

    def test_parse_base64_encoded_script(self):
        """Base64-encoded scripts should be decoded and parsed."""
        executor = GuestExecutor()
        original = "#!/bin/bash\necho hello"
        user_data = base64.b64encode(original.encode()).decode()

        parts = executor._parse_user_data(user_data)

        assert len(parts) == 1
        assert parts[0]["type"] == "shell"
        assert "echo hello" in parts[0]["content"]

    def test_parse_cloud_config(self):
        """Cloud-config format should be recognized."""
        executor = GuestExecutor()
        user_data = "#cloud-config\npackage_upgrade: true"

        parts = executor._parse_user_data(user_data)

        assert len(parts) == 1
        assert parts[0]["type"] == "cloud-config"

    def test_parse_mime_multipart(self):
        """MIME multi-part messages should be parsed into parts."""
        executor = GuestExecutor()
        user_data = """Content-Type: multipart/mixed; boundary="==BOUNDARY=="
MIME-Version: 1.0

--==BOUNDARY==
Content-Type: text/x-shellscript

#!/bin/bash
echo part1

--==BOUNDARY==
Content-Type: text/x-shellscript

#!/bin/bash
echo part2

--==BOUNDARY==--
"""

        parts = executor._parse_user_data(user_data)

        assert len(parts) == 2
        assert all(p["type"] == "shell" for p in parts)
        assert "part1" in parts[0]["content"]
        assert "part2" in parts[1]["content"]

    def test_parse_empty_user_data(self):
        """Empty user-data should return empty list."""
        executor = GuestExecutor()

        assert executor._parse_user_data(None) == []
        assert executor._parse_user_data("") == []
        assert executor._parse_user_data(b"") == []

    def test_parse_bytes_input(self):
        """Bytes input should be handled."""
        executor = GuestExecutor()
        user_data = b"#!/bin/bash\necho hello"

        parts = executor._parse_user_data(user_data)

        assert len(parts) == 1
        assert parts[0]["type"] == "shell"


class TestGuestExecutionResult:
    """Tests for GuestExecutionResult dataclass."""

    def test_to_dict(self):
        """Result should serialize to dict correctly."""
        result = GuestExecutionResult(
            instance_id="i-1234567890abcdef0",
            account_id="123456789012",
            region="us-east-1",
            start_time="2024-01-01T00:00:00Z",
            status="completed",
            commands=[
                ExecutionRecord(
                    timestamp="2024-01-01T00:00:01Z",
                    command="echo hello",
                    stdout="hello\n",
                    stderr="",
                    exit_code=0,
                    duration_ms=100.0,
                ),
            ],
        )

        data = result.to_dict()

        assert data["instance_id"] == "i-1234567890abcdef0"
        assert data["account_id"] == "123456789012"
        assert data["region"] == "us-east-1"
        assert data["status"] == "completed"
        assert len(data["commands"]) == 1
        assert data["commands"][0]["command"] == "echo hello"
        assert data["commands"][0]["exit_code"] == 0


class TestGuestExecutorState:
    """Tests for GuestExecutor state management."""

    def test_get_execution_result_not_found(self):
        """Getting non-existent execution should return None."""
        executor = GuestExecutor()

        result = executor.get_execution_result("i-nonexistent")

        assert result is None

    def test_list_executions_empty(self):
        """Listing executions when none exist should return empty list."""
        executor = GuestExecutor()

        results = executor.list_executions()

        assert results == []

    def test_list_executions_filtered_by_account(self):
        """Listing should filter by account_id."""
        executor = GuestExecutor()

        # Manually add some results
        result1 = GuestExecutionResult(
            instance_id="i-1",
            account_id="111111111111",
            region="us-east-1",
            start_time="2024-01-01T00:00:00Z",
        )
        result2 = GuestExecutionResult(
            instance_id="i-2",
            account_id="222222222222",
            region="us-east-1",
            start_time="2024-01-01T00:00:00Z",
        )

        executor._executions["i-1"] = result1
        executor._executions["i-2"] = result2

        results = executor.list_executions(account_id="111111111111")

        assert len(results) == 1
        assert results[0].instance_id == "i-1"

    def test_list_executions_filtered_by_region(self):
        """Listing should filter by region."""
        executor = GuestExecutor()

        result1 = GuestExecutionResult(
            instance_id="i-1",
            account_id="123456789012",
            region="us-east-1",
            start_time="2024-01-01T00:00:00Z",
        )
        result2 = GuestExecutionResult(
            instance_id="i-2",
            account_id="123456789012",
            region="us-west-2",
            start_time="2024-01-01T00:00:00Z",
        )

        executor._executions["i-1"] = result1
        executor._executions["i-2"] = result2

        results = executor.list_executions(region="us-west-2")

        assert len(results) == 1
        assert results[0].instance_id == "i-2"


class TestGuestExecutorContainerManagement:
    """Tests for container lifecycle management."""

    @patch("subprocess.run")
    def test_create_guest_container_success(self, mock_run):
        """Container creation should return container ID on success."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="abc123def456\n",
            stderr="",
        )

        executor = GuestExecutor()
        container_id = executor._create_guest_container(
            instance_id="i-1234567890abcdef0",
            instance_type="t2.micro",
            image="test-image",
            block_devices=[],
            iam_role_arn=None,
            account_id="123456789012",
            region="us-east-1",
        )

        assert container_id == "abc123def456"

    @patch("subprocess.run")
    def test_create_guest_container_failure(self, mock_run):
        """Container creation should return None on failure."""
        from subprocess import CalledProcessError

        # Mock responses for: network check, image check, docker run (raises exception)
        def side_effect(*args, **kwargs):
            cmd = args[0] if args else []
            if "images" in cmd:
                return MagicMock(returncode=0, stdout="test-image\n", stderr="")
            if "run" in cmd and "-d" in cmd:
                raise CalledProcessError(1, cmd, stderr="docker: error")
            return MagicMock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = side_effect

        executor = GuestExecutor()
        container_id = executor._create_guest_container(
            instance_id="i-1234567890abcdef0",
            instance_type="t2.micro",
            image="test-image",
            block_devices=[],
            iam_role_arn=None,
            account_id="123456789012",
            region="us-east-1",
        )

        assert container_id is None

    @patch("subprocess.run")
    def test_destroy_guest_container(self, mock_run):
        """Container destruction should call docker stop and rm."""
        mock_run.return_value = MagicMock(returncode=0)

        executor = GuestExecutor()
        executor._containers["i-123"] = "abc123"

        result = executor.terminate_instance("i-123")

        assert result is True
        assert "i-123" not in executor._containers

    @patch("subprocess.run")
    def test_destroy_nonexistent_container(self, mock_run):
        """Destroying non-tracked container should return False."""
        executor = GuestExecutor()

        result = executor._destroy_guest_container("i-nonexistent")

        assert result is False


class TestGuestExecutorExecution:
    """Tests for command execution."""

    @patch("subprocess.run")
    def test_execute_in_container_success(self, mock_run):
        """Command execution should capture output correctly."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="hello\n",
            stderr="",
        )

        executor = GuestExecutor()
        record = executor._execute_in_container("abc123", "echo hello")

        assert record.exit_code == 0
        assert record.stdout == "hello\n"
        assert record.stderr == ""
        assert record.command == "echo hello"
        assert record.duration_ms >= 0

    @patch("subprocess.run")
    def test_execute_in_container_failure(self, mock_run):
        """Command failure should be captured."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="error message",
        )

        executor = GuestExecutor()
        record = executor._execute_in_container("abc123", "false")

        assert record.exit_code == 1
        assert record.stderr == "error message"

    @patch("subprocess.run")
    def test_execute_in_container_timeout(self, mock_run):
        """Command timeout should be handled."""
        from subprocess import TimeoutExpired

        mock_run.side_effect = TimeoutExpired("cmd", 300)

        executor = GuestExecutor()
        record = executor._execute_in_container("abc123", "sleep 1000", timeout=1)

        assert record.exit_code == -1
        assert "timed out" in record.stderr.lower()


class TestGuestExecutorDisabled:
    """Tests for when guest executor is disabled."""

    @patch("robotocore.services.ec2.guest.executor.is_guest_executor_enabled")
    def test_launch_instance_when_disabled(self, mock_is_enabled):
        """Launch should return None when disabled."""
        mock_is_enabled.return_value = False
        executor = GuestExecutor()

        result = executor.launch_instance(
            instance_id="i-123",
            account_id="123456789012",
            region="us-east-1",
            user_data="#!/bin/bash\necho hello",
        )

        assert result is None
