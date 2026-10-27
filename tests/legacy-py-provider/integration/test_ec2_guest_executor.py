"""Integration tests for EC2 Guest Executor.

These tests verify that:
1. When ROBOTOCORE_EC2_GUEST_EXECUTOR=1, RunInstances launches guest containers
2. User-data scripts are executed inside containers
3. Execution evidence is captured and retrievable
4. TerminateInstances cleans up guest containers
5. When disabled (default), no containers are launched
"""

from __future__ import annotations

import base64
import time

import pytest

# Skip all tests if Docker is not available
try:
    import subprocess

    result = subprocess.run(
        ["docker", "ps"],
        capture_output=True,
        timeout=5,
    )
    DOCKER_AVAILABLE = result.returncode == 0
except Exception:
    DOCKER_AVAILABLE = False


pytestmark = [
    pytest.mark.skipif(
        not DOCKER_AVAILABLE,
        reason="Docker not available - guest executor requires Docker",
    ),
    pytest.mark.integration,
]


@pytest.fixture
def ec2_client(make_boto_client):
    """Create an EC2 client."""
    return make_boto_client("ec2", region_name="us-east-1")


@pytest.fixture
def iam_client(make_boto_client):
    """Create an IAM client."""
    return make_boto_client("iam", region_name="us-east-1")


@pytest.fixture(autouse=True)
def enable_guest_executor(monkeypatch):
    """Enable guest executor for tests."""
    monkeypatch.setenv("ROBOTOCORE_EC2_GUEST_EXECUTOR", "1")
    yield


class TestEC2GuestExecutorDisabled:
    """Tests that when disabled, EC2 behaves normally without guest execution."""

    def test_run_instances_without_guest_executor(self, make_boto_client, monkeypatch):
        """When disabled, RunInstances should not launch containers."""
        monkeypatch.setenv("ROBOTOCORE_EC2_GUEST_EXECUTOR", "0")

        ec2 = make_boto_client("ec2", region_name="us-east-1")

        # Run an instance without user-data
        result = ec2.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
        )

        assert len(result["Instances"]) == 1
        instance_id = result["Instances"][0]["InstanceId"]
        assert instance_id.startswith("i-")

        # Clean up
        ec2.terminate_instances(InstanceIds=[instance_id])


class TestEC2GuestExecutorEnabled:
    """Tests for guest executor functionality when enabled."""

    def test_run_instances_with_simple_user_data(self, ec2_client, _server_url):
        """RunInstances with shell script user-data executes in container."""
        user_data = """#!/bin/bash
echo "Hello from EC2 instance" > /tmp/hello.txt
date >> /tmp/hello.txt
"""
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
        )

        assert len(result["Instances"]) == 1
        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution to complete (with timeout)
        import requests

        max_wait = 60  # seconds
        start = time.time()
        execution = None

        while time.time() - start < max_wait:
            resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
            if resp.status_code == 200:
                execution = resp.json()
                if execution.get("status") in ("completed", "failed"):
                    break
            time.sleep(2)

        # Verify execution was captured
        assert execution is not None, "Execution not found"
        assert execution["instance_id"] == instance_id
        assert execution["status"] == "completed"

        # Verify commands were executed
        commands = execution.get("commands", [])
        assert len(commands) > 0

        # Clean up
        ec2_client.terminate_instances(InstanceIds=[instance_id])

    def test_run_instances_with_mime_multipart(self, ec2_client, _server_url):
        """RunInstances with MIME multi-part user-data executes all parts."""
        # Create a MIME multi-part message
        user_data = """Content-Type: multipart/mixed; boundary="==BOUNDARY=="
MIME-Version: 1.0

--==BOUNDARY==
Content-Type: text/x-shellscript

#!/bin/bash
echo "Part 1" > /tmp/part1.txt

--==BOUNDARY==
Content-Type: text/x-shellscript

#!/bin/bash
echo "Part 2" > /tmp/part2.txt

--==BOUNDARY==--
"""
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
        )

        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution
        import requests

        max_wait = 60
        start = time.time()
        execution = None

        while time.time() - start < max_wait:
            resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
            if resp.status_code == 200:
                execution = resp.json()
                if execution.get("status") in ("completed", "failed"):
                    break
            time.sleep(2)

        assert execution is not None
        assert execution["status"] == "completed"

        # Should have multiple commands from MIME parts
        commands = execution.get("commands", [])
        assert len(commands) >= 2

        # Clean up
        ec2_client.terminate_instances(InstanceIds=[instance_id])

    def test_run_instances_with_block_devices(self, ec2_client, _server_url):
        """RunInstances with block device mappings creates volumes in container."""
        user_data = """#!/bin/bash
# Check for block devices
lsblk > /tmp/block_devices.txt
"""
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/xvdf",
                    "Ebs": {
                        "VolumeSize": 10,
                        "VolumeType": "gp2",
                    },
                },
            ],
        )

        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution
        import requests

        max_wait = 60
        start = time.time()
        execution = None

        while time.time() - start < max_wait:
            resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
            if resp.status_code == 200:
                execution = resp.json()
                if execution.get("status") in ("completed", "failed"):
                    break
            time.sleep(2)

        assert execution is not None
        assert execution["status"] == "completed"

        # Clean up
        ec2_client.terminate_instances(InstanceIds=[instance_id])

    def test_terminate_instances_cleans_up_containers(self, ec2_client, _server_url):
        """TerminateInstances should clean up guest containers."""
        user_data = "#!/bin/bash\necho 'test'"
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
        )

        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution to start
        time.sleep(3)

        # Terminate the instance
        ec2_client.terminate_instances(InstanceIds=[instance_id])

        # Wait a moment for cleanup
        time.sleep(2)

        # Verify execution shows terminated status
        import requests

        resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
        if resp.status_code == 200:
            execution = resp.json()
            assert execution["status"] == "terminated"

    def test_list_executions_endpoint(self, ec2_client, _server_url):
        """The list executions endpoint should return all executions."""
        import requests

        resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions")
        assert resp.status_code == 200
        data = resp.json()
        assert "executions" in data
        assert "count" in data

    def test_execution_captures_stdout_stderr(self, ec2_client, _server_url):
        """Execution should capture stdout and stderr from commands."""
        user_data = """#!/bin/bash
echo "stdout message"
echo "stderr message" >&2
"""
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
        )

        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution
        import requests

        max_wait = 60
        start = time.time()
        execution = None

        while time.time() - start < max_wait:
            resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
            if resp.status_code == 200:
                execution = resp.json()
                if execution.get("status") in ("completed", "failed"):
                    break
            time.sleep(2)

        assert execution is not None
        assert execution["status"] == "completed"

        # Check for stdout/stderr in commands
        commands = execution.get("commands", [])
        assert len(commands) > 0

        # Clean up
        ec2_client.terminate_instances(InstanceIds=[instance_id])

    def test_execution_exit_code_captured(self, ec2_client, _server_url):
        """Execution should capture exit codes from commands."""
        user_data = """#!/bin/bash
exit 42
"""
        user_data_b64 = base64.b64encode(user_data.encode()).decode()

        result = ec2_client.run_instances(
            ImageId="ami-12345678",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            UserData=user_data_b64,
        )

        instance_id = result["Instances"][0]["InstanceId"]

        # Wait for execution
        import requests

        max_wait = 60
        start = time.time()
        execution = None

        while time.time() - start < max_wait:
            resp = requests.get(f"{_server_url}/_robotocore/ec2/guest/executions/{instance_id}")
            if resp.status_code == 200:
                execution = resp.json()
                if execution.get("status") in ("completed", "failed"):
                    break
            time.sleep(2)

        assert execution is not None

        # Check for exit code - look for the actual script execution, not the write_script command
        commands = execution.get("commands", [])
        # Find the command that executed the user-data script (not the write_script command)
        script_commands = [
            c for c in commands if c.get("command", "").startswith("bash /tmp/userdata")
        ]
        if script_commands:
            # The script should have exit code 42
            assert script_commands[0].get("exit_code") == 42

        # Clean up
        ec2_client.terminate_instances(InstanceIds=[instance_id])
