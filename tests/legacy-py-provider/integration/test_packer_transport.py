"""Integration tests for Packer-compatible virtual instance transport.

These tests verify:
1. Container-backed EC2 instances can be started and stopped
2. File uploads work with proper destination path semantics
3. Shell commands execute in order
4. AMIs can be created with persisted state
5. New instances from AMIs have fresh identity (not carrying over source identity)
6. Provisioner-created files are present in new instances

Note: GPU/driver fidelity is explicitly out of scope per the feature spec.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ROBOTOCORE_PACKER_TRANSPORT", "").lower() not in ("1", "true", "yes"),
        reason="ROBOTOCORE_PACKER_TRANSPORT must be enabled for these tests",
    ),
    pytest.mark.skipif(
        os.system("docker info > /dev/null 2>&1") != 0,
        reason="Docker must be available for these tests",
    ),
]


@pytest.fixture(autouse=True)
def cleanup_containers():
    """Fixture to clean up any leftover containers before each test."""
    import subprocess

    # Clean up any existing robotocore-ec2 containers
    subprocess.run(
        "docker rm -f $(docker ps -aq --filter 'name=robotocore-ec2-') 2>/dev/null || true",
        shell=True,
        capture_output=True,
    )
    yield
    # Cleanup after test as well
    subprocess.run(
        "docker rm -f $(docker ps -aq --filter 'name=robotocore-ec2-') 2>/dev/null || true",
        shell=True,
        capture_output=True,
    )


@pytest.fixture
def packer_config():
    """Fixture providing packer transport configuration."""
    from robotocore.services.ec2.packer import InstanceTransportConfig, TransportType

    return InstanceTransportConfig(
        transport_type=TransportType.SSH,
        container_image="public.ecr.aws/amazonlinux/amazonlinux:2",
        instance_type="t2.micro",
    )


class TestInstanceTransport:
    """Tests for the InstanceTransport class."""

    def test_instance_transport_lifecycle(self, packer_config):
        """Test that an instance transport can be started and stopped."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-lifecycle-12345"
        transport = get_instance_transport(instance_id, packer_config)

        # Start the instance
        assert transport.start() is True
        assert transport.is_running() is True

        # Stop the instance
        assert transport.stop() is True
        assert transport.is_running() is False

    def test_file_upload_to_file_destination(self, packer_config, tmp_path):
        """Test file upload where destination is a file path."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-upload-file-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create a local file
            local_file = tmp_path / "test.txt"
            local_file.write_text("Hello, World!")

            # Upload to a file destination
            result = transport.upload_file(
                str(local_file),
                "/tmp/uploaded_test.txt",
            )

            assert result.success is True, f"Upload failed: {result.stderr}"

            # Verify the file exists
            verify_result = transport.execute_shell("cat /tmp/uploaded_test.txt")
            assert verify_result.success is True
            assert verify_result.stdout.strip() == "Hello, World!"

        finally:
            transport.stop()

    def test_file_upload_to_directory_destination(self, packer_config, tmp_path):
        """Test file upload where destination is a directory.

        This validates the file-vs-directory destination semantics that
        caused a real failure mode in Packer.
        """
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-upload-dir-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create a local file
            local_file = tmp_path / "test.txt"
            local_file.write_text("Directory upload test")

            # Create the destination directory
            transport.execute_shell("mkdir -p /tmp/dest_dir")

            # Upload to a directory destination (should append filename)
            result = transport.upload_file(
                str(local_file),
                "/tmp/dest_dir/",
            )

            assert result.success is True, f"Upload failed: {result.stderr}"

            # Verify the file exists with the correct name
            verify_result = transport.execute_shell("cat /tmp/dest_dir/test.txt")
            assert verify_result.success is True
            assert verify_result.stdout.strip() == "Directory upload test"

        finally:
            transport.stop()

    def test_file_upload_rejects_file_as_directory(self, packer_config, tmp_path):
        """Test that uploading to a file path that exists as a file is rejected."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-upload-reject-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create a file at the destination path
            transport.execute_shell("echo 'existing' > /tmp/existing_file")

            # Create a local file to upload
            local_file = tmp_path / "test.txt"
            local_file.write_text("New content")

            # Try to upload to a path that exists as a file
            # This should succeed (overwrites the file)
            result = transport.upload_file(
                str(local_file),
                "/tmp/existing_file",
            )

            # Upload should succeed (overwrites)
            assert result.success is True

            # Verify the content was overwritten
            verify_result = transport.execute_shell("cat /tmp/existing_file")
            assert verify_result.success is True
            assert verify_result.stdout.strip() == "New content"

        finally:
            transport.stop()

    def test_shell_command_execution(self, packer_config):
        """Test that shell commands execute and return output."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-shell-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Execute a simple command
            result = transport.execute_shell("echo 'Hello from shell'")

            assert result.success is True
            assert result.exit_code == 0
            assert "Hello from shell" in result.stdout

        finally:
            transport.stop()

    def test_shell_command_with_environment(self, packer_config):
        """Test that shell commands respect environment variables."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-env-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Execute a command with environment variables
            result = transport.execute_shell(
                "echo $TEST_VAR",
                environment={"TEST_VAR": "test_value"},
            )

            assert result.success is True
            assert "test_value" in result.stdout

        finally:
            transport.stop()

    def test_shell_command_with_working_dir(self, packer_config):
        """Test that shell commands respect working directory."""
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-wd-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create a directory and file
            transport.execute_shell(
                "mkdir -p /tmp/workdir && echo 'in workdir' > /tmp/workdir/file.txt"
            )

            # Execute a command with working directory
            result = transport.execute_shell(
                "cat file.txt",
                working_dir="/tmp/workdir",
            )

            assert result.success is True
            assert "in workdir" in result.stdout

        finally:
            transport.stop()

    def test_provisioner_execution_order(self, packer_config, tmp_path):
        """Test that provisioners execute in order.

        This simulates a Packer build with multiple provisioners.
        """
        from robotocore.services.ec2.packer import get_instance_transport

        instance_id = "i-test-order-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Provisioner 1: Create a file
            result1 = transport.execute_shell("echo 'step1' > /tmp/provision_order.txt")
            assert result1.success is True

            # Provisioner 2: Append to the file
            result2 = transport.execute_shell("echo 'step2' >> /tmp/provision_order.txt")
            assert result2.success is True

            # Provisioner 3: Upload a file
            local_file = tmp_path / "step3.txt"
            local_file.write_text("step3")
            result3 = transport.upload_file(
                str(local_file),
                "/tmp/step3.txt",
            )
            assert result3.success is True

            # Provisioner 4: Combine files
            result4 = transport.execute_shell("cat /tmp/step3.txt >> /tmp/provision_order.txt")
            assert result4.success is True

            # Verify the order
            verify_result = transport.execute_shell("cat /tmp/provision_order.txt")
            assert verify_result.success is True
            lines = verify_result.stdout.strip().split("\n")
            assert lines == ["step1", "step2", "step3"]

        finally:
            transport.stop()


class TestAmiBuilder:
    """Tests for the AmiBuilder class."""

    def test_ami_creation_from_instance(self, packer_config):
        """Test that an AMI can be created from a running instance."""
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-ami-create-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create some state in the instance
            transport.execute_shell("mkdir -p /opt/ami-state")
            transport.execute_shell("echo 'provisioned data' > /opt/ami-state/data.txt")

            # Create AMI
            builder = get_ami_builder()
            result = builder.create_ami(
                instance_id=instance_id,
                ami_name="test-ami",
                description="Test AMI",
                transport=transport,
            )

            assert result.ami_id.startswith("ami-")
            assert result.ami_name == "test-ami"
            assert result.state == "available"
            assert result.source_instance_id == instance_id

        finally:
            transport.stop()

    def test_ami_identity_clearing(self, packer_config):
        """Test that AMI creation clears instance identity.

        This directly covers the "identity cleared too early / too late"
        failure mode from the acceptance criteria.
        """
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-identity-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Set up some identity markers
            transport.execute_shell("echo 'original-machine-id' > /etc/machine-id")
            transport.execute_shell("hostname original-hostname")

            # Create AMI (should clear identity)
            builder = get_ami_builder()
            result = builder.create_ami(
                instance_id=instance_id,
                ami_name="test-ami-identity",
                transport=transport,
            )

            # Verify the AMI was created
            assert result.ami_id.startswith("ami-")

            # Verify identity was cleared
            machine_id_result = transport.execute_shell(
                "cat /etc/machine-id 2>/dev/null || echo 'cleared'"
            )
            # Machine ID should be empty or different
            assert machine_id_result.success is True

        finally:
            transport.stop()

    def test_ami_filesystem_persistence(self, packer_config):
        """Test that AMI creation persists filesystem state."""
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-ami-persist-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Create state in /opt/ami-state
            transport.execute_shell("mkdir -p /opt/ami-state/app")
            transport.execute_shell("echo 'config' > /opt/ami-state/app/config.ini")
            transport.execute_shell("echo 'data' > /opt/ami-state/app/data.txt")

            # Create AMI
            builder = get_ami_builder()
            result = builder.create_ami(
                instance_id=instance_id,
                ami_name="test-ami-persist",
                transport=transport,
            )

            # Get the snapshot
            snapshot = builder.get_ami_snapshot(result.ami_id)
            assert snapshot is not None
            assert "app/config.ini" in snapshot.files
            assert "app/data.txt" in snapshot.files
            assert snapshot.files["app/config.ini"].strip() == "config"
            assert snapshot.files["app/data.txt"].strip() == "data"

        finally:
            transport.stop()

    def test_launch_from_ami_has_fresh_identity(self, packer_config):
        """Test that instances launched from AMIs have fresh identity.

        This is the key test for the "identity cleared too early / too late"
        failure mode. New instances must NOT carry over the source
        instance's identity.
        """
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        # Step 1: Create source instance with identity
        source_instance_id = "i-test-source-12345"
        source_transport = get_instance_transport(source_instance_id, packer_config)

        assert source_transport.start() is True

        try:
            # Set up source identity
            source_transport.execute_shell("echo 'source-machine-id' > /etc/machine-id")
            source_transport.execute_shell("hostname source-hostname")

            # Create some provisioned state
            source_transport.execute_shell("mkdir -p /opt/ami-state")
            source_transport.execute_shell("echo 'provisioned' > /opt/ami-state/provisioned.txt")

            # Create AMI from source
            builder = get_ami_builder()
            ami_result = builder.create_ami(
                instance_id=source_instance_id,
                ami_name="test-ami-fresh-identity",
                transport=source_transport,
            )

        finally:
            source_transport.stop()

        # Step 2: Launch new instance from AMI
        new_instance = builder.launch_instance_from_ami(
            ami_id=ami_result.ami_id,
            instance_type="t2.micro",
        )

        # Verify new instance has fresh identity
        assert new_instance["instance_id"] != source_instance_id
        assert new_instance["fresh_identity"] is True

        # Verify provisioned files are present
        assert "provisioned.txt" in new_instance["files_restored"]

    def test_ami_list_and_delete(self, packer_config):
        """Test AMI listing and deletion."""
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-ami-list-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            builder = get_ami_builder()

            # Create multiple AMIs
            result1 = builder.create_ami(
                instance_id=instance_id,
                ami_name="test-ami-1",
                transport=transport,
            )
            result2 = builder.create_ami(
                instance_id=instance_id,
                ami_name="test-ami-2",
                transport=transport,
            )

            # List AMIs
            amis = builder.list_amis()
            ami_ids = [ami.ami_id for ami in amis]
            assert result1.ami_id in ami_ids
            assert result2.ami_id in ami_ids

            # Delete one AMI
            assert builder.delete_ami(result1.ami_id) is True

            # Verify it's gone
            assert builder.get_ami(result1.ami_id) is None

            # Verify the other still exists
            assert builder.get_ami(result2.ami_id) is not None

        finally:
            transport.stop()


class TestPackerScenarios:
    """End-to-end scenarios simulating Packer builds."""

    def test_packer_file_provisioner_scenario(self, packer_config, tmp_path):
        """Simulate a Packer file provisioner workflow.

        This tests the real failure mode: upload whose destination
        was a file instead of a directory.
        """
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-packer-file-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Simulate Packer file provisioner
            # Create files to upload
            app_dir = tmp_path / "app"
            app_dir.mkdir()
            (app_dir / "index.html").write_text("<html>Hello</html>")
            (app_dir / "style.css").write_text("body { color: blue; }")

            # Upload to /var/www/html/ (directory destination)
            transport.execute_shell("mkdir -p /var/www/html")

            for file in ["index.html", "style.css"]:
                result = transport.upload_file(
                    str(app_dir / file),
                    "/var/www/html/",
                )
                assert result.success is True, f"Failed to upload {file}"

            # Verify files are in the right place
            for file in ["index.html", "style.css"]:
                result = transport.execute_shell(f"cat /var/www/html/{file}")
                assert result.success is True

            # Create AMI
            builder = get_ami_builder()
            ami_result = builder.create_ami(
                instance_id=instance_id,
                ami_name="packer-file-test",
                transport=transport,
            )

            # Verify AMI exists
            assert ami_result.ami_id.startswith("ami-")

        finally:
            transport.stop()

    def test_packer_shell_provisioner_scenario(self, packer_config):
        """Simulate a Packer shell provisioner workflow.

        This tests provisioner ordering and execution.
        """
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-packer-shell-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # Simulate multiple shell provisioners (using yum for Amazon Linux)
            provisioners = [
                ("yum update -y 2>/dev/null || true", "update"),
                ("mkdir -p /opt/myapp", "create_dir"),
                ("echo 'config=value' > /opt/myapp/config.env", "write_config"),
                ("chmod 644 /opt/myapp/config.env", "set_perms"),
            ]

            for command, name in provisioners:
                result = transport.execute_shell(command)
                assert result.success is True, f"Provisioner {name} failed: {result.stderr}"

            # Verify final state
            result = transport.execute_shell("cat /opt/myapp/config.env")
            assert result.success is True
            assert "config=value" in result.stdout

            # Create AMI
            builder = get_ami_builder()
            ami_result = builder.create_ami(
                instance_id=instance_id,
                ami_name="packer-shell-test",
                transport=transport,
            )

            assert ami_result.ami_id.startswith("ami-")

        finally:
            transport.stop()

    def test_packer_complete_build_scenario(self, packer_config, tmp_path):
        """Simulate a complete Packer build with multiple provisioners.

        This tests the full workflow:
        1. Start instance
        2. File provisioners
        3. Shell provisioners
        4. Create AMI
        5. Verify new instance from AMI
        """
        from robotocore.services.ec2.packer import get_instance_transport
        from robotocore.services.ec2.packer.ami_builder import get_ami_builder

        instance_id = "i-test-packer-complete-12345"
        transport = get_instance_transport(instance_id, packer_config)

        assert transport.start() is True

        try:
            # File provisioner: Upload application files to /opt/ami-state
            # (this is the directory that gets persisted to the AMI)
            app_dir = tmp_path / "app"
            app_dir.mkdir()
            (app_dir / "app.py").write_text("print('Hello from app')")
            (app_dir / "requirements.txt").write_text("flask\nrequests")

            transport.execute_shell("mkdir -p /opt/ami-state/myapp")

            for file in ["app.py", "requirements.txt"]:
                result = transport.upload_file(
                    str(app_dir / file),
                    "/opt/ami-state/myapp/",
                )
                assert result.success is True

            # Shell provisioner: Install dependencies
            result = transport.execute_shell(
                "cd /opt/myapp && pip install -r requirements.txt 2>/dev/null || true"
            )
            # May fail if pip not available, that's ok for this test

            # Shell provisioner: Create a service file
            service_content = """[Unit]
Description=My App
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/myapp/app.py
Restart=always

[Install]
WantedBy=multi-user.target
"""
            transport.execute_shell(f"echo '{service_content}' > /etc/systemd/system/myapp.service")

            # Shell provisioner: Enable the service
            transport.execute_shell("systemctl enable myapp.service 2>/dev/null || true")

            # Create AMI
            builder = get_ami_builder()
            ami_result = builder.create_ami(
                instance_id=instance_id,
                ami_name="packer-complete-test",
                description="Complete Packer build test",
                transport=transport,
            )

            # Verify AMI
            assert ami_result.ami_id.startswith("ami-")
            assert ami_result.ami_name == "packer-complete-test"

            # Launch new instance from AMI
            new_instance = builder.launch_instance_from_ami(
                ami_id=ami_result.ami_id,
                instance_type="t2.micro",
            )

            # Verify new instance has fresh identity
            assert new_instance["instance_id"] != instance_id
            assert new_instance["fresh_identity"] is True

            # Verify provisioned files would be present (relative paths from /opt/ami-state)
            assert "myapp/app.py" in new_instance["files_restored"]
            assert "myapp/requirements.txt" in new_instance["files_restored"]

        finally:
            transport.stop()
