"""Semantic/integration tests for pulumilocal CLI wrapper."""

import os
import sys
from unittest import mock

import pytest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "cli",
        "pulumilocal",
    ),
)

import pulumilocal as pl


class TestEndToEndEnvironment:
    """Test end-to-end: full environment setup is correct."""

    def test_full_environment_setup(self):
        """Verify that build_env with empty base produces a complete, correct environment."""
        env = pl.build_env({})
        # All required keys are present
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"
        assert env["AWS_ACCESS_KEY_ID"] == "test"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert env["AWS_DEFAULT_REGION"] == "us-east-1"
        assert env["PULUMI_CONFIG_PASSPHRASE"] == "test"
        assert env["PULUMI_BACKEND_URL"] == "file://~"

    def test_full_environment_preserves_existing(self):
        """Verify that user-set values are never overridden."""
        base = {
            "AWS_ACCESS_KEY_ID": "prod-key",
            "AWS_SECRET_ACCESS_KEY": "prod-secret",
            "AWS_DEFAULT_REGION": "ap-southeast-1",
            "PULUMI_CONFIG_PASSPHRASE": "supersecret",
            "PULUMI_BACKEND_URL": "s3://my-state-bucket",
            "ROBOTOCORE_ENDPOINT": "https://remote:8080",
        }
        env = pl.build_env(base)
        assert env["AWS_ACCESS_KEY_ID"] == "prod-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "prod-secret"
        assert env["AWS_DEFAULT_REGION"] == "ap-southeast-1"
        assert env["PULUMI_CONFIG_PASSPHRASE"] == "supersecret"
        assert env["PULUMI_BACKEND_URL"] == "s3://my-state-bucket"
        assert env["AWS_ENDPOINT_URL"] == "https://remote:8080"

    def test_build_command_and_env_together(self):
        """Verify command + env are both correct for a typical invocation."""
        cmd = pl.build_command(["up", "--yes"])
        env = pl.build_env({})
        assert cmd == ["pulumi", "up", "--yes"]
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"
        assert env["PULUMI_CONFIG_PASSPHRASE"] == "test"


class TestPulumiNotInstalled:
    """Test helpful error when pulumi is not installed."""

    def test_helpful_error_when_pulumi_not_found(self):
        """check_pulumi_installed raises a clear error when pulumi is missing."""
        with mock.patch("shutil.which", return_value=None):
            with pytest.raises(SystemExit) as exc_info:
                pl.check_pulumi_installed()
            # The exit should be non-zero
            assert exc_info.value.code != 0

    def test_no_error_when_pulumi_found(self):
        """check_pulumi_installed succeeds when pulumi is on PATH."""
        with mock.patch("shutil.which", return_value="/usr/local/bin/pulumi"):
            # Should not raise
            pl.check_pulumi_installed()


class TestBashScriptExists:
    """Verify the bash script exists and is executable."""

    def test_bash_script_exists(self):
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "cli",
            "pulumilocal",
            "pulumilocal",
        )
        assert os.path.exists(script_path), f"Bash script not found at {script_path}"

    def test_bash_script_is_executable(self):
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "cli",
            "pulumilocal",
            "pulumilocal",
        )
        assert os.access(script_path, os.X_OK), "Bash script is not executable"

    def test_bash_script_has_shebang(self):
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "cli",
            "pulumilocal",
            "pulumilocal",
        )
        with open(script_path) as f:
            first_line = f.readline()
        assert first_line.startswith("#!/"), "Bash script missing shebang"
