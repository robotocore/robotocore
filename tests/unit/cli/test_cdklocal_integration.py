"""Semantic/integration tests for cdklocal CLI wrapper."""

import os
import subprocess
import sys

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "cli", "cdklocal"),
)

from cdklocal_lib import build_command, build_env  # noqa: E402


class TestEndToEndDeployEnvironment:
    """Test that the full environment setup is correct for a deploy command."""

    def test_deploy_environment_complete(self):
        env = build_env({}, ["deploy"])
        # All required env vars must be set
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"
        assert env["AWS_ACCESS_KEY_ID"] == "test"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert env["CDK_DEFAULT_ACCOUNT"] == "000000000000"
        assert env["CDK_DEFAULT_REGION"] == "us-east-1"

    def test_deploy_command_passthrough(self):
        cmd = build_command(["deploy", "--all", "--require-approval", "never"])
        assert cmd == ["cdk", "deploy", "--all", "--require-approval", "never"]

    def test_deploy_no_bootstrap_flags(self):
        env = build_env({}, ["deploy"])
        assert "CDK_NEW_BOOTSTRAP" not in env


class TestEndToEndBootstrap:
    """Test that bootstrap gets the correct flags and environment."""

    def test_bootstrap_full_environment(self):
        env = build_env({}, ["bootstrap"])
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"
        assert env["AWS_ACCESS_KEY_ID"] == "test"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert env["CDK_DEFAULT_ACCOUNT"] == "000000000000"
        assert env["CDK_DEFAULT_REGION"] == "us-east-1"
        assert env["CDK_NEW_BOOTSTRAP"] == "1"

    def test_bootstrap_command_flags(self):
        cmd = build_command(["bootstrap"])
        assert cmd[0] == "cdk"
        assert "bootstrap" in cmd
        assert "--cloudformation-execution-policies" in cmd
        idx = cmd.index("--cloudformation-execution-policies")
        assert cmd[idx + 1] == "arn:aws:iam::aws:policy/AdministratorAccess"

    def test_bootstrap_with_qualifier(self):
        cmd = build_command(["bootstrap", "--qualifier", "myqual"])
        assert "--qualifier" in cmd
        assert "myqual" in cmd
        assert "--cloudformation-execution-policies" in cmd


class TestCdkNotInstalled:
    """Test helpful error when cdk is not installed."""

    def test_helpful_error_when_cdk_missing(self):
        """Run the Python wrapper with a PATH that excludes cdk."""
        script_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "cli", "cdklocal")
        script_path = os.path.join(script_dir, "cdklocal.py")

        # Run with an empty PATH so cdk won't be found
        result = subprocess.run(
            [sys.executable, script_path, "deploy"],
            capture_output=True,
            text=True,
            env={
                "PATH": "/nonexistent",
                "HOME": os.environ.get("HOME", "/tmp"),
            },
        )
        assert result.returncode != 0
        assert "cdk" in result.stderr.lower()
