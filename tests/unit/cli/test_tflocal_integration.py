"""Semantic / integration-style tests for tflocal.

These test end-to-end behavior: override generation, env passthrough, cleanup,
and error handling — all without requiring a real terraform binary.
"""

from __future__ import annotations

import json
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "cli", "tflocal"))

from tflocal import (  # noqa: E402
    OVERRIDE_FILENAME,
    TERRAFORM_AWS_SERVICES,
    run_terraform,
)


class TestEndToEndOverrideLifecycle:
    """Test the full lifecycle: generate override -> verify content -> cleanup."""

    def test_override_generated_and_cleaned_up(self, tmp_path):
        """Override file is created for 'plan', then removed after terraform exits."""
        override_path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)

        # Mock terraform binary as 'true' (exits 0)
        with mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"):
            exit_code = run_terraform(
                ["plan"],
                env={"PATH": os.environ.get("PATH", "")},
                working_dir=str(tmp_path),
            )

        assert exit_code == 0
        # Override file should be cleaned up
        assert not os.path.exists(override_path)

    def test_override_not_generated_for_fmt(self, tmp_path):
        """No override file is created for 'fmt'."""
        override_path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)

        with mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"):
            exit_code = run_terraform(
                ["fmt"],
                env={"PATH": os.environ.get("PATH", "")},
                working_dir=str(tmp_path),
            )

        assert exit_code == 0
        assert not os.path.exists(override_path)

    def test_override_content_is_correct(self, tmp_path):
        """When override is generated, it has the correct structure and content."""
        captured_content = {}

        def capture_override(*args, **kwargs):
            # Read the override file while terraform is "running"
            path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)
            if os.path.exists(path):
                with open(path) as f:
                    captured_content.update(json.load(f))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"),
            mock.patch("subprocess.run", side_effect=capture_override),
        ):
            run_terraform(
                ["apply"],
                env={"PATH": os.environ.get("PATH", "")},
                working_dir=str(tmp_path),
            )

        # Verify the override content
        assert "provider" in captured_content
        aws = captured_content["provider"]["aws"]
        assert aws["skip_credentials_validation"] is True
        assert aws["skip_metadata_api_check"] is True
        assert aws["skip_requesting_account_id"] is True
        assert len(aws["endpoints"]) == len(TERRAFORM_AWS_SERVICES)

    def test_override_cleaned_up_even_on_failure(self, tmp_path):
        """Override is cleaned up even if terraform fails."""
        override_path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)

        with mock.patch("tflocal.find_terraform", return_value="/usr/bin/false"):
            exit_code = run_terraform(
                ["plan"],
                env={"PATH": os.environ.get("PATH", "")},
                working_dir=str(tmp_path),
            )

        assert exit_code != 0
        assert not os.path.exists(override_path)


class TestEnvironmentPassthrough:
    """Test that environment variables are correctly passed to terraform."""

    def test_aws_credentials_set_in_env(self, tmp_path):
        """Default AWS credentials are passed to terraform subprocess."""
        captured_env = {}

        def capture_env(*args, **kwargs):
            captured_env.update(kwargs.get("env", {}))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"),
            mock.patch("subprocess.run", side_effect=capture_env),
        ):
            run_terraform(
                ["plan"],
                env={"PATH": "/usr/bin"},
                working_dir=str(tmp_path),
            )

        assert captured_env["AWS_ACCESS_KEY_ID"] == "test"
        assert captured_env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert captured_env["AWS_DEFAULT_REGION"] == "us-east-1"
        assert captured_env["AWS_ENDPOINT_URL"] == "http://localhost:4566"

    def test_existing_credentials_preserved(self, tmp_path):
        """User-provided AWS credentials are not overridden."""
        captured_env = {}

        def capture_env(*args, **kwargs):
            captured_env.update(kwargs.get("env", {}))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"),
            mock.patch("subprocess.run", side_effect=capture_env),
        ):
            run_terraform(
                ["plan"],
                env={
                    "PATH": "/usr/bin",
                    "AWS_ACCESS_KEY_ID": "real-key",
                    "AWS_SECRET_ACCESS_KEY": "real-secret",
                    "AWS_DEFAULT_REGION": "eu-west-1",
                },
                working_dir=str(tmp_path),
            )

        assert captured_env["AWS_ACCESS_KEY_ID"] == "real-key"
        assert captured_env["AWS_SECRET_ACCESS_KEY"] == "real-secret"
        assert captured_env["AWS_DEFAULT_REGION"] == "eu-west-1"

    def test_custom_endpoint_passthrough(self, tmp_path):
        """ROBOTOCORE_ENDPOINT is used as AWS_ENDPOINT_URL."""
        captured_env = {}

        def capture_env(*args, **kwargs):
            captured_env.update(kwargs.get("env", {}))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"),
            mock.patch("subprocess.run", side_effect=capture_env),
        ):
            run_terraform(
                ["plan"],
                env={
                    "PATH": "/usr/bin",
                    "ROBOTOCORE_ENDPOINT": "https://my-robotocore:8080",
                },
                working_dir=str(tmp_path),
            )

        assert captured_env["AWS_ENDPOINT_URL"] == "https://my-robotocore:8080"

    def test_compat_mode_respected(self, tmp_path):
        """TF_COMPAT_MODE=1 adds s3_use_path_style to override."""
        captured_content = {}

        def capture_override(*args, **kwargs):
            path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)
            if os.path.exists(path):
                with open(path) as f:
                    captured_content.update(json.load(f))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/true"),
            mock.patch("subprocess.run", side_effect=capture_override),
        ):
            run_terraform(
                ["apply"],
                env={"PATH": "/usr/bin", "TF_COMPAT_MODE": "1"},
                working_dir=str(tmp_path),
            )

        assert captured_content["provider"]["aws"]["s3_use_path_style"] is True


class TestNoTerraformInstalled:
    """Test behavior when terraform is not installed."""

    def test_returns_127_with_helpful_error(self, tmp_path, capsys):
        """Returns exit code 127 and prints helpful error when terraform not found."""
        with mock.patch("tflocal.find_terraform", return_value=None):
            exit_code = run_terraform(
                ["plan"],
                env={"PATH": ""},
                working_dir=str(tmp_path),
            )

        assert exit_code == 127
        stderr = capsys.readouterr().err
        assert "terraform not found" in stderr.lower()
        assert "Install" in stderr or "install" in stderr

    def test_no_override_created_when_no_terraform(self, tmp_path):
        """No override file is left behind when terraform is missing."""
        override_path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)

        with mock.patch("tflocal.find_terraform", return_value=None):
            run_terraform(
                ["plan"],
                env={"PATH": ""},
                working_dir=str(tmp_path),
            )

        assert not os.path.exists(override_path)


class TestAllArgsPassedThrough:
    """Test that all terraform arguments are forwarded unchanged."""

    def test_args_forwarded_to_subprocess(self, tmp_path):
        """All args after 'tflocal' are passed to terraform."""
        captured_args = []

        def capture_args(*args, **kwargs):
            captured_args.extend(args[0] if args else kwargs.get("args", []))
            return mock.Mock(returncode=0)

        with (
            mock.patch("tflocal.find_terraform", return_value="/usr/bin/terraform"),
            mock.patch("subprocess.run", side_effect=capture_args),
        ):
            run_terraform(
                ["plan", "-out=plan.tfplan", "-var", "foo=bar", "-auto-approve"],
                env={"PATH": "/usr/bin"},
                working_dir=str(tmp_path),
            )

        # First arg is the terraform binary, rest are our args
        assert captured_args[0] == "/usr/bin/terraform"
        assert captured_args[1:] == ["plan", "-out=plan.tfplan", "-var", "foo=bar", "-auto-approve"]
