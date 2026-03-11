"""Unit tests for tflocal — Terraform CLI wrapper for robotocore."""

from __future__ import annotations

import json
import os
import sys

import pytest

# Add cli/ to path so we can import tflocal
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "cli", "tflocal"))

from tflocal import (  # noqa: E402
    OVERRIDE_FILENAME,
    TERRAFORM_AWS_SERVICES,
    build_provider_override,
    cleanup_override_file,
    get_aws_env,
    get_endpoint_url,
    should_generate_override,
    write_override_file,
)


class TestGetEndpointUrl:
    """Test endpoint URL construction from env vars."""

    def test_default_endpoint(self):
        """Default is http://localhost:4566."""
        url = get_endpoint_url(env={})
        assert url == "http://localhost:4566"

    def test_custom_host(self):
        """ROBOTOCORE_HOST overrides host."""
        url = get_endpoint_url(env={"ROBOTOCORE_HOST": "192.168.1.10"})
        assert url == "http://192.168.1.10:4566"

    def test_custom_port(self):
        """ROBOTOCORE_PORT overrides port."""
        url = get_endpoint_url(env={"ROBOTOCORE_PORT": "5555"})
        assert url == "http://localhost:5555"

    def test_custom_host_and_port(self):
        """Both ROBOTOCORE_HOST and ROBOTOCORE_PORT work together."""
        url = get_endpoint_url(env={"ROBOTOCORE_HOST": "myhost", "ROBOTOCORE_PORT": "9999"})
        assert url == "http://myhost:9999"

    def test_robotocore_endpoint_overrides_host_port(self):
        """ROBOTOCORE_ENDPOINT takes precedence over HOST/PORT."""
        url = get_endpoint_url(
            env={
                "ROBOTOCORE_ENDPOINT": "https://custom.endpoint:8080",
                "ROBOTOCORE_HOST": "ignored",
                "ROBOTOCORE_PORT": "1111",
            }
        )
        assert url == "https://custom.endpoint:8080"

    def test_robotocore_endpoint_trailing_slash_stripped(self):
        """Trailing slash is stripped from ROBOTOCORE_ENDPOINT."""
        url = get_endpoint_url(env={"ROBOTOCORE_ENDPOINT": "http://localhost:4566/"})
        assert url == "http://localhost:4566"


class TestGetAwsEnv:
    """Test AWS credential environment variable defaults."""

    def test_defaults_set_when_empty(self):
        """Default credentials set when env is empty."""
        result = get_aws_env(env={})
        assert result["AWS_ACCESS_KEY_ID"] == "test"
        assert result["AWS_SECRET_ACCESS_KEY"] == "test"
        assert result["AWS_DEFAULT_REGION"] == "us-east-1"

    def test_access_key_not_overridden(self):
        """Existing AWS_ACCESS_KEY_ID is not overridden."""
        result = get_aws_env(env={"AWS_ACCESS_KEY_ID": "my-real-key"})
        assert "AWS_ACCESS_KEY_ID" not in result

    def test_secret_key_not_overridden(self):
        """Existing AWS_SECRET_ACCESS_KEY is not overridden."""
        result = get_aws_env(env={"AWS_SECRET_ACCESS_KEY": "my-real-secret"})
        assert "AWS_SECRET_ACCESS_KEY" not in result

    def test_region_not_overridden(self):
        """Existing AWS_DEFAULT_REGION is not overridden."""
        result = get_aws_env(env={"AWS_DEFAULT_REGION": "eu-west-1"})
        assert "AWS_DEFAULT_REGION" not in result

    def test_partial_override(self):
        """Only missing vars get defaults; existing vars are left alone."""
        result = get_aws_env(env={"AWS_ACCESS_KEY_ID": "custom"})
        assert "AWS_ACCESS_KEY_ID" not in result
        assert result["AWS_SECRET_ACCESS_KEY"] == "test"
        assert result["AWS_DEFAULT_REGION"] == "us-east-1"


class TestBuildProviderOverride:
    """Test provider override configuration generation."""

    def test_basic_structure(self):
        """Override has provider.aws with skip_* settings."""
        override = build_provider_override("http://localhost:4566")
        aws = override["provider"]["aws"]
        assert aws["skip_credentials_validation"] is True
        assert aws["skip_metadata_api_check"] is True
        assert aws["skip_requesting_account_id"] is True

    def test_endpoints_present(self):
        """All service endpoints are in the override."""
        override = build_provider_override("http://localhost:4566")
        endpoints = override["provider"]["aws"]["endpoints"]
        assert len(endpoints) == len(TERRAFORM_AWS_SERVICES)
        for svc in TERRAFORM_AWS_SERVICES:
            assert endpoints[svc] == "http://localhost:4566"

    def test_custom_endpoint(self):
        """Custom endpoint URL propagates to all services."""
        override = build_provider_override("http://myhost:9999")
        endpoints = override["provider"]["aws"]["endpoints"]
        for svc in TERRAFORM_AWS_SERVICES:
            assert endpoints[svc] == "http://myhost:9999"

    def test_credentials_in_override(self):
        """Override includes access_key and secret_key."""
        override = build_provider_override("http://localhost:4566")
        aws = override["provider"]["aws"]
        assert aws["access_key"] == "test"
        assert aws["secret_key"] == "test"
        assert aws["region"] == "us-east-1"

    def test_compat_mode_off(self):
        """Without compat mode, s3_use_path_style is not set."""
        override = build_provider_override("http://localhost:4566", compat_mode=False)
        aws = override["provider"]["aws"]
        assert "s3_use_path_style" not in aws

    def test_compat_mode_on(self):
        """With compat mode, s3_use_path_style is True."""
        override = build_provider_override("http://localhost:4566", compat_mode=True)
        aws = override["provider"]["aws"]
        assert aws["s3_use_path_style"] is True

    def test_well_known_services_present(self):
        """Key Terraform services are in the service list."""
        for svc in ["s3", "ec2", "iam", "lambda", "dynamodb", "sqs", "sns", "sts"]:
            assert svc in TERRAFORM_AWS_SERVICES, f"{svc} missing from TERRAFORM_AWS_SERVICES"


class TestShouldGenerateOverride:
    """Test which terraform commands get provider overrides."""

    @pytest.mark.parametrize("cmd", ["init", "plan", "apply", "destroy", "import", "refresh"])
    def test_override_generated_for_infra_commands(self, cmd: str):
        """Infrastructure commands need the override."""
        assert should_generate_override([cmd]) is True

    @pytest.mark.parametrize("cmd", ["fmt", "validate", "version"])
    def test_no_override_for_non_infra_commands(self, cmd: str):
        """Non-infrastructure commands do not need overrides."""
        assert should_generate_override([cmd]) is False

    def test_args_with_flags_before_command(self):
        """Flags before the command are skipped."""
        assert should_generate_override(["-chdir=./infra", "plan"]) is True

    def test_no_args(self):
        """No arguments means no override."""
        assert should_generate_override([]) is False

    def test_only_flags(self):
        """Only flags and no subcommand means no override."""
        assert should_generate_override(["-help"]) is False


class TestWriteAndCleanupOverride:
    """Test writing and cleaning up the override file."""

    def test_write_override_file(self, tmp_path):
        """Override file is written as valid JSON."""
        path = write_override_file(str(tmp_path), "http://localhost:4566")
        assert os.path.exists(path)
        assert path.endswith(OVERRIDE_FILENAME)
        with open(path) as f:
            data = json.load(f)
        assert "provider" in data
        assert "aws" in data["provider"]

    def test_cleanup_removes_file(self, tmp_path):
        """Cleanup removes the override file."""
        write_override_file(str(tmp_path), "http://localhost:4566")
        override_path = os.path.join(str(tmp_path), OVERRIDE_FILENAME)
        assert os.path.exists(override_path)
        cleanup_override_file(str(tmp_path))
        assert not os.path.exists(override_path)

    def test_cleanup_noop_when_missing(self, tmp_path):
        """Cleanup doesn't raise when file doesn't exist."""
        cleanup_override_file(str(tmp_path))  # should not raise

    def test_written_file_has_all_services(self, tmp_path):
        """Written file contains all service endpoints."""
        write_override_file(str(tmp_path), "http://localhost:4566")
        with open(os.path.join(str(tmp_path), OVERRIDE_FILENAME)) as f:
            data = json.load(f)
        endpoints = data["provider"]["aws"]["endpoints"]
        assert len(endpoints) == len(TERRAFORM_AWS_SERVICES)

    def test_written_file_compat_mode(self, tmp_path):
        """Written file includes compat mode settings."""
        write_override_file(str(tmp_path), "http://localhost:4566", compat_mode=True)
        with open(os.path.join(str(tmp_path), OVERRIDE_FILENAME)) as f:
            data = json.load(f)
        assert data["provider"]["aws"]["s3_use_path_style"] is True


class TestTerraformArgsPassthrough:
    """Test that terraform arguments are passed through unchanged."""

    def test_should_generate_preserves_all_args(self):
        """should_generate_override only inspects, doesn't modify args."""
        args = ["plan", "-out=plan.tfplan", "-var", "foo=bar"]
        original = list(args)
        should_generate_override(args)
        assert args == original

    def test_command_detection_with_complex_args(self):
        """Complex argument patterns still detect the subcommand."""
        assert should_generate_override(["-chdir=/opt/infra", "apply", "-auto-approve"]) is True
        assert should_generate_override(["-chdir=/opt/infra", "fmt", "-recursive"]) is False
