"""Unit tests for pulumilocal Python CLI wrapper."""

import os
import sys

# The module under test
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

# We'll import the module after writing it
import pulumilocal as pl


class TestDefaultEndpoint:
    """Test default endpoint URL construction."""

    def test_default_endpoint_url(self):
        env = pl.build_env({})
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"

    def test_default_uses_localhost_and_4566(self):
        env = pl.build_env({})
        assert "localhost" in env["AWS_ENDPOINT_URL"]
        assert "4566" in env["AWS_ENDPOINT_URL"]


class TestCustomHostPort:
    """Test custom host/port via ROBOTOCORE_HOST and ROBOTOCORE_PORT."""

    def test_custom_host(self):
        env = pl.build_env({"ROBOTOCORE_HOST": "myhost"})
        assert env["AWS_ENDPOINT_URL"] == "http://myhost:4566"

    def test_custom_port(self):
        env = pl.build_env({"ROBOTOCORE_PORT": "5555"})
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:5555"

    def test_custom_host_and_port(self):
        env = pl.build_env({"ROBOTOCORE_HOST": "myhost", "ROBOTOCORE_PORT": "9999"})
        assert env["AWS_ENDPOINT_URL"] == "http://myhost:9999"


class TestCustomEndpointOverride:
    """Test ROBOTOCORE_ENDPOINT overrides host/port."""

    def test_endpoint_overrides_host_port(self):
        env = pl.build_env(
            {
                "ROBOTOCORE_ENDPOINT": "https://custom:8888",
                "ROBOTOCORE_HOST": "ignored",
                "ROBOTOCORE_PORT": "1111",
            }
        )
        assert env["AWS_ENDPOINT_URL"] == "https://custom:8888"

    def test_endpoint_used_directly(self):
        env = pl.build_env({"ROBOTOCORE_ENDPOINT": "http://my-endpoint:4566"})
        assert env["AWS_ENDPOINT_URL"] == "http://my-endpoint:4566"


class TestCredentials:
    """Test credential environment variables."""

    def test_credentials_set_when_absent(self):
        env = pl.build_env({})
        assert env["AWS_ACCESS_KEY_ID"] == "test"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert env["AWS_DEFAULT_REGION"] == "us-east-1"

    def test_credentials_not_overridden_when_present(self):
        env = pl.build_env(
            {
                "AWS_ACCESS_KEY_ID": "mykey",
                "AWS_SECRET_ACCESS_KEY": "mysecret",
                "AWS_DEFAULT_REGION": "eu-west-1",
            }
        )
        assert env["AWS_ACCESS_KEY_ID"] == "mykey"
        assert env["AWS_SECRET_ACCESS_KEY"] == "mysecret"
        assert env["AWS_DEFAULT_REGION"] == "eu-west-1"

    def test_partial_credentials_not_overridden(self):
        env = pl.build_env({"AWS_ACCESS_KEY_ID": "mykey"})
        assert env["AWS_ACCESS_KEY_ID"] == "mykey"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"


class TestPulumiConfigPassphrase:
    """Test PULUMI_CONFIG_PASSPHRASE is set."""

    def test_passphrase_set_when_absent(self):
        env = pl.build_env({})
        assert env["PULUMI_CONFIG_PASSPHRASE"] == "test"

    def test_passphrase_not_overridden_when_present(self):
        env = pl.build_env({"PULUMI_CONFIG_PASSPHRASE": "mysecret"})
        assert env["PULUMI_CONFIG_PASSPHRASE"] == "mysecret"


class TestPulumiBackendUrl:
    """Test PULUMI_BACKEND_URL is set."""

    def test_backend_url_set_when_absent(self):
        env = pl.build_env({})
        assert env["PULUMI_BACKEND_URL"] == "file://~"

    def test_backend_url_not_overridden_when_present(self):
        env = pl.build_env({"PULUMI_BACKEND_URL": "s3://my-bucket"})
        assert env["PULUMI_BACKEND_URL"] == "s3://my-bucket"


class TestArgPassthrough:
    """Test all pulumi arguments are passed through."""

    def test_args_passed_through(self):
        cmd = pl.build_command(["up", "--yes", "--stack", "dev"])
        assert cmd == ["pulumi", "up", "--yes", "--stack", "dev"]

    def test_no_args(self):
        cmd = pl.build_command([])
        assert cmd == ["pulumi"]

    def test_version_flag(self):
        cmd = pl.build_command(["version"])
        assert cmd == ["pulumi", "version"]


class TestUpCommand:
    """Test environment for 'up' command."""

    def test_up_command_has_endpoint(self):
        env = pl.build_env({})
        assert "AWS_ENDPOINT_URL" in env

    def test_up_command_has_skip_flags(self):
        env = pl.build_env({})
        assert env.get("PULUMI_CONFIG_PASSPHRASE") == "test"


class TestPreviewCommand:
    """Test environment for 'preview' command."""

    def test_preview_command_has_endpoint(self):
        env = pl.build_env({})
        assert "AWS_ENDPOINT_URL" in env

    def test_preview_command_has_credentials(self):
        env = pl.build_env({})
        assert "AWS_ACCESS_KEY_ID" in env
        assert "AWS_SECRET_ACCESS_KEY" in env


class TestPulumiConfigGeneration:
    """Test Pulumi provider config generation."""

    def test_skip_credentials_validation(self):
        config = pl.build_pulumi_config("http://localhost:4566")
        assert config["aws:skipCredentialsValidation"] == "true"

    def test_skip_metadata_api_check(self):
        config = pl.build_pulumi_config("http://localhost:4566")
        assert config["aws:skipMetadataApiCheck"] == "true"

    def test_s3_force_path_style(self):
        config = pl.build_pulumi_config("http://localhost:4566")
        assert config["aws:s3UsePathStyle"] == "true"

    def test_endpoints_config(self):
        config = pl.build_pulumi_config("http://localhost:4566")
        assert "aws:endpoints" in config
        # Endpoints should be a JSON array with service endpoint mappings
        import json

        endpoints = json.loads(config["aws:endpoints"])
        # Check that it's a list of dicts with service endpoint pairs
        assert isinstance(endpoints, list)
        assert len(endpoints) > 0
        # Each entry should map a service to the endpoint
        first = endpoints[0]
        assert any(v == "http://localhost:4566" for v in first.values())

    def test_custom_endpoint_in_config(self):
        config = pl.build_pulumi_config("http://myhost:9999")
        import json

        endpoints = json.loads(config["aws:endpoints"])
        first = endpoints[0]
        assert any(v == "http://myhost:9999" for v in first.values())
