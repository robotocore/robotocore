"""Unit tests for cdklocal CLI wrapper."""

import os
import sys

# Add cli/cdklocal to path so we can import the module
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "cli", "cdklocal"),
)

from cdklocal_lib import build_command, build_env  # noqa: E402


class TestDefaultEndpointUrl:
    def test_default_endpoint_url(self):
        env = build_env({}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"

    def test_default_uses_http(self):
        env = build_env({}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"].startswith("http://")


class TestCustomHostPort:
    def test_custom_host(self):
        env = build_env({"ROBOTOCORE_HOST": "192.168.1.100"}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "http://192.168.1.100:4566"

    def test_custom_port(self):
        env = build_env({"ROBOTOCORE_PORT": "5555"}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:5555"

    def test_custom_host_and_port(self):
        env = build_env({"ROBOTOCORE_HOST": "myhost", "ROBOTOCORE_PORT": "9999"}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "http://myhost:9999"


class TestCustomEndpointOverride:
    def test_robotocore_endpoint_overrides_host_port(self):
        env = build_env(
            {
                "ROBOTOCORE_ENDPOINT": "https://custom.endpoint:8080",
                "ROBOTOCORE_HOST": "ignored",
                "ROBOTOCORE_PORT": "1111",
            },
            ["deploy"],
        )
        assert env["AWS_ENDPOINT_URL"] == "https://custom.endpoint:8080"

    def test_robotocore_endpoint_used_directly(self):
        env = build_env({"ROBOTOCORE_ENDPOINT": "http://my-robotocore:4566"}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "http://my-robotocore:4566"


class TestCredentials:
    def test_credentials_set_when_absent(self):
        env = build_env({}, ["deploy"])
        assert env["AWS_ACCESS_KEY_ID"] == "test"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"

    def test_credentials_not_overridden_when_present(self):
        env = build_env(
            {
                "AWS_ACCESS_KEY_ID": "my-key",
                "AWS_SECRET_ACCESS_KEY": "my-secret",
            },
            ["deploy"],
        )
        assert env["AWS_ACCESS_KEY_ID"] == "my-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "my-secret"

    def test_partial_credentials_filled(self):
        env = build_env({"AWS_ACCESS_KEY_ID": "my-key"}, ["deploy"])
        assert env["AWS_ACCESS_KEY_ID"] == "my-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"


class TestCdkDefaults:
    def test_cdk_default_account(self):
        env = build_env({}, ["deploy"])
        assert env["CDK_DEFAULT_ACCOUNT"] == "000000000000"

    def test_cdk_default_region(self):
        env = build_env({}, ["deploy"])
        assert env["CDK_DEFAULT_REGION"] == "us-east-1"

    def test_cdk_default_account_not_overridden(self):
        env = build_env({"CDK_DEFAULT_ACCOUNT": "111111111111"}, ["deploy"])
        assert env["CDK_DEFAULT_ACCOUNT"] == "111111111111"

    def test_cdk_default_region_not_overridden(self):
        env = build_env({"CDK_DEFAULT_REGION": "eu-west-1"}, ["deploy"])
        assert env["CDK_DEFAULT_REGION"] == "eu-west-1"


class TestBootstrapCommand:
    def test_bootstrap_gets_extra_config(self):
        cmd = build_command(["bootstrap"])
        assert "bootstrap" in cmd
        assert "--cloudformation-execution-policies" in cmd
        assert (
            "arn:aws:iam::aws:policy/AdministratorAccess"
            in cmd[cmd.index("--cloudformation-execution-policies") + 1]
        )

    def test_bootstrap_sets_cdk_new_bootstrap(self):
        env = build_env({}, ["bootstrap"])
        assert env["CDK_NEW_BOOTSTRAP"] == "1"

    def test_bootstrap_preserves_user_args(self):
        cmd = build_command(["bootstrap", "--profile", "myprofile"])
        assert "--profile" in cmd
        assert "myprofile" in cmd
        assert "bootstrap" in cmd


class TestPassthroughArgs:
    def test_all_args_passed_through(self):
        cmd = build_command(["deploy", "--require-approval", "never", "--verbose"])
        assert cmd[0] == "cdk"
        assert "deploy" in cmd
        assert "--require-approval" in cmd
        assert "never" in cmd
        assert "--verbose" in cmd

    def test_non_bootstrap_commands_no_extra_config(self):
        cmd = build_command(["deploy", "--app", "my-app"])
        assert "--cloudformation-execution-policies" not in cmd

    def test_synth_command(self):
        cmd = build_command(["synth"])
        assert cmd == ["cdk", "synth"]

    def test_destroy_command(self):
        cmd = build_command(["destroy", "--force"])
        assert cmd == ["cdk", "destroy", "--force"]


class TestHttpsEndpoint:
    def test_https_via_robotocore_endpoint(self):
        env = build_env({"ROBOTOCORE_ENDPOINT": "https://secure.local:4566"}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"] == "https://secure.local:4566"

    def test_https_not_default(self):
        env = build_env({}, ["deploy"])
        assert env["AWS_ENDPOINT_URL"].startswith("http://")
        assert not env["AWS_ENDPOINT_URL"].startswith("https://")
