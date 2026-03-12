"""Tests for the awsroboto CLI wrapper."""

import os
from unittest.mock import patch

import pytest

from robotocore.awsroboto import _find_aws_cli, _parse_wrapper_args, build_env

# ---------------------------------------------------------------------------
# _find_aws_cli
# ---------------------------------------------------------------------------


def test_find_aws_cli_found() -> None:
    with patch("robotocore.awsroboto.shutil.which", return_value="/usr/local/bin/aws"):
        assert _find_aws_cli() == "/usr/local/bin/aws"


def test_find_aws_cli_missing(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("robotocore.awsroboto.shutil.which", return_value=None):
        with pytest.raises(SystemExit) as exc_info:
            _find_aws_cli()
        assert exc_info.value.code == 127
        err = capsys.readouterr().err
        assert "aws" in err
        assert "not installed" in err


# ---------------------------------------------------------------------------
# _parse_wrapper_args
# ---------------------------------------------------------------------------


class TestParseWrapperArgs:
    def test_no_wrapper_flags(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(["s3", "ls"])
        assert endpoint is None
        assert port is None
        assert rest == ["s3", "ls"]

    def test_endpoint_url_space(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(
            ["--endpoint-url", "http://other:9999", "s3", "ls"]
        )
        assert endpoint == "http://other:9999"
        assert port is None
        assert rest == ["s3", "ls"]

    def test_endpoint_url_equals(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(["--endpoint-url=http://other:9999", "s3", "ls"])
        assert endpoint == "http://other:9999"
        assert rest == ["s3", "ls"]

    def test_port_space(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(["--port", "5555", "sqs", "list-queues"])
        assert endpoint is None
        assert port == "5555"
        assert rest == ["sqs", "list-queues"]

    def test_port_equals(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(["--port=5555", "sqs", "list-queues"])
        assert port == "5555"
        assert rest == ["sqs", "list-queues"]

    def test_empty_argv(self) -> None:
        endpoint, port, rest = _parse_wrapper_args([])
        assert endpoint is None
        assert port is None
        assert rest == []

    def test_both_flags(self) -> None:
        endpoint, port, rest = _parse_wrapper_args(
            ["--endpoint-url", "http://x:1", "--port", "2", "iam", "list-users"]
        )
        assert endpoint == "http://x:1"
        assert port == "2"
        assert rest == ["iam", "list-users"]


# ---------------------------------------------------------------------------
# build_env
# ---------------------------------------------------------------------------


class TestBuildEnv:
    def test_defaults(self) -> None:
        clean = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        clean.pop("ROBOTOCORE_PORT", None)
        with patch.dict(os.environ, clean, clear=True):
            env = build_env()
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:4566"
        assert env["AWS_ACCESS_KEY_ID"] == "123456789012"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test"
        assert env["AWS_DEFAULT_REGION"] == "us-east-1"

    def test_robotocore_port_env(self) -> None:
        clean = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        clean["ROBOTOCORE_PORT"] = "7777"
        with patch.dict(os.environ, clean, clear=True):
            env = build_env()
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:7777"

    def test_explicit_port_overrides_env(self) -> None:
        with patch.dict(os.environ, {"ROBOTOCORE_PORT": "7777"}, clear=False):
            env = build_env(port="8888")
        assert env["AWS_ENDPOINT_URL"] == "http://localhost:8888"

    def test_explicit_endpoint_url(self) -> None:
        env = build_env(endpoint_url="http://custom:1234")
        assert env["AWS_ENDPOINT_URL"] == "http://custom:1234"

    def test_endpoint_url_takes_precedence_over_port(self) -> None:
        env = build_env(endpoint_url="http://custom:1234", port="9999")
        assert env["AWS_ENDPOINT_URL"] == "http://custom:1234"

    def test_existing_aws_vars_preserved(self) -> None:
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "my-key",
                "AWS_SECRET_ACCESS_KEY": "my-secret",
                "AWS_DEFAULT_REGION": "eu-west-1",
            },
            clear=False,
        ):
            env = build_env()
        assert env["AWS_ACCESS_KEY_ID"] == "my-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "my-secret"
        assert env["AWS_DEFAULT_REGION"] == "eu-west-1"
