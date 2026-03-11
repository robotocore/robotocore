"""Tests for the Docker-based Lambda executor."""

import json
import os
import time
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.docker_executor import (
    DockerLambdaExecutor,
    WarmContainerPool,
    get_default_image_mapping,
    get_image_for_runtime,
    parse_docker_flags,
)


class TestRuntimeImageMapping:
    """Test runtime-to-Docker-image mapping for all supported runtimes."""

    def test_python312(self):
        assert get_image_for_runtime("python3.12") == "public.ecr.aws/lambda/python:3.12"

    def test_python311(self):
        assert get_image_for_runtime("python3.11") == "public.ecr.aws/lambda/python:3.11"

    def test_python313(self):
        assert get_image_for_runtime("python3.13") == "public.ecr.aws/lambda/python:3.13"

    def test_nodejs20(self):
        assert get_image_for_runtime("nodejs20.x") == "public.ecr.aws/lambda/nodejs:20"

    def test_nodejs18(self):
        assert get_image_for_runtime("nodejs18.x") == "public.ecr.aws/lambda/nodejs:18"

    def test_java21(self):
        assert get_image_for_runtime("java21") == "public.ecr.aws/lambda/java:21"

    def test_java17(self):
        assert get_image_for_runtime("java17") == "public.ecr.aws/lambda/java:17"

    def test_ruby33(self):
        assert get_image_for_runtime("ruby3.3") == "public.ecr.aws/lambda/ruby:3.3"

    def test_dotnet8(self):
        assert get_image_for_runtime("dotnet8") == "public.ecr.aws/lambda/dotnet:8"

    def test_provided_al2023(self):
        assert get_image_for_runtime("provided.al2023") == "public.ecr.aws/lambda/provided:al2023"

    def test_provided_al2(self):
        assert get_image_for_runtime("provided.al2") == "public.ecr.aws/lambda/provided:al2"

    def test_unknown_runtime_returns_none(self):
        assert get_image_for_runtime("cobol42") is None

    def test_default_mapping_has_all_major_runtimes(self):
        mapping = get_default_image_mapping()
        assert "python3.12" in mapping
        assert "nodejs20.x" in mapping
        assert "java21" in mapping
        assert "ruby3.3" in mapping
        assert "dotnet8" in mapping
        assert "provided.al2023" in mapping


class TestCustomImageMapping:
    """Test custom image mapping via env var."""

    def test_custom_mapping_json_string(self):
        custom = json.dumps({"python3.12": "my-registry/python:3.12"})
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": custom}):
            image = get_image_for_runtime("python3.12")
            assert image == "my-registry/python:3.12"

    def test_custom_mapping_overrides_default(self):
        custom = json.dumps({"python3.12": "custom/python:latest"})
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": custom}):
            image = get_image_for_runtime("python3.12")
            assert image == "custom/python:latest"
            # Other runtimes still use defaults
            image2 = get_image_for_runtime("nodejs20.x")
            assert image2 == "public.ecr.aws/lambda/nodejs:20"

    def test_custom_mapping_from_file(self, tmp_path):
        mapping_file = tmp_path / "mapping.json"
        mapping_file.write_text(json.dumps({"java21": "my-java:21"}))
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": str(mapping_file)}):
            image = get_image_for_runtime("java21")
            assert image == "my-java:21"


class TestContainerConfig:
    """Test container configuration generation."""

    def test_basic_container_config(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="my-func",
            handler="index.handler",
            timeout=30,
            memory_size=256,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        assert config["image"] == "public.ecr.aws/lambda/python:3.12"
        assert "/tmp/code" in str(config["volumes"])
        assert config["environment"]["AWS_LAMBDA_FUNCTION_NAME"] == "my-func"
        assert config["environment"]["_HANDLER"] == "index.handler"

    def test_container_env_vars(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="test-fn",
            handler="app.handler",
            timeout=10,
            memory_size=128,
            env_vars={"MY_VAR": "my_value", "DB_HOST": "localhost"},
            region="eu-west-1",
            account_id="999888777666",
            code_dir="/tmp/code",
        )

        env = config["environment"]
        assert env["MY_VAR"] == "my_value"
        assert env["DB_HOST"] == "localhost"
        assert env["AWS_REGION"] == "eu-west-1"
        assert env["AWS_DEFAULT_REGION"] == "eu-west-1"
        assert env["AWS_ACCOUNT_ID"] == "999888777666"

    def test_aws_endpoint_url_set_for_callback(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="callback-fn",
            handler="index.handler",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        env = config["environment"]
        assert env["AWS_ENDPOINT_URL"] == "http://host.docker.internal:4566"

    def test_lambda_env_vars_injected(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="my-func",
            handler="index.handler",
            timeout=30,
            memory_size=512,
            env_vars=None,
            region="ap-southeast-1",
            account_id="111222333444",
            code_dir="/tmp/code",
        )

        env = config["environment"]
        assert env["AWS_LAMBDA_FUNCTION_NAME"] == "my-func"
        assert env["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] == "512"
        assert env["AWS_LAMBDA_FUNCTION_TIMEOUT"] == "30"
        assert env["AWS_REGION"] == "ap-southeast-1"
        assert env["AWS_DEFAULT_REGION"] == "ap-southeast-1"
        assert env["AWS_ACCOUNT_ID"] == "111222333444"
        assert env["_HANDLER"] == "index.handler"
        assert env["AWS_ACCESS_KEY_ID"] == "testing"
        assert env["AWS_SECRET_ACCESS_KEY"] == "testing"

    def test_timeout_enforcement_config(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="timeout-fn",
            handler="index.handler",
            timeout=60,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        assert config["environment"]["AWS_LAMBDA_FUNCTION_TIMEOUT"] == "60"


class TestDockerNetworkConfig:
    """Test Docker network configuration."""

    def test_network_from_env(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = "my-network"
        executor._docker_dns = None
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="net-fn",
            handler="index.handler",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        assert config["network"] == "my-network"

    def test_dns_from_env(self):
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = "8.8.8.8"
        executor._docker_flags = {}
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="dns-fn",
            handler="index.handler",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        assert config["dns"] == ["8.8.8.8"]

    def test_extra_docker_flags(self):
        flags = {"mem_limit": "512m", "cpu_period": 100000}
        executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
        executor._docker_network = None
        executor._docker_dns = None
        executor._docker_flags = flags
        executor._gateway_port = 4566

        config = executor._build_container_config(
            image="public.ecr.aws/lambda/python:3.12",
            function_name="flags-fn",
            handler="index.handler",
            timeout=3,
            memory_size=128,
            env_vars=None,
            region="us-east-1",
            account_id="123456789012",
            code_dir="/tmp/code",
        )

        assert config["mem_limit"] == "512m"
        assert config["cpu_period"] == 100000


class TestDockerFlagsParsing:
    """Test Docker extra flags parsing from env var."""

    def test_parse_json_dict(self):
        flags = parse_docker_flags('{"mem_limit": "256m"}')
        assert flags == {"mem_limit": "256m"}

    def test_parse_empty(self):
        flags = parse_docker_flags("")
        assert flags == {}

    def test_parse_none(self):
        flags = parse_docker_flags(None)
        assert flags == {}

    def test_parse_invalid_json(self):
        flags = parse_docker_flags("not json")
        assert flags == {}


class TestWarmContainerPool:
    """Test warm container pool — container reuse on second invoke."""

    def test_pool_stores_container(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("my-func", mock_container)
        result = pool.get("my-func")
        assert result is mock_container

    def test_pool_returns_none_for_unknown_function(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        assert pool.get("unknown-func") is None

    def test_container_reuse_on_second_invoke(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("reuse-fn", mock_container)
        first = pool.get("reuse-fn")
        assert first is mock_container
        # After get, container is removed from pool (in use)
        assert pool.get("reuse-fn") is None

    def test_expired_container_cleaned_up(self):
        pool = WarmContainerPool(keepalive_ms=1)  # 1ms keepalive
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("expired-fn", mock_container)
        time.sleep(0.01)  # Wait for expiry
        result = pool.get("expired-fn")
        assert result is None
        mock_container.stop.assert_called_once()
        mock_container.remove.assert_called_once()

    def test_remove_containers_false_keeps_containers(self):
        pool = WarmContainerPool(keepalive_ms=1, remove_containers=False)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("keep-fn", mock_container)
        time.sleep(0.01)
        pool.get("keep-fn")  # Triggers cleanup check
        # Container should be stopped but not removed
        mock_container.remove.assert_not_called()

    def test_cleanup_all(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        c1 = MagicMock()
        c1.status = "running"
        c2 = MagicMock()
        c2.status = "running"

        pool.put("fn1", c1)
        pool.put("fn2", c2)
        pool.cleanup_all()

        c1.stop.assert_called_once()
        c1.remove.assert_called_once()
        c2.stop.assert_called_once()
        c2.remove.assert_called_once()


class TestPrebuildImages:
    """Test LAMBDA_PREBUILD_IMAGES triggers image pull at create time."""

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_pulls_image(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor(prebuild_images=True, synchronous_create=True)
        executor.prebuild_image("python3.12")

        mock_client.images.pull.assert_called_once_with("public.ecr.aws/lambda/python:3.12")

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_unknown_runtime_skips(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor(prebuild_images=True)
        executor.prebuild_image("cobol42")

        mock_client.images.pull.assert_not_called()


class TestSynchronousCreate:
    """Test LAMBDA_SYNCHRONOUS_CREATE blocks until image ready."""

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_synchronous_create_blocks(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor(prebuild_images=True, synchronous_create=True)
        executor.prebuild_image("python3.12")

        # Should have called pull (which blocks)
        mock_client.images.pull.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_async_create_uses_thread(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor(prebuild_images=True, synchronous_create=False)
        executor.prebuild_image("python3.12")

        # Still pulls, but in background — we just verify it was called
        # (in real code it's submitted to a thread pool)
        mock_client.images.pull.assert_called_once()


class TestFallbackToLocalExecutor:
    """Test fallback to local executor when Docker unavailable."""

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_when_docker_unavailable(self, mock_docker_mod):
        mock_docker_mod.from_env.side_effect = Exception("Docker not available")

        executor = DockerLambdaExecutor()
        assert executor._docker_client is None
        assert executor._fallback is True

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_execute_uses_local(self, mock_docker_mod):
        mock_docker_mod.from_env.side_effect = Exception("Docker not available")

        executor = DockerLambdaExecutor()

        with patch("robotocore.services.lambda_.executor.execute_python_handler") as mock_local:
            mock_local.return_value = ({"result": "ok"}, None, "logs")

            result, error, logs = executor.execute(
                code_zip=b"PK\x03\x04fake",
                handler="index.handler",
                event={"key": "value"},
                function_name="fallback-fn",
                runtime="python3.12",
                timeout=3,
                memory_size=128,
                region="us-east-1",
                account_id="123456789012",
            )

            assert result == {"result": "ok"}
            assert error is None
            mock_local.assert_called_once()
