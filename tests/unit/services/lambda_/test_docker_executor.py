"""Tests for the Docker-based Lambda executor."""

import json
import os
import time
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.docker_executor import (
    ContainerInfo,
    DockerLambdaExecutor,
    WarmContainerPool,
    _load_custom_mapping,
    get_default_image_mapping,
    get_executor_mode,
    get_image_for_runtime,
    parse_docker_flags,
)

# ---------------------------------------------------------------------------
# Runtime image mapping
# ---------------------------------------------------------------------------


class TestRuntimeImageMapping:
    """Test runtime-to-Docker-image mapping for all supported runtimes."""

    def test_python312(self):
        assert get_image_for_runtime("python3.12") == "public.ecr.aws/lambda/python:3.12"

    def test_python311(self):
        assert get_image_for_runtime("python3.11") == "public.ecr.aws/lambda/python:3.11"

    def test_python313(self):
        assert get_image_for_runtime("python3.13") == "public.ecr.aws/lambda/python:3.13"

    def test_python310(self):
        assert get_image_for_runtime("python3.10") == "public.ecr.aws/lambda/python:3.10"

    def test_python39(self):
        assert get_image_for_runtime("python3.9") == "public.ecr.aws/lambda/python:3.9"

    def test_python38(self):
        assert get_image_for_runtime("python3.8") == "public.ecr.aws/lambda/python:3.8"

    def test_nodejs20(self):
        assert get_image_for_runtime("nodejs20.x") == "public.ecr.aws/lambda/nodejs:20"

    def test_nodejs18(self):
        assert get_image_for_runtime("nodejs18.x") == "public.ecr.aws/lambda/nodejs:18"

    def test_nodejs16(self):
        assert get_image_for_runtime("nodejs16.x") == "public.ecr.aws/lambda/nodejs:16"

    def test_nodejs22(self):
        assert get_image_for_runtime("nodejs22.x") == "public.ecr.aws/lambda/nodejs:22"

    def test_java21(self):
        assert get_image_for_runtime("java21") == "public.ecr.aws/lambda/java:21"

    def test_java17(self):
        assert get_image_for_runtime("java17") == "public.ecr.aws/lambda/java:17"

    def test_java11(self):
        assert get_image_for_runtime("java11") == "public.ecr.aws/lambda/java:11"

    def test_java8(self):
        assert get_image_for_runtime("java8") == "public.ecr.aws/lambda/java:8"

    def test_java8_al2(self):
        assert get_image_for_runtime("java8.al2") == "public.ecr.aws/lambda/java:8.al2"

    def test_ruby33(self):
        assert get_image_for_runtime("ruby3.3") == "public.ecr.aws/lambda/ruby:3.3"

    def test_ruby32(self):
        assert get_image_for_runtime("ruby3.2") == "public.ecr.aws/lambda/ruby:3.2"

    def test_dotnet8(self):
        assert get_image_for_runtime("dotnet8") == "public.ecr.aws/lambda/dotnet:8"

    def test_dotnet6(self):
        assert get_image_for_runtime("dotnet6") == "public.ecr.aws/lambda/dotnet:6"

    def test_provided_al2023(self):
        assert get_image_for_runtime("provided.al2023") == "public.ecr.aws/lambda/provided:al2023"

    def test_provided_al2(self):
        assert get_image_for_runtime("provided.al2") == "public.ecr.aws/lambda/provided:al2"

    def test_provided_base(self):
        assert get_image_for_runtime("provided") == "public.ecr.aws/lambda/provided:al2"

    def test_unknown_runtime_returns_none(self):
        assert get_image_for_runtime("cobol42") is None

    def test_empty_string_returns_none(self):
        assert get_image_for_runtime("") is None

    def test_default_mapping_has_all_major_runtimes(self):
        mapping = get_default_image_mapping()
        assert "python3.12" in mapping
        assert "nodejs20.x" in mapping
        assert "java21" in mapping
        assert "ruby3.3" in mapping
        assert "dotnet8" in mapping
        assert "provided.al2023" in mapping

    def test_default_mapping_returns_copy(self):
        """Modifying the returned mapping does not affect the original."""
        mapping = get_default_image_mapping()
        mapping["python3.12"] = "hacked"
        assert get_image_for_runtime("python3.12") == "public.ecr.aws/lambda/python:3.12"


# ---------------------------------------------------------------------------
# Custom image mapping
# ---------------------------------------------------------------------------


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

    def test_custom_mapping_invalid_json_string_returns_empty(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": "not json {{{"}):
            result = _load_custom_mapping()
            assert result == {}

    def test_custom_mapping_non_dict_json_returns_empty(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": '["a","b"]'}):
            result = _load_custom_mapping()
            assert result == {}

    def test_custom_mapping_file_with_invalid_json(self, tmp_path):
        mapping_file = tmp_path / "bad.json"
        mapping_file.write_text("not valid json!!!")
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": str(mapping_file)}):
            result = _load_custom_mapping()
            assert result == {}

    def test_custom_mapping_file_with_non_dict_json(self, tmp_path):
        mapping_file = tmp_path / "list.json"
        mapping_file.write_text(json.dumps(["a", "b"]))
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": str(mapping_file)}):
            result = _load_custom_mapping()
            assert result == {}

    def test_custom_mapping_empty_env_returns_empty(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_IMAGE_MAPPING": ""}):
            result = _load_custom_mapping()
            assert result == {}

    def test_custom_mapping_not_set_returns_empty(self):
        env = os.environ.copy()
        env.pop("LAMBDA_RUNTIME_IMAGE_MAPPING", None)
        with patch.dict(os.environ, env, clear=True):
            result = _load_custom_mapping()
            assert result == {}


# ---------------------------------------------------------------------------
# Container configuration
# ---------------------------------------------------------------------------


def _make_bare_executor(**overrides):
    """Create a DockerLambdaExecutor without calling __init__ (no Docker needed)."""
    executor = DockerLambdaExecutor.__new__(DockerLambdaExecutor)
    executor._docker_network = overrides.get("network", None)
    executor._docker_dns = overrides.get("dns", None)
    executor._docker_flags = overrides.get("flags", {})
    executor._gateway_port = overrides.get("gateway_port", 4566)
    executor._fallback = overrides.get("fallback", True)
    executor._docker_client = overrides.get("docker_client", None)
    executor._prebuild_images = overrides.get("prebuild_images", False)
    executor._synchronous_create = overrides.get("synchronous_create", False)
    executor._warm_pool = WarmContainerPool()
    return executor


_DEFAULT_CONFIG_KWARGS = dict(
    image="public.ecr.aws/lambda/python:3.12",
    function_name="test-fn",
    handler="index.handler",
    timeout=30,
    memory_size=256,
    env_vars=None,
    region="us-east-1",
    account_id="123456789012",
    code_dir="/tmp/code",
)


class TestContainerConfig:
    """Test container configuration generation."""

    def test_basic_container_config(self):
        executor = _make_bare_executor()
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)

        assert config["image"] == "public.ecr.aws/lambda/python:3.12"
        assert config["volumes"]["/tmp/code"]["bind"] == "/var/task"
        assert config["volumes"]["/tmp/code"]["mode"] == "ro"
        assert config["detach"] is True
        assert config["auto_remove"] is False

    def test_container_env_vars_user_override(self):
        executor = _make_bare_executor()
        config = executor._build_container_config(
            **{**_DEFAULT_CONFIG_KWARGS, "env_vars": {"MY_VAR": "val", "DB_HOST": "localhost"}},
        )
        env = config["environment"]
        assert env["MY_VAR"] == "val"
        assert env["DB_HOST"] == "localhost"

    def test_user_env_vars_override_defaults(self):
        """User-provided env vars can override the AWS defaults."""
        executor = _make_bare_executor()
        config = executor._build_container_config(
            **{**_DEFAULT_CONFIG_KWARGS, "env_vars": {"AWS_REGION": "custom-region"}},
        )
        assert config["environment"]["AWS_REGION"] == "custom-region"

    def test_aws_endpoint_url_set_for_callback(self):
        executor = _make_bare_executor()
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert config["environment"]["AWS_ENDPOINT_URL"] == "http://host.docker.internal:4566"

    def test_custom_gateway_port_in_endpoint_url(self):
        executor = _make_bare_executor(gateway_port=5555)
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert config["environment"]["AWS_ENDPOINT_URL"] == "http://host.docker.internal:5555"

    def test_lambda_env_vars_injected(self):
        executor = _make_bare_executor()
        config = executor._build_container_config(
            **{
                **_DEFAULT_CONFIG_KWARGS,
                "function_name": "my-func",
                "handler": "app.handler",
                "timeout": 60,
                "memory_size": 512,
                "region": "ap-southeast-1",
                "account_id": "111222333444",
            },
        )
        env = config["environment"]
        assert env["AWS_LAMBDA_FUNCTION_NAME"] == "my-func"
        assert env["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] == "512"
        assert env["AWS_LAMBDA_FUNCTION_TIMEOUT"] == "60"
        assert env["AWS_REGION"] == "ap-southeast-1"
        assert env["AWS_DEFAULT_REGION"] == "ap-southeast-1"
        assert env["AWS_ACCOUNT_ID"] == "111222333444"
        assert env["_HANDLER"] == "app.handler"
        assert env["AWS_ACCESS_KEY_ID"] == "testing"
        assert env["AWS_SECRET_ACCESS_KEY"] == "testing"

    def test_no_network_key_when_none(self):
        executor = _make_bare_executor(network=None)
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert "network" not in config

    def test_no_dns_key_when_none(self):
        executor = _make_bare_executor(dns=None)
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert "dns" not in config


class TestDockerNetworkConfig:
    """Test Docker network configuration."""

    def test_network_set_in_config(self):
        executor = _make_bare_executor(network="my-network")
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert config["network"] == "my-network"

    def test_dns_set_as_list(self):
        executor = _make_bare_executor(dns="8.8.8.8")
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert config["dns"] == ["8.8.8.8"]

    def test_extra_docker_flags_merged(self):
        executor = _make_bare_executor(flags={"mem_limit": "512m", "cpu_period": 100000})
        config = executor._build_container_config(**_DEFAULT_CONFIG_KWARGS)
        assert config["mem_limit"] == "512m"
        assert config["cpu_period"] == 100000


# ---------------------------------------------------------------------------
# Docker flags parsing
# ---------------------------------------------------------------------------


class TestDockerFlagsParsing:
    """Test Docker extra flags parsing from env var."""

    def test_parse_json_dict(self):
        flags = parse_docker_flags('{"mem_limit": "256m"}')
        assert flags == {"mem_limit": "256m"}

    def test_parse_empty(self):
        assert parse_docker_flags("") == {}

    def test_parse_none(self):
        assert parse_docker_flags(None) == {}

    def test_parse_invalid_json(self):
        assert parse_docker_flags("not json") == {}

    def test_parse_json_list_returns_empty(self):
        """A JSON list is not a valid flags value."""
        assert parse_docker_flags('["a"]') == {}

    def test_parse_json_string_returns_empty(self):
        assert parse_docker_flags('"just a string"') == {}

    def test_parse_complex_flags(self):
        raw = json.dumps({"mem_limit": "1g", "cpu_period": 100000, "privileged": True})
        flags = parse_docker_flags(raw)
        assert flags["mem_limit"] == "1g"
        assert flags["cpu_period"] == 100000
        assert flags["privileged"] is True


# ---------------------------------------------------------------------------
# ContainerInfo dataclass
# ---------------------------------------------------------------------------


class TestContainerInfo:
    """Test the ContainerInfo dataclass."""

    def test_created_at_auto_set(self):
        mock_container = MagicMock()
        before = time.time()
        info = ContainerInfo(container=mock_container, function_name="fn")
        after = time.time()
        assert before <= info.created_at <= after

    def test_explicit_created_at(self):
        mock_container = MagicMock()
        info = ContainerInfo(container=mock_container, function_name="fn", created_at=1000.0)
        assert info.created_at == 1000.0

    def test_stores_function_name(self):
        mock_container = MagicMock()
        info = ContainerInfo(container=mock_container, function_name="my-func")
        assert info.function_name == "my-func"
        assert info.container is mock_container


# ---------------------------------------------------------------------------
# Warm container pool
# ---------------------------------------------------------------------------


class TestWarmContainerPool:
    """Test warm container pool — container reuse on second invoke."""

    def test_pool_stores_and_retrieves_container(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("my-func", mock_container)
        result = pool.get("my-func")
        assert result is mock_container

    def test_pool_returns_none_for_unknown_function(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        assert pool.get("unknown-func") is None

    def test_get_removes_container_from_pool(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("reuse-fn", mock_container)
        first = pool.get("reuse-fn")
        assert first is mock_container
        assert pool.get("reuse-fn") is None

    def test_expired_container_cleaned_up(self):
        pool = WarmContainerPool(keepalive_ms=1)  # 1ms keepalive
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("expired-fn", mock_container)
        time.sleep(0.01)
        result = pool.get("expired-fn")
        assert result is None
        mock_container.stop.assert_called_once_with(timeout=2)
        mock_container.remove.assert_called_once_with(force=True)

    def test_remove_containers_false_skips_remove(self):
        pool = WarmContainerPool(keepalive_ms=1, remove_containers=False)
        mock_container = MagicMock()
        mock_container.status = "running"

        pool.put("keep-fn", mock_container)
        time.sleep(0.01)
        pool.get("keep-fn")
        mock_container.stop.assert_called_once()
        mock_container.remove.assert_not_called()

    def test_non_running_container_cleaned_up(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        mock_container = MagicMock()
        mock_container.status = "exited"

        pool.put("dead-fn", mock_container)
        result = pool.get("dead-fn")
        assert result is None
        mock_container.stop.assert_called_once_with(timeout=2)
        mock_container.remove.assert_called_once_with(force=True)

    def test_put_replaces_existing_and_cleans_old(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        old_container = MagicMock()
        new_container = MagicMock()
        new_container.status = "running"

        pool.put("fn", old_container)
        pool.put("fn", new_container)

        old_container.stop.assert_called_once_with(timeout=2)
        old_container.remove.assert_called_once_with(force=True)

        result = pool.get("fn")
        assert result is new_container

    def test_cleanup_all_stops_and_removes_everything(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        containers = []
        for i in range(5):
            c = MagicMock()
            c.status = "running"
            pool.put(f"fn{i}", c)
            containers.append(c)

        pool.cleanup_all()

        for c in containers:
            c.stop.assert_called_once_with(timeout=2)
            c.remove.assert_called_once_with(force=True)

        # Pool should be empty after cleanup
        for i in range(5):
            assert pool.get(f"fn{i}") is None

    def test_cleanup_all_tolerates_stop_exception(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        c = MagicMock()
        c.stop.side_effect = RuntimeError("already stopped")
        pool.put("fn", c)
        pool.cleanup_all()  # Should not raise
        c.remove.assert_called_once_with(force=True)

    def test_cleanup_all_tolerates_remove_exception(self):
        pool = WarmContainerPool(keepalive_ms=600000)
        c = MagicMock()
        c.remove.side_effect = RuntimeError("already removed")
        pool.put("fn", c)
        pool.cleanup_all()  # Should not raise

    def test_pool_with_zero_keepalive_expires_quickly(self):
        pool = WarmContainerPool(keepalive_ms=0)
        c = MagicMock()
        c.status = "running"
        pool.put("fn", c)
        time.sleep(0.002)  # Ensure at least 1ms elapses (keepalive check is strict >)
        result = pool.get("fn")
        assert result is None


# ---------------------------------------------------------------------------
# Executor mode
# ---------------------------------------------------------------------------


class TestExecutorMode:
    def test_default_is_local(self):
        env = os.environ.copy()
        env.pop("LAMBDA_RUNTIME_EXECUTOR", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_executor_mode() == "local"

    def test_docker_mode(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "docker"}):
            assert get_executor_mode() == "docker"

    def test_local_mode_explicit(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "local"}):
            assert get_executor_mode() == "local"

    def test_case_insensitive(self):
        with patch.dict(os.environ, {"LAMBDA_RUNTIME_EXECUTOR": "DOCKER"}):
            assert get_executor_mode() == "docker"


# ---------------------------------------------------------------------------
# DockerLambdaExecutor init
# ---------------------------------------------------------------------------


class TestDockerLambdaExecutorInit:
    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_successful_init_with_docker(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor()
        assert executor._fallback is False
        assert executor._docker_client is mock_client
        mock_client.ping.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_when_docker_unavailable(self, mock_docker_mod):
        mock_docker_mod.from_env.side_effect = Exception("Docker not available")

        executor = DockerLambdaExecutor()
        assert executor._docker_client is None
        assert executor._fallback is True

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_when_ping_fails(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_client.ping.side_effect = Exception("connection refused")
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor()
        assert executor._fallback is True
        assert executor._docker_client is None

    @patch("robotocore.services.lambda_.docker_executor.docker", None)
    def test_fallback_when_docker_package_missing(self):
        executor = DockerLambdaExecutor()
        assert executor._fallback is True

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_env_var_config_defaults(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        env = os.environ.copy()
        for k in [
            "LAMBDA_KEEPALIVE_MS",
            "LAMBDA_REMOVE_CONTAINERS",
            "LAMBDA_PREBUILD_IMAGES",
            "LAMBDA_SYNCHRONOUS_CREATE",
            "LAMBDA_DOCKER_NETWORK",
            "LAMBDA_DOCKER_DNS",
            "LAMBDA_DOCKER_FLAGS",
        ]:
            env.pop(k, None)

        with patch.dict(os.environ, env, clear=True):
            executor = DockerLambdaExecutor()
            assert executor._prebuild_images is False
            assert executor._synchronous_create is False
            assert executor._docker_network is None
            assert executor._docker_dns is None
            assert executor._docker_flags == {}

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_env_var_config_overrides(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        with patch.dict(
            os.environ,
            {
                "LAMBDA_KEEPALIVE_MS": "30000",
                "LAMBDA_REMOVE_CONTAINERS": "false",
                "LAMBDA_PREBUILD_IMAGES": "true",
                "LAMBDA_SYNCHRONOUS_CREATE": "true",
                "LAMBDA_DOCKER_NETWORK": "custom-net",
                "LAMBDA_DOCKER_DNS": "1.1.1.1",
                "LAMBDA_DOCKER_FLAGS": '{"privileged": true}',
                "GATEWAY_PORT": "5000",
            },
        ):
            executor = DockerLambdaExecutor()
            assert executor._prebuild_images is True
            assert executor._synchronous_create is True
            assert executor._docker_network == "custom-net"
            assert executor._docker_dns == "1.1.1.1"
            assert executor._docker_flags == {"privileged": True}
            assert executor._gateway_port == 5000

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_explicit_init_params_override_env(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        with patch.dict(
            os.environ,
            {
                "LAMBDA_KEEPALIVE_MS": "999999",
                "LAMBDA_REMOVE_CONTAINERS": "true",
                "LAMBDA_PREBUILD_IMAGES": "true",
                "LAMBDA_SYNCHRONOUS_CREATE": "true",
            },
        ):
            executor = DockerLambdaExecutor(
                keepalive_ms=5000,
                remove_containers=False,
                prebuild_images=False,
                synchronous_create=False,
            )
            assert executor._prebuild_images is False
            assert executor._synchronous_create is False


# ---------------------------------------------------------------------------
# Prebuild images
# ---------------------------------------------------------------------------


class TestPrebuildImages:
    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_pulls_image_synchronously(self, mock_docker_mod):
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

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_async_uses_thread(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor(prebuild_images=True, synchronous_create=False)
        with patch("robotocore.services.lambda_.docker_executor.threading") as mock_threading:
            mock_thread = MagicMock()
            mock_threading.Thread.return_value = mock_thread
            executor.prebuild_image("python3.12")
            mock_threading.Thread.assert_called_once()
            mock_thread.start.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_noop_in_fallback_mode(self, mock_docker_mod):
        mock_docker_mod.from_env.side_effect = Exception("no docker")

        executor = DockerLambdaExecutor()
        executor.prebuild_image("python3.12")  # Should not raise

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_prebuild_pull_failure_logged_not_raised(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client
        mock_client.images.pull.side_effect = RuntimeError("network error")

        executor = DockerLambdaExecutor(prebuild_images=True, synchronous_create=True)
        executor.prebuild_image("python3.12")  # Should not raise


# ---------------------------------------------------------------------------
# Fallback to local executor
# ---------------------------------------------------------------------------


class TestFallbackToLocalExecutor:
    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_when_docker_unavailable(self, mock_docker_mod):
        mock_docker_mod.from_env.side_effect = Exception("Docker not available")
        executor = DockerLambdaExecutor()
        assert executor._docker_client is None
        assert executor._fallback is True

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_execute_delegates_to_local(self, mock_docker_mod):
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
            assert logs == "logs"
            mock_local.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_fallback_when_no_image_for_runtime(self, mock_docker_mod):
        """If runtime has no Docker image, fall back to local even with Docker available."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor()
        assert executor._fallback is False  # Docker is available

        with patch("robotocore.services.lambda_.executor.execute_python_handler") as mock_local:
            mock_local.return_value = ({"ok": True}, None, "")

            result, error, logs = executor.execute(
                code_zip=b"fake",
                handler="index.handler",
                event={},
                function_name="fn",
                runtime="cobol42",  # No image for this
                timeout=3,
                memory_size=128,
                region="us-east-1",
                account_id="123456789012",
            )

            assert result == {"ok": True}
            mock_local.assert_called_once()


# ---------------------------------------------------------------------------
# Execute with mocked Docker
# ---------------------------------------------------------------------------


def _make_mock_container(
    exit_code: int = 0,
    stdout: bytes = b'{"statusCode": 200}',
    stderr: bytes = b"START RequestId\nEND RequestId\n",
):
    """Create a mock Docker container with standard responses."""
    container = MagicMock()
    container.wait.return_value = {"StatusCode": exit_code}
    container.status = "exited"
    container.id = "abc123"

    _stdout = stdout
    _stderr = stderr

    def mock_logs(stdout=True, stderr=False):
        if stdout and not stderr:
            return _stdout
        if stderr and not stdout:
            return _stderr
        parts = []
        if stdout:
            parts.append(_stdout)
        if stderr:
            parts.append(_stderr)
        return b"".join(parts)

    container.logs = mock_logs
    return container


class TestDockerExecute:
    """Test the execute method with mocked Docker client."""

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_successful_json_response(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        payload = json.dumps({"statusCode": 200, "body": "hello"}).encode()
        mock_client.containers.run.return_value = _make_mock_container(stdout=payload)

        executor = DockerLambdaExecutor()
        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={"key": "value"},
            function_name="fn",
            runtime="python3.12",
            timeout=30,
            memory_size=256,
            code_dir=str(tmp_path),
        )

        assert result == {"statusCode": 200, "body": "hello"}
        assert error is None
        mock_client.containers.run.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_successful_string_response(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(stdout=b'"just a string"')

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert result == "just a string"
        assert error is None

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_successful_non_json_stdout(self, mock_docker_mod, tmp_path):
        """Non-JSON stdout from a successful container is returned as-is."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(
            stdout=b"plain text response"
        )

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert result == "plain text response"
        assert error is None

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_empty_stdout_returns_none(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(stdout=b"")

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert result is None
        assert error is None

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_timeout_returns_timed_out_error(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_container = MagicMock()
        mock_container.wait.side_effect = Exception("deadline exceeded")
        mock_container.status = "running"
        mock_client.containers.run.return_value = mock_container

        executor = DockerLambdaExecutor()
        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=1,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert result is None
        assert error == "Task.TimedOut"
        assert "timed out" in logs.lower()
        mock_container.kill.assert_called_once()

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_nonzero_exit_with_structured_error(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        error_payload = json.dumps(
            {"errorMessage": "division by zero", "errorType": "ZeroDivisionError"}
        ).encode()
        mock_client.containers.run.return_value = _make_mock_container(
            exit_code=1, stdout=error_payload
        )

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert error == "Handled"
        assert result["errorMessage"] == "division by zero"
        assert result["errorType"] == "ZeroDivisionError"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_nonzero_exit_with_plain_text(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(
            exit_code=1, stdout=b"segfault"
        )

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert error == "Unhandled"
        assert result["errorType"] == "Runtime.ExitError"
        assert result["errorMessage"] == "segfault"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_nonzero_exit_empty_stdout(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(exit_code=1, stdout=b"")

        executor = DockerLambdaExecutor()
        result, error, _ = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert error == "Unhandled"
        assert result["errorMessage"] == "Function execution failed"

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_docker_run_exception_returns_error(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client
        mock_client.containers.run.side_effect = RuntimeError("image not found")

        executor = DockerLambdaExecutor()
        result, error, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert error == "Unhandled"
        assert result["errorType"] == "DockerExecutionError"
        assert "image not found" in result["errorMessage"]

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_event_passed_as_command(self, mock_docker_mod, tmp_path):
        """The event JSON is passed as the container command."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(stdout=b'"ok"')

        executor = DockerLambdaExecutor()
        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={"key": "value"},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        call_kwargs = mock_client.containers.run.call_args
        assert call_kwargs.kwargs["command"] == json.dumps({"key": "value"})

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_container_wait_timeout_is_timeout_plus_five(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        container = _make_mock_container(stdout=b'"ok"')
        container.wait = MagicMock(return_value={"StatusCode": 0})
        mock_client.containers.run.return_value = container

        executor = DockerLambdaExecutor()
        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=30,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        container.wait.assert_called_once_with(timeout=35)

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_container_cleaned_up_in_finally(self, mock_docker_mod, tmp_path):
        """Container is stopped and removed even on success."""
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        container = MagicMock()
        container.wait.return_value = {"StatusCode": 0}
        container.logs = MagicMock(return_value=b'"ok"')
        mock_client.containers.run.return_value = container

        executor = DockerLambdaExecutor()
        executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        container.stop.assert_called_once_with(timeout=2)
        container.remove.assert_called_once_with(force=True)

    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_logs_collected_from_stderr(self, mock_docker_mod, tmp_path):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        mock_client.containers.run.return_value = _make_mock_container(
            stdout=b'"ok"', stderr=b"START\nEND\nREPORT Duration: 50ms\n"
        )

        executor = DockerLambdaExecutor()
        _, _, logs = executor.execute(
            code_zip=b"fake",
            handler="index.handler",
            event={},
            function_name="fn",
            runtime="python3.12",
            timeout=3,
            memory_size=128,
            code_dir=str(tmp_path),
        )

        assert "REPORT Duration: 50ms" in logs


# ---------------------------------------------------------------------------
# Executor cleanup
# ---------------------------------------------------------------------------


class TestExecutorCleanup:
    @patch("robotocore.services.lambda_.docker_executor.docker")
    def test_cleanup_delegates_to_warm_pool(self, mock_docker_mod):
        mock_client = MagicMock()
        mock_docker_mod.from_env.return_value = mock_client

        executor = DockerLambdaExecutor()
        executor._warm_pool = MagicMock()
        executor.cleanup()
        executor._warm_pool.cleanup_all.assert_called_once()


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestSingleton:
    def test_get_docker_executor_returns_singleton(self):
        import robotocore.services.lambda_.docker_executor as mod

        mod._docker_executor = None

        with patch.object(mod, "DockerLambdaExecutor") as mock_cls:
            instance = MagicMock()
            mock_cls.return_value = instance

            result1 = mod.get_docker_executor()
            result2 = mod.get_docker_executor()

            assert result1 is result2
            mock_cls.assert_called_once()

        # Clean up
        mod._docker_executor = None

    def test_singleton_thread_safe(self):
        """Multiple threads calling get_docker_executor only create one instance."""
        import threading

        import robotocore.services.lambda_.docker_executor as mod

        mod._docker_executor = None
        results = []

        with patch.object(mod, "DockerLambdaExecutor") as mock_cls:
            instance = MagicMock()
            mock_cls.return_value = instance

            def get():
                results.append(mod.get_docker_executor())

            threads = [threading.Thread(target=get) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert len(results) == 10
        assert all(r is results[0] for r in results)

        mod._docker_executor = None
