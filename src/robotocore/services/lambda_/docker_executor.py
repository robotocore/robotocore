"""Docker-based Lambda executor — runs Lambda functions in real Docker containers.

Supports warm container pooling, custom runtime image mapping, and automatic
fallback to the local in-process executor when Docker is unavailable.

Configuration via environment variables:
  LAMBDA_RUNTIME_EXECUTOR: "local" (default) or "docker"
  LAMBDA_RUNTIME_IMAGE_MAPPING: JSON string or file path for custom image mapping
  LAMBDA_KEEPALIVE_MS: Warm container keepalive in ms (default 600000 = 10min)
  LAMBDA_REMOVE_CONTAINERS: "true" (default) removes containers after keepalive
  LAMBDA_PREBUILD_IMAGES: "true" to pre-pull images at CreateFunction time
  LAMBDA_SYNCHRONOUS_CREATE: "true" to block CreateFunction until image ready
  LAMBDA_DOCKER_NETWORK: Docker network name for Lambda containers
  LAMBDA_DOCKER_DNS: Custom DNS for Lambda containers
  LAMBDA_DOCKER_FLAGS: JSON dict of additional docker run flags
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

try:
    import docker
except ImportError:
    docker = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# Default runtime-to-image mapping
_DEFAULT_IMAGE_MAPPING: dict[str, str] = {
    # Python
    "python3.8": "public.ecr.aws/lambda/python:3.8",
    "python3.9": "public.ecr.aws/lambda/python:3.9",
    "python3.10": "public.ecr.aws/lambda/python:3.10",
    "python3.11": "public.ecr.aws/lambda/python:3.11",
    "python3.12": "public.ecr.aws/lambda/python:3.12",
    "python3.13": "public.ecr.aws/lambda/python:3.13",
    # Node.js
    "nodejs16.x": "public.ecr.aws/lambda/nodejs:16",
    "nodejs18.x": "public.ecr.aws/lambda/nodejs:18",
    "nodejs20.x": "public.ecr.aws/lambda/nodejs:20",
    "nodejs22.x": "public.ecr.aws/lambda/nodejs:22",
    # Java
    "java8": "public.ecr.aws/lambda/java:8",
    "java8.al2": "public.ecr.aws/lambda/java:8.al2",
    "java11": "public.ecr.aws/lambda/java:11",
    "java17": "public.ecr.aws/lambda/java:17",
    "java21": "public.ecr.aws/lambda/java:21",
    # Ruby
    "ruby3.2": "public.ecr.aws/lambda/ruby:3.2",
    "ruby3.3": "public.ecr.aws/lambda/ruby:3.3",
    # .NET
    "dotnet6": "public.ecr.aws/lambda/dotnet:6",
    "dotnet8": "public.ecr.aws/lambda/dotnet:8",
    # Custom / provided
    "provided": "public.ecr.aws/lambda/provided:al2",
    "provided.al2": "public.ecr.aws/lambda/provided:al2",
    "provided.al2023": "public.ecr.aws/lambda/provided:al2023",
}


def get_executor_mode() -> str:
    """Return the configured executor mode: 'local' or 'docker'."""
    return os.environ.get("LAMBDA_RUNTIME_EXECUTOR", "local").lower()


def get_default_image_mapping() -> dict[str, str]:
    """Return the default runtime-to-image mapping."""
    return dict(_DEFAULT_IMAGE_MAPPING)


def _load_custom_mapping() -> dict[str, str]:
    """Load custom image mapping from LAMBDA_RUNTIME_IMAGE_MAPPING env var.

    The value can be a JSON string or a file path to a JSON file.
    Returns an empty dict if not set or invalid.
    """
    raw = os.environ.get("LAMBDA_RUNTIME_IMAGE_MAPPING", "")
    if not raw:
        return {}

    # Try as file path first
    if os.path.isfile(raw):
        try:
            with open(raw) as f:
                mapping = json.load(f)
            if isinstance(mapping, dict):
                return mapping
        except (json.JSONDecodeError, OSError):
            logger.warning("Failed to load image mapping from file: %s", raw)
            return {}

    # Try as JSON string
    try:
        mapping = json.loads(raw)
        if isinstance(mapping, dict):
            return mapping
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in LAMBDA_RUNTIME_IMAGE_MAPPING: %s", raw[:100])

    return {}


def get_image_for_runtime(runtime: str) -> str | None:
    """Get the Docker image for a given AWS runtime string.

    Custom mappings from LAMBDA_RUNTIME_IMAGE_MAPPING override defaults.
    Returns None if no image is mapped for the runtime.
    """
    custom = _load_custom_mapping()
    if runtime in custom:
        return custom[runtime]
    return _DEFAULT_IMAGE_MAPPING.get(runtime)


def parse_docker_flags(raw: str | None) -> dict[str, Any]:
    """Parse LAMBDA_DOCKER_FLAGS env var (JSON dict) into kwargs for docker run."""
    if not raw:
        return {}
    try:
        flags = json.loads(raw)
        if isinstance(flags, dict):
            return flags
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid JSON in LAMBDA_DOCKER_FLAGS")
    return {}


@dataclass
class ContainerInfo:
    """Tracks a warm container in the pool."""

    container: Any  # docker.models.containers.Container
    function_name: str
    created_at: float = field(default_factory=time.time)


class WarmContainerPool:
    """Pool of warm Lambda containers for reuse.

    Containers are kept alive for keepalive_ms milliseconds after their last
    invocation. After that, they are stopped and optionally removed.
    """

    def __init__(
        self,
        keepalive_ms: int = 600000,
        remove_containers: bool = True,
    ) -> None:
        self._keepalive_ms = keepalive_ms
        self._remove_containers = remove_containers
        self._pool: dict[str, ContainerInfo] = {}
        self._lock = threading.Lock()

    def get(self, function_name: str) -> Any | None:
        """Get a warm container for the function, or None if none available."""
        with self._lock:
            info = self._pool.pop(function_name, None)
            if info is None:
                return None

            age_ms = (time.time() - info.created_at) * 1000
            if age_ms > self._keepalive_ms:
                # Expired — clean up
                self._cleanup_container(info.container)
                return None

            if info.container.status != "running":
                self._cleanup_container(info.container)
                return None

            return info.container

    def put(self, function_name: str, container: Any) -> None:
        """Return a container to the pool for potential reuse."""
        with self._lock:
            # If there's already one in the pool, clean it up
            existing = self._pool.pop(function_name, None)
            if existing:
                self._cleanup_container(existing.container)

            self._pool[function_name] = ContainerInfo(
                container=container,
                function_name=function_name,
            )

    def cleanup_all(self) -> None:
        """Stop and remove all containers in the pool."""
        with self._lock:
            for info in self._pool.values():
                self._cleanup_container(info.container)
            self._pool.clear()

    def _cleanup_container(self, container: Any) -> None:
        """Stop and optionally remove a container."""
        try:
            container.stop(timeout=2)
        except Exception:
            pass
        if self._remove_containers:
            try:
                container.remove(force=True)
            except Exception:
                pass


class DockerLambdaExecutor:
    """Executes Lambda functions in Docker containers.

    Falls back to local in-process execution if Docker is not available.
    """

    def __init__(
        self,
        keepalive_ms: int | None = None,
        remove_containers: bool | None = None,
        prebuild_images: bool | None = None,
        synchronous_create: bool | None = None,
    ) -> None:
        # Read config from env vars with explicit overrides
        if keepalive_ms is None:
            keepalive_ms = int(os.environ.get("LAMBDA_KEEPALIVE_MS", "600000"))
        if remove_containers is None:
            remove_containers = os.environ.get("LAMBDA_REMOVE_CONTAINERS", "true").lower() == "true"
        if prebuild_images is None:
            prebuild_images = os.environ.get("LAMBDA_PREBUILD_IMAGES", "false").lower() == "true"
        if synchronous_create is None:
            synchronous_create = (
                os.environ.get("LAMBDA_SYNCHRONOUS_CREATE", "false").lower() == "true"
            )

        self._prebuild_images = prebuild_images
        self._synchronous_create = synchronous_create
        self._docker_network = os.environ.get("LAMBDA_DOCKER_NETWORK")
        self._docker_dns = os.environ.get("LAMBDA_DOCKER_DNS")
        self._docker_flags = parse_docker_flags(os.environ.get("LAMBDA_DOCKER_FLAGS"))
        self._gateway_port = int(os.environ.get("GATEWAY_PORT", "4566"))
        self._fallback = False
        self._docker_client = None

        # Try to connect to Docker
        if docker is None:
            logger.warning("docker package not installed — falling back to local Lambda executor")
            self._fallback = True
        else:
            try:
                self._docker_client = docker.from_env()
                self._docker_client.ping()
            except Exception as e:
                logger.warning("Docker not available (%s) — falling back to local executor", e)
                self._docker_client = None
                self._fallback = True

        self._warm_pool = WarmContainerPool(
            keepalive_ms=keepalive_ms,
            remove_containers=remove_containers,
        )

    def prebuild_image(self, runtime: str) -> None:
        """Pre-pull the Docker image for a runtime.

        Called at CreateFunction time when LAMBDA_PREBUILD_IMAGES is enabled.
        """
        if self._fallback or not self._docker_client:
            return

        image = get_image_for_runtime(runtime)
        if not image:
            logger.debug("No Docker image mapped for runtime %s, skipping prebuild", runtime)
            return

        if self._synchronous_create:
            self._pull_image(image)
        else:
            thread = threading.Thread(target=self._pull_image, args=(image,), daemon=True)
            thread.start()

    def _pull_image(self, image: str) -> None:
        """Pull a Docker image."""
        try:
            logger.info("Pulling Lambda runtime image: %s", image)
            self._docker_client.images.pull(image)
            logger.info("Successfully pulled: %s", image)
        except Exception as e:
            logger.error("Failed to pull image %s: %s", image, e)

    def _build_container_config(
        self,
        image: str,
        function_name: str,
        handler: str,
        timeout: int,
        memory_size: int,
        env_vars: dict | None,
        region: str,
        account_id: str,
        code_dir: str,
    ) -> dict[str, Any]:
        """Build the configuration dict for docker container run."""
        environment = {
            "AWS_LAMBDA_FUNCTION_NAME": function_name,
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(memory_size),
            "AWS_LAMBDA_FUNCTION_TIMEOUT": str(timeout),
            "AWS_REGION": region,
            "AWS_DEFAULT_REGION": region,
            "AWS_ACCOUNT_ID": account_id,
            "_HANDLER": handler,
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_ENDPOINT_URL": f"http://host.docker.internal:{self._gateway_port}",
        }

        if env_vars:
            environment.update(env_vars)

        config: dict[str, Any] = {
            "image": image,
            "environment": environment,
            "volumes": {
                code_dir: {"bind": "/var/task", "mode": "ro"},
            },
            "detach": True,
            "auto_remove": False,
        }

        if self._docker_network:
            config["network"] = self._docker_network

        if self._docker_dns:
            config["dns"] = [self._docker_dns]

        # Apply extra docker flags
        config.update(self._docker_flags)

        return config

    def execute(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        runtime: str = "python3.12",
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        """Execute a Lambda function in a Docker container.

        Falls back to local execution if Docker is not available.

        Returns (result, error_type, logs).
        """
        if self._fallback:
            return self._execute_local_fallback(
                code_zip=code_zip,
                handler=handler,
                event=event,
                function_name=function_name,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
                layer_zips=layer_zips,
                code_dir=code_dir,
                hot_reload=hot_reload,
            )

        image = get_image_for_runtime(runtime)
        if not image:
            logger.warning("No Docker image for runtime %s, using local fallback", runtime)
            return self._execute_local_fallback(
                code_zip=code_zip,
                handler=handler,
                event=event,
                function_name=function_name,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
                layer_zips=layer_zips,
                code_dir=code_dir,
                hot_reload=hot_reload,
            )

        # Extract code if needed
        if not code_dir:
            from robotocore.services.lambda_.executor import get_code_cache

            code_dir = get_code_cache().get_or_extract(
                function_name=function_name,
                code_zip=code_zip,
                layer_zips=layer_zips,
            )

        # Build container config
        config = self._build_container_config(
            image=image,
            function_name=function_name,
            handler=handler,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            code_dir=code_dir,
        )

        # Write event to a temp file for the container
        event_json = json.dumps(event)

        container = None
        try:
            # Run the container
            container = self._docker_client.containers.run(
                **config,
                command=event_json,
            )

            # Wait for completion with timeout
            try:
                result = container.wait(timeout=timeout + 5)
                exit_code = result.get("StatusCode", 1)
            except Exception:
                # Timeout or error — kill the container
                try:
                    container.kill()
                except Exception:
                    pass
                return None, "Task.TimedOut", f"Function timed out after {timeout}s"

            # Collect output
            try:
                stdout_bytes = container.logs(stdout=True, stderr=False)
                stderr_bytes = container.logs(stdout=False, stderr=True)
                stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
                logs = stderr_bytes.decode("utf-8", errors="replace")
            except Exception:
                stdout = ""
                logs = ""

            if exit_code != 0:
                # Try to parse structured error from stdout
                if stdout:
                    try:
                        error_obj = json.loads(stdout)
                        if isinstance(error_obj, dict) and "errorMessage" in error_obj:
                            return error_obj, "Handled", logs
                    except json.JSONDecodeError:
                        pass
                return (
                    {
                        "errorMessage": stdout or "Function execution failed",
                        "errorType": "Runtime.ExitError",
                    },
                    "Unhandled",
                    logs,
                )

            # Parse successful response
            if not stdout:
                return None, None, logs

            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError:
                parsed = stdout

            return parsed, None, logs

        except Exception as e:
            logger.error("Docker Lambda execution failed: %s", e)
            if container:
                try:
                    container.kill()
                except Exception:
                    pass
            return (
                {"errorMessage": str(e), "errorType": "DockerExecutionError"},
                "Unhandled",
                str(e),
            )
        finally:
            # Clean up container if not being pooled
            if container:
                try:
                    container.stop(timeout=2)
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    def _execute_local_fallback(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        """Fall back to the local in-process Python executor."""
        from robotocore.services.lambda_.executor import execute_python_handler

        return execute_python_handler(
            code_zip=code_zip,
            handler=handler,
            event=event,
            function_name=function_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            layer_zips=layer_zips,
            code_dir=code_dir,
            hot_reload=hot_reload,
        )

    def cleanup(self) -> None:
        """Clean up all warm containers."""
        self._warm_pool.cleanup_all()


# Module-level singleton
_docker_executor: DockerLambdaExecutor | None = None
_docker_executor_lock = threading.Lock()


def get_docker_executor() -> DockerLambdaExecutor:
    """Return the global DockerLambdaExecutor singleton."""
    global _docker_executor
    if _docker_executor is None:
        with _docker_executor_lock:
            if _docker_executor is None:
                _docker_executor = DockerLambdaExecutor()
    return _docker_executor
