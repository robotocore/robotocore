"""Docker-based Lambda executor -- runs Lambda functions in isolated Docker containers.

Uses the docker CLI via subprocess (no docker-py dependency) to run functions in
official AWS Lambda runtime images. Supports all AWS runtimes: Python, Node.js,
Java, .NET, Ruby, Go, and custom runtimes.

Configuration via environment variables:
  LAMBDA_EXECUTOR: "local" (default) or "docker"
  LAMBDA_DOCKER_NETWORK: Docker network name for Lambda containers
  LAMBDA_DOCKER_DNS: Custom DNS for Lambda containers
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading

logger = logging.getLogger(__name__)

# Default runtime-to-image mapping (official AWS Lambda runtime images)
RUNTIME_IMAGES: dict[str, str] = {
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
    # .NET
    "dotnet6": "public.ecr.aws/lambda/dotnet:6",
    "dotnet8": "public.ecr.aws/lambda/dotnet:8",
    # Go (uses custom runtime)
    "go1.x": "public.ecr.aws/lambda/go:1",
    # Ruby
    "ruby3.2": "public.ecr.aws/lambda/ruby:3.2",
    "ruby3.3": "public.ecr.aws/lambda/ruby:3.3",
    # Custom / provided
    "provided": "public.ecr.aws/lambda/provided:al2",
    "provided.al2": "public.ecr.aws/lambda/provided:al2",
    "provided.al2023": "public.ecr.aws/lambda/provided:al2023",
}


def get_executor_mode() -> str:
    """Return the configured executor mode: 'local' or 'docker'.

    Checks LAMBDA_EXECUTOR first (matches the documented config name),
    then falls back to LAMBDA_RUNTIME_EXECUTOR for backward compatibility.
    """
    mode = os.environ.get("LAMBDA_EXECUTOR", "")
    if not mode:
        mode = os.environ.get("LAMBDA_RUNTIME_EXECUTOR", "local")
    return mode.lower()


def get_image_for_runtime(runtime: str) -> str | None:
    """Get the Docker image for a given AWS runtime string.

    Returns None if no image is mapped for the runtime.
    """
    return RUNTIME_IMAGES.get(runtime)


def is_docker_available() -> bool:
    """Check if the docker CLI is available and the daemon is running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def _build_docker_run_cmd(
    image: str,
    container_name: str,
    code_dir: str,
    handler: str,
    function_name: str,
    timeout: int,
    memory_size: int,
    env_vars: dict[str, str] | None,
    region: str,
    account_id: str,
    gateway_port: int,
    docker_network: str | None = None,
    docker_dns: str | None = None,
) -> list[str]:
    """Build the `docker run` command line for a Lambda invocation."""
    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        # Mount function code read-only
        "-v",
        f"{code_dir}:/var/task:ro",
        # Core Lambda environment variables
        "-e",
        f"AWS_LAMBDA_FUNCTION_NAME={function_name}",
        "-e",
        f"AWS_LAMBDA_FUNCTION_MEMORY_SIZE={memory_size}",
        "-e",
        f"AWS_LAMBDA_FUNCTION_TIMEOUT={timeout}",
        "-e",
        f"AWS_REGION={region}",
        "-e",
        f"AWS_DEFAULT_REGION={region}",
        "-e",
        f"AWS_ACCOUNT_ID={account_id}",
        "-e",
        f"_HANDLER={handler}",
        "-e",
        "AWS_ACCESS_KEY_ID=testing",
        "-e",
        "AWS_SECRET_ACCESS_KEY=testing",
        "-e",
        f"AWS_ENDPOINT_URL=http://host.docker.internal:{gateway_port}",
    ]

    # Pass through function environment variables
    if env_vars:
        for key, value in env_vars.items():
            cmd.extend(["-e", f"{key}={value}"])

    # Docker network
    if docker_network:
        cmd.extend(["--network", docker_network])

    # Custom DNS
    if docker_dns:
        cmd.extend(["--dns", docker_dns])

    # Image and handler (the Lambda runtime images accept the event as the command)
    cmd.append(image)
    cmd.append(handler)

    return cmd


def _invoke_via_runtime_interface(
    container_name: str,
    event: dict,
    timeout: int,
) -> tuple[str, str]:
    """Invoke the Lambda function via the Runtime Interface Client.

    The official Lambda runtime images start an HTTP server on port 8080
    that accepts POST /2015-03-31/functions/function/invocations.

    Returns (stdout, stderr) from the curl call.
    """
    event_json = json.dumps(event)
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "curl",
                "-s",
                "-X",
                "POST",
                "http://localhost:8080/2015-03-31/functions/function/invocations",
                "-d",
                event_json,
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )
        return result.stdout.strip(), result.stderr
    except subprocess.TimeoutExpired:
        return "", f"Function invocation timed out after {timeout}s"
    except FileNotFoundError:
        return "", "curl not available in container"


class DockerLambdaExecutor:
    """Executes Lambda functions in Docker containers using the docker CLI.

    Falls back to local in-process execution if Docker is not available.
    """

    def __init__(self) -> None:
        self._gateway_port = int(os.environ.get("GATEWAY_PORT", "4566"))
        self._docker_network = os.environ.get("LAMBDA_DOCKER_NETWORK")
        self._docker_dns = os.environ.get("LAMBDA_DOCKER_DNS")
        self._docker_available = is_docker_available()

        if not self._docker_available:
            logger.warning("Docker not available -- falling back to local Lambda executor")

    def execute(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        runtime: str = "python3.12",
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict[str, str] | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        """Execute a Lambda function in a Docker container.

        Falls back to local execution if Docker is not available or if
        the runtime has no mapped image.

        Returns (result, error_type, logs).
        """
        if not self._docker_available:
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

        # Extract code to a temp directory if not provided
        if not code_dir:
            from robotocore.services.lambda_.executor import get_code_cache

            code_dir = get_code_cache().get_or_extract(
                function_name=function_name,
                code_zip=code_zip,
                layer_zips=layer_zips,
            )

        # Generate a unique container name
        import uuid

        container_name = f"robotocore-lambda-{function_name}-{uuid.uuid4().hex[:8]}"

        # Build the docker run command
        cmd = _build_docker_run_cmd(
            image=image,
            container_name=container_name,
            code_dir=code_dir,
            handler=handler,
            function_name=function_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            gateway_port=self._gateway_port,
            docker_network=self._docker_network,
            docker_dns=self._docker_dns,
        )

        # The Lambda runtime images accept the event JSON as stdin or as the
        # command argument. We use stdin for reliability with large payloads.
        # Replace the handler at the end with just the image (handler is set via _HANDLER env).
        # Actually, the official images use the CMD as the handler override,
        # and read the event via the Runtime Interface. We run the container
        # in foreground mode: it starts the runtime, and we POST the event
        # to port 8080.
        #
        # Simpler approach: use `docker run --rm` with the event passed to
        # the container's stdin. The official Lambda images' entrypoint
        # accepts the event as the command argument for simple invocations.
        event_json = json.dumps(event)

        try:
            proc = subprocess.run(
                cmd,
                input=event_json,
                capture_output=True,
                text=True,
                timeout=timeout + 10,  # grace period for container startup
            )
        except subprocess.TimeoutExpired:
            # Kill the container on timeout
            self._force_remove_container(container_name)
            return (
                {"errorMessage": f"Task timed out after {timeout}s", "errorType": "Task.TimedOut"},
                "Task.TimedOut",
                f"Function timed out after {timeout}s",
            )
        except FileNotFoundError:
            logger.error("docker CLI not found")
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

        stdout = proc.stdout.strip()
        logs = proc.stderr

        if proc.returncode != 0:
            # Try to parse structured error from stdout
            if stdout:
                try:
                    error_obj = json.loads(stdout)
                    if isinstance(error_obj, dict) and "errorMessage" in error_obj:
                        return error_obj, "Handled", logs
                except json.JSONDecodeError:
                    pass  # stdout isn't JSON; fall through to generic error
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

    def _force_remove_container(self, container_name: str) -> None:
        """Force-remove a container by name. Best-effort, ignores errors."""
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                capture_output=True,
                timeout=10,
            )
        except Exception:
            pass  # Best-effort cleanup; container may already be gone

    def _execute_local_fallback(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict[str, str] | None = None,
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
        """No-op for subprocess-based executor (containers use --rm)."""
        pass


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


def reset_docker_executor() -> None:
    """Reset the singleton (for testing)."""
    global _docker_executor
    with _docker_executor_lock:
        _docker_executor = None
