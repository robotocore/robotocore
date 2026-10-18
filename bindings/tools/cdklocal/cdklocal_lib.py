"""Shared logic for cdklocal CLI wrappers (bash helper and Python version).

Builds the environment variables and command-line arguments needed to run
AWS CDK against a robotocore endpoint.
"""

from __future__ import annotations

DEFAULT_HOST = "localhost"
DEFAULT_PORT = "4566"
DEFAULT_SCHEME = "http"
DEFAULT_ACCOUNT = "000000000000"
DEFAULT_REGION = "us-east-1"


def build_endpoint(env: dict[str, str]) -> str:
    """Build the endpoint URL from environment variables.

    Priority:
    1. ROBOTOCORE_ENDPOINT (full URL override)
    2. ROBOTOCORE_HOST + ROBOTOCORE_PORT (components)
    3. Defaults: http://localhost:4566
    """
    if "ROBOTOCORE_ENDPOINT" in env:
        return env["ROBOTOCORE_ENDPOINT"]

    host = env.get("ROBOTOCORE_HOST", DEFAULT_HOST)
    port = env.get("ROBOTOCORE_PORT", DEFAULT_PORT)
    return f"{DEFAULT_SCHEME}://{host}:{port}"


def build_env(current_env: dict[str, str], args: list[str]) -> dict[str, str]:
    """Build the environment dict for running CDK.

    Sets defaults for endpoint, credentials, account, and region.
    Does NOT override values the user has already set.
    """
    env: dict[str, str] = dict(current_env)

    # Endpoint
    env.setdefault("AWS_ENDPOINT_URL", build_endpoint(current_env))

    # Credentials — don't override user-set values
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")

    # CDK defaults
    env.setdefault("CDK_DEFAULT_ACCOUNT", DEFAULT_ACCOUNT)
    env.setdefault("CDK_DEFAULT_REGION", DEFAULT_REGION)

    # Bootstrap-specific
    if _is_bootstrap(args):
        env.setdefault("CDK_NEW_BOOTSTRAP", "1")

    return env


def build_command(args: list[str]) -> list[str]:
    """Build the full command to execute.

    For bootstrap commands, adds --cloudformation-execution-policies
    if not already specified by the user.
    """
    cmd = ["cdk"] + list(args)

    if _is_bootstrap(args) and "--cloudformation-execution-policies" not in args:
        cmd.extend(
            [
                "--cloudformation-execution-policies",
                "arn:aws:iam::aws:policy/AdministratorAccess",
            ]
        )

    return cmd


def _is_bootstrap(args: list[str]) -> bool:
    """Check if the CDK command is a bootstrap command."""
    return "bootstrap" in args
