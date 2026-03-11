"""awsroboto — thin CLI wrapper around `aws` that targets a local Robotocore instance.

Equivalent to LocalStack's ``awslocal``.  Sets environment variables so that
every ``aws`` invocation hits the local emulator and then ``exec``s the real
CLI so signals, TTY, and exit codes pass through transparently.
"""

import os
import shutil
import sys

_DEFAULT_PORT = "4566"
_DEFAULT_ENDPOINT = "http://localhost:{port}"
_DEFAULT_ACCESS_KEY = "123456789012"
_DEFAULT_SECRET_KEY = "test"
_DEFAULT_REGION = "us-east-1"


def _find_aws_cli() -> str:
    """Return the absolute path to ``aws``, or exit with a helpful message."""
    path = shutil.which("aws")
    if path is None:
        print(
            "error: the AWS CLI (aws) is not installed or not on $PATH.\n"
            "\n"
            "Install it with one of:\n"
            "  brew install awscli          # macOS\n"
            "  pip install awscli           # pip\n"
            "  uv pip install awscli        # uv\n"
            "\n"
            "See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
            file=sys.stderr,
        )
        sys.exit(127)
    return path


def _parse_wrapper_args(argv: list[str]) -> tuple[str | None, str | None, list[str]]:
    """Parse awsroboto-specific flags that appear *before* the aws arguments.

    Returns ``(endpoint_url, port, remaining_argv)``.
    """
    endpoint_url: str | None = None
    port: str | None = None
    rest: list[str] = []

    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--endpoint-url" and i + 1 < len(argv):
            endpoint_url = argv[i + 1]
            i += 2
        elif arg.startswith("--endpoint-url="):
            endpoint_url = arg.split("=", 1)[1]
            i += 1
        elif arg == "--port" and i + 1 < len(argv):
            port = argv[i + 1]
            i += 2
        elif arg.startswith("--port="):
            port = arg.split("=", 1)[1]
            i += 1
        else:
            # First non-wrapper arg — everything from here goes to aws.
            rest = argv[i:]
            break
        continue
    return endpoint_url, port, rest


def build_env(
    endpoint_url: str | None = None,
    port: str | None = None,
) -> dict[str, str]:
    """Return a copy of ``os.environ`` with Robotocore defaults applied."""
    env = os.environ.copy()

    # Determine the endpoint URL (explicit flag > ROBOTOCORE_PORT env > default).
    if endpoint_url is not None:
        resolved_endpoint = endpoint_url
    elif port is not None:
        resolved_endpoint = _DEFAULT_ENDPOINT.format(port=port)
    else:
        env_port = env.get("ROBOTOCORE_PORT", _DEFAULT_PORT)
        resolved_endpoint = _DEFAULT_ENDPOINT.format(port=env_port)

    env["AWS_ENDPOINT_URL"] = resolved_endpoint

    env.setdefault("AWS_ACCESS_KEY_ID", _DEFAULT_ACCESS_KEY)
    env.setdefault("AWS_SECRET_ACCESS_KEY", _DEFAULT_SECRET_KEY)
    env.setdefault("AWS_DEFAULT_REGION", _DEFAULT_REGION)

    return env


def main() -> None:  # pragma: no cover — exec replaces the process
    """Entry point for the ``awsroboto`` console script."""
    aws_path = _find_aws_cli()
    endpoint_url, port, aws_args = _parse_wrapper_args(sys.argv[1:])
    env = build_env(endpoint_url=endpoint_url, port=port)

    # Replace this process with `aws ...`.
    os.environ.update(env)
    os.execvp(aws_path, [aws_path, *aws_args])
