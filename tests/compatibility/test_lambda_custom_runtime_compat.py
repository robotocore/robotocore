"""Lambda custom runtime (provided, provided.al2, provided.al2023) compatibility tests.

Tests bootstrap-based Lambda execution for Go/Rust/compiled-language runtimes.
The bootstrap executable is a shell script that reads JSON from stdin and writes
JSON to stdout, mimicking the custom runtime API contract.
"""

import io
import json
import platform
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client

# Skip entire module on Windows (bash required for bootstrap scripts)
pytestmark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="Custom runtime tests require bash (Unix only)",
)

ENDPOINT_URL = "http://localhost:4566"


def _unique_name(prefix: str = "crt") -> str:
    """Generate a unique function name."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_bootstrap_zip(script: str, filename: str = "bootstrap") -> bytes:
    """Create a zip with an executable bootstrap script.

    Uses ZipInfo.external_attr to set Unix executable permissions (0o755).
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        info = zipfile.ZipInfo(filename)
        # Set Unix permissions: rwxr-xr-x (0o755) in the external_attr field
        info.external_attr = 0o755 << 16
        zf.writestr(info, script)
    return buf.getvalue()


def _make_zip_no_bootstrap() -> bytes:
    """Create a zip without a bootstrap file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("README.txt", "no bootstrap here")
    return buf.getvalue()


def _make_zip_non_executable_bootstrap(script: str) -> bytes:
    """Create a zip with a non-executable bootstrap file (permissions 0o644)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        info = zipfile.ZipInfo("bootstrap")
        # Set Unix permissions: rw-r--r-- (0o644) — NOT executable
        info.external_attr = 0o644 << 16
        zf.writestr(info, script)
    return buf.getvalue()


# --- Shared fixtures ---


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def iam_role():
    iam = make_client("iam")
    role_name = f"lambda-crt-role-{uuid.uuid4().hex[:8]}"
    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    arn = f"arn:aws:iam::123456789012:role/{role_name}"
    yield arn
    iam.delete_role(RoleName=role_name)


# Simple echo bootstrap: reads stdin, returns it as-is
ECHO_BOOTSTRAP = """#!/bin/bash
read event
echo "$event"
"""

# Bootstrap that transforms the event
TRANSFORM_BOOTSTRAP = (
    "#!/bin/bash\n"
    "read event\n"
    'name=$(echo "$event" | python3 -c "'
    "import sys,json; "
    "print(json.loads(sys.stdin.read()).get('name','world'))"
    '")\n'
    'echo "{\\"greeting\\": \\"Hello, $name!\\"}"'
    "\n"
)

# Bootstrap that reads env vars
ENV_BOOTSTRAP = (
    "#!/bin/bash\n"
    "read event\n"
    'fn_name="$AWS_LAMBDA_FUNCTION_NAME"\n'
    'region="$AWS_REGION"\n'
    'handler="$_HANDLER"\n'
    'echo "{\\"function_name\\": \\"$fn_name\\",'
    ' \\"region\\": \\"$region\\",'
    ' \\"handler\\": \\"$handler\\"}"\n'
)

# Bootstrap that exits with error
ERROR_BOOTSTRAP = """#!/bin/bash
echo "something went wrong" >&2
exit 1
"""

# Bootstrap that times out
TIMEOUT_BOOTSTRAP = """#!/bin/bash
sleep 30
echo "{}"
"""

# Bootstrap that writes to stderr (logs)
STDERR_BOOTSTRAP = """#!/bin/bash
read event
echo "LOG: processing event" >&2
echo "LOG: event received" >&2
echo "{\\\"status\\\": \\\"ok\\\"}"
"""

# Bootstrap with custom env var access
CUSTOM_ENV_BOOTSTRAP = """#!/bin/bash
read event
echo "{\\\"my_var\\\": \\\"$MY_CUSTOM_VAR\\\"}"
"""


class TestCustomRuntimeBasic:
    """Basic custom runtime execution tests."""

    def test_shell_script_bootstrap_echo(self, lam, iam_role):
        """Shell script bootstrap reads stdin and echoes JSON response."""
        fname = _unique_name("echo")
        code = _make_bootstrap_zip(ECHO_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"message": "hello custom runtime"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["message"] == "hello custom runtime"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_event_processing(self, lam, iam_role):
        """Bootstrap parses JSON event and returns transformed data."""
        fname = _unique_name("transform")
        code = _make_bootstrap_zip(TRANSFORM_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"name": "Robotocore"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["greeting"] == "Hello, Robotocore!"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_env_vars(self, lam, iam_role):
        """Bootstrap can access Lambda environment variables."""
        fname = _unique_name("envvars")
        code = _make_bootstrap_zip(ENV_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap.handler",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["function_name"] == fname
            assert payload["region"] == "us-east-1"
            assert payload["handler"] == "bootstrap.handler"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_custom_env_var(self, lam, iam_role):
        """Custom environment variables passed via function config are accessible."""
        fname = _unique_name("customenv")
        code = _make_bootstrap_zip(CUSTOM_ENV_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
            Environment={"Variables": {"MY_CUSTOM_VAR": "custom_value_42"}},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["my_var"] == "custom_value_42"
        finally:
            lam.delete_function(FunctionName=fname)


class TestCustomRuntimeErrors:
    """Error handling for custom runtime execution."""

    def test_bootstrap_error_nonzero_exit(self, lam, iam_role):
        """Bootstrap exits with non-zero code produces error response."""
        fname = _unique_name("error")
        code = _make_bootstrap_zip(ERROR_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
            )
            payload = json.loads(resp["Payload"].read())
            # Should indicate an error
            assert "errorMessage" in payload or "errorType" in payload
            # FunctionError header should be present
            assert resp.get("FunctionError") is not None
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_not_found(self, lam, iam_role):
        """Zip without bootstrap file produces meaningful error."""
        fname = _unique_name("noboot")
        code = _make_zip_no_bootstrap()
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
            )
            # FunctionError header indicates the error type
            assert resp.get("FunctionError") is not None
            assert "InvalidEntrypoint" in resp["FunctionError"]
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_not_executable_still_runs(self, lam, iam_role):
        """Bootstrap without +x permission is auto-fixed by executor (chmod)."""
        fname = _unique_name("noexec")
        # The executor does os.chmod(bootstrap_path, st.st_mode | stat.S_IEXEC)
        # so even non-executable bootstraps should work
        code = _make_zip_non_executable_bootstrap(ECHO_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"fixed": True}),
            )
            payload = json.loads(resp["Payload"].read())
            # Should succeed because executor chmod's the file
            assert payload["fixed"] is True
        finally:
            lam.delete_function(FunctionName=fname)

    def test_bootstrap_timeout(self, lam, iam_role):
        """Bootstrap that exceeds timeout produces timeout error."""
        fname = _unique_name("timeout")
        code = _make_bootstrap_zip(TIMEOUT_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=3,  # 3 second timeout, bootstrap sleeps 30
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
            )
            # FunctionError header indicates a timeout
            assert resp.get("FunctionError") is not None
            assert "TimedOut" in resp["FunctionError"]
        finally:
            lam.delete_function(FunctionName=fname)


class TestCustomRuntimeLogs:
    """Log capture from custom runtime execution."""

    def test_bootstrap_stderr_captured(self, lam, iam_role):
        """Stderr output from bootstrap is captured in log result."""
        fname = _unique_name("stderr")
        code = _make_bootstrap_zip(STDERR_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({}),
                LogType="Tail",
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["status"] == "ok"
            # LogResult should be present when LogType=Tail
            assert "LogResult" in resp
        finally:
            lam.delete_function(FunctionName=fname)


class TestCustomRuntimeIsolation:
    """Verify invocation isolation between calls."""

    def test_multiple_invocations_isolated(self, lam, iam_role):
        """Multiple invocations of same function return independent results."""
        fname = _unique_name("isolation")
        code = _make_bootstrap_zip(ECHO_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime="provided.al2023",
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            # Invoke with different payloads
            for i in range(3):
                resp = lam.invoke(
                    FunctionName=fname,
                    Payload=json.dumps({"invocation": i}),
                )
                payload = json.loads(resp["Payload"].read())
                assert payload["invocation"] == i
        finally:
            lam.delete_function(FunctionName=fname)


class TestCustomRuntimeVariants:
    """Test all three custom runtime variants."""

    @pytest.mark.parametrize(
        "runtime",
        ["provided", "provided.al2", "provided.al2023"],
        ids=["provided", "provided-al2", "provided-al2023"],
    )
    def test_runtime_variant_executes(self, lam, iam_role, runtime):
        """Each custom runtime variant (provided, provided.al2, provided.al2023) works."""
        fname = _unique_name(f"variant-{runtime.replace('.', '-')}")
        code = _make_bootstrap_zip(ECHO_BOOTSTRAP)
        lam.create_function(
            FunctionName=fname,
            Runtime=runtime,
            Role=iam_role,
            Handler="bootstrap",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"runtime": runtime}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["runtime"] == runtime
        finally:
            lam.delete_function(FunctionName=fname)
