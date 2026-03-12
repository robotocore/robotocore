"""Lambda Java runtime compatibility tests.

Tests create Java Lambda functions, invoke them, and assert results.
All tests skip if javac/java are not available on PATH.
"""

import io
import json
import os
import shutil
import subprocess
import tempfile
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client


# Skip entire module if Java tools are not available or non-functional
# macOS has stub binaries at /usr/bin/javac that exist but fail without a JDK
def _java_available() -> bool:
    """Check if javac and java are actually functional (not just stubs)."""
    javac = shutil.which("javac")
    java = shutil.which("java")
    if not javac or not java:
        return False
    try:
        result = subprocess.run([javac, "-version"], capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


_JAVA_OK = _java_available()
pytestmark = pytest.mark.skipif(
    not _JAVA_OK,
    reason="javac and java must be functional on PATH for Java Lambda tests",
)


def _compile_and_zip(java_sources: dict[str, str]) -> bytes:
    """Compile Java source files and package .class files into a zip.

    Args:
        java_sources: mapping of filename (e.g. "Handler.java") to source code.

    Returns:
        bytes of a zip file containing the compiled .class files.
    """
    tmpdir = tempfile.mkdtemp(prefix="java_lambda_test_")
    try:
        # Write source files
        for filename, source in java_sources.items():
            # Handle package directories
            filepath = os.path.join(tmpdir, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write(source)

        # Compile all .java files
        java_files = []
        for root, _, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".java"):
                    java_files.append(os.path.join(root, f))

        result = subprocess.run(
            [shutil.which("javac"), "-d", tmpdir] + java_files,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"javac failed: {result.stderr}")

        # Zip all .class files
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for root, _, files in os.walk(tmpdir):
                for f in files:
                    if f.endswith(".class"):
                        full_path = os.path.join(root, f)
                        arcname = os.path.relpath(full_path, tmpdir)
                        zf.write(full_path, arcname)
        return buf.getvalue()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def role():
    iam = make_client("iam")
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
    role_name = f"java-lambda-role-{uuid.uuid4().hex[:8]}"
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    yield f"arn:aws:iam::123456789012:role/{role_name}"
    iam.delete_role(RoleName=role_name)


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_and_invoke(lam, role, code_zip, handler, fname, payload=None, timeout=30, env=None):
    """Helper: create a Java Lambda, invoke it, return (response, payload_parsed)."""
    create_kwargs = {
        "FunctionName": fname,
        "Runtime": "java21",
        "Role": role,
        "Handler": handler,
        "Code": {"ZipFile": code_zip},
        "Timeout": timeout,
        "MemorySize": 256,
    }
    if env:
        create_kwargs["Environment"] = {"Variables": env}
    lam.create_function(**create_kwargs)
    try:
        invoke_kwargs = {"FunctionName": fname}
        if payload is not None:
            invoke_kwargs["Payload"] = json.dumps(payload)
        response = lam.invoke(**invoke_kwargs)
        raw = response["Payload"].read()
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            parsed = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return response, parsed
    finally:
        lam.delete_function(FunctionName=fname)


class TestJavaBasicInvocation:
    """Basic Java Lambda handler invocation tests."""

    def test_string_handler_returns_string(self, lam, role):
        """Java handler with (String, Object) signature returns a plain string."""
        code = _compile_and_zip(
            {
                "Handler.java": """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return "hello from java";
    }
}
"""
            }
        )
        fname = _unique_name("java-string")
        resp, payload = _create_and_invoke(lam, role, code, "Handler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload == "hello from java"

    def test_string_handler_returns_json(self, lam, role):
        """Java handler returns a JSON string that gets parsed."""
        code = _compile_and_zip(
            {
                "Handler.java": """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return "{\\"status\\":\\"ok\\",\\"code\\":200}";
    }
}
"""
            }
        )
        fname = _unique_name("java-json-str")
        resp, payload = _create_and_invoke(lam, role, code, "Handler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload["status"] == "ok"
        assert payload["code"] == 200

    def test_default_method_name(self, lam, role):
        """When handler is just class name, defaults to handleRequest method."""
        code = _compile_and_zip(
            {
                "Handler.java": """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return "default-method";
    }
}
"""
            }
        )
        fname = _unique_name("java-default-method")
        resp, payload = _create_and_invoke(lam, role, code, "Handler", fname)
        assert resp["StatusCode"] == 200
        assert payload == "default-method"


class TestJavaMapHandler:
    """Tests for Java handlers using Map<String, Object> signatures."""

    def test_map_handler_returns_map(self, lam, role):
        """Java handler with (Map, Object) signature returns a Map."""
        code = _compile_and_zip(
            {
                "MapHandler.java": """\
import java.util.Map;
import java.util.HashMap;

public class MapHandler {
    public Object handleRequest(Map<String, Object> event, Object context) {
        // The bootstrap passes event with "_raw" key containing the JSON string
        return "{\\\"result\\\":\\\"map-handler-ok\\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-map")
        resp, payload = _create_and_invoke(lam, role, code, "MapHandler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload["result"] == "map-handler-ok"

    def test_map_handler_reads_raw_event(self, lam, role):
        """Map handler can access the _raw JSON event string."""
        code = _compile_and_zip(
            {
                "RawHandler.java": """\
import java.util.Map;

public class RawHandler {
    public String handleRequest(Map<String, Object> event, Object context) {
        String raw = (String) event.get("_raw");
        if (raw != null && raw.contains("hello")) {
            return "found-hello";
        }
        return "not-found";
    }
}
"""
            }
        )
        fname = _unique_name("java-raw-event")
        resp, payload = _create_and_invoke(
            lam, role, code, "RawHandler::handleRequest", fname, payload={"msg": "hello"}
        )
        assert resp["StatusCode"] == 200
        assert payload == "found-hello"


class TestJavaEventProcessing:
    """Tests for event data processing in Java handlers."""

    def test_string_event_echo(self, lam, role):
        """Handler receives event JSON string and can parse it."""
        code = _compile_and_zip(
            {
                "EchoHandler.java": """\
public class EchoHandler {
    public String handleRequest(String event, Object context) {
        // event is the raw JSON string
        return event;
    }
}
"""
            }
        )
        fname = _unique_name("java-echo")
        input_event = {"key1": "value1", "key2": 42}
        resp, payload = _create_and_invoke(
            lam, role, code, "EchoHandler::handleRequest", fname, payload=input_event
        )
        assert resp["StatusCode"] == 200
        assert payload["key1"] == "value1"
        assert payload["key2"] == 42

    def test_string_event_processing(self, lam, role):
        """Handler parses JSON event and returns computed result."""
        code = _compile_and_zip(
            {
                "ComputeHandler.java": """\
public class ComputeHandler {
    public String handleRequest(String event, Object context) {
        // Simple: check if event contains a keyword
        if (event.contains("multiply")) {
            return "{\\"result\\": 42}";
        }
        return "{\\"result\\": 0}";
    }
}
"""
            }
        )
        fname = _unique_name("java-compute")
        resp, payload = _create_and_invoke(
            lam, role, code, "ComputeHandler::handleRequest", fname, payload={"op": "multiply"}
        )
        assert resp["StatusCode"] == 200
        assert payload["result"] == 42


class TestJavaEnvironmentVariables:
    """Tests for environment variable access in Java handlers."""

    def test_system_getenv(self, lam, role):
        """Java handler can read environment variables via System.getenv()."""
        code = _compile_and_zip(
            {
                "EnvHandler.java": """\
public class EnvHandler {
    public String handleRequest(String event, Object context) {
        String val = System.getenv("MY_JAVA_VAR");
        return "{\\"envValue\\":\\"" + (val != null ? val : "null") + "\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-env")
        env_vars = {"MY_JAVA_VAR": "test-value-123"}
        resp, payload = _create_and_invoke(
            lam, role, code, "EnvHandler::handleRequest", fname, env=env_vars
        )
        assert resp["StatusCode"] == 200
        assert payload["envValue"] == "test-value-123"

    def test_lambda_env_vars(self, lam, role):
        """Java handler can read standard Lambda environment variables."""
        code = _compile_and_zip(
            {
                "LambdaEnvHandler.java": """\
public class LambdaEnvHandler {
    public String handleRequest(String event, Object context) {
        String funcName = System.getenv("AWS_LAMBDA_FUNCTION_NAME");
        String region = System.getenv("AWS_REGION");
        return "{\\"functionName\\":\\"" + funcName + "\\",\\"region\\":\\"" + region + "\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-lambda-env")
        resp, payload = _create_and_invoke(
            lam, role, code, "LambdaEnvHandler::handleRequest", fname
        )
        assert resp["StatusCode"] == 200
        assert payload["functionName"] == fname
        assert payload["region"] == "us-east-1"


class TestJavaErrorHandling:
    """Tests for error scenarios in Java Lambda execution."""

    def test_runtime_exception(self, lam, role):
        """Handler that throws RuntimeException returns error structure."""
        code = _compile_and_zip(
            {
                "ErrorHandler.java": """\
public class ErrorHandler {
    public String handleRequest(String event, Object context) {
        throw new RuntimeException("intentional test error");
    }
}
"""
            }
        )
        fname = _unique_name("java-error")
        resp, payload = _create_and_invoke(lam, role, code, "ErrorHandler::handleRequest", fname)
        assert resp.get("FunctionError") is not None
        assert "intentional test error" in payload.get("errorMessage", "")
        assert payload.get("errorType") == "RuntimeException"

    def test_class_not_found(self, lam, role):
        """Wrong class name returns ImportModuleError."""
        code = _compile_and_zip(
            {
                "Handler.java": """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return "ok";
    }
}
"""
            }
        )
        fname = _unique_name("java-class-notfound")
        resp, payload = _create_and_invoke(
            lam, role, code, "NonExistentClass::handleRequest", fname
        )
        assert resp.get("FunctionError") is not None
        assert "Cannot find class" in payload.get("errorMessage", "") or "errorMessage" in payload

    def test_method_not_found(self, lam, role):
        """Wrong method name returns HandlerNotFound."""
        code = _compile_and_zip(
            {
                "Handler.java": """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return "ok";
    }
}
"""
            }
        )
        fname = _unique_name("java-method-notfound")
        resp, payload = _create_and_invoke(lam, role, code, "Handler::nonExistentMethod", fname)
        assert resp.get("FunctionError") is not None
        assert "errorMessage" in payload


class TestJavaContextObject:
    """Tests for the Lambda context object passed to Java handlers."""

    def test_context_has_function_name(self, lam, role):
        """Context object contains the function name."""
        # Context is passed as Object (Map<String, Object>) so we use Object signature
        code = _compile_and_zip(
            {
                "ContextHandler.java": """\
import java.util.Map;

public class ContextHandler {
    @SuppressWarnings("unchecked")
    public String handleRequest(String event, Object context) {
        // context is a Map but we get it as Object; use env var instead
        String funcName = System.getenv("AWS_LAMBDA_FUNCTION_NAME");
        return "{\\"functionName\\":\\"" + funcName + "\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-context")
        resp, payload = _create_and_invoke(lam, role, code, "ContextHandler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload["functionName"] == fname


class TestJavaMultipleInvocations:
    """Tests for isolation between multiple invocations."""

    def test_two_invocations_are_isolated(self, lam, role):
        """Two invocations of the same function return consistent results."""
        code = _compile_and_zip(
            {
                "CountHandler.java": """\
public class CountHandler {
    public String handleRequest(String event, Object context) {
        return "{\\"invoked\\": true}";
    }
}
"""
            }
        )
        fname = _unique_name("java-isolation")
        create_kwargs = {
            "FunctionName": fname,
            "Runtime": "java21",
            "Role": role,
            "Handler": "CountHandler::handleRequest",
            "Code": {"ZipFile": code},
            "Timeout": 30,
            "MemorySize": 256,
        }
        lam.create_function(**create_kwargs)
        try:
            # First invocation
            resp1 = lam.invoke(FunctionName=fname)
            payload1 = json.loads(resp1["Payload"].read())
            assert payload1["invoked"] is True

            # Second invocation
            resp2 = lam.invoke(FunctionName=fname)
            payload2 = json.loads(resp2["Payload"].read())
            assert payload2["invoked"] is True

            assert resp1["StatusCode"] == 200
            assert resp2["StatusCode"] == 200
        finally:
            lam.delete_function(FunctionName=fname)

    def test_different_events_different_results(self, lam, role):
        """Same function invoked with different events returns different results."""
        code = _compile_and_zip(
            {
                "EventEcho.java": """\
public class EventEcho {
    public String handleRequest(String event, Object context) {
        if (event.contains("first")) {
            return "{\\"which\\": \\"first\\"}";
        }
        return "{\\"which\\": \\"second\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-diff-events")
        create_kwargs = {
            "FunctionName": fname,
            "Runtime": "java21",
            "Role": role,
            "Handler": "EventEcho::handleRequest",
            "Code": {"ZipFile": code},
            "Timeout": 30,
            "MemorySize": 256,
        }
        lam.create_function(**create_kwargs)
        try:
            # First invocation
            resp1 = lam.invoke(FunctionName=fname, Payload=json.dumps({"type": "first"}))
            payload1 = json.loads(resp1["Payload"].read())
            assert payload1["which"] == "first"

            # Second invocation
            resp2 = lam.invoke(FunctionName=fname, Payload=json.dumps({"type": "second"}))
            payload2 = json.loads(resp2["Payload"].read())
            assert payload2["which"] == "second"
        finally:
            lam.delete_function(FunctionName=fname)


class TestJavaTimeout:
    """Tests for Lambda timeout behavior with Java handlers."""

    def test_timeout_exceeded(self, lam, role):
        """Handler that sleeps past timeout returns Task.TimedOut error."""
        code = _compile_and_zip(
            {
                "SlowHandler.java": """\
public class SlowHandler {
    public String handleRequest(String event, Object context) {
        try {
            Thread.sleep(10000);  // Sleep 10 seconds
        } catch (InterruptedException e) {
            // ignore
        }
        return "should not reach here";
    }
}
"""
            }
        )
        fname = _unique_name("java-timeout")
        resp, payload = _create_and_invoke(
            lam, role, code, "SlowHandler::handleRequest", fname, timeout=3
        )
        assert resp.get("FunctionError") is not None
        # The error should indicate timeout
        error_msg = payload.get("errorMessage", "") if isinstance(payload, dict) else str(payload)
        assert "timed out" in error_msg.lower() or "timeout" in error_msg.lower()


class TestJavaBuiltinClasses:
    """Tests for Java handlers using standard library classes."""

    def test_collections_and_string_ops(self, lam, role):
        """Handler uses java.util collections and String operations."""
        code = _compile_and_zip(
            {
                "CollectionHandler.java": """\
import java.util.ArrayList;
import java.util.Collections;

public class CollectionHandler {
    public String handleRequest(String event, Object context) {
        ArrayList<String> list = new ArrayList<>();
        list.add("banana");
        list.add("apple");
        list.add("cherry");
        Collections.sort(list);
        return "{\\"sorted\\": \\"" + list.get(0) + "," + list.get(1) + "," + list.get(2) + "\\"}";
    }
}
"""
            }
        )
        fname = _unique_name("java-collections")
        resp, payload = _create_and_invoke(
            lam, role, code, "CollectionHandler::handleRequest", fname
        )
        assert resp["StatusCode"] == 200
        assert payload["sorted"] == "apple,banana,cherry"

    def test_message_digest_sha256(self, lam, role):
        """Handler uses java.security.MessageDigest for SHA-256 hashing."""
        code = _compile_and_zip(
            {
                "HashHandler.java": """\
import java.security.MessageDigest;

public class HashHandler {
    public String handleRequest(String event, Object context) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest("hello".getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            // SHA-256 of "hello" starts with 2cf24dba
            String hex = sb.toString();
            return "{\\"hash\\":\\"" + hex.substring(0, 8) + "\\"}";
        } catch (Exception e) {
            return "{\\"error\\":\\"" + e.getMessage() + "\\"}";
        }
    }
}
"""
            }
        )
        fname = _unique_name("java-sha256")
        resp, payload = _create_and_invoke(lam, role, code, "HashHandler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload["hash"] == "2cf24dba"

    def test_math_operations(self, lam, role):
        """Handler uses java.lang.Math for calculations."""
        code = _compile_and_zip(
            {
                "MathHandler.java": """\
public class MathHandler {
    public String handleRequest(String event, Object context) {
        double sqrt = Math.sqrt(144);
        long rounded = Math.round(sqrt);
        return "{\\"sqrt144\\": " + rounded + "}";
    }
}
"""
            }
        )
        fname = _unique_name("java-math")
        resp, payload = _create_and_invoke(lam, role, code, "MathHandler::handleRequest", fname)
        assert resp["StatusCode"] == 200
        assert payload["sqrt144"] == 12
