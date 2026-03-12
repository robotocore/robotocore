"""Lambda Ruby runtime compatibility tests — create, invoke, assert results."""

import io
import json
import shutil
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client

# Skip entire module if ruby binary is not available
pytestmark = pytest.mark.skipif(
    shutil.which("ruby") is None,
    reason="Ruby binary not found on PATH",
)


def _make_ruby_zip(code: str, filename: str = "lambda_function.rb") -> bytes:
    """Create a zip file containing a Ruby handler."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
    return buf.getvalue()


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
    role_name = f"ruby-lambda-role-{uuid.uuid4().hex[:8]}"
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    yield f"arn:aws:iam::123456789012:role/{role_name}"
    iam.delete_role(RoleName=role_name)


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_and_invoke(lam, role, code, fname, payload=None, timeout=3, memory=128, env=None):
    """Helper: create a Ruby Lambda, invoke it, return (response, payload_parsed)."""
    zip_bytes = _make_ruby_zip(code)
    kwargs = {
        "FunctionName": fname,
        "Runtime": "ruby3.2",
        "Role": role,
        "Handler": "lambda_function.handler",
        "Code": {"ZipFile": zip_bytes},
        "Timeout": timeout,
        "MemorySize": memory,
    }
    if env:
        kwargs["Environment"] = {"Variables": env}
    lam.create_function(**kwargs)
    invoke_kwargs = {"FunctionName": fname}
    if payload is not None:
        invoke_kwargs["Payload"] = json.dumps(payload)
    response = lam.invoke(**invoke_kwargs)
    parsed = json.loads(response["Payload"].read())
    return response, parsed


class TestRubyBasicHandler:
    """Basic Ruby Lambda handler creation and invocation."""

    def test_simple_hash_return(self, lam, role):
        """Handler returning a Ruby Hash should become a JSON object."""
        code = """
def handler(event:, context:)
  { statusCode: 200, body: "hello from ruby" }
end
"""
        fname = _unique_name("ruby-simple")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert resp["StatusCode"] == 200
        assert payload["statusCode"] == 200
        assert payload["body"] == "hello from ruby"
        lam.delete_function(FunctionName=fname)

    def test_array_return(self, lam, role):
        """Handler returning a Ruby Array should become a JSON array."""
        code = """
def handler(event:, context:)
  [1, 2, 3, "four"]
end
"""
        fname = _unique_name("ruby-array")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert resp["StatusCode"] == 200
        assert payload == [1, 2, 3, "four"]
        lam.delete_function(FunctionName=fname)

    def test_string_return(self, lam, role):
        """Handler returning a plain string."""
        code = """
def handler(event:, context:)
  "just a string"
end
"""
        fname = _unique_name("ruby-string")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert resp["StatusCode"] == 200
        assert payload == "just a string"
        lam.delete_function(FunctionName=fname)

    def test_integer_return(self, lam, role):
        """Handler returning an integer."""
        code = """
def handler(event:, context:)
  42
end
"""
        fname = _unique_name("ruby-int")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert resp["StatusCode"] == 200
        assert payload == 42
        lam.delete_function(FunctionName=fname)

    def test_nil_return(self, lam, role):
        """Handler returning nil should produce null."""
        code = """
def handler(event:, context:)
  nil
end
"""
        fname = _unique_name("ruby-nil")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        raw = response["Payload"].read().decode()
        # nil serializes to JSON "null"
        assert raw.strip() == "null" or raw.strip() == ""
        lam.delete_function(FunctionName=fname)


class TestRubyEventProcessing:
    """Tests that verify event data flows correctly to the Ruby handler."""

    def test_event_echo(self, lam, role):
        """Handler that echoes back the event."""
        code = """
def handler(event:, context:)
  event
end
"""
        fname = _unique_name("ruby-echo")
        resp, payload = _create_and_invoke(lam, role, code, fname, payload={"key": "value"})
        assert payload["key"] == "value"
        lam.delete_function(FunctionName=fname)

    def test_event_processing(self, lam, role):
        """Handler that processes event data and returns computed result."""
        code = """
def handler(event:, context:)
  nums = event["numbers"] || []
  { sum: nums.sum, count: nums.length }
end
"""
        fname = _unique_name("ruby-process")
        resp, payload = _create_and_invoke(
            lam, role, code, fname, payload={"numbers": [10, 20, 30]}
        )
        assert payload["sum"] == 60
        assert payload["count"] == 3
        lam.delete_function(FunctionName=fname)

    def test_nested_event(self, lam, role):
        """Handler accessing nested event fields."""
        code = """
def handler(event:, context:)
  { name: event.dig("user", "name"), age: event.dig("user", "age") }
end
"""
        fname = _unique_name("ruby-nested")
        resp, payload = _create_and_invoke(
            lam, role, code, fname, payload={"user": {"name": "Alice", "age": 30}}
        )
        assert payload["name"] == "Alice"
        assert payload["age"] == 30
        lam.delete_function(FunctionName=fname)


class TestRubyContext:
    """Tests that verify the Lambda context object is populated correctly."""

    def test_context_function_name(self, lam, role):
        """Context should contain the function name."""
        code = """
def handler(event:, context:)
  { function_name: context.function_name }
end
"""
        fname = _unique_name("ruby-ctx-name")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["function_name"] == fname
        lam.delete_function(FunctionName=fname)

    def test_context_memory_limit(self, lam, role):
        """Context should contain memory_limit_in_mb."""
        code = """
def handler(event:, context:)
  { memory: context.memory_limit_in_mb }
end
"""
        fname = _unique_name("ruby-ctx-mem")
        resp, payload = _create_and_invoke(lam, role, code, fname, memory=256)
        assert payload["memory"] == 256
        lam.delete_function(FunctionName=fname)

    def test_context_function_version(self, lam, role):
        """Context should have function_version as $LATEST."""
        code = """
def handler(event:, context:)
  { version: context.function_version }
end
"""
        fname = _unique_name("ruby-ctx-ver")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["version"] == "$LATEST"
        lam.delete_function(FunctionName=fname)

    def test_context_invoked_function_arn(self, lam, role):
        """Context should have a properly formatted ARN."""
        code = """
def handler(event:, context:)
  { arn: context.invoked_function_arn }
end
"""
        fname = _unique_name("ruby-ctx-arn")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert fname in payload["arn"]
        assert payload["arn"].startswith("arn:aws:lambda:")
        lam.delete_function(FunctionName=fname)

    def test_context_aws_request_id(self, lam, role):
        """Context should have a UUID-format request ID."""
        code = """
def handler(event:, context:)
  { request_id: context.aws_request_id }
end
"""
        fname = _unique_name("ruby-ctx-reqid")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        # Should be a valid UUID (36 chars with hyphens)
        assert len(payload["request_id"]) == 36
        assert payload["request_id"].count("-") == 4
        lam.delete_function(FunctionName=fname)


class TestRubyEnvironmentVariables:
    """Tests that environment variables are accessible in Ruby handlers."""

    def test_custom_env_vars(self, lam, role):
        """Custom environment variables should be accessible via ENV."""
        code = """
def handler(event:, context:)
  { my_var: ENV["MY_CUSTOM_VAR"], another: ENV["ANOTHER_VAR"] }
end
"""
        fname = _unique_name("ruby-env")
        resp, payload = _create_and_invoke(
            lam, role, code, fname, env={"MY_CUSTOM_VAR": "hello", "ANOTHER_VAR": "world"}
        )
        assert payload["my_var"] == "hello"
        assert payload["another"] == "world"
        lam.delete_function(FunctionName=fname)

    def test_aws_env_vars(self, lam, role):
        """AWS-injected env vars should be present (AWS_REGION, etc.)."""
        code = """
def handler(event:, context:)
  { region: ENV["AWS_REGION"], func_name: ENV["AWS_LAMBDA_FUNCTION_NAME"] }
end
"""
        fname = _unique_name("ruby-aws-env")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["region"] == "us-east-1"
        assert payload["func_name"] == fname
        lam.delete_function(FunctionName=fname)


class TestRubyErrorHandling:
    """Tests for error scenarios in Ruby Lambda execution."""

    def test_runtime_error(self, lam, role):
        """Handler that raises RuntimeError should return FunctionError."""
        code = """
def handler(event:, context:)
  raise RuntimeError, "something went wrong"
end
"""
        fname = _unique_name("ruby-rterr")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "something went wrong" in payload["errorMessage"]
        assert payload["errorType"] == "RuntimeError"
        lam.delete_function(FunctionName=fname)

    def test_standard_error(self, lam, role):
        """Handler that raises StandardError."""
        code = """
def handler(event:, context:)
  raise StandardError, "std error"
end
"""
        fname = _unique_name("ruby-stderr")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "std error" in payload["errorMessage"]
        lam.delete_function(FunctionName=fname)

    def test_handler_not_found_wrong_method(self, lam, role):
        """Wrong handler method name should produce an error."""
        code = """
def some_other_method(event:, context:)
  { ok: true }
end
"""
        fname = _unique_name("ruby-nomethod")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "errorMessage" in payload
        lam.delete_function(FunctionName=fname)

    def test_handler_not_found_wrong_module(self, lam, role):
        """Wrong module name should produce an import error."""
        code = """
def handler(event:, context:)
  { ok: true }
end
"""
        fname = _unique_name("ruby-nomod")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="nonexistent_module.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "errorMessage" in payload
        lam.delete_function(FunctionName=fname)

    def test_syntax_error(self, lam, role):
        """Ruby file with syntax error should produce an error."""
        code = """
def handler(event:, context:
  { ok: true }
end
"""
        fname = _unique_name("ruby-syntax")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "errorMessage" in payload
        lam.delete_function(FunctionName=fname)


class TestRubyMultipleInvocations:
    """Tests for invoking the same function multiple times."""

    def test_multiple_invocations_different_inputs(self, lam, role):
        """Same function, different inputs, verify isolation."""
        code = """
def handler(event:, context:)
  { doubled: event["x"] * 2 }
end
"""
        fname = _unique_name("ruby-multi")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        for x_val in [5, 10, 100]:
            response = lam.invoke(FunctionName=fname, Payload=json.dumps({"x": x_val}))
            payload = json.loads(response["Payload"].read())
            assert payload["doubled"] == x_val * 2
        lam.delete_function(FunctionName=fname)

    def test_stateless_between_invocations(self, lam, role):
        """Global state should not leak between subprocess invocations."""
        code = """
$counter = 0

def handler(event:, context:)
  $counter += 1
  { counter: $counter }
end
"""
        fname = _unique_name("ruby-stateless")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        # Each invocation is a fresh subprocess, so counter should always be 1
        for _ in range(3):
            response = lam.invoke(FunctionName=fname)
            payload = json.loads(response["Payload"].read())
            assert payload["counter"] == 1
        lam.delete_function(FunctionName=fname)


class TestRubyTimeout:
    """Tests for timeout behavior."""

    def test_timeout_exceeded(self, lam, role):
        """Handler that sleeps past timeout should return error."""
        code = """
def handler(event:, context:)
  sleep 10
  { ok: true }
end
"""
        fname = _unique_name("ruby-timeout")
        zip_bytes = _make_ruby_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime="ruby3.2",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=2,
        )
        response = lam.invoke(FunctionName=fname)
        assert response.get("FunctionError") is not None
        payload = json.loads(response["Payload"].read())
        assert "errorMessage" in payload
        # Should mention timeout
        err_msg = payload["errorMessage"].lower()
        assert "timed out" in err_msg or "timeout" in err_msg
        lam.delete_function(FunctionName=fname)


class TestRubyBuiltinModules:
    """Tests for using Ruby standard library modules."""

    def test_json_module(self, lam, role):
        """Handler using JSON module explicitly."""
        code = """
require "json"

def handler(event:, context:)
  parsed = JSON.parse('{"inner": "data"}')
  { result: parsed["inner"] }
end
"""
        fname = _unique_name("ruby-json")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["result"] == "data"
        lam.delete_function(FunctionName=fname)

    def test_base64_module(self, lam, role):
        """Handler using Base64 encoding."""
        code = """
require "base64"

def handler(event:, context:)
  encoded = Base64.strict_encode64("hello world")
  decoded = Base64.strict_decode64(encoded)
  { encoded: encoded, decoded: decoded }
end
"""
        fname = _unique_name("ruby-base64")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["encoded"] == "aGVsbG8gd29ybGQ="
        assert payload["decoded"] == "hello world"
        lam.delete_function(FunctionName=fname)

    def test_digest_module(self, lam, role):
        """Handler using Digest for SHA256."""
        code = """
require "digest"

def handler(event:, context:)
  hash = Digest::SHA256.hexdigest("test")
  { sha256: hash }
end
"""
        fname = _unique_name("ruby-digest")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        expected = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        assert payload["sha256"] == expected
        lam.delete_function(FunctionName=fname)

    def test_securerandom_module(self, lam, role):
        """Handler using SecureRandom."""
        code = """
require "securerandom"

def handler(event:, context:)
  { uuid: SecureRandom.uuid, hex: SecureRandom.hex(8) }
end
"""
        fname = _unique_name("ruby-secrand")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert len(payload["uuid"]) == 36
        assert len(payload["hex"]) == 16  # 8 bytes = 16 hex chars
        lam.delete_function(FunctionName=fname)


class TestRubyStringEncoding:
    """Tests for string encoding handling."""

    def test_utf8_string(self, lam, role):
        """Handler returning UTF-8 strings with special characters."""
        code = """
def handler(event:, context:)
  { greeting: "Hello, \u4e16\u754c!", emoji: "\u2764\ufe0f" }
end
"""
        fname = _unique_name("ruby-utf8")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert resp["StatusCode"] == 200
        # The handler returns unicode escape sequences which Ruby processes
        assert "greeting" in payload
        lam.delete_function(FunctionName=fname)

    def test_multiline_string(self, lam, role):
        """Handler returning multiline string."""
        code = """
def handler(event:, context:)
  { text: "line1\\nline2\\nline3" }
end
"""
        fname = _unique_name("ruby-multiline")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert "line1" in payload["text"]
        assert "line2" in payload["text"]
        lam.delete_function(FunctionName=fname)


class TestRubyBooleanAndNumericTypes:
    """Tests for correct type serialization from Ruby."""

    def test_boolean_values(self, lam, role):
        """Ruby true/false should serialize to JSON true/false."""
        code = """
def handler(event:, context:)
  { t: true, f: false }
end
"""
        fname = _unique_name("ruby-bool")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert payload["t"] is True
        assert payload["f"] is False
        lam.delete_function(FunctionName=fname)

    def test_float_values(self, lam, role):
        """Ruby floats should serialize correctly."""
        code = """
def handler(event:, context:)
  { pi: 3.14159, neg: -1.5 }
end
"""
        fname = _unique_name("ruby-float")
        resp, payload = _create_and_invoke(lam, role, code, fname)
        assert abs(payload["pi"] - 3.14159) < 0.001
        assert payload["neg"] == -1.5
        lam.delete_function(FunctionName=fname)
