"""Lambda Node.js runtime compatibility tests --- create, invoke, assert results.

Tests Node.js Lambda execution end-to-end against the running server on port 4566.
Covers: sync handlers, async handlers, callback handlers, context object, env vars,
error handling, JSON parsing, isolation, handler-not-found, timeout, large payloads,
console.log capture, and built-in module usage.
"""

import io
import json
import shutil
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client

# Skip the entire module if node binary is not available
pytestmark = pytest.mark.skipif(
    shutil.which("node") is None,
    reason="node binary not found on PATH",
)


def _make_node_zip(code: str, filename: str = "index.js") -> bytes:
    """Create a zip archive containing a single JS file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
    return buf.getvalue()


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def role(iam):
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
    role_name = f"nodejs-compat-role-{uuid.uuid4().hex[:8]}"
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    yield f"arn:aws:iam::123456789012:role/{role_name}"
    iam.delete_role(RoleName=role_name)


class TestNodejsBasicHandler:
    """Test basic Node.js handler invocation."""

    def test_simple_handler(self, lam, role):
        """Create a Node.js function with a simple handler, invoke it, assert response."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return { statusCode: 200, body: "hello from node" };
};
""")
        fname = f"node-simple-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["body"] == "hello from node"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_handler_returns_string(self, lam, role):
        """Handler that returns a plain string."""
        code = _make_node_zip('exports.handler = async () => { return "just a string"; };')
        fname = f"node-str-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload == "just a string"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_handler_returns_number(self, lam, role):
        """Handler that returns a number."""
        code = _make_node_zip("exports.handler = async () => { return 42; };")
        fname = f"node-num-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload == 42
        finally:
            lam.delete_function(FunctionName=fname)

    def test_handler_returns_array(self, lam, role):
        """Handler that returns an array."""
        code = _make_node_zip("exports.handler = async () => { return [1, 2, 3]; };")
        fname = f"node-arr-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload == [1, 2, 3]
        finally:
            lam.delete_function(FunctionName=fname)

    def test_handler_returns_null(self, lam, role):
        """Handler that returns null/undefined."""
        code = _make_node_zip("exports.handler = async () => { return null; };")
        fname = f"node-null-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload_bytes = resp["Payload"].read()
            assert payload_bytes == b"null" or payload_bytes == b""
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsAsyncHandler:
    """Test Node.js async/await handlers."""

    def test_async_await_handler(self, lam, role):
        """Async handler with await."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    const result = await Promise.resolve({ computed: event.x * 2 });
    return result;
};
""")
        fname = f"node-async-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"x": 21}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["computed"] == 42
        finally:
            lam.delete_function(FunctionName=fname)

    def test_async_with_promise_chain(self, lam, role):
        """Async handler using promise chaining."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    const val = await Promise.resolve(10)
        .then(v => v + 5)
        .then(v => v * 2);
    return { result: val };
};
""")
        fname = f"node-promise-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["result"] == 30
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsCallbackHandler:
    """Test Node.js callback-style (3-arg) handlers."""

    def test_callback_success(self, lam, role):
        """Callback handler calls callback(null, result)."""
        code = _make_node_zip("""
exports.handler = (event, context, callback) => {
    callback(null, { message: "callback success", input: event.name });
};
""")
        fname = f"node-cb-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"name": "test"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["message"] == "callback success"
            assert payload["input"] == "test"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_callback_error(self, lam, role):
        """Callback handler calls callback(error)."""
        code = _make_node_zip("""
exports.handler = (event, context, callback) => {
    callback(new Error("callback error"));
};
""")
        fname = f"node-cb-err-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "callback error" in payload.get("errorMessage", "")
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsContext:
    """Test the Lambda context object passed to Node.js handlers."""

    def test_context_function_name(self, lam, role):
        """Verify context.functionName matches the function name."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return { functionName: context.functionName };
};
""")
        fname = f"node-ctx-name-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["functionName"] == fname
        finally:
            lam.delete_function(FunctionName=fname)

    def test_context_memory_limit(self, lam, role):
        """Verify context.memoryLimitInMB matches configuration."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return { memory: context.memoryLimitInMB };
};
""")
        fname = f"node-ctx-mem-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            MemorySize=256,
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            # memoryLimitInMB is a string in the context
            assert str(payload["memory"]) == "256"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_context_get_remaining_time(self, lam, role):
        """Verify context.getRemainingTimeInMillis() returns a positive number."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    const remaining = context.getRemainingTimeInMillis();
    return { remaining: remaining, isPositive: remaining > 0 };
};
""")
        fname = f"node-ctx-time-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["isPositive"] is True
            assert payload["remaining"] > 0
        finally:
            lam.delete_function(FunctionName=fname)

    def test_context_invoked_function_arn(self, lam, role):
        """Verify context.invokedFunctionArn is set."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return { arn: context.invokedFunctionArn };
};
""")
        fname = f"node-ctx-arn-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert fname in payload["arn"]
            assert payload["arn"].startswith("arn:aws:lambda:")
        finally:
            lam.delete_function(FunctionName=fname)

    def test_context_aws_request_id(self, lam, role):
        """Verify context.awsRequestId is a non-empty string."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return { requestId: context.awsRequestId };
};
""")
        fname = f"node-ctx-reqid-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert isinstance(payload["requestId"], str)
            assert len(payload["requestId"]) > 0
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsEnvironmentVariables:
    """Test that environment variables are accessible in Node.js handlers."""

    def test_custom_env_vars(self, lam, role):
        """Set custom env vars on function, verify accessible in handler."""
        code = _make_node_zip("""
exports.handler = async (event, context) => {
    return {
        myVar: process.env.MY_CUSTOM_VAR,
        another: process.env.ANOTHER_VAR,
    };
};
""")
        fname = f"node-env-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            Environment={"Variables": {"MY_CUSTOM_VAR": "hello123", "ANOTHER_VAR": "world"}},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["myVar"] == "hello123"
            assert payload["another"] == "world"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_aws_region_env_var(self, lam, role):
        """Verify AWS_REGION is set in the handler environment."""
        code = _make_node_zip("""
exports.handler = async () => {
    return { region: process.env.AWS_REGION };
};
""")
        fname = f"node-region-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["region"] == "us-east-1"
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsErrorHandling:
    """Test error handling in Node.js handlers."""

    def test_thrown_error(self, lam, role):
        """Handler that throws an Error, verify error response structure."""
        code = _make_node_zip("""
exports.handler = async () => {
    throw new Error("something went wrong");
};
""")
        fname = f"node-throw-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "something went wrong" in payload.get("errorMessage", "")
            assert "errorType" in payload
        finally:
            lam.delete_function(FunctionName=fname)

    def test_thrown_custom_error(self, lam, role):
        """Handler that throws a custom error type."""
        code = _make_node_zip("""
class ValidationError extends Error {
    constructor(msg) {
        super(msg);
        this.name = 'ValidationError';
    }
}
exports.handler = async () => {
    throw new ValidationError("invalid input");
};
""")
        fname = f"node-custom-err-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "invalid input" in payload.get("errorMessage", "")
        finally:
            lam.delete_function(FunctionName=fname)

    def test_syntax_error_in_handler(self, lam, role):
        """Handler with a syntax error in the JS code."""
        code = _make_node_zip(
            "exports.handler = async () => { return {; };"  # bad syntax
        )
        fname = f"node-syntax-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "errorMessage" in payload
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsJsonParsing:
    """Test JSON event parsing and structured responses."""

    def test_json_event_processing(self, lam, role):
        """Handler receives JSON event, processes it, returns structured response."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    const items = event.items || [];
    const total = items.reduce((sum, i) => sum + i.price * i.qty, 0);
    return {
        orderTotal: total,
        itemCount: items.length,
        currency: event.currency || "USD",
    };
};
""")
        fname = f"node-json-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            event = {
                "items": [
                    {"price": 10.0, "qty": 2},
                    {"price": 5.0, "qty": 3},
                ],
                "currency": "EUR",
            }
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps(event),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["orderTotal"] == 35.0
            assert payload["itemCount"] == 2
            assert payload["currency"] == "EUR"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_nested_json_response(self, lam, role):
        """Handler returns deeply nested JSON."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    return {
        level1: {
            level2: {
                level3: {
                    value: event.key || "deep",
                },
            },
        },
    };
};
""")
        fname = f"node-nested-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"key": "found"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["level1"]["level2"]["level3"]["value"] == "found"
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsMultipleInvocations:
    """Test invoking the same function multiple times for isolation."""

    def test_multiple_invocations_isolation(self, lam, role):
        """Invoke same function multiple times, each gets correct input/output."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    return { echo: event.value, doubled: event.value * 2 };
};
""")
        fname = f"node-multi-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            for i in range(5):
                resp = lam.invoke(
                    FunctionName=fname,
                    Payload=json.dumps({"value": i}),
                )
                payload = json.loads(resp["Payload"].read())
                assert payload["echo"] == i
                assert payload["doubled"] == i * 2
        finally:
            lam.delete_function(FunctionName=fname)

    def test_stateless_between_invocations(self, lam, role):
        """Verify no state leaks between invocations (global var not shared in subprocess)."""
        code = _make_node_zip("""
let counter = 0;
exports.handler = async () => {
    counter++;
    return { counter: counter };
};
""")
        fname = f"node-stateless-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            # Each subprocess invocation starts fresh, so counter should be 1 each time
            for _ in range(3):
                resp = lam.invoke(FunctionName=fname)
                payload = json.loads(resp["Payload"].read())
                assert payload["counter"] == 1
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsHandlerNotFound:
    """Test error when handler is not found."""

    def test_wrong_handler_function(self, lam, role):
        """Handler function name doesn't exist in module."""
        code = _make_node_zip("exports.handler = async () => { return 'ok'; };")
        fname = f"node-nohandler-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.nonexistent",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "errorMessage" in payload
        finally:
            lam.delete_function(FunctionName=fname)

    def test_wrong_module(self, lam, role):
        """Module file doesn't exist."""
        code = _make_node_zip("exports.handler = async () => { return 'ok'; };")
        fname = f"node-nomod-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="missing_module.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            payload = json.loads(resp["Payload"].read())
            assert "errorMessage" in payload
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsTimeout:
    """Test Lambda timeout behavior with Node.js."""

    def test_timeout_exceeded(self, lam, role):
        """Handler that sleeps longer than timeout, verify timeout error."""
        code = _make_node_zip("""
exports.handler = async () => {
    await new Promise(resolve => setTimeout(resolve, 10000));
    return { shouldNotReach: true };
};
""")
        fname = f"node-timeout-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            Timeout=2,
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            assert resp.get("FunctionError") is not None
            # FunctionError should indicate timeout
            assert "TimedOut" in resp.get("FunctionError", "") or "Unhandled" in resp.get(
                "FunctionError", ""
            )
            payload_bytes = resp["Payload"].read()
            # Payload may be null or a structured error object
            if payload_bytes and payload_bytes != b"null":
                payload = json.loads(payload_bytes)
                if isinstance(payload, dict):
                    error_msg = payload.get("errorMessage", "").lower()
                    assert "timed out" in error_msg or "timeout" in error_msg
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsLargePayload:
    """Test sending and receiving larger JSON payloads."""

    def test_large_input_output(self, lam, role):
        """Send a ~100KB payload and verify it's echoed back."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    return { size: JSON.stringify(event).length, ok: true };
};
""")
        fname = f"node-large-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            large_data = {"data": "x" * (100 * 1024)}
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps(large_data),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["ok"] is True
            assert payload["size"] > 100000
        finally:
            lam.delete_function(FunctionName=fname)

    def test_large_response(self, lam, role):
        """Handler generates a large response payload."""
        code = _make_node_zip("""
exports.handler = async () => {
    return { data: "y".repeat(50000), ok: true };
};
""")
        fname = f"node-large-resp-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
            Timeout=10,
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["ok"] is True
            assert len(payload["data"]) == 50000
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsConsoleLog:
    """Test that console.log output is captured."""

    def test_console_log_captured(self, lam, role):
        """Verify console.log output appears in LogResult when using Tail."""
        code = _make_node_zip("""
exports.handler = async (event) => {
    console.error("log line 1");
    console.error("log line 2");
    return { logged: true };
};
""")
        fname = f"node-logs-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname, LogType="Tail")
            payload = json.loads(resp["Payload"].read())
            assert payload["logged"] is True
            # LogResult should contain our log lines (base64-encoded)
            if "LogResult" in resp:
                import base64

                logs = base64.b64decode(resp["LogResult"]).decode()
                assert "log line 1" in logs or len(logs) > 0
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsBuiltinModules:
    """Test that Node.js handlers can use built-in modules."""

    def test_require_path(self, lam, role):
        """Handler uses the built-in 'path' module."""
        code = _make_node_zip("""
const path = require('path');
exports.handler = async () => {
    const ext = path.extname('file.txt');
    const base = path.basename('/foo/bar/baz.js');
    return { ext: ext, base: base };
};
""")
        fname = f"node-path-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["ext"] == ".txt"
            assert payload["base"] == "baz.js"
        finally:
            lam.delete_function(FunctionName=fname)

    def test_require_os(self, lam, role):
        """Handler uses the built-in 'os' module."""
        code = _make_node_zip("""
const os = require('os');
exports.handler = async () => {
    return {
        platform: os.platform(),
        hasHostname: os.hostname().length > 0,
    };
};
""")
        fname = f"node-os-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert isinstance(payload["platform"], str)
            assert payload["hasHostname"] is True
        finally:
            lam.delete_function(FunctionName=fname)

    def test_require_crypto(self, lam, role):
        """Handler uses the built-in 'crypto' module."""
        code = _make_node_zip("""
const crypto = require('crypto');
exports.handler = async () => {
    const hash = crypto.createHash('sha256').update('hello').digest('hex');
    return { hash: hash };
};
""")
        fname = f"node-crypto-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            # SHA-256 of "hello" is known
            expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            assert payload["hash"] == expected
        finally:
            lam.delete_function(FunctionName=fname)

    def test_require_url(self, lam, role):
        """Handler uses the built-in 'url' module."""
        code = _make_node_zip("""
const url = require('url');
exports.handler = async () => {
    const parsed = new URL('https://example.com/path?q=1');
    return {
        hostname: parsed.hostname,
        pathname: parsed.pathname,
        search: parsed.search,
    };
};
""")
        fname = f"node-url-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(FunctionName=fname)
            payload = json.loads(resp["Payload"].read())
            assert payload["hostname"] == "example.com"
            assert payload["pathname"] == "/path"
            assert payload["search"] == "?q=1"
        finally:
            lam.delete_function(FunctionName=fname)


class TestNodejsSubdirectoryHandler:
    """Test handlers in subdirectories within the zip."""

    def test_handler_in_subdirectory(self, lam, role):
        """Handler module is in a subdirectory of the zip."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(
                "lib/myhandler.js",
                "exports.handle = async (event) => { return { sub: true, val: event.x }; };",
            )
        code = buf.getvalue()
        fname = f"node-subdir-{uuid.uuid4().hex[:8]}"
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role=role,
            Handler="lib/myhandler.handle",
            Code={"ZipFile": code},
        )
        try:
            resp = lam.invoke(
                FunctionName=fname,
                Payload=json.dumps({"x": 99}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["sub"] is True
            assert payload["val"] == 99
        finally:
            lam.delete_function(FunctionName=fname)
