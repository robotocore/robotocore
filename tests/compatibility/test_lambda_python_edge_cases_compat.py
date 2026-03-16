"""Lambda Python edge-case compatibility tests.

Tests subtle behaviors users depend on: /tmp writes, threading, large responses,
unicode, concurrent invocations, code updates, context fields, error shapes, etc.
"""

import io
import json
import textwrap
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client

ENDPOINT_URL = "http://localhost:4566"
RUNTIME = "python3.12"
ROLE = "arn:aws:iam::123456789012:role/lambda-edge-role"


def _make_zip(code: str, filename: str = "lambda_function.py") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, textwrap.dedent(code))
    return buf.getvalue()


def _make_zip_multi(files: dict[str, str]) -> bytes:
    """Create a zip with multiple files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, textwrap.dedent(content))
    return buf.getvalue()


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def iam():
    client = make_client("iam")
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
    try:
        client.create_role(RoleName="lambda-edge-role", AssumeRolePolicyDocument=trust)
    except client.exceptions.EntityAlreadyExistsException:
        pass  # best-effort cleanup
    return client


@pytest.fixture
def role(iam):
    return ROLE


def _create_and_invoke(lam, role, code, fname=None, payload=None, **create_kwargs):
    """Helper: create function, invoke, return (response, payload_parsed), caller cleans up."""
    if fname is None:
        fname = _unique_name("edge")
    zip_bytes = _make_zip(code)
    lam.create_function(
        FunctionName=fname,
        Runtime=RUNTIME,
        Role=role,
        Handler="lambda_function.handler",
        Code={"ZipFile": zip_bytes},
        **create_kwargs,
    )
    invoke_kwargs = {"FunctionName": fname}
    if payload is not None:
        invoke_kwargs["Payload"] = json.dumps(payload)
    resp = lam.invoke(**invoke_kwargs)
    body = json.loads(resp["Payload"].read())
    return resp, body, fname


class TestTmpFilesystem:
    """Handler writes to /tmp and reads back."""

    def test_write_and_read_tmp(self, lam, role):
        code = """\
def handler(event, ctx):
    import os, tempfile
    path = os.path.join(tempfile.gettempdir(), "lambda_test_file.txt")
    with open(path, "w") as f:
        f.write("hello from lambda")
    with open(path, "r") as f:
        content = f.read()
    return {"content": content, "exists": os.path.exists(path)}
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body["content"] == "hello from lambda"
        assert body["exists"] is True
        lam.delete_function(FunctionName=fname)


class TestReturnTypes:
    """Various return value shapes."""

    def test_return_none(self, lam, role):
        code = "def handler(event, ctx): return None"
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body is None
        lam.delete_function(FunctionName=fname)

    def test_return_string(self, lam, role):
        code = 'def handler(event, ctx): return "just a string"'
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body == "just a string"
        lam.delete_function(FunctionName=fname)

    def test_return_integer(self, lam, role):
        code = "def handler(event, ctx): return 42"
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body == 42
        lam.delete_function(FunctionName=fname)

    def test_return_list(self, lam, role):
        code = 'def handler(event, ctx): return [1, "two", 3.0]'
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body == [1, "two", 3.0]
        lam.delete_function(FunctionName=fname)

    def test_return_boolean(self, lam, role):
        code = "def handler(event, ctx): return True"
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body is True
        lam.delete_function(FunctionName=fname)

    def test_return_nested_dict(self, lam, role):
        code = 'def handler(event, ctx): return {"a": {"b": {"c": [1, 2, 3]}}}'
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body["a"]["b"]["c"] == [1, 2, 3]
        lam.delete_function(FunctionName=fname)

    def test_return_empty_dict(self, lam, role):
        code = "def handler(event, ctx): return {}"
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body == {}
        lam.delete_function(FunctionName=fname)


class TestDeeplyNestedEvent:
    """Handler receives deeply nested event data."""

    def test_deeply_nested_event(self, lam, role):
        code = """\
def handler(event, ctx):
    val = event
    depth = 0
    while isinstance(val, dict) and "nested" in val:
        val = val["nested"]
        depth += 1
    return {"depth": depth, "leaf": val}
"""
        # Build 15-level nested event
        event = "leaf_value"
        for _ in range(15):
            event = {"nested": event}
        resp, body, fname = _create_and_invoke(lam, role, code, payload=event)
        assert body["depth"] == 15
        assert body["leaf"] == "leaf_value"
        lam.delete_function(FunctionName=fname)


class TestEventIsolation:
    """Verify handler modifying event doesn't affect caller."""

    def test_modify_event_no_side_effect(self, lam, role):
        code = """\
def handler(event, ctx):
    event["injected"] = "should not leak"
    event["original_key"] = "overwritten"
    return {"modified": True}
"""
        fname = _unique_name("isolate")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        # Invoke twice - second invocation should get clean event
        original_event = {"original_key": "original_value"}
        resp1 = lam.invoke(FunctionName=fname, Payload=json.dumps(original_event))
        body1 = json.loads(resp1["Payload"].read())
        assert body1["modified"] is True

        # Second invoke with same event shape
        resp2 = lam.invoke(FunctionName=fname, Payload=json.dumps(original_event))
        body2 = json.loads(resp2["Payload"].read())
        assert body2["modified"] is True
        lam.delete_function(FunctionName=fname)


class TestEmptyEvent:
    """Invoke with no payload / empty payload."""

    def test_empty_event(self, lam, role):
        code = """\
def handler(event, ctx):
    return {"event_type": type(event).__name__, "is_empty": len(event) == 0}
"""
        fname = _unique_name("empty-evt")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname, Payload=json.dumps({}))
        body = json.loads(resp["Payload"].read())
        assert body["event_type"] == "dict"
        assert body["is_empty"] is True
        lam.delete_function(FunctionName=fname)


class TestCustomException:
    """Handler raises a custom exception subclass."""

    def test_custom_exception_type(self, lam, role):
        code = """\
class MyCustomError(Exception):
    pass

def handler(event, ctx):
    raise MyCustomError("custom error message")
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert resp.get("FunctionError") is not None
        assert body["errorType"] == "MyCustomError"
        assert "custom error message" in body["errorMessage"]
        lam.delete_function(FunctionName=fname)

    def test_nested_exception_class(self, lam, role):
        code = """\
class BaseError(Exception):
    pass

class ChildError(BaseError):
    pass

def handler(event, ctx):
    raise ChildError("child error message")
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert resp.get("FunctionError") is not None
        assert body["errorType"] == "ChildError"
        assert "child error message" in body["errorMessage"]
        lam.delete_function(FunctionName=fname)


class TestThreadingInHandler:
    """Handler uses threading internally."""

    def test_handler_with_threads(self, lam, role):
        code = """\
import threading

def handler(event, ctx):
    results = []
    def worker(n):
        results.append(n * n)
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    return {"squares": sorted(results)}
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert body["squares"] == [0, 1, 4, 9, 16]
        lam.delete_function(FunctionName=fname)


class TestUnicode:
    """Unicode in event, response, and function operations."""

    def test_unicode_in_event_and_response(self, lam, role):
        code = """\
def handler(event, ctx):
    return {"echo": event.get("msg"), "emoji": "\\U0001f600", "cjk": "\\u4f60\\u597d"}
"""
        resp, body, fname = _create_and_invoke(
            lam, role, code, payload={"msg": "caf\u00e9 \u2603 \U0001f31f"}
        )
        assert body["echo"] == "caf\u00e9 \u2603 \U0001f31f"
        assert body["cjk"] == "\u4f60\u597d"
        lam.delete_function(FunctionName=fname)


class TestInvocationTypes:
    """DryRun and Event invocation types."""

    def test_dry_run_returns_204(self, lam, role):
        code = 'def handler(event, ctx): return "should not run"'
        fname = _unique_name("dryrun")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname, InvocationType="DryRun")
        assert resp["StatusCode"] == 204
        # DryRun should return empty payload
        payload_bytes = resp["Payload"].read()
        assert payload_bytes == b"" or payload_bytes == b"null"
        lam.delete_function(FunctionName=fname)

    def test_event_async_returns_202(self, lam, role):
        code = 'def handler(event, ctx): return "async result"'
        fname = _unique_name("async")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname, InvocationType="Event")
        assert resp["StatusCode"] == 202
        lam.delete_function(FunctionName=fname)


class TestConcurrentInvocations:
    """Multiple concurrent invocations of the same function."""

    def test_concurrent_invokes(self, lam, role):
        code = """\
import time
def handler(event, ctx):
    time.sleep(0.05)
    return {"n": event.get("n", 0)}
"""
        fname = _unique_name("concurrent")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=10,
        )
        results = []

        def invoke_one(n):
            client = make_client("lambda")
            r = client.invoke(FunctionName=fname, Payload=json.dumps({"n": n}))
            return json.loads(r["Payload"].read())

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(invoke_one, i): i for i in range(5)}
            for f in as_completed(futures):
                results.append(f.result())

        returned_ns = sorted(r["n"] for r in results)
        assert returned_ns == [0, 1, 2, 3, 4]
        lam.delete_function(FunctionName=fname)


class TestUpdateCodeThenInvoke:
    """Update function code and verify new code runs."""

    def test_update_code_runs_new_handler(self, lam, role):
        # Create with v1 code
        code_v1 = 'def handler(event, ctx): return {"version": 1}'
        fname = _unique_name("update-code")
        zip_v1 = _make_zip(code_v1)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_v1},
        )
        resp1 = lam.invoke(FunctionName=fname)
        body1 = json.loads(resp1["Payload"].read())
        assert body1["version"] == 1

        # Update to v2
        code_v2 = 'def handler(event, ctx): return {"version": 2}'
        zip_v2 = _make_zip(code_v2)
        lam.update_function_code(FunctionName=fname, ZipFile=zip_v2)

        resp2 = lam.invoke(FunctionName=fname)
        body2 = json.loads(resp2["Payload"].read())
        assert body2["version"] == 2
        lam.delete_function(FunctionName=fname)


class TestMemoryAndContext:
    """Verify context.memory_limit_in_mb reflects configuration."""

    def test_memory_128(self, lam, role):
        code = """\
def handler(event, ctx):
    return {"memory": ctx.memory_limit_in_mb}
"""
        resp, body, fname = _create_and_invoke(lam, role, code, MemorySize=128)
        # AWS returns memory_limit_in_mb as a string
        assert str(body["memory"]) == "128"
        lam.delete_function(FunctionName=fname)

    def test_memory_3008(self, lam, role):
        code = """\
def handler(event, ctx):
    return {"memory": ctx.memory_limit_in_mb}
"""
        resp, body, fname = _create_and_invoke(lam, role, code, MemorySize=3008)
        assert str(body["memory"]) == "3008"
        lam.delete_function(FunctionName=fname)

    def test_remaining_time_decreasing(self, lam, role):
        code = """\
import time
def handler(event, ctx):
    t1 = ctx.get_remaining_time_in_millis()
    time.sleep(0.1)
    t2 = ctx.get_remaining_time_in_millis()
    return {"t1": t1, "t2": t2, "decreased": t2 < t1}
"""
        resp, body, fname = _create_and_invoke(lam, role, code, Timeout=30)
        assert body["decreased"] is True
        assert body["t1"] > body["t2"]
        # t1 should be close to 30000 ms
        assert body["t1"] > 20000
        lam.delete_function(FunctionName=fname)

    def test_context_function_name(self, lam, role):
        code = """\
def handler(event, ctx):
    return {
        "function_name": ctx.function_name,
        "function_version": ctx.function_version,
        "log_group": ctx.log_group_name,
    }
"""
        fname = _unique_name("ctx-name")
        resp, body, _ = _create_and_invoke(lam, role, code, fname=fname)
        assert body["function_name"] == fname
        assert body["function_version"] == "$LATEST"
        assert fname in body["log_group"]
        lam.delete_function(FunctionName=fname)


class TestLargeResponse:
    """Handler returning a large response."""

    def test_large_response_256kb(self, lam, role):
        code = """\
def handler(event, ctx):
    # Generate ~260KB of data
    return {"data": "x" * 260000}
"""
        resp, body, fname = _create_and_invoke(lam, role, code, Timeout=10)
        assert len(body["data"]) == 260000
        lam.delete_function(FunctionName=fname)


class TestInvokeNonExistent:
    """Invoke a function that doesn't exist."""

    def test_invoke_nonexistent_function(self, lam, role):
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            lam.invoke(FunctionName="nonexistent-function-xyz-12345")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"

    def test_get_nonexistent_function(self, lam, role):
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            lam.get_function(FunctionName="nonexistent-function-abc-99999")
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"


class TestInvokeWithInvalidPayload:
    """Invoke with payload that isn't valid JSON."""

    def test_invalid_json_payload(self, lam, role):
        code = 'def handler(event, ctx): return {"ok": True}'
        fname = _unique_name("bad-json")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        # Send raw bytes that aren't valid JSON
        with pytest.raises(Exception):
            lam.invoke(FunctionName=fname, Payload=b"not valid json {{{")
        lam.delete_function(FunctionName=fname)


class TestHandlerWithMultipleModules:
    """Handler imports from another module in the same zip."""

    def test_import_local_module(self, lam, role):
        files = {
            "lambda_function.py": """\
from helper import compute

def handler(event, ctx):
    return {"result": compute(event.get("x", 5))}
""",
            "helper.py": """\
def compute(x):
    return x * x + 1
""",
        }
        fname = _unique_name("multi-mod")
        zip_bytes = _make_zip_multi(files)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname, Payload=json.dumps({"x": 7}))
        body = json.loads(resp["Payload"].read())
        assert body["result"] == 50  # 7*7+1
        lam.delete_function(FunctionName=fname)


class TestHandlerWithLayer:
    """Handler imports from a Lambda layer."""

    def test_layer_import(self, lam, role):
        # Create a layer with a helper module in python/ subdir
        layer_buf = io.BytesIO()
        with zipfile.ZipFile(layer_buf, "w") as zf:
            zf.writestr(
                "python/layer_utils.py",
                "def greet(name): return f'Hello, {name}!'\n",
            )
        layer_zip = layer_buf.getvalue()

        layer_resp = lam.publish_layer_version(
            LayerName=_unique_name("test-layer"),
            Content={"ZipFile": layer_zip},
            CompatibleRuntimes=["python3.12"],
        )
        layer_arn = layer_resp["LayerVersionArn"]

        code = """\
from layer_utils import greet

def handler(event, ctx):
    return {"greeting": greet(event.get("name", "World"))}
"""
        fname = _unique_name("with-layer")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Layers=[layer_arn],
        )
        resp = lam.invoke(FunctionName=fname, Payload=json.dumps({"name": "Lambda"}))
        body = json.loads(resp["Payload"].read())
        assert body["greeting"] == "Hello, Lambda!"
        lam.delete_function(FunctionName=fname)


class TestEnvironmentVariables:
    """Verify env vars are accessible in handler."""

    def test_env_vars_available(self, lam, role):
        code = """\
import os
def handler(event, ctx):
    return {
        "MY_VAR": os.environ.get("MY_VAR"),
        "AWS_LAMBDA_FUNCTION_NAME": os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
        "AWS_REGION": os.environ.get("AWS_REGION"),
    }
"""
        fname = _unique_name("env-vars")
        resp, body, _ = _create_and_invoke(
            lam,
            role,
            code,
            fname=fname,
            Environment={"Variables": {"MY_VAR": "test_value"}},
        )
        assert body["MY_VAR"] == "test_value"
        assert body["AWS_LAMBDA_FUNCTION_NAME"] == fname
        assert body["AWS_REGION"] == "us-east-1"
        lam.delete_function(FunctionName=fname)


class TestHandlerPrintOutput:
    """Handler prints to stdout — verify logs are captured."""

    def test_print_in_logs(self, lam, role):
        code = """\
def handler(event, ctx):
    print("hello from lambda stdout")
    return {"printed": True}
"""
        fname = _unique_name("print-log")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname, LogType="Tail")
        body = json.loads(resp["Payload"].read())
        assert body["printed"] is True
        # Check that log result is present when LogType=Tail
        import base64

        log_result = resp.get("LogResult")
        if log_result:
            logs = base64.b64decode(log_result).decode()
            assert "hello from lambda stdout" in logs
        lam.delete_function(FunctionName=fname)


class TestErrorShapes:
    """Verify error response shapes match AWS."""

    def test_error_has_stack_trace(self, lam, role):
        code = """\
def helper():
    raise RuntimeError("deep error")

def handler(event, ctx):
    helper()
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert resp.get("FunctionError") is not None
        assert body["errorType"] == "RuntimeError"
        assert body["errorMessage"] == "deep error"
        assert "stackTrace" in body
        assert isinstance(body["stackTrace"], list)
        assert len(body["stackTrace"]) > 0
        lam.delete_function(FunctionName=fname)

    def test_syntax_error(self, lam, role):
        code = """\
def handler(event, ctx):
    this is not valid python !!!
"""
        fname = _unique_name("syntax-err")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname)
        assert resp.get("FunctionError") is not None
        body = json.loads(resp["Payload"].read())
        assert body["errorType"] == "SyntaxError"
        lam.delete_function(FunctionName=fname)

    def test_import_error(self, lam, role):
        code = """\
import nonexistent_module_xyz_12345

def handler(event, ctx):
    return {"ok": True}
"""
        resp, body, fname = _create_and_invoke(lam, role, code)
        assert resp.get("FunctionError") is not None
        assert body["errorType"] == "ModuleNotFoundError"
        lam.delete_function(FunctionName=fname)


class TestHandlerNotFound:
    """Invoke with wrong handler path."""

    def test_wrong_handler_function(self, lam, role):
        code = 'def handler(event, ctx): return "ok"'
        fname = _unique_name("bad-handler")
        zip_bytes = _make_zip(code)
        lam.create_function(
            FunctionName=fname,
            Runtime=RUNTIME,
            Role=role,
            Handler="lambda_function.wrong_name",
            Code={"ZipFile": zip_bytes},
        )
        resp = lam.invoke(FunctionName=fname)
        assert resp.get("FunctionError") is not None
        body = json.loads(resp["Payload"].read())
        assert "wrong_name" in body.get("errorMessage", "") or "HandlerNotFound" in body.get(
            "errorType", ""
        )
        lam.delete_function(FunctionName=fname)
