"""Tests for the base subprocess executor utilities."""

import os

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    cleanup,
    extract_code,
    run_subprocess,
)
from tests.unit.services.lambda_.helpers import make_zip


class TestExtractCode:
    def test_extracts_files(self):
        code_zip = make_zip({"handler.py": "print('hello')"})
        tmpdir = extract_code(code_zip)
        try:
            assert os.path.exists(os.path.join(tmpdir, "handler.py"))
            with open(os.path.join(tmpdir, "handler.py")) as f:
                assert f.read() == "print('hello')"
        finally:
            cleanup(tmpdir)

    def test_layers_extracted_first(self):
        layer_zip = make_zip({"shared.txt": "from_layer"})
        code_zip = make_zip({"shared.txt": "from_function", "main.py": ""})
        tmpdir = extract_code(code_zip, [layer_zip])
        try:
            # Function code should override layer
            with open(os.path.join(tmpdir, "shared.txt")) as f:
                assert f.read() == "from_function"
        finally:
            cleanup(tmpdir)

    def test_invalid_layer_zip_skipped(self):
        code_zip = make_zip({"main.py": "ok"})
        tmpdir = extract_code(code_zip, [b"not-a-zip"])
        try:
            assert os.path.exists(os.path.join(tmpdir, "main.py"))
        finally:
            cleanup(tmpdir)


class TestBuildEnv:
    def test_sets_aws_vars(self):
        env = build_env("my-fn", "us-west-2", "111222333444", 30, 256, "index.handler")
        assert env["AWS_LAMBDA_FUNCTION_NAME"] == "my-fn"
        assert env["AWS_REGION"] == "us-west-2"
        assert env["AWS_DEFAULT_REGION"] == "us-west-2"
        assert env["AWS_ACCOUNT_ID"] == "111222333444"
        assert env["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] == "256"
        assert env["AWS_LAMBDA_FUNCTION_TIMEOUT"] == "30"
        assert env["_HANDLER"] == "index.handler"

    def test_merges_custom_env_vars(self):
        env = build_env("fn", "us-east-1", "123", 3, 128, "h", {"MY_VAR": "custom"})
        assert env["MY_VAR"] == "custom"

    def test_sets_dummy_credentials(self):
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        assert env["AWS_ACCESS_KEY_ID"] == "testing"
        assert env["AWS_SECRET_ACCESS_KEY"] == "testing"


class TestRunSubprocess:
    def test_simple_echo(self):
        """Run a shell command that echoes event JSON back."""
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            # Use python to echo stdin to stdout
            cmd = ["python3", "-c", "import sys, json; print(json.dumps(json.load(sys.stdin)))"]
            result, error_type, logs = run_subprocess(cmd, {"key": "val"}, tmpdir, env, 5)
            assert result == {"key": "val"}
            assert error_type is None
        finally:
            cleanup(tmpdir)

    def test_timeout(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 1, 128, "h")
        try:
            cmd = ["python3", "-c", "import time; time.sleep(10)"]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 1)
            assert error_type == "Task.TimedOut"
        finally:
            cleanup(tmpdir)

    def test_nonzero_exit_code(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            cmd = ["python3", "-c", "import sys; sys.exit(1)"]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 5)
            assert error_type == "Unhandled"
        finally:
            cleanup(tmpdir)

    def test_structured_error_on_exit(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            script = (
                "import sys, json;"
                ' print(json.dumps({"errorMessage": "boom", "errorType": "Err"}));'
                " sys.exit(1)"
            )
            cmd = ["python3", "-c", script]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 5)
            assert error_type == "Handled"
            assert result["errorMessage"] == "boom"
        finally:
            cleanup(tmpdir)

    def test_missing_binary(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            result, error_type, logs = run_subprocess(["/nonexistent/binary"], {}, tmpdir, env, 5)
            assert error_type == "Runtime.InvalidRuntime"
        finally:
            cleanup(tmpdir)

    def test_null_result(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            cmd = ["python3", "-c", "import sys; sys.stdin.read()"]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 5)
            assert result is None
            assert error_type is None
        finally:
            cleanup(tmpdir)

    def test_non_json_stdout(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            cmd = ["python3", "-c", "import sys; sys.stdin.read(); print('plain text')"]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 5)
            assert result == "plain text"
            assert error_type is None
        finally:
            cleanup(tmpdir)

    def test_stderr_captured_as_logs(self):
        tmpdir = extract_code(make_zip({}))
        env = build_env("fn", "us-east-1", "123", 3, 128, "h")
        try:
            script = (
                "import sys, json;"
                ' sys.stderr.write("log line\\n");'
                " sys.stdin.read();"
                ' print(json.dumps("ok"))'
            )
            cmd = ["python3", "-c", script]
            result, error_type, logs = run_subprocess(cmd, {}, tmpdir, env, 5)
            assert "log line" in logs
            assert result == "ok"
        finally:
            cleanup(tmpdir)
