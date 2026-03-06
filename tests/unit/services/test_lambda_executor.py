"""Unit tests for Lambda in-process Python executor."""

import io
import os
import time
import zipfile

from robotocore.services.lambda_.executor import (
    LambdaContext,
    execute_python_handler,
    get_layer_zips,
)

# ---------------------------------------------------------------------------
# Helpers — create zip bytes containing Python modules
# ---------------------------------------------------------------------------


def _make_zip(files: dict[str, str]) -> bytes:
    """Create a zip archive in memory. files maps filename -> content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _simple_handler_zip(body: str = "return event") -> bytes:
    code = f"def handler(event, context):\n    {body}\n"
    return _make_zip({"lambda_function.py": code})


# ---------------------------------------------------------------------------
# LambdaContext
# ---------------------------------------------------------------------------


class TestLambdaContext:
    def test_defaults(self):
        ctx = LambdaContext(function_name="my-fn")
        assert ctx.function_name == "my-fn"
        assert ctx.function_version == "$LATEST"
        assert ctx.memory_limit_in_mb == 128
        assert ctx.aws_request_id  # non-empty UUID

    def test_get_remaining_time_positive(self):
        ctx = LambdaContext(function_name="fn", _timeout=10, _start_time=time.time())
        remaining = ctx.get_remaining_time_in_millis()
        assert 9000 <= remaining <= 10000

    def test_get_remaining_time_expired(self):
        ctx = LambdaContext(function_name="fn", _timeout=0, _start_time=time.time() - 5)
        assert ctx.get_remaining_time_in_millis() == 0

    def test_custom_fields(self):
        ctx = LambdaContext(
            function_name="fn",
            function_version="42",
            memory_limit_in_mb=512,
            invoked_function_arn="arn:test",
            log_group_name="/aws/lambda/fn",
        )
        assert ctx.function_version == "42"
        assert ctx.memory_limit_in_mb == 512
        assert ctx.invoked_function_arn == "arn:test"
        assert ctx.log_group_name == "/aws/lambda/fn"


# ---------------------------------------------------------------------------
# execute_python_handler — success cases
# ---------------------------------------------------------------------------


class TestExecutePythonHandlerSuccess:
    def test_simple_return(self):
        code_zip = _simple_handler_zip("return {'result': 'ok'}")
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"key": "value"},
            function_name="test-fn",
        )
        assert result == {"result": "ok"}
        assert error_type is None

    def test_returns_event(self):
        code_zip = _simple_handler_zip("return event")
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"hello": "world"},
            function_name="echo-fn",
        )
        assert result == {"hello": "world"}
        assert error_type is None

    def test_returns_string(self):
        code_zip = _simple_handler_zip("return 'hello'")
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="str-fn",
        )
        assert result == "hello"
        assert error_type is None

    def test_returns_none(self):
        code_zip = _simple_handler_zip("pass")
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="none-fn",
        )
        assert result is None
        assert error_type is None

    def test_context_available(self):
        code = (
            "def handler(event, context):\n"
            "    return {\n"
            "        'fn_name': context.function_name,\n"
            "        'version': context.function_version,\n"
            "        'remaining': context.get_remaining_time_in_millis() > 0,\n"
            "    }\n"
        )
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="ctx-fn",
        )
        assert result["fn_name"] == "ctx-fn"
        assert result["version"] == "$LATEST"
        assert result["remaining"] is True
        assert error_type is None

    def test_print_captured_in_logs(self):
        code = "def handler(event, context):\n    print('log line')\n    return 'ok'\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="log-fn",
        )
        assert result == "ok"
        assert "log line" in logs

    def test_env_vars_set(self):
        code = (
            "import os\n"
            "def handler(event, context):\n"
            "    return {"
            "        'custom': os.environ.get('MY_VAR'),"
            "        'fn': os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),\n"
            "    }\n"
        )
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="env-fn",
            env_vars={"MY_VAR": "custom-value"},
        )
        assert result["custom"] == "custom-value"
        assert result["fn"] == "env-fn"
        assert error_type is None

    def test_env_vars_cleaned_up(self):
        """Environment variables from one invocation must not leak."""
        sentinel = "LAMBDA_TEST_SENTINEL_XYZ"
        code_zip = _simple_handler_zip("return 'ok'")
        execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            env_vars={sentinel: "1"},
        )
        assert sentinel not in os.environ


# ---------------------------------------------------------------------------
# execute_python_handler — error cases
# ---------------------------------------------------------------------------


class TestExecutePythonHandlerErrors:
    def test_bad_handler_format(self):
        code_zip = _simple_handler_zip()
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="nomodule",
            event={},
            function_name="bad-handler",
        )
        assert error_type == "Runtime.HandlerNotFound"
        assert "Bad handler format" in logs

    def test_missing_module(self):
        code_zip = _simple_handler_zip()
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="nonexistent_module.handler",
            event={},
            function_name="missing-mod",
        )
        assert error_type == "Runtime.ImportModuleError"

    def test_missing_function(self):
        code = "def some_other_func(event, context):\n    return 'nope'\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="missing-func",
        )
        assert error_type == "Runtime.HandlerNotFound"
        assert "handler" in logs

    def test_handler_raises_exception(self):
        code = "def handler(event, context):\n    raise ValueError('boom')\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="error-fn",
        )
        assert error_type == "Handled"
        assert result["errorType"] == "ValueError"
        assert result["errorMessage"] == "boom"
        assert "Traceback" in logs

    def test_import_error_in_handler(self):
        code = "import nonexistent_package_xyz\ndef handler(event, context):\n    return 'ok'\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="import-err",
        )
        assert error_type == "Handled"
        assert "nonexistent_package_xyz" in result["errorMessage"]


# ---------------------------------------------------------------------------
# execute_python_handler — layers
# ---------------------------------------------------------------------------


class TestExecutePythonHandlerLayers:
    def test_layer_code_available(self):
        """Code from a layer zip should be importable."""
        layer_code = "LAYER_VALUE = 42\n"
        layer_zip = _make_zip({"python/my_layer.py": layer_code})

        handler_code = (
            "import my_layer\n"
            "def handler(event, context):\n"
            "    return {'val': my_layer.LAYER_VALUE}\n"
        )
        code_zip = _make_zip({"lambda_function.py": handler_code})

        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="layer-fn",
            layer_zips=[layer_zip],
        )
        assert error_type is None
        assert result == {"val": 42}

    def test_function_code_overrides_layer(self):
        """Function code files should override same-named layer files."""
        layer_zip = _make_zip({"shared.py": "VALUE = 'from_layer'\n"})
        code_zip = _make_zip(
            {
                "shared.py": "VALUE = 'from_function'\n",
                "lambda_function.py": (
                    "import shared\ndef handler(event, context):\n    return shared.VALUE\n"
                ),
            }
        )

        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="override-fn",
            layer_zips=[layer_zip],
        )
        assert error_type is None
        assert result == "from_function"

    def test_invalid_layer_zip_skipped(self):
        """Invalid layer zip bytes should not cause failure."""
        code_zip = _simple_handler_zip("return 'ok'")
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            layer_zips=[b"not-a-zip"],
        )
        assert error_type is None
        assert result == "ok"


# ---------------------------------------------------------------------------
# get_layer_zips (with mocked Moto backend)
# ---------------------------------------------------------------------------


class TestGetLayerZips:
    def test_no_layers_returns_empty(self):
        """A function with no layers returns an empty list."""

        class FakeFn:
            layers = []

        assert get_layer_zips(FakeFn(), "123456789012", "us-east-1") == []

    def test_none_layers_returns_empty(self):
        class FakeFn:
            layers = None

        assert get_layer_zips(FakeFn(), "123456789012", "us-east-1") == []

    def test_no_layers_attr_returns_empty(self):
        class FakeFn:
            pass

        assert get_layer_zips(FakeFn(), "123456789012", "us-east-1") == []
