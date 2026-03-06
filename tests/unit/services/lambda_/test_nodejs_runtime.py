"""Tests for the Node.js runtime executor."""

import shutil

import pytest

from robotocore.services.lambda_.runtimes.node import NodejsExecutor
from tests.unit.services.lambda_.helpers import make_zip

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="Node.js not installed")


class TestNodejsExecutor:
    def setup_method(self):
        self.executor = NodejsExecutor()

    def test_simple_return(self):
        code_zip = make_zip(
            {"index.js": "exports.handler = async (event) => { return { result: 'ok' }; };\n"}
        )
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={"key": "value"},
            function_name="node-fn",
            timeout=10,
        )
        assert error_type is None
        assert result == {"result": "ok"}

    def test_returns_event(self):
        code_zip = make_zip({"index.js": "exports.handler = async (event) => event;\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={"hello": "node"},
            function_name="echo",
            timeout=10,
        )
        assert error_type is None
        assert result == {"hello": "node"}

    def test_handler_with_context(self):
        code_zip = make_zip(
            {
                "index.js": (
                    "exports.handler = async (event, context) => {\n"
                    "  return {\n"
                    "    fnName: context.functionName,\n"
                    "    requestId: context.awsRequestId,\n"
                    "    hasRemaining: context.getRemainingTimeInMillis() > 0,\n"
                    "  };\n"
                    "};\n"
                )
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="ctx-fn",
            timeout=10,
        )
        assert error_type is None
        assert result["fnName"] == "ctx-fn"
        assert result["hasRemaining"] is True
        assert result["requestId"]  # non-empty

    def test_callback_style_handler(self):
        code_zip = make_zip(
            {
                "index.js": (
                    "exports.handler = (event, context, callback) => {\n"
                    "  callback(null, { style: 'callback' });\n"
                    "};\n"
                )
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="cb-fn",
            timeout=10,
        )
        assert error_type is None
        assert result == {"style": "callback"}

    def test_handler_throws(self):
        code_zip = make_zip(
            {"index.js": ("exports.handler = async () => {\n  throw new Error('boom');\n};\n")}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="err-fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert result["errorMessage"] == "boom"
        assert result["errorType"] == "Error"

    def test_missing_module(self):
        code_zip = make_zip({"other.js": "exports.handler = async () => 'ok';\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert "Cannot find module" in result["errorMessage"]

    def test_missing_function(self):
        code_zip = make_zip({"index.js": "exports.other = async () => 'ok';\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert "not a function" in result["errorMessage"]

    def test_console_log_in_stderr(self):
        code_zip = make_zip(
            {
                "index.js": (
                    "exports.handler = async (event) => {\n"
                    "  console.error('log line');\n"
                    "  return 'ok';\n"
                    "};\n"
                )
            }
        )
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="log-fn",
            timeout=10,
        )
        assert error_type is None
        assert "log line" in logs

    def test_returns_null(self):
        code_zip = make_zip({"index.js": "exports.handler = async () => {};\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        # undefined becomes null in JSON

    def test_returns_string(self):
        code_zip = make_zip({"index.js": "exports.handler = async () => 'hello';\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result == "hello"

    def test_returns_array(self):
        code_zip = make_zip({"index.js": "exports.handler = async () => [1, 2, 3];\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result == [1, 2, 3]

    def test_env_vars(self):
        code_zip = make_zip(
            {
                "index.js": (
                    "exports.handler = async () => ({\n"
                    "  custom: process.env.MY_VAR,\n"
                    "  fn: process.env.AWS_LAMBDA_FUNCTION_NAME,\n"
                    "});\n"
                )
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={},
            function_name="env-fn",
            timeout=10,
            env_vars={"MY_VAR": "custom-value"},
        )
        assert error_type is None
        assert result["custom"] == "custom-value"
        assert result["fn"] == "env-fn"

    def test_nested_handler_path(self):
        code_zip = make_zip(
            {"src/handler.js": "exports.process = async (event) => ({ nested: true, ...event });\n"}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="src/handler.process",
            event={"x": 1},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result["nested"] is True
        assert result["x"] == 1

    def test_require_local_module(self):
        code_zip = make_zip(
            {
                "utils.js": "module.exports.greet = (name) => `Hello ${name}`;\n",
                "index.js": (
                    "const utils = require('./utils');\n"
                    "exports.handler = async (event) => ({ msg: utils.greet(event.name) });\n"
                ),
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="index.handler",
            event={"name": "World"},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result["msg"] == "Hello World"
