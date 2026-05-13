"""Tests for the Node.js runtime executor."""

import shutil
from unittest.mock import patch

import pytest

from robotocore.services.lambda_.runtimes import clear_executor_cache, get_executor_for_runtime
from robotocore.services.lambda_.runtimes.node import _RUNTIME_BINARY, NodejsExecutor
from tests.unit.services.lambda_.helpers import make_zip

_NODE_LOGGER = "robotocore.services.lambda_.runtimes.node.logger"

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


class TestNodejsVersionRouting:
    """Verify that each runtime identifier resolves to the correct binary."""

    def test_runtime_binary_map_covers_known_versions(self):
        assert "nodejs18.x" in _RUNTIME_BINARY
        assert "nodejs20.x" in _RUNTIME_BINARY
        assert "nodejs22.x" in _RUNTIME_BINARY

    def test_versioned_binary_preferred_when_present(self):
        executor = NodejsExecutor(runtime="nodejs20.x")

        def _which(name):
            return f"/usr/bin/{name}" if name in ("node20", "node") else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/node20"

    def test_falls_back_to_node_when_versioned_binary_missing(self):
        executor = NodejsExecutor(runtime="nodejs20.x")

        def _which(name):
            return "/usr/bin/node" if name == "node" else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/node"

    def test_returns_none_when_no_node_at_all(self):
        executor = NodejsExecutor(runtime="nodejs20.x")
        with patch("shutil.which", return_value=None):
            assert executor._resolve_binary() is None

    def test_executor_with_no_node_returns_invalid_runtime(self):
        executor = NodejsExecutor(runtime="nodejs20.x")
        with patch.object(executor, "_resolve_binary", return_value=None):
            result, error_type, _ = executor.execute(
                code_zip=b"", handler="index.handler", event={}, function_name="fn"
            )
        assert error_type == "Runtime.InvalidRuntime"

    def test_get_executor_for_runtime_returns_versioned_instance(self):
        clear_executor_cache()
        ex18 = get_executor_for_runtime("nodejs18.x")
        ex20 = get_executor_for_runtime("nodejs20.x")
        ex22 = get_executor_for_runtime("nodejs22.x")
        assert isinstance(ex18, NodejsExecutor)
        assert ex18 is not ex20
        assert ex20 is not ex22
        # Same runtime reuses the cached singleton
        assert get_executor_for_runtime("nodejs20.x") is ex20

    def test_each_version_routes_to_distinct_binary_name(self):
        for runtime, expected_bin in _RUNTIME_BINARY.items():
            executor = NodejsExecutor(runtime=runtime)

            def _which(name, b=expected_bin):
                return f"/usr/bin/{name}" if name == b else None

            with patch("shutil.which", side_effect=_which):
                assert executor._resolve_binary() == f"/usr/bin/{expected_bin}", (
                    f"Failed for {runtime}"
                )

    def test_unknown_runtime_logs_warning_and_falls_back(self):
        executor = NodejsExecutor(runtime="nodejs16.x")
        with patch("shutil.which", return_value="/usr/bin/node"):
            with patch(_NODE_LOGGER + ".warning") as mock_warn:
                result = executor._resolve_binary()
        assert result == "/usr/bin/node"
        mock_warn.assert_called_once()
        assert "nodejs16.x" in mock_warn.call_args.args[1]

    def test_known_runtime_with_missing_versioned_binary_warns(self):
        # nodejs20.x is in _RUNTIME_BINARY, but node20 isn't on PATH; only the
        # default `node` is. We must warn so the Node version divergence is visible.
        executor = NodejsExecutor(runtime="nodejs20.x")

        def _which(name):
            return "/usr/bin/node" if name == "node" else None

        with patch("shutil.which", side_effect=_which):
            with patch(_NODE_LOGGER + ".warning") as mock_warn:
                result = executor._resolve_binary()
        assert result == "/usr/bin/node"
        mock_warn.assert_called_once()
        warn_args = mock_warn.call_args.args
        assert "node20" in warn_args
        assert "nodejs20.x" in warn_args
