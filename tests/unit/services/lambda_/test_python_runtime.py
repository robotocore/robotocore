"""Tests for the Python runtime executor (in-process)."""

import sys
from unittest.mock import patch

from robotocore.services.lambda_.runtimes import clear_executor_cache, get_executor_for_runtime
from robotocore.services.lambda_.runtimes.python import _RUNTIME_BINARY, PythonExecutor
from tests.unit.services.lambda_.helpers import make_zip


class TestPythonExecutor:
    def setup_method(self):
        self.executor = PythonExecutor()

    def test_simple_return(self):
        code_zip = make_zip(
            {"lambda_function.py": "def handler(event, context):\n    return {'result': 'ok'}\n"}
        )
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"key": "value"},
            function_name="test-fn",
        )
        assert result == {"result": "ok"}
        assert error_type is None

    def test_returns_event(self):
        code_zip = make_zip(
            {"lambda_function.py": "def handler(event, context):\n    return event\n"}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"hello": "world"},
            function_name="echo",
        )
        assert result == {"hello": "world"}
        assert error_type is None

    def test_handler_exception(self):
        code_zip = make_zip(
            {"lambda_function.py": "def handler(event, context):\n    raise ValueError('boom')\n"}
        )
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="err-fn",
        )
        assert error_type == "Handled"
        assert result["errorType"] == "ValueError"

    def test_missing_handler(self):
        code_zip = make_zip({"lambda_function.py": "def other(event, context):\n    pass\n"})
        _, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
        )
        assert error_type == "Runtime.HandlerNotFound"


class TestPythonVersionRouting:
    """Verify python runtime identifiers route to per-runtime executor instances."""

    def test_runtime_binary_map_covers_known_versions(self):
        for v in ("python3.10", "python3.11", "python3.12", "python3.13"):
            assert v in _RUNTIME_BINARY

    def test_get_executor_for_runtime_returns_versioned_instance(self):
        clear_executor_cache()
        e11 = get_executor_for_runtime("python3.11")
        e12 = get_executor_for_runtime("python3.12")
        e13 = get_executor_for_runtime("python3.13")
        assert isinstance(e11, PythonExecutor)
        assert e11 is not e12
        assert e12 is not e13
        assert get_executor_for_runtime("python3.12") is e12

    def test_executor_records_runtime(self):
        executor = PythonExecutor(runtime="python3.12")
        assert executor._runtime == "python3.12"

    def test_version_mismatch_warns_once(self):
        import robotocore.services.lambda_.runtimes.python as py_mod

        host = (sys.version_info.major, sys.version_info.minor)
        # Pick a runtime whose version does NOT match the host.
        other = next(rt for rt, ver in _RUNTIME_BINARY.items() if ver != host)
        executor = PythonExecutor(runtime=other)
        with patch.object(py_mod.logger, "warning") as mock_warn:
            executor._check_version_match()
            executor._check_version_match()  # should not log twice
        mock_warn.assert_called_once()

    def test_matching_runtime_does_not_warn(self):
        import robotocore.services.lambda_.runtimes.python as py_mod

        host = (sys.version_info.major, sys.version_info.minor)
        matching = next(
            (rt for rt, ver in _RUNTIME_BINARY.items() if ver == host),
            None,
        )
        if matching is None:
            return  # host python isn't in our map; nothing to assert
        executor = PythonExecutor(runtime=matching)
        with patch.object(py_mod.logger, "warning") as mock_warn:
            executor._check_version_match()
        mock_warn.assert_not_called()

    def test_unknown_runtime_warns(self):
        import robotocore.services.lambda_.runtimes.python as py_mod

        executor = PythonExecutor(runtime="python2.7")
        with patch.object(py_mod.logger, "warning") as mock_warn:
            executor._check_version_match()
        mock_warn.assert_called_once()
        assert "python2.7" in mock_warn.call_args.args[1]
