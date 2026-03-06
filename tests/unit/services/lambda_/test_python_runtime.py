"""Tests for the Python runtime executor (in-process)."""

from robotocore.services.lambda_.runtimes.python import PythonExecutor
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
