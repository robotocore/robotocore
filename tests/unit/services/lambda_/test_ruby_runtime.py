"""Tests for the Ruby runtime executor."""

import shutil

import pytest

from robotocore.services.lambda_.runtimes.ruby import RubyExecutor
from tests.unit.services.lambda_.helpers import make_zip

pytestmark = pytest.mark.skipif(shutil.which("ruby") is None, reason="Ruby not installed")


class TestRubyExecutor:
    def setup_method(self):
        self.executor = RubyExecutor()

    def test_simple_return(self):
        code_zip = make_zip(
            {"lambda_function.rb": ("def handler(event:, context:)\n  { result: 'ok' }\nend\n")}
        )
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"key": "value"},
            function_name="ruby-fn",
            timeout=10,
        )
        assert error_type is None
        assert result == {"result": "ok"}

    def test_returns_event(self):
        code_zip = make_zip(
            {"lambda_function.rb": ("def handler(event:, context:)\n  event\nend\n")}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={"hello": "ruby"},
            function_name="echo",
            timeout=10,
        )
        assert error_type is None
        assert result == {"hello": "ruby"}

    def test_handler_with_context(self):
        code_zip = make_zip(
            {
                "lambda_function.rb": (
                    "def handler(event:, context:)\n"
                    "  {\n"
                    "    fn_name: context.function_name,\n"
                    "    request_id: context.aws_request_id,\n"
                    "    has_remaining: context.get_remaining_time_in_millis > 0\n"
                    "  }\n"
                    "end\n"
                )
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="ctx-fn",
            timeout=10,
        )
        assert error_type is None
        assert result["fn_name"] == "ctx-fn"
        assert result["has_remaining"] is True

    def test_handler_raises(self):
        code_zip = make_zip(
            {"lambda_function.rb": ("def handler(event:, context:)\n  raise 'boom'\nend\n")}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="err-fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert "boom" in result["errorMessage"]

    def test_missing_module(self):
        code_zip = make_zip({"other.rb": "def handler(event:, context:)\n  'ok'\nend\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert "Cannot find module" in result["errorMessage"] or "ImportModuleError" in str(result)

    def test_missing_method(self):
        code_zip = make_zip({"lambda_function.rb": "def other(event:, context:)\n  'ok'\nend\n"})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert "HandlerNotFound" in str(result)

    def test_returns_string(self):
        code_zip = make_zip(
            {"lambda_function.rb": ("def handler(event:, context:)\n  'hello from ruby'\nend\n")}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result == "hello from ruby"

    def test_returns_array(self):
        code_zip = make_zip(
            {"lambda_function.rb": ("def handler(event:, context:)\n  [1, 2, 3]\nend\n")}
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result == [1, 2, 3]

    def test_env_vars(self):
        code_zip = make_zip(
            {
                "lambda_function.rb": (
                    "def handler(event:, context:)\n"
                    "  {\n"
                    "    custom: ENV['MY_VAR'],\n"
                    "    fn: ENV['AWS_LAMBDA_FUNCTION_NAME']\n"
                    "  }\n"
                    "end\n"
                )
            }
        )
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="env-fn",
            timeout=10,
            env_vars={"MY_VAR": "ruby-custom"},
        )
        assert error_type is None
        assert result["custom"] == "ruby-custom"
        assert result["fn"] == "env-fn"
