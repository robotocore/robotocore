"""Tests for the Ruby runtime executor."""

import shutil
from unittest.mock import patch

import pytest

from robotocore.services.lambda_.runtimes import clear_executor_cache, get_executor_for_runtime
from robotocore.services.lambda_.runtimes.ruby import _RUNTIME_BINARY, RubyExecutor
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


class TestRubyVersionRouting:
    """Verify that each runtime identifier resolves to the correct binary."""

    def test_runtime_binary_map_covers_known_versions(self):
        assert "ruby3.2" in _RUNTIME_BINARY
        assert "ruby3.3" in _RUNTIME_BINARY
        assert "ruby3.4" in _RUNTIME_BINARY

    def test_versioned_binary_preferred_when_present(self):
        executor = RubyExecutor(runtime="ruby3.3")

        def _which(name):
            return f"/usr/bin/{name}" if name in ("ruby3.3", "ruby") else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/ruby3.3"

    def test_falls_back_to_ruby_when_versioned_binary_missing(self):
        executor = RubyExecutor(runtime="ruby3.3")

        def _which(name):
            return "/usr/bin/ruby" if name == "ruby" else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/ruby"

    def test_returns_none_when_no_ruby_at_all(self):
        executor = RubyExecutor(runtime="ruby3.3")
        with patch("shutil.which", return_value=None):
            assert executor._resolve_binary() is None

    def test_executor_with_no_ruby_returns_invalid_runtime(self):
        executor = RubyExecutor(runtime="ruby3.3")
        with patch.object(executor, "_resolve_binary", return_value=None):
            result, error_type, _ = executor.execute(
                code_zip=b"", handler="lambda_function.handler", event={}, function_name="fn"
            )
        assert error_type == "Runtime.InvalidRuntime"

    def test_get_executor_for_runtime_returns_versioned_instance(self):
        clear_executor_cache()
        ex32 = get_executor_for_runtime("ruby3.2")
        ex33 = get_executor_for_runtime("ruby3.3")
        ex34 = get_executor_for_runtime("ruby3.4")
        assert isinstance(ex32, RubyExecutor)
        assert ex32 is not ex33
        assert ex33 is not ex34
        assert get_executor_for_runtime("ruby3.3") is ex33

    def test_each_version_routes_to_distinct_binary_name(self):
        for runtime, expected_bin in _RUNTIME_BINARY.items():
            executor = RubyExecutor(runtime=runtime)

            def _which(name, b=expected_bin):
                return f"/usr/bin/{name}" if name == b else None

            with patch("shutil.which", side_effect=_which):
                assert executor._resolve_binary() == f"/usr/bin/{expected_bin}", (
                    f"Failed for {runtime}"
                )

    def test_unknown_runtime_logs_warning_and_falls_back(self):
        import robotocore.services.lambda_.runtimes.ruby as ruby_mod

        executor = RubyExecutor(runtime="ruby2.7")
        with patch("shutil.which", return_value="/usr/bin/ruby"):
            with patch.object(ruby_mod.logger, "warning") as mock_warn:
                result = executor._resolve_binary()
        assert result == "/usr/bin/ruby"
        mock_warn.assert_called_once()
        assert "ruby2.7" in mock_warn.call_args.args[1]
