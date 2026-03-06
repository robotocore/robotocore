"""Tests for the custom runtime executor (provided/provided.al2/go/rust)."""

import io
import zipfile

from robotocore.services.lambda_.runtimes.custom import CustomRuntimeExecutor
from tests.unit.services.lambda_.helpers import make_zip


def _make_executable_zip(scripts: dict[str, str]) -> bytes:
    """Create a zip with executable scripts.

    Scripts are shell scripts that follow the Lambda bootstrap protocol:
    read event JSON from stdin, write result JSON to stdout.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in scripts.items():
            info = zipfile.ZipInfo(name)
            # Set executable permission
            info.external_attr = 0o755 << 16
            zf.writestr(info, content)
    return buf.getvalue()


ECHO_BOOTSTRAP = """\
#!/bin/sh
# Read stdin and echo it back to stdout
cat
"""

ERROR_BOOTSTRAP = """\
#!/bin/sh
echo '{"errorMessage": "custom boom", "errorType": "CustomError"}' >&1
exit 1
"""

LOG_BOOTSTRAP = """\
#!/bin/sh
echo "log line from bootstrap" >&2
input=$(cat)
echo "$input"
"""


class TestCustomRuntimeExecutor:
    def setup_method(self):
        self.executor = CustomRuntimeExecutor()

    def test_echo_bootstrap(self):
        code_zip = _make_executable_zip({"bootstrap": ECHO_BOOTSTRAP})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={"hello": "custom"},
            function_name="custom-fn",
            timeout=10,
        )
        assert error_type is None
        assert result == {"hello": "custom"}

    def test_error_bootstrap(self):
        code_zip = _make_executable_zip({"bootstrap": ERROR_BOOTSTRAP})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={},
            function_name="err-fn",
            timeout=10,
        )
        assert error_type == "Handled"
        assert result["errorMessage"] == "custom boom"

    def test_logs_captured(self):
        code_zip = _make_executable_zip({"bootstrap": LOG_BOOTSTRAP})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={"x": 1},
            function_name="log-fn",
            timeout=10,
        )
        assert error_type is None
        assert "log line from bootstrap" in logs

    def test_no_bootstrap_found(self):
        code_zip = make_zip({"handler.py": "# not a bootstrap"})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Runtime.InvalidEntrypoint"

    def test_handler_name_as_bootstrap_fallback(self):
        """If no `bootstrap` file, look for file named after handler."""
        code_zip = _make_executable_zip({"myhandler": ECHO_BOOTSTRAP})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="myhandler.process",
            event={"key": "val"},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result == {"key": "val"}

    def test_env_vars_passed(self):
        bootstrap = """\
#!/bin/sh
cat > /dev/null
echo "{\\"custom\\": \\"$MY_VAR\\", \\"fn\\": \\"$AWS_LAMBDA_FUNCTION_NAME\\"}"
"""
        code_zip = _make_executable_zip({"bootstrap": bootstrap})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={},
            function_name="env-fn",
            timeout=10,
            env_vars={"MY_VAR": "custom-val"},
        )
        assert error_type is None
        assert result["custom"] == "custom-val"
        assert result["fn"] == "env-fn"

    def test_python_bootstrap(self):
        """Custom runtime with a Python bootstrap script."""
        bootstrap = """\
#!/usr/bin/env python3
import sys, json
event = json.load(sys.stdin)
event['processed_by'] = 'custom_python'
print(json.dumps(event))
"""
        code_zip = _make_executable_zip({"bootstrap": bootstrap})
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="handler",
            event={"data": 42},
            function_name="fn",
            timeout=10,
        )
        assert error_type is None
        assert result["data"] == 42
        assert result["processed_by"] == "custom_python"
