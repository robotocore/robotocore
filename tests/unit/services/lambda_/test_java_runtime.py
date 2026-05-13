"""Tests for the Java runtime executor."""

import io
import os
import shutil
import subprocess
import tempfile
import zipfile
from unittest.mock import patch

import pytest

import robotocore.services.lambda_.runtimes.java as java_mod
from robotocore.services.lambda_.runtimes import clear_executor_cache, get_executor_for_runtime
from robotocore.services.lambda_.runtimes.java import _RUNTIME_BINARY, JavaExecutor
from tests.unit.services.lambda_.helpers import make_zip


def _java_available() -> bool:
    """Check if Java is actually usable (not just a macOS stub)."""
    javac = shutil.which("javac")
    if not javac:
        return False
    try:
        proc = subprocess.run([javac, "-version"], capture_output=True, timeout=5)
        return proc.returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _java_available(), reason="Java (javac + java) not available")


def _compile_java_to_zip(class_source: str, class_name: str) -> bytes:
    """Compile a Java source file and return a zip containing the .class file."""
    tmpdir = tempfile.mkdtemp(prefix="java_compile_")
    try:
        src_file = os.path.join(tmpdir, f"{class_name}.java")
        with open(src_file, "w") as f:
            f.write(class_source)
        proc = subprocess.run(
            ["javac", "-d", tmpdir, src_file],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            pytest.skip(f"Java compilation failed: {proc.stderr}")

        # Find the .class file(s) and package into a zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for root, dirs, files in os.walk(tmpdir):
                for f in files:
                    if f.endswith(".class"):
                        full = os.path.join(root, f)
                        arcname = os.path.relpath(full, tmpdir)
                        zf.write(full, arcname)
        return buf.getvalue()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


SIMPLE_HANDLER = """\
public class Handler {
    public String handleRequest(String event, Object context) {
        return event;
    }
}
"""

DICT_HANDLER = """\
import java.util.Map;
public class DictHandler {
    public String handleRequest(String event, Object context) {
        return "{\\"processed\\": true, \\"input\\": " + event + "}";
    }
}
"""

ERROR_HANDLER = """\
public class ErrorHandler {
    public String handleRequest(String event, Object context) {
        throw new RuntimeException("java boom");
    }
}
"""


class TestJavaExecutor:
    def setup_method(self):
        self.executor = JavaExecutor()

    def test_simple_echo(self):
        code_zip = _compile_java_to_zip(SIMPLE_HANDLER, "Handler")
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="Handler::handleRequest",
            event={"hello": "java"},
            function_name="java-fn",
            timeout=30,
        )
        assert error_type is None
        # Java handler receives event as JSON string and returns it
        assert result == {"hello": "java"}

    def test_handler_throws(self):
        code_zip = _compile_java_to_zip(ERROR_HANDLER, "ErrorHandler")
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="ErrorHandler::handleRequest",
            event={},
            function_name="err-fn",
            timeout=30,
        )
        assert error_type == "Handled"
        assert "java boom" in result["errorMessage"]

    def test_missing_class(self):
        code_zip = make_zip({"dummy.txt": "nothing"})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="NonExistent::handleRequest",
            event={},
            function_name="fn",
            timeout=30,
        )
        assert error_type == "Handled"
        assert "Cannot find class" in result.get("errorMessage", "")

    def test_env_vars(self):
        source = """\
public class EnvHandler {
    public String handleRequest(String event, Object context) {
        String custom = System.getenv("MY_VAR");
        String fn = System.getenv("AWS_LAMBDA_FUNCTION_NAME");
        return "{\\"custom\\": \\"" + custom + "\\", \\"fn\\": \\"" + fn + "\\"}";
    }
}
"""
        code_zip = _compile_java_to_zip(source, "EnvHandler")
        result, error_type, _ = self.executor.execute(
            code_zip=code_zip,
            handler="EnvHandler::handleRequest",
            event={},
            function_name="env-fn",
            timeout=30,
            env_vars={"MY_VAR": "java-custom"},
        )
        assert error_type is None
        assert result["custom"] == "java-custom"
        assert result["fn"] == "env-fn"


class TestJavaVersionRouting:
    """Verify that each runtime identifier resolves to the correct binary."""

    def test_runtime_binary_map_covers_known_versions(self):
        assert "java8" in _RUNTIME_BINARY
        assert "java8.al2" in _RUNTIME_BINARY
        assert "java11" in _RUNTIME_BINARY
        assert "java17" in _RUNTIME_BINARY
        assert "java21" in _RUNTIME_BINARY

    def test_versioned_binary_preferred_when_present(self):
        executor = JavaExecutor(runtime="java17")

        def _which(name):
            return f"/usr/bin/{name}" if name in ("java17", "java") else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/java17"

    def test_falls_back_to_java_when_versioned_binary_missing(self):
        executor = JavaExecutor(runtime="java17")

        def _which(name):
            return "/usr/bin/java" if name == "java" else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/java"

    def test_returns_none_when_no_java_at_all(self):
        executor = JavaExecutor(runtime="java17")
        with patch("shutil.which", return_value=None):
            assert executor._resolve_binary() is None

    def test_executor_with_no_java_returns_invalid_runtime(self):
        executor = JavaExecutor(runtime="java17")
        with patch.object(executor, "_resolve_binary", return_value=None):
            result, error_type, _ = executor.execute(
                code_zip=b"", handler="Handler::handle", event={}, function_name="fn"
            )
        assert error_type == "Runtime.InvalidRuntime"

    def test_get_executor_for_runtime_returns_versioned_instance(self):
        clear_executor_cache()
        ex8 = get_executor_for_runtime("java8")
        ex11 = get_executor_for_runtime("java11")
        ex17 = get_executor_for_runtime("java17")
        ex21 = get_executor_for_runtime("java21")
        assert isinstance(ex8, JavaExecutor)
        assert ex8 is not ex11
        assert ex11 is not ex17
        assert ex17 is not ex21
        assert get_executor_for_runtime("java17") is ex17

    def test_java8_al2_routes_to_java8_binary(self):
        executor = JavaExecutor(runtime="java8.al2")

        def _which(name):
            return f"/usr/bin/{name}" if name == "java8" else None

        with patch("shutil.which", side_effect=_which):
            assert executor._resolve_binary() == "/usr/bin/java8"

    def test_unknown_runtime_logs_warning_and_falls_back(self):
        executor = JavaExecutor(runtime="java42")
        with patch("shutil.which", return_value="/usr/bin/java"):
            with patch.object(java_mod.logger, "warning") as mock_warn:
                result = executor._resolve_binary()
        assert result == "/usr/bin/java"
        mock_warn.assert_called_once()
        assert "java42" in mock_warn.call_args.args[1]

    def test_known_runtime_with_missing_versioned_binary_warns(self):
        # java17 is in _RUNTIME_BINARY, but java17 isn't on PATH; only the
        # default `java` is. We must warn so the JVM divergence is visible.
        executor = JavaExecutor(runtime="java17")

        def _which(name):
            return "/usr/bin/java" if name == "java" else None

        with patch("shutil.which", side_effect=_which):
            with patch.object(java_mod.logger, "warning") as mock_warn:
                result = executor._resolve_binary()
        assert result == "/usr/bin/java"
        mock_warn.assert_called_once()
        warn_args = mock_warn.call_args.args
        assert "java17" in warn_args
