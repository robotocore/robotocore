"""Tests for the Java runtime executor."""

import io
import os
import shutil
import subprocess
import tempfile
import zipfile

import pytest

from robotocore.services.lambda_.runtimes.java import JavaExecutor
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
