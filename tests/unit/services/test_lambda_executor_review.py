"""Failing tests for Lambda executor edge cases.

Each test documents correct behavior that is currently missing or broken.
These tests should all FAIL — they are written to drive future fixes.
"""

import io
import os
import sys
import threading
import time
import zipfile

import pytest

from robotocore.services.lambda_.executor import (
    CodeCache,
    LambdaContext,
    _code_cache,
    execute_python_handler,
)


@pytest.fixture(autouse=True)
def _clean_lambda_modules():
    """Clean up Lambda-injected modules and code cache between tests."""
    yield
    stale = [k for k in sys.modules if k.startswith("_lambda_")]
    for k in stale:
        del sys.modules[k]
    with _code_cache._lock:
        _code_cache._cache.clear()


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _simple_handler_zip(body: str = "return event") -> bytes:
    code = f"def handler(event, context):\n    {body}\n"
    return _make_zip({"lambda_function.py": code})


# ---------------------------------------------------------------------------
# 1. Timeout enforcement — Lambda should kill execution after timeout
#
# Real AWS Lambda terminates execution after the configured timeout and
# returns a Task.TimedOut error. The current implementation passes the
# timeout to LambdaContext but never actually enforces it — the handler
# runs indefinitely.
# ---------------------------------------------------------------------------


class TestTimeoutEnforcement:
    def test_handler_killed_after_timeout(self):
        """Handler sleeping longer than timeout should be terminated within
        a reasonable margin (not allowed to run for 30s on a 1s timeout)."""
        code = (
            "import time\n"
            "def handler(event, context):\n"
            "    time.sleep(30)\n"
            "    return 'should not reach here'\n"
        )
        code_zip = _make_zip({"lambda_function.py": code})
        start = time.time()
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="timeout-fn",
            timeout=1,
        )
        elapsed = time.time() - start
        # Should complete within ~1s, not 30s
        assert elapsed < 3, f"Handler ran for {elapsed:.1f}s, should have timed out at 1s"
        assert error_type is not None, "Should return an error for timed-out execution"

    def test_timeout_returns_task_timed_out_error_type(self):
        """AWS returns errorType 'Task.TimedOut' for timed-out invocations,
        not 'Handled' (which is for user exceptions)."""
        code = "import time\ndef handler(event, context):\n    time.sleep(30)\n    return 'nope'\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="timeout-err-fn",
            timeout=1,
        )
        assert error_type == "Task.TimedOut"


# ---------------------------------------------------------------------------
# 2. Environment variable isolation between concurrent invocations
#
# The current implementation mutates the global os.environ dict, so two
# concurrent invocations overwrite each other's variables. Real AWS
# Lambda runs each invocation in an isolated environment.
# ---------------------------------------------------------------------------


class TestEnvVarIsolation:
    def test_concurrent_invocations_have_isolated_env_vars(self):
        """Two concurrent invocations with different env vars should each
        see only their own values throughout the entire execution."""
        code = (
            "import os, time\n"
            "def handler(event, context):\n"
            "    my_val = os.environ.get('ISOLATION_VAR', 'MISSING')\n"
            "    time.sleep(0.2)\n"
            "    my_val_after = os.environ.get('ISOLATION_VAR', 'MISSING')\n"
            "    return {'before': my_val, 'after': my_val_after}\n"
        )
        code_zip = _make_zip({"lambda_function.py": code})

        results = {}

        def invoke(fn_name, var_value):
            result, err, _logs = execute_python_handler(
                code_zip=code_zip,
                handler="lambda_function.handler",
                event={},
                function_name=fn_name,
                env_vars={"ISOLATION_VAR": var_value},
            )
            results[fn_name] = (result, err)

        t1 = threading.Thread(target=invoke, args=("fn-a", "value-a"))
        t2 = threading.Thread(target=invoke, args=("fn-b", "value-b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        result_a, error_a = results["fn-a"]
        result_b, error_b = results["fn-b"]
        assert error_a is None
        assert error_b is None
        assert result_a["before"] == "value-a"
        assert result_a["after"] == "value-a", (
            f"fn-a saw '{result_a['after']}' after sleep "
            "— env var was overwritten by concurrent invocation"
        )
        assert result_b["before"] == "value-b"
        assert result_b["after"] == "value-b", (
            f"fn-b saw '{result_b['after']}' after sleep "
            "— env var was overwritten by concurrent invocation"
        )


# ---------------------------------------------------------------------------
# 3. sys.path thread safety under concurrent invocations
#
# The executor inserts code_dir into the global sys.path and restores it
# in a finally block, but concurrent invocations can interleave their
# insert/restore operations, leaving stale entries in sys.path.
# ---------------------------------------------------------------------------


class TestSysPathConcurrency:
    def test_concurrent_invocations_dont_corrupt_sys_path(self):
        """After 10 concurrent invocations complete, sys.path should be
        identical to what it was before any of them started."""
        code = "import sys\ndef handler(event, context):\n    return {'path_len': len(sys.path)}\n"
        code_zip = _make_zip({"lambda_function.py": code})

        errors = []
        path_before = sys.path[:]

        def invoke(fn_name):
            try:
                execute_python_handler(
                    code_zip=code_zip,
                    handler="lambda_function.handler",
                    event={},
                    function_name=fn_name,
                )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=invoke, args=(f"fn-{i}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Concurrent invocations raised errors: {errors}"
        assert sys.path == path_before, (
            f"sys.path was corrupted: had {len(path_before)} entries "
            f"before, now has {len(sys.path)} entries"
        )


# ---------------------------------------------------------------------------
# 3b. Concurrent stdout capture isolation
#
# When multiple Lambda invocations run concurrently, each invocation's
# print() output must go to its own log buffer, not leak into others.
# ---------------------------------------------------------------------------


class TestConcurrentStdoutCapture:
    def test_concurrent_prints_go_to_correct_logs(self):
        """Each concurrent invocation's print output appears only in its own logs."""
        results = {}
        errors = []

        def invoke(fn_name, marker):
            code = f"def handler(event, context):\n    print('{marker}')\n    return '{marker}'\n"
            code_zip = _make_zip({"lambda_function.py": code})
            try:
                result, error_type, logs = execute_python_handler(
                    code_zip=code_zip,
                    handler="lambda_function.handler",
                    event={},
                    function_name=fn_name,
                )
                results[fn_name] = (result, error_type, logs)
            except Exception as e:
                errors.append((fn_name, e))

        threads = [
            threading.Thread(target=invoke, args=(f"fn-{i}", f"MARKER_{i}")) for i in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Invocations raised errors: {errors}"
        for i in range(8):
            fn = f"fn-{i}"
            marker = f"MARKER_{i}"
            result, error_type, logs = results[fn]
            assert error_type is None, f"{fn} had error: {error_type}"
            assert marker in logs, f"{fn}: expected '{marker}' in logs but got:\n{logs}"


# ---------------------------------------------------------------------------
# 4. Error reporting format matching real AWS Lambda
#
# AWS Lambda's stackTrace field is a list of frame descriptions, not raw
# traceback lines. The "Traceback (most recent call last):" header and
# the final exception line should not appear in stackTrace.
# ---------------------------------------------------------------------------


class TestErrorReportingFormat:
    def test_error_stacktrace_excludes_traceback_header(self):
        """stackTrace should not include the 'Traceback (most recent call
        last):' header — AWS omits it and only includes frame entries."""
        code = (
            "def helper():\n"
            "    raise ValueError('test error')\n"
            "def handler(event, context):\n"
            "    return helper()\n"
        )
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="stacktrace-fn",
        )
        assert error_type == "Handled"
        stack = result["stackTrace"]
        assert isinstance(stack, list)
        # AWS format omits the Traceback header line
        assert not any(entry.startswith("Traceback") for entry in stack), (
            "stackTrace should not contain 'Traceback' header line"
        )

    def test_error_log_includes_request_id(self):
        """AWS Lambda log output includes START/END/REPORT lines with the
        RequestId. The executor should emit these for parity."""
        code = "def handler(event, context):\n    raise RuntimeError('boom')\n"
        code_zip = _make_zip({"lambda_function.py": code})
        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler="lambda_function.handler",
            event={},
            function_name="reqid-fn",
        )
        assert "RequestId" in logs, "Logs should contain RequestId like real AWS Lambda"


# ---------------------------------------------------------------------------
# 5. CodeCache eviction deletes directories that may still be in use
#
# When the cache is full and a new entry triggers LRU eviction, the evicted
# directory is deleted immediately via shutil.rmtree. If another thread is
# still executing code from that directory, it will fail.
# ---------------------------------------------------------------------------


class TestCodeCacheEvictionSafety:
    def test_cache_eviction_during_concurrent_use(self):
        """Evicting a cache entry while another thread uses its directory
        should not cause the directory to disappear under it."""
        cache = CodeCache(max_size=2)

        zip1 = _simple_handler_zip("return 1")
        zip2 = _simple_handler_zip("return 2")
        dir1 = cache.get_or_extract("fn-1", zip1)
        cache.get_or_extract("fn-2", zip2)

        errors = []
        using_dir = threading.Event()
        done = threading.Event()

        def use_dir():
            using_dir.set()
            done.wait(timeout=2)
            try:
                assert os.path.isdir(dir1), f"Directory {dir1} was deleted while in use!"
            except AssertionError as e:
                errors.append(e)

        t = threading.Thread(target=use_dir)
        t.start()
        using_dir.wait()

        # Add a third entry to trigger eviction of fn-1 (LRU)
        zip3 = _simple_handler_zip("return 3")
        cache.get_or_extract("fn-3", zip3)

        done.set()
        t.join()

        assert not errors, f"Cache eviction caused error: {errors[0]}"


# ---------------------------------------------------------------------------
# 6. LambdaContext.memory_limit_in_mb should be a string
#
# In real AWS Lambda (Python runtime), context.memory_limit_in_mb is a
# string like "128", not an int. Many user Lambda functions do
# int(context.memory_limit_in_mb) which works with strings but would
# also work with ints — however, string comparisons and logging differ.
# See: https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
# ---------------------------------------------------------------------------


class TestContextFieldTypes:
    def test_memory_limit_is_string_in_real_aws(self):
        """context.memory_limit_in_mb should be a string per AWS docs."""
        ctx = LambdaContext(function_name="fn", memory_limit_in_mb=256)
        assert isinstance(ctx.memory_limit_in_mb, str), (
            "memory_limit_in_mb should be a string (AWS behavior), "
            f"got {type(ctx.memory_limit_in_mb)}"
        )
        assert ctx.memory_limit_in_mb == "256"
