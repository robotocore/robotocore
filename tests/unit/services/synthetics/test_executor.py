"""Tests for canary executor: script execution, success/failure capture, timeout."""

import base64
import time

from robotocore.services.synthetics.executor import (
    CanaryRunResult,
    clear_runs,
    execute_canary,
    get_runs,
    store_run,
)


class TestExecuteCanaryNodeJS:
    """Node.js runtimes should return mock success without executing code."""

    def test_nodejs_puppeteer_returns_passed(self):
        result = execute_canary(
            canary_name="test-canary",
            runtime_version="syn-nodejs-puppeteer-9.1",
            handler="index.handler",
            code_content=None,
        )
        assert result.status == "PASSED"
        assert result.canary_name == "test-canary"
        assert result.state_reason_code == "CANARY_SUCCESS"

    def test_nodejs_mock_has_valid_timing(self):
        result = execute_canary(
            canary_name="timing-canary",
            runtime_version="syn-nodejs-puppeteer-7.0",
            handler="index.handler",
            code_content=None,
        )
        assert result.duration_ms >= 0
        assert result.start_time <= result.end_time

    def test_nodejs_with_custom_run_id(self):
        result = execute_canary(
            canary_name="id-canary",
            runtime_version="syn-nodejs-puppeteer-6.2",
            handler="index.handler",
            code_content=None,
            run_id="custom-run-123",
        )
        assert result.run_id == "custom-run-123"


class TestExecuteCanaryPython:
    """Python runtimes should actually execute the handler code."""

    def test_successful_python_handler(self):
        script = "def handler():\n    return {'status': 'ok'}\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        result = execute_canary(
            canary_name="py-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=code_b64,
        )
        assert result.status == "PASSED"
        assert result.state_reason_code == "CANARY_SUCCESS"

    def test_failing_python_handler(self):
        script = "def handler():\n    raise ValueError('test error')\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        result = execute_canary(
            canary_name="fail-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=code_b64,
        )
        assert result.status == "FAILED"
        assert "ValueError" in result.error_message
        assert "test error" in result.error_message
        assert result.state_reason_code == "CANARY_FAILURE"

    def test_missing_handler_function(self):
        script = "def other_function():\n    pass\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        result = execute_canary(
            canary_name="missing-handler",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=code_b64,
        )
        assert result.status == "FAILED"
        assert "not found" in result.error_message

    def test_invalid_handler_format(self):
        result = execute_canary(
            canary_name="bad-handler",
            runtime_version="syn-python-selenium-3.0",
            handler="no_dot_here",
            code_content=base64.b64encode(b"x=1").decode(),
        )
        assert result.status == "FAILED"
        assert "Invalid handler format" in result.error_message

    def test_no_code_content(self):
        result = execute_canary(
            canary_name="no-code",
            runtime_version="syn-python-selenium-3.0",
            handler="module.handler",
            code_content=None,
        )
        assert result.status == "FAILED"
        assert "No code content" in result.error_message

    def test_raw_script_text(self):
        """Non-base64 content should be treated as raw script text."""
        script = "def handler():\n    pass\n"
        result = execute_canary(
            canary_name="raw-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=script,
        )
        assert result.status == "PASSED"

    def test_timeout(self):
        script = "import time\ndef handler():\n    time.sleep(10)\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        start = time.monotonic()
        result = execute_canary(
            canary_name="timeout-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=code_b64,
            timeout_seconds=1,
        )
        elapsed = time.monotonic() - start

        assert result.status == "FAILED"
        assert "timed out" in result.error_message
        assert elapsed < 5  # Should not wait full 10s


class TestCanaryRunResult:
    """Test CanaryRunResult serialization."""

    def test_to_dict(self):
        from datetime import UTC, datetime

        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        result = CanaryRunResult(
            run_id="run-123",
            canary_name="my-canary",
            status="PASSED",
            start_time=start,
            end_time=end,
            duration_ms=1000.0,
            state_reason_code="CANARY_SUCCESS",
        )
        d = result.to_dict()
        assert d["Id"] == "run-123"
        assert d["Name"] == "my-canary"
        assert d["Status"]["State"] == "PASSED"
        assert d["Status"]["StateReasonCode"] == "CANARY_SUCCESS"
        assert "2026-01-01" in d["Timeline"]["Started"]
        assert d["ArtifactS3Location"] == "s3://cw-syn-results/canary"


class TestRunStore:
    """Test run storage functions."""

    def test_store_and_get_runs(self):
        from datetime import UTC, datetime

        clear_runs("111", "us-east-1", "store-canary")
        result = CanaryRunResult(
            run_id="r1",
            canary_name="store-canary",
            status="PASSED",
            start_time=datetime.now(tz=UTC),
            end_time=datetime.now(tz=UTC),
            duration_ms=100.0,
        )
        store_run("111", "us-east-1", "store-canary", result)
        runs = get_runs("111", "us-east-1", "store-canary")
        assert len(runs) == 1
        assert runs[0].run_id == "r1"

    def test_max_runs_cap(self):
        from datetime import UTC, datetime

        clear_runs("111", "us-east-1", "cap-canary")
        from robotocore.services.synthetics.executor import MAX_RUNS_PER_CANARY

        for i in range(MAX_RUNS_PER_CANARY + 10):
            store_run(
                "111",
                "us-east-1",
                "cap-canary",
                CanaryRunResult(
                    run_id=f"r{i}",
                    canary_name="cap-canary",
                    status="PASSED",
                    start_time=datetime.now(tz=UTC),
                    end_time=datetime.now(tz=UTC),
                    duration_ms=10.0,
                ),
            )
        runs = get_runs("111", "us-east-1", "cap-canary")
        assert len(runs) == MAX_RUNS_PER_CANARY

    def test_clear_runs(self):
        from datetime import UTC, datetime

        store_run(
            "111",
            "us-east-1",
            "clear-canary",
            CanaryRunResult(
                run_id="r1",
                canary_name="clear-canary",
                status="PASSED",
                start_time=datetime.now(tz=UTC),
                end_time=datetime.now(tz=UTC),
                duration_ms=10.0,
            ),
        )
        clear_runs("111", "us-east-1", "clear-canary")
        assert len(get_runs("111", "us-east-1", "clear-canary")) == 0

    def test_get_runs_nonexistent(self):
        assert len(get_runs("999", "eu-west-1", "no-canary")) == 0
