"""Advanced tests for CloudWatch Synthetics executor and scheduler."""

import base64
from unittest.mock import MagicMock, patch

from robotocore.services.synthetics.executor import (
    MAX_RUNS_PER_CANARY,
    CanaryRunResult,
    clear_runs,
    execute_canary,
    get_runs,
    publish_canary_metrics,
    store_run,
)
from robotocore.services.synthetics.scheduler import (
    parse_cron_minutes,
    parse_rate_seconds,
)


def _make_script(code: str) -> str:
    """Encode a Python script as base64 for canary execution."""
    return base64.b64encode(code.encode("utf-8")).decode("utf-8")


class TestCanaryHandlerRaisesException:
    """Canary handler that raises exception produces FAILED run."""

    def test_exception_in_handler_produces_failed(self):
        script = _make_script('def handler():\n    raise ValueError("boom")\n')
        result = execute_canary(
            canary_name="failing-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content=script,
            timeout_seconds=10,
            run_id="run-fail-1",
        )
        assert result.status == "FAILED"
        assert "ValueError" in result.error_message
        assert "boom" in result.error_message
        assert result.state_reason_code == "CANARY_FAILURE"

    def test_successful_handler_produces_passed(self):
        script = _make_script("def handler():\n    pass\n")
        result = execute_canary(
            canary_name="ok-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content=script,
            timeout_seconds=10,
            run_id="run-ok-1",
        )
        assert result.status == "PASSED"
        assert result.state_reason_code == "CANARY_SUCCESS"

    def test_import_error_handler_produces_failed(self):
        script = _make_script("import nonexistent_module_xyz\ndef handler():\n    pass\n")
        result = execute_canary(
            canary_name="import-err",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content=script,
            timeout_seconds=10,
        )
        assert result.status == "FAILED"
        assert "ModuleNotFoundError" in result.error_message or "No module" in result.error_message


class TestCanaryHandlerTimeout:
    """Canary handler timeout produces FAILED run with timeout message."""

    def test_timeout_produces_failed_with_message(self):
        script = _make_script("import time\ndef handler():\n    time.sleep(100)\n")
        result = execute_canary(
            canary_name="timeout-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content=script,
            timeout_seconds=1,
            run_id="run-timeout-1",
        )
        assert result.status == "FAILED"
        assert "timed out" in result.error_message
        assert "1s" in result.error_message
        assert result.state_reason_code == "CANARY_FAILURE"


class TestCanaryRunHistoryCapped:
    """Canary run history capped at MAX_RUNS_PER_CANARY (100)."""

    def setup_method(self):
        clear_runs("acct", "us-east-1", "capped-canary")

    def test_runs_capped_at_max(self):
        from datetime import UTC, datetime

        for i in range(MAX_RUNS_PER_CANARY + 20):
            result = CanaryRunResult(
                run_id=f"run-{i}",
                canary_name="capped-canary",
                status="PASSED",
                start_time=datetime.now(tz=UTC),
                end_time=datetime.now(tz=UTC),
                duration_ms=10.0,
            )
            store_run("acct", "us-east-1", "capped-canary", result)

        runs = get_runs("acct", "us-east-1", "capped-canary")
        assert len(runs) == MAX_RUNS_PER_CANARY
        # Most recent run should be the last one stored
        assert runs[-1].run_id == f"run-{MAX_RUNS_PER_CANARY + 19}"
        # Oldest should be the 20th (first 20 evicted)
        assert runs[0].run_id == "run-20"


class TestCloudWatchMetricsPublished:
    """CloudWatch metrics published after each run."""

    def test_success_metrics_published(self):
        from datetime import UTC, datetime

        result = CanaryRunResult(
            run_id="run-1",
            canary_name="metric-canary",
            status="PASSED",
            start_time=datetime.now(tz=UTC),
            end_time=datetime.now(tz=UTC),
            duration_ms=150.0,
        )
        mock_backend = MagicMock()
        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            publish_canary_metrics("metric-canary", result, "123456789012", "us-east-1")

        mock_backend.put_metric_data.assert_called_once()
        call_kwargs = mock_backend.put_metric_data.call_args[1]
        assert call_kwargs["namespace"] == "CloudWatchSynthetics"
        metric_data = call_kwargs["metric_data"]
        assert len(metric_data) == 2

        success_metric = next(m for m in metric_data if m["MetricName"] == "SuccessPercent")
        assert success_metric["Value"] == 100.0
        assert success_metric["Dimensions"][0]["Value"] == "metric-canary"

        duration_metric = next(m for m in metric_data if m["MetricName"] == "Duration")
        assert duration_metric["Value"] == 150.0

    def test_failure_metrics_published(self):
        from datetime import UTC, datetime

        result = CanaryRunResult(
            run_id="run-2",
            canary_name="fail-canary",
            status="FAILED",
            start_time=datetime.now(tz=UTC),
            end_time=datetime.now(tz=UTC),
            duration_ms=50.0,
            error_message="something broke",
        )
        mock_backend = MagicMock()
        with patch("moto.backends.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            publish_canary_metrics("fail-canary", result, "123456789012", "us-east-1")

        call_kwargs = mock_backend.put_metric_data.call_args[1]
        metric_data = call_kwargs["metric_data"]
        success_metric = next(m for m in metric_data if m["MetricName"] == "SuccessPercent")
        assert success_metric["Value"] == 0.0

    def test_publish_failure_does_not_crash(self):
        from datetime import UTC, datetime

        result = CanaryRunResult(
            run_id="run-3",
            canary_name="crash-canary",
            status="PASSED",
            start_time=datetime.now(tz=UTC),
            end_time=datetime.now(tz=UTC),
            duration_ms=10.0,
        )
        with patch(
            "moto.backends.get_backend",
            side_effect=RuntimeError("backend gone"),
        ):
            # Should not raise
            publish_canary_metrics("crash-canary", result, "123456789012", "us-east-1")


class TestScheduleExpressionParsing:
    """Schedule expression edge cases."""

    def test_rate_1_minute(self):
        assert parse_rate_seconds("rate(1 minute)") == 60

    def test_rate_5_minutes(self):
        assert parse_rate_seconds("rate(5 minutes)") == 300

    def test_rate_1_hour(self):
        assert parse_rate_seconds("rate(1 hour)") == 3600

    def test_rate_1_day(self):
        assert parse_rate_seconds("rate(1 day)") == 86400

    def test_rate_0_minutes_is_disabled(self):
        assert parse_rate_seconds("rate(0 minutes)") is None

    def test_rate_invalid_format(self):
        assert parse_rate_seconds("not-a-rate") is None

    def test_cron_every_5_minutes(self):
        assert parse_cron_minutes("cron(0/5 * * * ? *)") == 300

    def test_cron_every_hour(self):
        assert parse_cron_minutes("cron(0 * * * ? *)") == 3600

    def test_cron_star_step(self):
        assert parse_cron_minutes("cron(*/10 * * * ? *)") == 600

    def test_cron_complex_defaults_to_300(self):
        # Complex cron with specific hours
        result = parse_cron_minutes("cron(0 8 * * ? *)")
        # Minute=0, Hour=8 (not *), so doesn't match the simple "hourly" pattern
        # defaults to 300
        assert result == 300

    def test_cron_invalid(self):
        assert parse_cron_minutes("not-a-cron") is None


class TestNodejsCanaryMockSuccess:
    """Node.js runtime returns mock success."""

    def test_nodejs_runtime_passes(self):
        result = execute_canary(
            canary_name="node-canary",
            runtime_version="syn-nodejs-puppeteer-9.1",
            handler="index.handler",
            code_content=None,
            timeout_seconds=30,
        )
        assert result.status == "PASSED"
        assert result.state_reason_code == "CANARY_SUCCESS"

    def test_nodejs_no_code_still_passes(self):
        result = execute_canary(
            canary_name="node-empty",
            runtime_version="syn-nodejs-puppeteer-9.1",
            handler="index.handler",
            code_content=None,
        )
        assert result.status == "PASSED"


class TestCanaryEdgeCases:
    """Edge cases for canary execution."""

    def test_no_code_content_fails(self):
        result = execute_canary(
            canary_name="no-code",
            runtime_version="syn-python-selenium-3.0",
            handler="mod.handler",
            code_content=None,
        )
        assert result.status == "FAILED"
        assert "No code content" in result.error_message

    def test_invalid_handler_format_fails(self):
        result = execute_canary(
            canary_name="bad-handler",
            runtime_version="syn-python-selenium-3.0",
            handler="no_dot_here",
            code_content=_make_script("def handler(): pass"),
        )
        assert result.status == "FAILED"
        assert "Invalid handler format" in result.error_message

    def test_missing_handler_function_fails(self):
        script = _make_script("def not_the_handler():\n    pass\n")
        result = execute_canary(
            canary_name="wrong-fn",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content=script,
        )
        assert result.status == "FAILED"
        assert "not found" in result.error_message

    def test_canary_run_result_to_dict(self):
        from datetime import UTC, datetime

        result = CanaryRunResult(
            run_id="r1",
            canary_name="test",
            status="PASSED",
            start_time=datetime(2026, 1, 1, tzinfo=UTC),
            end_time=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            duration_ms=1000.0,
            error_message="",
            state_reason_code="CANARY_SUCCESS",
        )
        d = result.to_dict()
        assert d["Id"] == "r1"
        assert d["Name"] == "test"
        assert d["Status"]["State"] == "PASSED"
        assert "2026-01-01" in d["Timeline"]["Started"]

    def test_raw_script_text_not_base64(self):
        """Code content that is not valid base64 is treated as raw script text."""
        result = execute_canary(
            canary_name="raw-script",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_mod.handler",
            code_content="def handler():\n    pass\n",
        )
        assert result.status == "PASSED"
