"""End-to-end tests for Synthetics: create canary -> start -> get runs -> verify metrics."""

import base64
from unittest.mock import MagicMock, patch

from robotocore.services.synthetics.executor import (
    clear_runs,
    execute_canary,
    get_runs,
    publish_canary_metrics,
    store_run,
)
from robotocore.services.synthetics.scheduler import CanaryScheduler


class TestCanaryLifecycle:
    """Test full canary lifecycle: create, execute, store runs, check results."""

    def test_execute_store_retrieve(self):
        """Execute a Python canary and verify runs are stored and retrievable."""
        account_id = "123456789012"
        region = "us-east-1"
        canary_name = "lifecycle-canary"

        clear_runs(account_id, region, canary_name)

        script = "def handler():\n    return True\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        result = execute_canary(
            canary_name=canary_name,
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=code_b64,
        )

        assert result.status == "PASSED"

        # Store the run
        store_run(account_id, region, canary_name, result)

        # Retrieve and verify
        runs = get_runs(account_id, region, canary_name)
        assert len(runs) == 1
        assert runs[0].status == "PASSED"
        assert runs[0].canary_name == canary_name

        # Verify serialization
        run_dict = runs[0].to_dict()
        assert run_dict["Status"]["State"] == "PASSED"
        assert run_dict["Name"] == canary_name

    def test_multiple_runs_stored_in_order(self):
        """Multiple executions are stored chronologically."""
        account_id = "123456789012"
        region = "us-east-1"
        canary_name = "multi-run-canary"

        clear_runs(account_id, region, canary_name)

        # Success run
        success_script = "def handler():\n    pass\n"
        r1 = execute_canary(
            canary_name=canary_name,
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=base64.b64encode(success_script.encode()).decode(),
            run_id="run-1",
        )
        store_run(account_id, region, canary_name, r1)

        # Failure run
        fail_script = "def handler():\n    raise RuntimeError('boom')\n"
        r2 = execute_canary(
            canary_name=canary_name,
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=base64.b64encode(fail_script.encode()).decode(),
            run_id="run-2",
        )
        store_run(account_id, region, canary_name, r2)

        runs = get_runs(account_id, region, canary_name)
        assert len(runs) == 2
        assert runs[0].run_id == "run-1"
        assert runs[0].status == "PASSED"
        assert runs[1].run_id == "run-2"
        assert runs[1].status == "FAILED"

    def test_nodejs_canary_lifecycle(self):
        """Node.js canary returns mock success without real execution."""
        account_id = "123456789012"
        region = "us-west-2"
        canary_name = "nodejs-canary"

        clear_runs(account_id, region, canary_name)

        result = execute_canary(
            canary_name=canary_name,
            runtime_version="syn-nodejs-puppeteer-9.1",
            handler="index.handler",
            code_content=None,
        )
        store_run(account_id, region, canary_name, result)

        runs = get_runs(account_id, region, canary_name)
        assert len(runs) == 1
        assert runs[0].status == "PASSED"


class TestMetricsPublishing:
    """Test CloudWatch metrics integration."""

    @patch("moto.backends.get_backend")
    def test_publish_success_metrics(self, mock_get_backend):
        """Successful canary should publish SuccessPercent=100."""
        mock_cw = MagicMock()
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_cw}}

        script = "def handler():\n    pass\n"
        result = execute_canary(
            canary_name="metrics-canary",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=base64.b64encode(script.encode()).decode(),
        )

        publish_canary_metrics("metrics-canary", result, "123456789012", "us-east-1")

        mock_cw.put_metric_data.assert_called_once()
        call_args = mock_cw.put_metric_data.call_args
        assert call_args.kwargs["namespace"] == "CloudWatchSynthetics"
        metrics = call_args.kwargs["metric_data"]
        assert len(metrics) == 2

        success_metric = next(m for m in metrics if m["MetricName"] == "SuccessPercent")
        assert success_metric["Value"] == 100.0
        assert success_metric["Dimensions"][0]["Name"] == "CanaryName"
        assert success_metric["Dimensions"][0]["Value"] == "metrics-canary"

        duration_metric = next(m for m in metrics if m["MetricName"] == "Duration")
        assert duration_metric["Value"] >= 0
        assert duration_metric["Unit"] == "Milliseconds"

    @patch("moto.backends.get_backend")
    def test_publish_failure_metrics(self, mock_get_backend):
        """Failed canary should publish SuccessPercent=0."""
        mock_cw = MagicMock()
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_cw}}

        script = "def handler():\n    raise Exception('fail')\n"
        result = execute_canary(
            canary_name="fail-metrics",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=base64.b64encode(script.encode()).decode(),
        )

        publish_canary_metrics("fail-metrics", result, "123456789012", "us-east-1")

        call_args = mock_cw.put_metric_data.call_args
        metrics = call_args.kwargs["metric_data"]
        success_metric = next(m for m in metrics if m["MetricName"] == "SuccessPercent")
        assert success_metric["Value"] == 0.0

    @patch("moto.backends.get_backend")
    def test_metrics_failure_is_silent(self, mock_get_backend):
        """If metrics publishing fails, it should not raise."""
        mock_get_backend.side_effect = Exception("CloudWatch unavailable")

        script = "def handler():\n    pass\n"
        result = execute_canary(
            canary_name="silent-fail",
            runtime_version="syn-python-selenium-3.0",
            handler="canary_module.handler",
            code_content=base64.b64encode(script.encode()).decode(),
        )

        # Should not raise
        publish_canary_metrics("silent-fail", result, "123456789012", "us-east-1")


class TestSchedulerTrigger:
    """Test scheduler trigger_immediate functionality."""

    def test_trigger_immediate_stores_result(self):
        """trigger_immediate should execute and store a run result."""
        account_id = "123456789012"
        region = "us-east-1"
        canary_name = "trigger-canary"

        clear_runs(account_id, region, canary_name)

        # Create a mock canary object
        mock_canary = MagicMock()
        mock_canary.name = canary_name
        mock_canary.runtime_version = "syn-nodejs-puppeteer-9.1"
        mock_canary.code = {"Handler": "index.handler"}
        mock_canary.run_config = {"TimeoutInSeconds": 30}
        mock_canary.runs = []
        mock_canary.last_run = None

        scheduler = CanaryScheduler()

        with patch("robotocore.services.synthetics.scheduler.publish_canary_metrics"):
            with patch("robotocore.services.synthetics.scheduler._update_moto_canary_run"):
                result = scheduler.trigger_immediate(mock_canary, account_id, region)

        assert result.status == "PASSED"
        assert result.canary_name == canary_name

        runs = get_runs(account_id, region, canary_name)
        assert len(runs) == 1

    def test_trigger_immediate_python_canary(self):
        """trigger_immediate with Python runtime should execute code."""
        account_id = "123456789012"
        region = "us-east-1"
        canary_name = "trigger-py"

        clear_runs(account_id, region, canary_name)

        script = "def handler():\n    return 42\n"
        code_b64 = base64.b64encode(script.encode()).decode()

        mock_canary = MagicMock()
        mock_canary.name = canary_name
        mock_canary.runtime_version = "syn-python-selenium-3.0"
        mock_canary.code = {"Handler": "canary_module.handler", "Script": code_b64}
        mock_canary.run_config = {"TimeoutInSeconds": 30}
        mock_canary.runs = []
        mock_canary.last_run = None

        scheduler = CanaryScheduler()

        with patch("robotocore.services.synthetics.scheduler.publish_canary_metrics"):
            with patch("robotocore.services.synthetics.scheduler._update_moto_canary_run"):
                result = scheduler.trigger_immediate(mock_canary, account_id, region)

        assert result.status == "PASSED"
