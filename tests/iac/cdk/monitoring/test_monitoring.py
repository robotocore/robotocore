"""IaC test: cdk - monitoring.

Deploys an SNS Topic, CloudWatch Alarm (CPUUtilization > 80), and
CloudWatch LogGroup (7-day retention). Validates all resources.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_cloudwatch_alarm_exists,
    assert_log_group_exists,
)

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestMonitoring:
    """CDK monitoring stack with SNS, CloudWatch Alarm, and LogGroup."""

    @pytest.fixture(autouse=True)
    def deploy(self, cdk_runner):
        """Deploy the CDK app and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, "MonitoringStack")
        assert result.returncode == 0, f"cdk deploy failed: {result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, "MonitoringStack")

    def test_alarm_created(self):
        """Verify CloudWatch alarm exists with correct configuration."""
        cloudwatch = make_client("cloudwatch")
        alarm = assert_cloudwatch_alarm_exists(cloudwatch, "monitoring-cpu-alarm")
        assert alarm["MetricName"] == "CPUUtilization"
        assert alarm["Namespace"] == "AWS/EC2"
        assert alarm["Threshold"] == 80.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"

    def test_log_group_created(self):
        """Verify CloudWatch log group exists with retention."""
        logs = make_client("logs")
        log_group = assert_log_group_exists(logs, "monitoring-app-logs")
        assert log_group["retentionInDays"] == 7

    def test_sns_topic_created(self):
        """Verify SNS topic exists."""
        sns = make_client("sns")
        resp = sns.list_topics()
        topic_arns = [t["TopicArn"] for t in resp["Topics"]]
        matching = [a for a in topic_arns if "monitoring-alarm-topic" in a]
        assert len(matching) >= 1, "SNS topic 'monitoring-alarm-topic' not found"
