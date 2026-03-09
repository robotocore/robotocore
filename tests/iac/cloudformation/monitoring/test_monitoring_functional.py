"""Functional test: deploy monitoring stack and exercise SNS, CloudWatch, and Logs."""

from pathlib import Path

import pytest

from tests.iac.conftest import ACCOUNT_ID, REGION, make_client
from tests.iac.helpers.functional_validator import (
    publish_metric_and_check_alarm,
    put_log_event_and_query,
    subscribe_sns_to_sqs_and_publish,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestMonitoringFunctional:
    """Deploy monitoring stack and exercise alarms, logs, and notifications."""

    def test_sns_alarm_notification(self, deploy_stack, test_run_id):
        """Subscribe SQS to alarm topic and publish a notification."""
        stack = deploy_stack("mon-func-sns", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        topic_arn = outputs["TopicArn"]

        sqs = make_client("sqs")
        sns = make_client("sns")
        queue_name = f"{test_run_id}-mon-notify"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}"

        msg = subscribe_sns_to_sqs_and_publish(
            sns, sqs, topic_arn, queue_arn, queue_url, "CPU alert triggered"
        )
        assert "CPU alert triggered" in msg["Body"]

    def test_cloudwatch_alarm_metric(self, deploy_stack):
        """Publish a metric and verify the CloudWatch alarm is describable."""
        stack = deploy_stack("mon-func-cw", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        alarm_name = outputs["AlarmName"]

        cw = make_client("cloudwatch")
        alarm = publish_metric_and_check_alarm(
            cw,
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            alarm_name=alarm_name,
            value=95.0,
        )
        assert alarm["AlarmName"] == alarm_name
        assert alarm["Threshold"] == 80.0
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"

    def test_log_group_write_and_query(self, deploy_stack, test_run_id):
        """Write a log event and query it back from the log group."""
        stack = deploy_stack("mon-func-logs", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        log_group = outputs["LogGroupName"]

        logs = make_client("logs")
        stream_name = f"{test_run_id}-test-stream"
        message = f"Application started successfully run={test_run_id}"

        events = put_log_event_and_query(logs, log_group, stream_name, message)
        assert len(events) >= 1
        assert message in events[0]["message"]
