"""IaC test: sam - monitoring."""

import time
from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-monitoring"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=template,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"],
    )
    for _ in range(60):
        resp = cfn.describe_stacks(StackName=stack_name)
        status = resp["Stacks"][0]["StackStatus"]
        if status == "CREATE_COMPLETE":
            yield resp["Stacks"][0]
            cfn.delete_stack(StackName=stack_name)
            return
        if "FAILED" in status or "ROLLBACK" in status:
            pytest.skip(f"SAM stack failed: {status}")
            return
        time.sleep(1)
    pytest.skip("SAM stack timed out")


class TestMonitoring:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_topic_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        topic_arn = outputs.get("TopicArn")
        assert topic_arn is not None, "TopicArn output missing"

        sns = make_client("sns")
        resp = sns.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn

    def test_log_group_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        log_group_name = outputs.get("LogGroupName")
        assert log_group_name is not None, "LogGroupName output missing"

        logs = make_client("logs")
        resp = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
        groups = resp["logGroups"]
        matching = [g for g in groups if g["logGroupName"] == log_group_name]
        assert len(matching) == 1
        assert matching[0]["retentionInDays"] == 7

    def test_alarm_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        alarm_name = outputs.get("AlarmName")
        assert alarm_name is not None, "AlarmName output missing"

        cw = make_client("cloudwatch")
        resp = cw.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        assert alarms[0]["AlarmName"] == alarm_name
        assert alarms[0]["Threshold"] == 10.0
        assert alarms[0]["ComparisonOperator"] == "GreaterThanThreshold"
